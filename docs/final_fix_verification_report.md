# RD-Agent选股功能完整修复验证报告

## 执行日期
2026-01-31 22:45

## 修复总结

### ✅ 已完成的修复

#### 1. 推理引擎修复（已验证通过）

**问题：** 推理引擎将所有因子都当作Alpha158基线因子处理

**修复内容：**
- 修改`_infer_expected_features`函数，返回三元组：`(factor_order, alpha158_factors, dynamic_factors)`
- 从`factor_order.json`的v2版本中正确读取`alpha158_factors`和`dynamic_factors`
- 只对`alpha158_factors`调用Alpha158计算库
- 对`dynamic_factors`从`factor.py`获取
- 按`factor_order`正确顺序组合特征

**验证结果：** ✅ 通过
- 推理引擎正确区分了两类因子
- 错误提示清晰准确
- 不再尝试从Alpha158库计算SOTA动态因子

#### 2. 同步服务修复（已验证通过）

**问题：** `factor_order.json`中的`dynamic_factors`包含错误的因子名称

**修复内容：**
- 添加`_parse_factor_names_from_factor_py`函数
- 从`factor.py`解析`calculate_`开头的函数，提取实际因子名称
- 优先使用从`factor.py`解析的因子名称
- 保持向后兼容，回退到从`combined_factors_df.parquet`提取

**验证结果：** ✅ 通过
- 成功从`factor.py`解析到1个SOTA因子：`MF_Intensity_GBDT_Residual`
- `factor_order.json`生成正确，来源标记为`factor.py + model_meta.json`
- 不再包含错误的因子名称

### ❌ 发现的新问题

#### 模型特征数量不匹配

**错误信息：**
```
特征数量不匹配：模型期望 29 个特征，实际提供 21 个特征。
  - Alpha158 基线因子: 20 个
  - SOTA 动态因子: 1 个
  - 总计: 21 个
```

**问题分析：**

1. **训练时的特征数量：** 29个
   - Alpha158基线因子：20个
   - SOTA动态因子：9个

2. **当前提供的特征数量：** 21个
   - Alpha158基线因子：20个
   - SOTA动态因子：1个

3. **差异：** 缺少8个SOTA动态因子

**根本原因：**

RD-Agent在迭代过程中可能发生了以下情况之一：

1. **因子合并：** 多个SOTA因子被合并到一个`factor.py`中，但只保留了最后一个
2. **因子演化：** 训练时使用了9个SOTA因子，但最终只保留了1个有效因子
3. **数据不一致：** `combined_factors_df.parquet`记录的是训练时的因子，但`factor.py`只包含最终的因子

## 详细测试结果

### 测试1：推理引擎修复验证

**测试时间：** 2026-01-31 19:00 - 19:24
**测试Task：** `2025-12-30_10-24-18-730664`
**测试结果：** ✅ 推理引擎修复通过

**关键发现：**
- 推理引擎正确识别了`dynamic_factors`中的因子
- 尝试从`factor.py`获取`VolAdjVolumeWeightedMomentum`
- 发现`factor.py`只返回`MF_Intensity_GBDT_Residual`
- 错误提示清晰：`SOTA 动态因子 VolAdjVolumeWeightedMomentum 计算失败，无法找到该列`

### 测试2：同步服务修复验证

**测试时间：** 2026-01-31 22:43
**测试Task：** `2025-12-30_10-24-18-730664`（重新同步）
**测试结果：** ✅ 同步服务修复通过

**同步结果：**
```json
{
  "version": "v2",
  "source": "factor.py + model_meta.json",
  "alpha158_factors": [...20个因子...],
  "dynamic_factors": ["MF_Intensity_GBDT_Residual"],
  "total_factors": 21,
  "alpha158_count": 20,
  "dynamic_count": 1
}
```

**关键改进：**
- 不再从`combined_factors_df.parquet`提取错误的因子名称
- 直接从`factor.py`解析实际计算的因子
- 数据来源标记为`factor.py + model_meta.json`

### 测试3：选股功能验证

**测试时间：** 2026-01-31 22:43 - 23:07
**测试Task：** `2025-12-30_10-24-18-730664`
**测试结果：** ❌ 特征数量不匹配

**错误详情：**
- 状态码：500
- 耗时：23.8分钟
- 错误：模型期望29个特征，实际提供21个

## 问题根源分析

### RD-Agent因子演化机制

根据测试结果和代码分析，问题的根源在于：

1. **训练阶段：**
   - RD-Agent迭代生成了多个SOTA因子
   - 最终训练模型使用了9个SOTA因子
   - 模型权重文件记录了期望29个特征（20+9）

2. **因子整合阶段：**
   - RD-Agent将多个因子整合到一个`factor.py`
   - 但`factor.py`只包含1个计算函数：`calculate_MF_Intensity_GBDT_Residual`
   - 其他8个因子的计算逻辑丢失或被合并

3. **同步阶段：**
   - 之前从`combined_factors_df.parquet`提取因子名称（包含训练时的9个因子）
   - 修复后从`factor.py`解析（只有1个因子）
   - 导致因子数量从9个变为1个

### 数据一致性问题

| 数据源 | 因子数量 | 因子来源 | 准确性 |
|--------|----------|----------|--------|
| 模型权重 | 29个（20+9） | 训练时记录 | ✓ 准确 |
| combined_factors_df.parquet | 22个（20+2） | 训练时数据 | ❌ 不完整 |
| factor.py | 21个（20+1） | 最终代码 | ❌ 不完整 |
| based_factors/*.py | 4个文件 | 历史因子 | ❓ 未知 |

## 解决方案建议

### 方案1：从based_factors恢复缺失因子（推荐）

**思路：** 检查`based_factors`目录中的因子文件，可能包含缺失的8个因子

**步骤：**
1. 分析`based_factors/based_factor_0.py`到`based_factor_3.py`
2. 提取其中的因子计算函数
3. 将缺失的因子整合到`factor.py`或单独调用
4. 更新`factor_order.json`

**优点：**
- 可能恢复所有缺失的因子
- 保持与训练时的一致性

**缺点：**
- 需要分析和整合多个文件
- 可能存在因子重复或冲突

### 方案2：重新训练模型（彻底解决）

**思路：** 使用当前的1个SOTA因子重新训练模型

**步骤：**
1. 在RD-Agent中重新运行训练流程
2. 只使用`MF_Intensity_GBDT_Residual`这1个SOTA因子
3. 生成新的模型权重（期望21个特征）
4. 重新同步到AIstock

**优点：**
- 数据完全一致
- 避免因子缺失问题

**缺点：**
- 需要重新训练（耗时）
- 模型性能可能下降

### 方案3：使用combined_factors_df.parquet作为兜底（临时）

**思路：** 如果`factor.py`解析失败或因子数量不足，回退到从parquet提取

**步骤：**
1. 修改同步服务逻辑
2. 检查从`factor.py`解析的因子数量
3. 如果与模型期望不匹配，使用parquet中的因子名称
4. 添加警告日志

**优点：**
- 快速解决当前问题
- 保持向后兼容

**缺点：**
- 治标不治本
- 可能导致运行时错误（因子计算失败）

## 修复文件清单

### 已修改文件

1. **`f:\Dev\AIstock\backend\inference_engine.py`**
   - 行497-558：修改`_infer_expected_features`函数
   - 行638-643：更新调用处，区分两类因子
   - 行833-914：修改因子组合逻辑

2. **`f:\Dev\AIstock\backend\services\rdagent_task_sync_service.py`**
   - 行51-73：添加`_parse_factor_names_from_factor_py`函数
   - 行755-796：添加从`factor.py`解析因子的逻辑
   - 行798-913：保持parquet提取作为回退方案

### 创建的文档

1. **`f:\Dev\AIstock\docs\selection_fix_and_optimization.md`**
   - 修复方案和性能优化建议

2. **`f:\Dev\AIstock\docs\selection_test_results.md`**
   - 第一次测试验证结果

3. **`f:\Dev\AIstock\docs\final_fix_verification_report.md`**（本文档）
   - 完整的修复验证和问题分析

### 调试工具

1. **`f:\Dev\AIstock\debug_tools\test_selection_after_fix.py`**
   - 选股功能测试脚本

2. **`f:\Dev\AIstock\debug_tools\parse_factor_py.py`**
   - 因子解析工具

## 性能分析

### SOTA因子计算耗时

**第一次测试：** 21分17秒（1267秒）
**第二次测试：** 23分48秒（1428秒）

**性能稳定性：** 两次测试耗时相近，说明性能瓶颈稳定

**主要瓶颈：**
1. GBDT模型训练（滚动窗口60天）
2. 数据量大（4679只股票 × 90天）
3. 单线程顺序计算
4. 无缓存机制

## 下一步建议

### 立即执行

1. **分析based_factors目录**
   ```bash
   # 检查based_factors中的因子
   ls -la f:\Dev\AIstock\rdagent_assets\rdagent_tasks\2025-12-30_10-24-18-730664\based_factors\
   
   # 解析每个文件中的因子函数
   grep -n "def calculate_" based_factors/*.py
   ```

2. **验证因子数量**
   ```python
   # 统计所有可用的因子
   # main factor: 1个
   # based factors: ?个
   # 总计应该等于9个
   ```

3. **决定修复方案**
   - 如果based_factors包含缺失因子 → 方案1
   - 如果based_factors不完整 → 方案2或方案3

### 后续优化

1. **性能优化**
   - 并行计算SOTA因子
   - 缓存中间结果
   - 减少数据获取量

2. **数据一致性检查**
   - 添加同步时的因子数量验证
   - 对比模型期望特征数与实际因子数
   - 提前发现不匹配问题

3. **测试另一个task**
   - 同步并测试`2025-12-26_06-19-42-126375`
   - 验证修复的通用性

## 总结

### 修复成果

1. ✅ **推理引擎修复成功**
   - 正确区分Alpha158和SOTA因子
   - 从不同来源获取不同类型的因子
   - 错误提示清晰准确

2. ✅ **同步服务修复成功**
   - 从`factor.py`解析实际因子名称
   - 避免使用不准确的parquet数据
   - 数据来源可追溯

3. ❌ **选股功能仍有问题**
   - 因子数量不匹配（21 vs 29）
   - 需要恢复缺失的8个SOTA因子
   - 或重新训练模型

### 核心问题

**RD-Agent因子演化与代码生成的不一致性：**
- 训练时使用了9个SOTA因子
- 最终代码只包含1个SOTA因子
- 导致模型无法正常推理

### 修复信心

**推理引擎和同步服务：** ⭐⭐⭐⭐⭐ (已验证通过)

**选股功能完整修复：** ⭐⭐⭐ (需要恢复缺失因子)

**整体方案可行性：** ⭐⭐⭐⭐ (方向正确，需要进一步处理)
