# 代码修复计划

## 分析结果

### 问题1: aistock_task_catalog表创建时机不符合生产环境要求

**位置**: `backend/services/rdagent_task_sync_service.py`

**问题**:
- `_ensure_task_catalog_table()` 在多个地方被调用（list_local_tasks、sync_task_from_log等）
- 每次操作都会执行 `CREATE TABLE IF NOT EXISTS`
- 不符合生产环境要求：表应该在应用启动时创建一次

**修复方案**:
1. 在应用启动时（`backend/main.py`）调用一次 `_ensure_task_catalog_table()`
2. 从其他函数中移除 `_ensure_task_catalog_table()` 调用

### 问题2: Alpha158因子提取使用兜底方案

**位置**: `backend/services/rdagent_task_sync_service.py` (sync_task_from_log函数)

**问题**:
- 使用多个兜底方案提取Alpha158因子
- 如果失败则使用默认值 `DEFAULT_ALPHA158_BASELINE`
- 违反用户要求：不能推断、猜测、不能使用兜底方案

**修复方案**:
1. **唯一来源**: 从 `model_meta.json` 的 `FilterCol.col_list` 获取Alpha158因子列表
2. **严格验证**: 如果 `FilterCol.col_list` 不存在或为空，必须报错，不能使用兜底方案
3. **去除所有兜底逻辑**: 删除所有 `if not alpha_baseline_factors:` 的兜底代码

### 问题3: factor_order.json只记录文件路径，不记录因子名称

**位置**: `backend/services/rdagent_task_sync_service.py` (sync_task_from_log函数)

**问题**:
- `factor_order.json` 的 `dynamic_factors` 只记录文件路径（如 "based_factor_0.py"）
- 不记录实际的因子名称（如 "MomentumVolAdj_20D"）
- 导致实盘选股时无法知道每个文件产生哪些因子

**修复方案**:
1. 从 `combined_factors_df.parquet` 读取列顺序
2. 生成 `factor_order.json`，记录完整的因子名称列表（不是文件路径）
3. 区分Alpha158因子和动态SOTA因子

### 问题4: 缺少从combined_factors_df.parquet提取因子顺序的逻辑

**位置**: `backend/services/rdagent_task_sync_service.py` (第1873-1875行)

**问题**:
- 代码跳过了 `combined_factors_df.parquet` 的同步（避免误用回测数据）
- 但没有读取 parquet 的列顺序并写入 `factor_order.json`
- 导致无法获取动态因子顺序

**修复方案**:
1. 在跳过 parquet 同步的同时，读取其列顺序
2. 生成 `factor_order.json` 文件
3. 更新 manifest，添加 `factor_order_relpath` 字段

## 修复顺序

### 步骤1: 修复表创建时机
- 在 `backend/main.py` 启动时创建表
- 从其他函数中移除 `_ensure_task_catalog_table()` 调用

### 步骤2: 修复Alpha158因子提取逻辑
- 只从 `model_meta.json` 的 `FilterCol.col_list` 获取
- 去除所有兜底方案
- 如果获取失败必须报错

### 步骤3: 修复factor_order.json生成逻辑
- 从 `combined_factors_df.parquet` 读取列顺序
- 生成包含完整因子名称列表的 `factor_order.json`
- 更新 manifest

### 步骤4: 修复inference_engine.py中的推测逻辑
- 去除所有推测和兜底方案
- 严格按照 `factor_order.json` 的顺序组装特征

### 步骤5: 重新测试
- 清理数据库和文件资产
- 重新同步task
- 验证数据准确性
- 测试选股功能

## 关键原则

1. **不能推断、猜测**：所有数据必须从明确的来源获取
2. **不能使用兜底方案**：如果数据不存在或获取失败，必须报错
3. **不能屏蔽报错**：所有错误必须暴露给用户
4. **严格验证**：特征数量和顺序必须与模型训练时完全一致
