# 现有Task同步实现问题分析报告

## 一、RD-Agent UI实现验证（参考源码）

### 1.1 关键API实现（results_api_server.py）

**`/tasks/{task_id}/sota_factor_anchor` API** (920-1066行):
- ✅ 严格按v2方案：仅从最后一个被接受因子实验的file_dict取回测权重
- ✅ 返回`resolved_model_weight_key`和`resolved_model_weight_source`
- ✅ 返回`resolved_model_meta_key`（用于获取alpha基线因子）
- ✅ 返回`based_factor_entries`（所有基线因子信息）
- ✅ 支持`auto_backfill`参数，从workspace回填缺失的权重文件

**`/tasks/{task_id}/asset_bytes` API** (1068-1163行):
- ✅ 支持下载因子实验file_dict中的任何文件
- ✅ 支持命名空间key：`based_factor_{i}/<basename>`
- ✅ 支持workspace fallback（model.pkl, params.pkl, model_meta.json）

**核心逻辑**：
```python
# 1. 找到最后一个SOTA因子实验
anchor_info, anchor_err = _resolve_sota_anchor(task_id, session_obj)
factor_exp = anchor_info.get("_last_sota_factor_exp")

# 2. 从因子实验file_dict提取权重
factor_fd, factor_fd_err = _extract_file_dict_from_sub_workspaces(factor_exp)
resolved_key, resolved_err = _pick_weight_key_from_file_dict(factor_fd)

# 3. 提取model_meta.json
resolved_model_meta_key, resolved_model_meta_err = _pick_model_meta_key_from_file_dict(factor_fd)

# 4. 提取based factors
for i, bexp in enumerate(_collect_based_factor_experiments(factor_exp)):
    # 获取每个based factor的file_dict
    # 命名空间：based_factor_{i}/factor.py
```

## 二、AIstock现有实现问题分析

### 2.1 sync_task_from_log函数问题（322-438行）

**问题1：未调用sota_factor_anchor API**
```python
# 当前代码（346-358行）：
manifest_resp = _rdagent_client.get_task_manifest(task_id=tid)
remote_manifest = manifest_resp.get("manifest") or manifest_resp
primary_assets = remote_manifest.get("primary_assets") or {}
```
- ❌ 只调用了get_task_manifest，未调用sota_factor_anchor
- ❌ 依赖manifest返回的primary_assets，但这可能不完整
- ❌ 没有获取resolved_model_weight_key和resolved_model_meta_key

**问题2：未下载model_meta.json**
```python
# 当前代码（360-380行）：
weight_rel = primary_assets.get("model_weight_relpath")
factor_rel = primary_assets.get("factor_entry_relpath")
```
- ❌ 没有下载model_meta.json（包含alpha基线因子信息）
- ❌ 没有生成factor_order.json（文档第14章要求）

**问题3：未下载based factors**
```python
# 当前代码（382-395行）：
all_assets = remote_manifest.get("all_assets") or []
for asset_item in all_assets:
    if rel and rel.startswith("sota_factors/"):
        # 下载SOTA因子
```
- ❌ 只下载了all_assets中的sota_factors/，但based factors不在这里
- ❌ 需要通过sota_factor_anchor API获取based_factor_entries
- ❌ 需要使用命名空间key下载：`based_factor_{i}/factor.py`

**问题4：未填充数据库catalog表**
```python
# 当前代码（409-422行）：
_upsert_task_catalog(tid, {
    "sync_status": "success",
    ...
})
```
- ❌ 没有填充rdagent_factor_catalog表
- ❌ 没有填充rdagent_loop_catalog表
- ❌ 没有填充rdagent_alpha_baseline_factors表
- ❌ 没有填充rdagent_factor_order表

### 2.2 数据库表字段冗余分析

**aistock_task_catalog表**：
- ❌ `sessions_count` - 未使用，可删除
- ❌ `loops_count` - 未使用，应使用total_loops_count
- ❌ `sota_factors_count` - 已有，需要填充
- ❌ `sota_models_count` - 未使用，可删除
- ❌ `candidate_model_workspace_id` - 未使用，可删除
- ❌ `candidate_model_run_id` - 未使用，可删除
- ✅ `total_loops_count` - 需要填充
- ✅ `has_model_weight` - 需要填充
- ✅ `has_factor_order` - 需要填充
- ✅ `dir_exists` - 已填充

## 三、修复方案

### 3.1 核心修复点

**1. 调用sota_factor_anchor API获取完整信息**
```python
# 调用API
anchor_resp = _rdagent_client.get_task_sota_factor_anchor(task_id=tid)

# 获取关键信息
resolved_model_weight_key = anchor_resp.get("resolved_model_weight_key")
resolved_model_meta_key = anchor_resp.get("resolved_model_meta_key")
based_factor_entries = anchor_resp.get("based_factor_entries", [])
```

**2. 下载所有必需文件**
```python
# 下载模型权重
if resolved_model_weight_key:
    weight_bytes = _rdagent_client.download_task_asset_bytes(tid, resolved_model_weight_key)
    
# 下载model_meta.json
if resolved_model_meta_key:
    meta_bytes = _rdagent_client.download_task_asset_bytes(tid, resolved_model_meta_key)
    
# 下载主因子
factor_entry_key = anchor_resp.get("resolved_factor_entry_key")
if factor_entry_key:
    factor_bytes = _rdagent_client.download_task_asset_bytes(tid, factor_entry_key)
    
# 下载所有based factors
for entry in based_factor_entries:
    based_key = entry.get("resolved_factor_entry_key")
    if based_key:
        based_bytes = _rdagent_client.download_task_asset_bytes(tid, based_key)
```

**3. 生成factor_order.json**
```python
# 从model_meta.json提取alpha基线因子顺序
if meta_bytes:
    meta_json = json.loads(meta_bytes)
    filter_col = meta_json.get("FilterCol", {})
    col_list = filter_col.get("col_list", [])
    
    # 生成factor_order.json
    factor_order = {
        "version": "v1",
        "task_id": tid,
        "alpha158_factors": col_list,
        "dynamic_factors": [主因子名称] + [based因子名称列表]
    }
```

**4. 填充catalog表**
```python
# 填充factor catalog
for factor in all_factors:
    _upsert_factor_catalog(factor)
    
# 填充loop catalog
for loop in all_loops:
    _upsert_loop_catalog(loop)
    
# 填充alpha baseline factors
for alpha_factor in col_list:
    _upsert_alpha_baseline_factor(tid, alpha_factor)
    
# 填充factor order
_upsert_factor_order(tid, factor_order)
```

### 3.2 修复优先级

**P0（必须修复）**：
1. 调用sota_factor_anchor API
2. 下载model_meta.json
3. 下载based factors
4. 生成factor_order.json

**P1（重要）**：
5. 填充factor catalog表
6. 填充alpha baseline factors表
7. 填充factor order表

**P2（优化）**：
8. 填充loop catalog表
9. 清理冗余字段

## 四、实施步骤

### 步骤1：修改sync_task_from_log函数
- 添加sota_factor_anchor API调用
- 下载所有必需文件（权重、meta、因子代码）
- 生成factor_order.json

### 步骤2：实现catalog表填充函数
- _upsert_factor_catalog
- _upsert_alpha_baseline_factor
- _upsert_factor_order

### 步骤3：测试验证
- 测试单个Task同步
- 验证所有文件下载完整
- 验证catalog表数据正确
- 测试选股功能

## 五、关键注意事项

1. **严格按v2方案**：只从最后一个SOTA因子实验获取权重
2. **命名空间**：based factors使用`based_factor_{i}/factor.py`格式
3. **fallback机制**：如果file_dict缺失，使用workspace fallback
4. **错误处理**：每个步骤都要有详细的错误信息记录
5. **兼容性**：保持现有文件资产目录结构不变

## 六、预期结果

修复后应该实现：
- ✅ 所有SOTA因子源码正确下载
- ✅ 模型权重从正确位置获取
- ✅ model_meta.json正确下载
- ✅ factor_order.json正确生成
- ✅ 所有catalog表正确填充
- ✅ Task选股功能正常工作
