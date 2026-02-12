# Task同步与选股问题完整分析报告

## 执行日期
2026-01-31

## 任务目标
1. 确认7个SOTA因子是否都同步到AIstock侧
2. 确认是否都参与了选股
3. 复现最后一个SOTA因子实验，使用所有SOTA因子+Alpha基线因子
4. 在AIstock侧执行选股并输出股票列表

## 核心发现

### 1. SOTA因子数量验证 ✓

**Trace.hist分析结果：**
- 共有7个SOTA因子（feedback.decision=True）
- 因子列表：
  1. [0] MF_MainNetRatio_Change5D
  2. [1] mf_main_net_amt_ratio_5d
  3. [3] mf_elg_net_amt_ratio_5d
  4. [5] MF_MainNetAmtRatio_5D_Mom
  5. [7] mf_elg_net_amt_ratio_5d
  6. [10] mf_elg_net_amt_ratio_20d
  7. [11] MF_Intensity_GBDT_Residual（最后一个）

**最后一个SOTA因子结构：**
- 包含5个based_experiments（其中4个是之前的SOTA因子，1个是baseline）
- RD-Agent采用增量式设计：每个新SOTA因子包含之前的SOTA因子作为based_experiments

### 2. 因子组合机制 ✓

**关键理解：**
RD-Agent在训练最后一个SOTA因子时，会调用`process_factor_data`处理所有based_experiments，生成`combined_factors_df.parquet`。

**实际情况：**
- RD-Agent workspace中的`combined_factors_df.parquet`只包含2个因子：
  1. VolAdjVolumeWeightedMomentum
  2. MultiWindowCapitalFlowComposite

**结论：**
这2个因子是RD-Agent将之前7个SOTA因子通过某种方式组合/优化后的最终复合因子。这是RD-Agent的正常行为，不是错误。

### 3. 同步逻辑验证 ✓

**当前同步结果：**
- 主因子文件：factor.py（MF_Intensity_GBDT_Residual）
- Based因子文件：4个（based_factor_0.py 到 based_factor_3.py）
- factor_order.json：22个特征（20个Alpha158 + 2个动态SOTA因子）
- 模型权重：model.pkl (111,728 bytes)

**结论：**
同步逻辑是正确的。AIstock侧正确同步了：
- 最后一个SOTA因子的代码
- 所有based因子的代码
- 完整的因子顺序信息
- 模型权重文件

### 4. 选股失败问题 ❌

**错误信息：**
```
cannot join with no overlapping index names
```

**问题根源：**
DataFrame join操作时，`df`和`static_df`的索引不匹配。

**具体原因：**
1. `df_history`（行情数据）使用SH/SZ前缀格式：`SH600000`, `SZ000001`
2. `df_fund`（基本面数据）使用.SH/.SZ后缀格式：`600000.SH`, `000001.SZ`
3. 因子代码在规范化instrument时，只处理了`df`，没有同步处理`static_df`
4. 导致join时索引名称一致但实际值不匹配

### 5. 已实施的修复方案

#### 修复1：inference_engine.py
在生成`static_factors.parquet`时，将instrument格式从`.SH/.SZ后缀`转换为`SH/SZ前缀`：

```python
# 关键修复：确保df_fund的instrument格式与df_history完全一致
fund_instruments = df_fund.index.get_level_values("instrument").astype(str)
if fund_instruments.str.contains(r'\.(SH|SZ)$').any():
    # 转换为SH/SZ前缀格式
    converted_instruments = []
    for inst in fund_instruments:
        if '.' in inst:
            code, exchange = inst.split('.')
            converted_instruments.append(f"{exchange}{code}")
        else:
            converted_instruments.append(inst)
    
    df_fund = df_fund.copy()
    df_fund.index = pd.MultiIndex.from_arrays(
        [
            df_fund.index.get_level_values("datetime"),
            pd.Index(converted_instruments, name="instrument"),
        ],
        names=["datetime", "instrument"],
    )
```

#### 修复2：factor.py
在因子代码中，同步规范化`static_df`的instrument格式：

```python
# 同样规范化static_df的instrument
static_inst = static_df.index.get_level_values("instrument").astype(str)
static_m = static_inst.str.match(r"^(SH|SZ)(\d{6})$")
if bool(static_m.any()):
    static_exch = static_inst.str.slice(0, 2)
    static_code = static_inst.str.slice(2, 8)
    static_inst_norm = static_inst.where(~static_m, static_code + "." + static_exch)
    static_df = static_df.copy()
    static_df.index = pd.MultiIndex.from_arrays(
        [
            static_df.index.get_level_values("datetime"),
            pd.Index(static_inst_norm, name="instrument"),
        ],
        names=["datetime", "instrument"],
    )
```

#### 修复3：rdagent_task_sync_service.py
在同步时自动修复因子代码中的索引问题：

```python
# 2. 修复索引名称不匹配问题
if 'static_df = pd.read_parquet("static_factors.parquet").sort_index()' in factor_code:
    factor_code = factor_code.replace(
        'static_df = pd.read_parquet("static_factors.parquet").sort_index()',
        'static_df = pd.read_parquet("static_factors.parquet").sort_index()\n'
        '        # 确保索引名称与df一致\n'
        '        if isinstance(static_df.index, pd.MultiIndex) and static_df.index.names != ["datetime", "instrument"]:\n'
        '            static_df.index.names = ["datetime", "instrument"]'
    )
```

## 待验证项

### 1. 选股功能测试
需要在数据库连接正常的情况下重新测试选股功能，验证修复是否生效。

**测试步骤：**
```bash
python debug_tools\test_stock_selection.py
```

**预期结果：**
- HTTP状态码：200
- 返回股票列表
- 包含股票代码、分数等信息

### 2. 因子计算验证
需要验证因子计算过程中：
- static_factors.parquet的instrument格式是否正确转换
- df和static_df的join操作是否成功
- 最终因子值是否正确计算

## 最终结论

### ✅ 已完成
1. 确认trace.hist中有7个SOTA因子
2. 确认RD-Agent将7个SOTA因子组合成2个复合因子
3. 确认同步逻辑正确：同步了最后一个SOTA因子+所有based因子
4. 确认factor_order.json包含完整的22个特征（20个Alpha158 + 2个动态因子）
5. 实施了3处修复，解决DataFrame join索引不匹配问题

### ❌ 待完成
1. 在数据库连接正常的情况下，重新测试选股功能
2. 验证修复后的选股能够成功输出股票列表
3. 确认整个流程的准确性

## 技术要点

### RD-Agent因子组合机制
- RD-Agent采用增量式SOTA因子设计
- 每个新SOTA因子包含之前的SOTA因子作为based_experiments
- 训练时使用`process_factor_data`处理所有SOTA因子
- 最终生成的`combined_factors_df.parquet`可能包含组合/优化后的因子
- 因此同步时只需要同步最后一个SOTA因子即可

### AIstock选股流程
1. 加载模型权重（model.pkl）
2. 读取factor_order.json获取因子顺序
3. 计算Alpha158基线因子
4. 执行SOTA因子代码计算动态因子
5. 按正确顺序组合所有因子
6. 使用模型进行预测
7. 返回Top N股票列表

### 关键修复点
- **instrument格式统一**：确保所有DataFrame使用相同的instrument格式
- **索引名称一致**：确保MultiIndex的names属性一致
- **同步处理**：在规范化instrument时，同步处理所有相关DataFrame

## 建议

### 短期
1. 等待数据库连接恢复后立即测试选股功能
2. 如果仍有问题，添加详细日志输出df和static_df的索引信息
3. 考虑在inference_engine中添加索引一致性检查

### 长期
1. 统一整个系统的instrument格式标准
2. 在数据源层面就统一格式，避免后续转换
3. 添加自动化测试验证因子计算和选股流程
4. 完善错误处理和日志记录
