# Alpha158 因子计算内存优化分析报告

## 问题描述

在执行选股计算时，Alpha158基线因子计算导致内存占用超过50GB，远超正常水平。

## 根因分析

### 1. Qlib 的 Alpha158 计算方式

从 RD-Agent 的代码分析，Qlib 使用 `Alpha158DL` (DataLoader) 来计算因子：

```python
# RD-Agent: app_tpl/all/V3/rdagent/scenarios/qlib/experiment/custom_loaders.py
class CombinedAlpha158DynamicFactorsLoader:
    def __init__(self, alpha158_config: dict, ...):
        # Qlib 的 Alpha158DL 是一个 DataLoader，不是一次性计算所有因子
        self.alpha_loader = Alpha158DL(config=alpha158_config)
    
    def load(self, instruments, start_time, end_time):
        # 只加载需要的时间窗口和股票
        df_alpha = self.alpha_loader.load(
            instruments=instruments, 
            start_time=start_time, 
            end_time=end_time
        )
```

**关键特点：**
- Alpha158DL 是 **懒加载** (lazy loading)
- 只加载需要的时间窗口 (start_time, end_time)
- 只加载需要的股票池 (instruments)
- **不会一次性计算所有历史数据**

### 2. AIstock 当前的实现问题

```python
# AIstock: backend/inference_engine.py
def _compute_alpha158_subset(self, df_history: pd.DataFrame, col_list: List[str]):
    """计算 Alpha158 的子集特征。"""
    # 问题1: df_history 包含150天 × 4679只股票的完整历史数据
    # 问题2: 对每个因子都进行全量计算
    # 问题3: 中间结果没有及时释放
    
    # 例如：计算滚动回归
    def _rolling_reg_r2_and_resi(y: pd.Series, win: int):
        # 对每只股票的每个窗口都计算回归
        # 内存占用 = 股票数 × 天数 × 窗口大小 × 中间变量
        res = y.groupby(level="instrument").rolling(win).apply(
            lambda x: _calc(x)[0]
        ).reset_index(level=0, drop=True)
```

**内存占用估算：**
- 股票数：4679
- 历史天数：150
- 数据点总数：4679 × 150 = 701,850
- 每个因子计算都需要创建临时数组
- 20个Alpha158因子 × 多个中间变量 = **巨大的内存占用**

### 3. 内存占用的具体来源

#### 3.1 滚动计算的内存问题

```python
# 当前实现
def _rolling_reg_r2_and_resi(y: pd.Series, win: int):
    # 每次 rolling().apply() 都会创建大量临时对象
    res = y.groupby(level="instrument").rolling(win).apply(
        lambda x: _calc(x)[0]  # 每个窗口都调用一次
    )
    # 内存占用 = 股票数 × (天数 - 窗口 + 1) × 临时数组大小
```

#### 3.2 重复计算问题

```python
# 当前对每个因子都单独计算
if "RSQR5" in col_list: out["RSQR5"] = _rolling_reg_r2_and_resi(close, 5)[0]
if "RESI5" in col_list: out["RESI5"] = _rolling_reg_r2_and_resi(close, 5)[1]
# 如果两个因子都需要，会计算两次！
```

#### 3.3 数据复制问题

```python
# 多次数据复制
close = df[close_col]  # 复制1
g_close = close.groupby(level="instrument")  # 复制2
ret = g_close.pct_change()  # 复制3
```

## 优化方案

### 方案1: 只计算最后一天的因子值（推荐）

**核心思想：** 选股只需要最新一天的因子值，不需要完整的历史因子序列。

```python
def _compute_alpha158_for_last_day(self, df_history: pd.DataFrame, col_list: List[str]) -> pd.DataFrame:
    """只计算最后一天的 Alpha158 因子值"""
    # 1. 获取最后一天的日期
    last_date = df_history.index.get_level_values("datetime").max()
    
    # 2. 对于需要滚动窗口的因子，只计算最后一个窗口
    # 3. 立即释放中间结果
    # 4. 只返回最后一天的因子值
```

**内存节省：**
- 原来：150天 × 4679股票 × 20因子 = 14M+ 数据点
- 优化后：1天 × 4679股票 × 20因子 = 93,580 数据点
- **内存减少 99%+**

### 方案2: 分批计算

```python
def _compute_alpha158_in_batches(self, df_history: pd.DataFrame, col_list: List[str], batch_size: int = 500):
    """分批计算 Alpha158 因子"""
    instruments = df_history.index.get_level_values("instrument").unique()
    results = []
    
    for i in range(0, len(instruments), batch_size):
        batch_instruments = instruments[i:i+batch_size]
        batch_df = df_history.loc[(slice(None), batch_instruments), :]
        batch_result = self._compute_alpha158_subset(batch_df, col_list)
        results.append(batch_result)
        # 立即释放批次数据
        del batch_df, batch_result
        gc.collect()
    
    return pd.concat(results)
```

### 方案3: 使用 Numba 加速并减少内存

```python
import numba

@numba.jit(nopython=True)
def _calc_rolling_reg_fast(arr, win):
    """使用 Numba 加速滚动回归，减少内存分配"""
    n = len(arr)
    r2_out = np.empty(n)
    resi_out = np.empty(n)
    # ... 优化的计算逻辑
    return r2_out, resi_out
```

## 推荐实施方案

**优先级1：** 只计算最后一天的因子值（方案1）
- 最大的内存节省
- 符合选股业务需求
- 实现简单

**优先级2：** 优化滚动计算逻辑
- 避免重复计算
- 及时释放中间结果
- 使用更高效的算法

**优先级3：** 分批处理（方案2）
- 作为兜底方案
- 适用于极端情况

## 实施步骤

1. 创建新的优化版本 `_compute_alpha158_for_last_day()`
2. 在 `_run_inference_impl()` 中使用新方法
3. 测试内存占用和计算时间
4. 验证因子值的正确性
5. 部署到生产环境

## 预期效果

- **内存占用：** 从 50GB+ 降低到 < 2GB
- **计算时间：** 从 30s+ 降低到 < 5s
- **准确性：** 保持不变（只是优化计算方式）
