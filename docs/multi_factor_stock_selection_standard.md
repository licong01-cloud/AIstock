# 多因子选股的业界标准计算方式

## 📋 核心原则

### 选股场景的因子计算逻辑

**✅ 正确理解：**

1. **只需计算当天的因子值**
   - 选股目标：基于当天（T日）的因子值，选出未来（T+1日）表现最好的股票
   - 不需要计算所有历史日期的因子值序列
   - 只需要当天这一个时间点的因子快照

2. **使用历史数据窗口计算**
   - 5日因子：使用 T-4 到 T 日的5天数据计算当天因子值
   - 10日因子：使用 T-9 到 T 日的10天数据计算当天因子值
   - 60日因子：使用 T-59 到 T 日的60天数据计算当天因子值
   - **关键：** 只输出T日的因子值，不输出历史序列

3. **适用于所有因子类型**
   - ✅ Alpha158 基线因子
   - ✅ SOTA 动态因子
   - ✅ 任何自定义因子

---

## 🔍 与回测场景的区别

### 回测场景（Qlib/RD-Agent）

```python
# 回测需要完整的历史因子序列
# 例如：2020-01-01 到 2025-12-31 的每日因子值
df_factors = compute_factors(
    start_date="2020-01-01",
    end_date="2025-12-31"
)
# 输出：每天都有因子值
# shape: (N_days × N_stocks, N_factors)
```

**用途：**
- 评估因子在历史上的表现
- 计算IC/IR等统计指标
- 模拟历史交易

### 选股场景（AIstock实时推理）

```python
# 选股只需要当天的因子值
# 例如：只计算 2026-01-20 这一天的因子值
df_factors = compute_factors_for_date(
    trade_date="2026-01-20",
    history_window=150  # 用于计算滚动窗口因子
)
# 输出：只有2026-01-20这一天的因子值
# shape: (N_stocks, N_factors)
```

**用途：**
- 基于最新因子值排序选股
- 生成当天的交易信号
- 实时推理预测

---

## 💡 为什么这是业界标准？

### 1. **内存效率**

| 场景 | 数据量 | 内存占用 |
|------|--------|----------|
| 回测（1000天） | 1000天 × 5000股 × 50因子 | **~20GB** |
| 选股（1天） | 1天 × 5000股 × 50因子 | **~20MB** |

**差异：1000倍**

### 2. **计算效率**

```python
# 回测：需要计算所有历史日期
for date in date_range:  # 1000次循环
    factors[date] = compute_factors(date)

# 选股：只计算当天
factors = compute_factors(today)  # 1次计算
```

**差异：1000倍**

### 3. **业务需求**

- **回测：** 需要历史序列来评估策略
- **选股：** 只需要当天值来生成信号

### 4. **行业实践**

所有主流量化平台都采用这种模式：

- **WorldQuant Alpha101/191：** 只输出当天因子值
- **Alphalens（Quantopian）：** 因子分析时才需要历史序列
- **生产环境：** 实时计算当天因子，不存储历史

---

## 🎯 AIstock的实施方案

### Alpha158 基线因子（已完成）

```python
def _compute_alpha158_last_day_only(df_history, col_list):
    """只计算最后一天的Alpha158因子值"""
    last_date = df_history.index.get_level_values("datetime").max()
    
    # 对于5日滚动因子
    if "WVMA5" in col_list:
        # 使用最后5天数据计算，只输出最后一天的值
        def _calc_wvma(group):
            arr = group.values
            if len(arr) < 5:
                return np.nan
            return func(arr[-5:])  # 只取最后5天
        
        # 只返回最后一天的因子值
        return last_day_factor_value
```

### SOTA 动态因子（待实施）

**当前问题：** SOTA因子计算可能也在计算完整历史序列

**优化方案：** 应用相同逻辑

```python
def compute_sota_factor_last_day_only(df_history):
    """只计算最后一天的SOTA因子值"""
    last_date = df_history.index.get_level_values("datetime").max()
    
    # 执行因子代码，但只返回最后一天的值
    df_factor_full = exec_factor_code(df_history)
    
    # 只保留最后一天
    return df_factor_full.loc[last_date]
```

---

## ⚠️ 移除兜底方案

### 当前存在的兜底方案

1. **因子缺失时填充0**
   ```python
   # ❌ 错误的兜底方案
   if factor_missing:
       factor_value = 0.0  # 填充0
   ```

2. **特征数量不匹配时补零**
   ```python
   # ❌ 错误的兜底方案
   if len(features) < expected:
       features = np.pad(features, ...)  # 补零
   ```

### 正确的处理方式

```python
# ✅ 正确：直接报错，不使用兜底方案
if factor_missing:
    raise ValueError(f"因子 {factor_name} 计算失败，无法进行选股")

if len(features) != expected:
    raise ValueError(
        f"特征数量不匹配：期望 {expected}，实际 {len(features)}"
    )
```

**原因：**
- 兜底方案会掩盖真实问题
- 填充0会导致模型预测错误
- 应该暴露问题，而不是隐藏问题

---

## 📊 预期效果

### 内存占用

| 组件 | 优化前 | 优化后 | 改善 |
|------|--------|--------|------|
| Alpha158 | 50GB | <2GB | **96%↓** |
| SOTA因子 | 待测 | <500MB | **预计90%+↓** |
| 总计 | 50GB+ | <3GB | **94%↓** |

### 计算时间

| 组件 | 优化前 | 优化后 | 改善 |
|------|--------|--------|------|
| Alpha158 | 30s | <5s | **83%↓** |
| SOTA因子 | 待测 | <2s | **预计80%+↓** |
| 总计 | 30s+ | <7s | **77%↓** |

---

## 📝 总结

**选股场景的标准做法：**

1. ✅ 只计算当天的因子值
2. ✅ 使用历史数据窗口（5日/10日/60日等）
3. ✅ 适用于所有因子类型（Alpha158 + SOTA）
4. ✅ 不使用兜底方案，直接暴露问题
5. ✅ 这是业界标准的多因子选股计算方式

**这种方式确保：**
- 基于当天和历史数据选出最优股票
- 内存和计算效率最优
- 结果准确可靠
- 问题及时暴露
