# AIstock Task选股数据服务层最终设计方案

> 版本：v1.0
> 日期：2026-02-11
> 状态：最终版本

---

## 一、问题背景

### 1.1 核心问题

RDAgent侧使用预计算的因子数据文件（`static_factors.parquet`），而AIstock侧需要基于实盘数据库实时计算。两侧数据来源和字段命名存在差异，导致同步到AIstock的Task在执行选股时无法找到因子代码期望的数据字段。

### 1.2 问题表现

| 问题类型 | 具体表现 |
|---------|---------|
| 字段缺失 | 因子代码期望 `size_log_mv`，但AIstock只提供 `db_circ_mv` |
| 字段命名不一致 | 因子代码期望 `liquidity_turnover`，AIstock提供 `db_turnover_rate` |
| 预计算字段缺失 | 滚动聚合字段如 `mf_main_net_amt_ratio_20d` 未计算 |

### 1.3 数据来源对比

| 环境 | 数据来源 | 特点 |
|------|---------|------|
| RDAgent侧（训练/回测） | 预计算的 `static_factors.parquet` | 包含所有派生字段，历史快照 |
| AIstock侧（实盘选股） | TimescaleDB 实时查询 | 只有原始字段，需要实时计算派生字段 |

---

## 二、设计目标

1. **数据完整性**：AIstock侧生成的 `static_factors.parquet` 必须包含RDAgent因子代码期望的所有字段
2. **数据实时性**：所有数据必须来自TimescaleDB实时查询，禁止使用历史快照文件
3. **字段一致性**：字段命名必须与RDAgent侧 `generate_static_factors_bundle.py` 生成的schema完全一致
4. **兼容性**：不修改RDAgent生成的因子代码，通过数据服务层适配

---

## 三、完整字段清单

### 3.1 daily_basic 原始字段（数据库直接获取）

| 数据库字段 | 映射后字段名 | 含义 |
|-----------|-------------|------|
| `turnover_rate` | `db_turnover_rate` | 换手率 |
| `turnover_rate_f` | `db_turnover_rate_f` | 自由流通换手率 |
| `volume_ratio` | `db_volume_ratio` | 量比 |
| `pe` | `db_pe` | 市盈率 |
| `pe_ttm` | `db_pe_ttm` | 市盈率TTM |
| `pb` | `db_pb` | 市净率 |
| `ps` | `db_ps` | 市销率 |
| `ps_ttm` | `db_ps_ttm` | 市销率TTM |
| `dv_ratio` | `db_dv_ratio` | 股息率 |
| `dv_ttm` | `db_dv_ttm` | 股息率TTM |
| `total_share` | `db_total_share` | 总股本 |
| `float_share` | `db_float_share` | 流通股本 |
| `free_share` | `db_free_share` | 自由流通股本 |
| `total_mv` | `db_total_mv` | 总市值 |
| `circ_mv` | `db_circ_mv` | 流通市值 |

### 3.2 moneyflow 原始字段（数据库直接获取）

| 数据库字段 | 映射后字段名 | 含义 |
|-----------|-------------|------|
| `buy_sm_amount` | `mf_sm_buy_amt` | 小单买入金额 |
| `sell_sm_amount` | `mf_sm_sell_amt` | 小单卖出金额 |
| `buy_md_amount` | `mf_md_buy_amt` | 中单买入金额 |
| `sell_md_amount` | `mf_md_sell_amt` | 中单卖出金额 |
| `buy_lg_amount` | `mf_lg_buy_amt` | 大单买入金额 |
| `sell_lg_amount` | `mf_lg_sell_amt` | 大单卖出金额 |
| `buy_elg_amount` | `mf_elg_buy_amt` | 特大单买入金额 |
| `sell_elg_amount` | `mf_elg_sell_amt` | 特大单卖出金额 |
| `buy_sm_vol` | `mf_sm_buy_vol` | 小单买入量 |
| `sell_sm_vol` | `mf_sm_sell_vol` | 小单卖出量 |
| `buy_md_vol` | `mf_md_buy_vol` | 中单买入量 |
| `sell_md_vol` | `mf_md_sell_vol` | 中单卖出量 |
| `buy_lg_vol` | `mf_lg_buy_vol` | 大单买入量 |
| `sell_lg_vol` | `mf_lg_sell_vol` | 大单卖出量 |
| `buy_elg_vol` | `mf_elg_buy_vol` | 特大单买入量 |
| `sell_elg_vol` | `mf_elg_sell_vol` | 特大单卖出量 |
| `net_mf_amount` | `mf_net_amt` | 净流入金额 |
| `net_mf_vol` | `mf_net_vol` | 净流入量 |

### 3.3 预计算派生字段（需要在推理引擎中计算）

#### 3.3.1 估值/规模/流动性因子

| 字段名 | 计算公式 | 状态 |
|-------|---------|------|
| `value_pe_inv` | `1 / db_pe_ttm`（优先）或 `1 / db_pe`；分母为0=>NaN | ✅ 已实现 |
| `value_pb_inv` | `1 / db_pb`；分母为0=>NaN | ✅ 已实现 |
| `size_log_mv` | `log(db_circ_mv)`（优先）或 `log(db_total_mv)`；<=0=>NaN | ✅ 已实现 |
| `liquidity_turnover` | `= db_turnover_rate` | ✅ 已实现 |
| `liquidity_vol_ratio` | `= db_volume_ratio` | ✅ 已实现 |

#### 3.3.2 资金流净值字段

| 字段名 | 计算公式 | 状态 |
|-------|---------|------|
| `mf_total_net_amt` | `= mf_net_amt` | ✅ 已实现 |
| `mf_total_net_vol` | `= mf_net_vol` | ❌ 缺失 |
| `mf_main_net_amt` | `(mf_lg_buy_amt + mf_elg_buy_amt) - (mf_lg_sell_amt + mf_elg_sell_amt)` | ❌ 缺失（仅中间变量） |
| `mf_main_net_vol` | `(mf_lg_buy_vol + mf_elg_buy_vol) - (mf_lg_sell_vol + mf_elg_sell_vol)` | ❌ 缺失 |
| `mf_elg_net_amt` | `mf_elg_buy_amt - mf_elg_sell_amt` | ❌ 缺失（仅中间变量） |
| `mf_elg_net_vol` | `mf_elg_buy_vol - mf_elg_sell_vol` | ❌ 缺失 |

#### 3.3.3 资金流强度字段（ratio）

| 字段名 | 计算公式 | 状态 |
|-------|---------|------|
| `mf_total_net_amt_ratio` | `mf_total_net_amt / amount` | ✅ 已实现 |
| `mf_total_net_vol_ratio` | `mf_total_net_vol / volume` | ❌ 缺失 |
| `mf_main_net_amt_ratio` | `mf_main_net_amt / amount` | ❌ 缺失 |
| `mf_main_net_vol_ratio` | `mf_main_net_vol / volume` | ❌ 缺失 |
| `mf_elg_net_amt_ratio` | `mf_elg_net_amt / amount` | ❌ 缺失 |
| `mf_elg_net_vol_ratio` | `mf_elg_net_vol / volume` | ❌ 缺失 |
| `mf_elg_share_in_main_amt` | `mf_elg_net_amt / mf_main_net_amt` | ✅ 已实现 |
| `mf_elg_share_in_main_vol` | `mf_elg_net_vol / mf_main_net_vol` | ❌ 缺失 |

#### 3.3.4 滚动聚合字段（5D/20D）

| 字段名 | 计算公式 | 状态 |
|-------|---------|------|
| `mf_total_net_amt_5d` | `rolling_sum(mf_total_net_amt, 5)` | ❌ 缺失 |
| `mf_total_net_amt_20d` | `rolling_sum(mf_total_net_amt, 20)` | ❌ 缺失 |
| `mf_main_net_amt_5d` | `rolling_sum(mf_main_net_amt, 5)` | ❌ 缺失 |
| `mf_main_net_amt_20d` | `rolling_sum(mf_main_net_amt, 20)` | ❌ 缺失 |
| `mf_elg_net_amt_5d` | `rolling_sum(mf_elg_net_amt, 5)` | ❌ 缺失 |
| `mf_elg_net_amt_20d` | `rolling_sum(mf_elg_net_amt, 20)` | ❌ 缺失 |
| `mf_total_net_amt_ratio_5d` | `mf_total_net_amt_5d / rolling_sum(amount, 5)` | ❌ 缺失 |
| `mf_total_net_amt_ratio_20d` | `mf_total_net_amt_20d / rolling_sum(amount, 20)` | ❌ 缺失 |
| `mf_main_net_amt_ratio_5d` | `mf_main_net_amt_5d / rolling_sum(amount, 5)` | ✅ 已实现 |
| `mf_main_net_amt_ratio_20d` | `mf_main_net_amt_20d / rolling_sum(amount, 20)` | ❌ 缺失 |
| `mf_elg_net_amt_ratio_5d` | `mf_elg_net_amt_5d / rolling_sum(amount, 5)` | ✅ 已实现 |
| `mf_elg_net_amt_ratio_20d` | `mf_elg_net_amt_20d / rolling_sum(amount, 20)` | ❌ 缺失 |

#### 3.3.5 价格动量字段

| 字段名 | 计算公式 | 状态 |
|-------|---------|------|
| `PriceStrength_10D` | `close.pct_change(10)` | ✅ 已实现 |

---

## 四、实现方案

### 4.1 架构设计

```
┌─────────────────────────────────────────────────────────────────┐
│                    选股请求 (trade_date)                         │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   inference_engine.py                            │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  数据获取层（全部来自 TimescaleDB）                        │   │
│  │                                                          │   │
│  │  ┌─────────────┐  ┌──────────────┐  ┌───────────────┐   │   │
│  │  │ kline_qfq   │  │ daily_basic  │  │ moneyflow_ts  │   │   │
│  │  │ (OHLCV+复权) │  │ (基本面指标)  │  │ (资金流向)     │   │   │
│  │  └──────┬──────┘  └──────┬───────┘  └───────┬───────┘   │   │
│  │         │               │                   │           │   │
│  │         ▼               └─────────┬─────────┘           │   │
│  │    df_history                     ▼                     │   │
│  │                              df_fund_raw                │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  预计算因子服务层（新增模块）                               │   │
│  │  preprocessor.py                                         │   │
│  │                                                          │   │
│  │  输入: df_fund_raw + df_history(amount, volume, close)   │   │
│  │  输出: df_fund (包含所有预计算字段)                        │   │
│  │                                                          │   │
│  │  计算内容:                                                │   │
│  │  - 估值因子: value_pe_inv, value_pb_inv                  │   │
│  │  - 规模因子: size_log_mv                                 │   │
│  │  - 流动性因子: liquidity_turnover, liquidity_vol_ratio   │   │
│  │  - 资金流净值: mf_*_net_amt, mf_*_net_vol                │   │
│  │  - 资金流强度: mf_*_ratio                                │   │
│  │  - 滚动聚合: mf_*_5d, mf_*_20d                           │   │
│  │  - 价格动量: PriceStrength_10D                           │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  临时文件生成                                              │   │
│  │                                                          │   │
│  │  df_history ──写入──▶ daily_pv.h5                        │   │
│  │  df_fund    ──写入──▶ static_factors.parquet             │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  因子执行 + 模型预测                                       │   │
│  │                                                          │   │
│  │  factor.py::calculate_xxx()                              │   │
│  │    ├── pd.read_hdf("daily_pv.h5")                        │   │
│  │    ├── pd.read_parquet("static_factors.parquet")          │   │
│  │    └── return DataFrame (因子值)                          │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 新增模块：预计算因子服务 (preprocessor.py)

建议在 `backend/data_service/` 目录下新增 `preprocessor.py` 模块，集中管理所有预计算字段的计算逻辑。

#### 4.2.1 模块职责

1. 接收原始数据（df_fund_raw, df_history）
2. 计算所有预计算派生字段
3. 返回完整的 df_fund（包含原始字段 + 派生字段）

#### 4.2.2 接口设计

```python
def compute_precomputed_factors(
    df_fund_raw: pd.DataFrame,
    df_history: pd.DataFrame,
) -> pd.DataFrame:
    """
    计算所有预计算因子字段

    Args:
        df_fund_raw: 从数据库获取的原始基本面+资金流数据
                     索引: MultiIndex(datetime, instrument)
                     列: db_*, mf_* 原始字段
        df_history: 从数据库获取的OHLCV行情数据
                    索引: MultiIndex(datetime, instrument)
                    列: open, high, low, close, volume, amount

    Returns:
        df_fund: 包含原始字段 + 所有预计算派生字段的DataFrame
    """
```

### 4.3 预计算字段计算逻辑

#### 4.3.1 估值/规模/流动性因子

```python
# 估值因子
df['value_pe_inv'] = 1.0 / df['db_pe_ttm'].replace(0, np.nan)
df['value_pb_inv'] = 1.0 / df['db_pb'].replace(0, np.nan)

# 规模因子（优先使用流通市值）
mv_col = 'db_circ_mv' if 'db_circ_mv' in df.columns else 'db_total_mv'
df['size_log_mv'] = np.log(df[mv_col].where(df[mv_col] > 0)).replace(-np.inf, np.nan)

# 流动性因子（直接映射）
df['liquidity_turnover'] = df['db_turnover_rate']
df['liquidity_vol_ratio'] = df['db_volume_ratio']
```

#### 4.3.2 资金流净值字段

```python
# 全档净流入
df['mf_total_net_amt'] = df['mf_net_amt']
df['mf_total_net_vol'] = df['mf_net_vol']

# 主力净流入（大单+特大单）
df['mf_main_net_amt'] = (
    (df['mf_lg_buy_amt'] + df['mf_elg_buy_amt']) -
    (df['mf_lg_sell_amt'] + df['mf_elg_sell_amt'])
)
df['mf_main_net_vol'] = (
    (df['mf_lg_buy_vol'] + df['mf_elg_buy_vol']) -
    (df['mf_lg_sell_vol'] + df['mf_elg_sell_vol'])
)

# 特大单净流入
df['mf_elg_net_amt'] = df['mf_elg_buy_amt'] - df['mf_elg_sell_amt']
df['mf_elg_net_vol'] = df['mf_elg_buy_vol'] - df['mf_elg_sell_vol']
```

#### 4.3.3 资金流强度字段

```python
def safe_div(numer, denom):
    """安全除法，分母为0时返回NaN"""
    return numer / denom.replace(0, np.nan)

# 需要从df_history获取amount和volume
amount = df_history['amount']
volume = df_history['volume']

# 全档强度
df['mf_total_net_amt_ratio'] = safe_div(df['mf_total_net_amt'], amount)
df['mf_total_net_vol_ratio'] = safe_div(df['mf_total_net_vol'], volume)

# 主力强度
df['mf_main_net_amt_ratio'] = safe_div(df['mf_main_net_amt'], amount)
df['mf_main_net_vol_ratio'] = safe_div(df['mf_main_net_vol'], volume)

# 特大单强度
df['mf_elg_net_amt_ratio'] = safe_div(df['mf_elg_net_amt'], amount)
df['mf_elg_net_vol_ratio'] = safe_div(df['mf_elg_net_vol'], volume)

# 特大单占主力比例
df['mf_elg_share_in_main_amt'] = safe_div(df['mf_elg_net_amt'], df['mf_main_net_amt'])
df['mf_elg_share_in_main_vol'] = safe_div(df['mf_elg_net_vol'], df['mf_main_net_vol'])
```

#### 4.3.4 滚动聚合字段

```python
def rolling_sum_by_instrument(s: pd.Series, window: int) -> pd.Series:
    """按股票分组计算滚动和"""
    return (
        s.groupby(level='instrument')
        .rolling(window=window, min_periods=window)
        .sum()
        .reset_index(level=0, drop=True)
    )

# 5日和20日滚动
for w in [5, 20]:
    # 净流入金额滚动和
    df[f'mf_total_net_amt_{w}d'] = rolling_sum_by_instrument(df['mf_total_net_amt'], w)
    df[f'mf_main_net_amt_{w}d'] = rolling_sum_by_instrument(df['mf_main_net_amt'], w)
    df[f'mf_elg_net_amt_{w}d'] = rolling_sum_by_instrument(df['mf_elg_net_amt'], w)

    # 成交额滚动和（用于计算强度）
    amount_w = rolling_sum_by_instrument(amount, w)

    # 强度滚动
    df[f'mf_total_net_amt_ratio_{w}d'] = safe_div(df[f'mf_total_net_amt_{w}d'], amount_w)
    df[f'mf_main_net_amt_ratio_{w}d'] = safe_div(df[f'mf_main_net_amt_{w}d'], amount_w)
    df[f'mf_elg_net_amt_ratio_{w}d'] = safe_div(df[f'mf_elg_net_amt_{w}d'], amount_w)
```

#### 4.3.5 价格动量字段

```python
# 10日价格强度
df['PriceStrength_10D'] = df_history.groupby(level='instrument')['close'].pct_change(10)
```

---

## 五、修改清单

### 5.1 需要修改的文件

| 文件 | 修改内容 | 优先级 |
|-----|---------|-------|
| `backend/data_service/preprocessor.py` | 新增预计算因子服务模块 | **高** |
| `backend/inference_engine.py` | 调用preprocessor计算预计算字段 | **高** |

### 5.2 inference_engine.py 修改点

在 `_run_inference_impl` 方法中，将现有的预计算字段计算逻辑替换为调用 `preprocessor.compute_precomputed_factors()`：

```python
# 修改前（分散在多处的计算逻辑）
# ... 884-996行的预计算字段计算代码 ...

# 修改后（集中调用）
from .data_service.preprocessor import compute_precomputed_factors

df_fund = compute_precomputed_factors(
    df_fund_raw=df_fund,
    df_history=df_history,
)
```

---

## 六、数据窗口要求

### 6.1 当前窗口设置

| 数据类型 | 当前窗口 | 实际交易日 |
|---------|---------|-----------|
| OHLCV行情 | 90自然日 | ~63交易日 |
| 基本面/资金流 | 90自然日（上限180天） | ~63交易日 |

### 6.2 因子对历史数据的需求

| 计算项 | 所需最小窗口 | 当前窗口是否满足 |
|-------|------------|----------------|
| 5日滚动聚合 | 5交易日 | ✅ 满足 |
| 10日价格强度 | 10交易日 | ✅ 满足 |
| 20日滚动聚合 | 20交易日 | ✅ 满足 |
| Alpha158因子（最大60日窗口） | 60交易日 | ✅ 刚好满足 |

### 6.3 建议

将行情窗口从90天扩大到**120天**（约84个交易日），为更长窗口因子提供安全余量。

---

## 七、验证方案

### 7.1 字段完整性验证

在推理引擎生成 `static_factors.parquet` 后，验证是否包含所有必需字段：

```python
REQUIRED_FIELDS = [
    # 估值/规模/流动性
    'value_pe_inv', 'value_pb_inv', 'size_log_mv',
    'liquidity_turnover', 'liquidity_vol_ratio',
    # 资金流净值
    'mf_total_net_amt', 'mf_total_net_vol',
    'mf_main_net_amt', 'mf_main_net_vol',
    'mf_elg_net_amt', 'mf_elg_net_vol',
    # 资金流强度
    'mf_total_net_amt_ratio', 'mf_total_net_vol_ratio',
    'mf_main_net_amt_ratio', 'mf_main_net_vol_ratio',
    'mf_elg_net_amt_ratio', 'mf_elg_net_vol_ratio',
    'mf_elg_share_in_main_amt', 'mf_elg_share_in_main_vol',
    # 滚动聚合
    'mf_total_net_amt_5d', 'mf_total_net_amt_20d',
    'mf_main_net_amt_5d', 'mf_main_net_amt_20d',
    'mf_elg_net_amt_5d', 'mf_elg_net_amt_20d',
    'mf_total_net_amt_ratio_5d', 'mf_total_net_amt_ratio_20d',
    'mf_main_net_amt_ratio_5d', 'mf_main_net_amt_ratio_20d',
    'mf_elg_net_amt_ratio_5d', 'mf_elg_net_amt_ratio_20d',
    # 价格动量
    'PriceStrength_10D',
]

def validate_static_factors(df: pd.DataFrame) -> bool:
    missing = [f for f in REQUIRED_FIELDS if f not in df.columns]
    if missing:
        logger.error(f"static_factors缺失字段: {missing}")
        return False
    return True
```

### 7.2 端到端测试

1. 选择一个已同步的Task
2. 执行选股推理
3. 验证因子计算无KeyError
4. 验证模型预测正常输出

---

## 八、总结

### 8.1 现有方案评估

| 方案文档 | 覆盖内容 | 缺失内容 |
|---------|---------|---------|
| `factor_selection_data_service_analysis.md` | 问题分析、数据来源确认、部分预计算字段 | 完整字段清单、实现代码 |
| `选股数据服务层分析与设计方案.md` | 详细的数据流分析、性能分析、优化方案 | 完整的预计算字段实现 |

### 8.2 本方案补充内容

1. **完整字段清单**：对齐RDAgent侧 `generate_static_factors_bundle.py` 的所有字段
2. **缺失字段识别**：明确标注当前推理引擎中缺失的18个预计算字段
3. **实现代码**：提供完整的计算逻辑代码
4. **模块化设计**：建议新增 `preprocessor.py` 集中管理预计算逻辑
5. **验证方案**：提供字段完整性验证代码

### 8.3 实施优先级

| 优先级 | 任务 | 预计工作量 |
|-------|------|-----------|
| P0 | 补全18个缺失的预计算字段 | 2小时 |
| P1 | 新增preprocessor.py模块 | 1小时 |
| P2 | 添加字段完整性验证 | 0.5小时 |
| P3 | 扩大数据窗口到120天 | 0.5小时 |

---

## 九、性能分析与优化方案

### 9.1 当前性能消耗分析

#### 9.1.1 单次选股耗时分解

| 步骤 | 操作 | 耗时估算 | 占比 |
|-----|------|---------|------|
| ① 加载因子模块 | 动态import factor.py | <0.1s | ~1% |
| ② 解析factor_order.json | JSON读取 | <0.01s | ~0% |
| ③ 加载模型权重 | pickle反序列化 | 0.5-2s | ~10% |
| ④ SQL查询-行情 | 查询kline_qfq，~30万行 | 1-3s | ~15% |
| ⑤ SQL查询-基本面 | 查询daily_basic，~30万行 | 2-5s | ~20% |
| ⑥ SQL查询-资金流 | 查询moneyflow_ts，~30万行 | 2-5s | ~20% |
| ⑦ 预计算字段计算 | 滚动窗口、分组计算 | 0.5-1s | ~5% |
| ⑧ 写入daily_pv.h5 | HDF5序列化，~50-100MB | 0.5-1s | ~5% |
| ⑨ 写入static_factors.parquet | Parquet序列化，~30-80MB | 0.3-0.5s | ~3% |
| ⑩ 因子代码读取文件 | 读取H5+Parquet | 1-3s | ~10% |
| ⑪ 因子计算 | SOTA因子逻辑 | 0.1-1s | ~3% |
| ⑫ Alpha158因子计算 | 优化版本：只计算最后一天 | 2-5s | ~15% |
| ⑬ 模型预测 | LGB/PyTorch推理 | 0.1-1s | ~3% |
| ⑭ 保存信号到DB | INSERT ~5000行 | 0.2-0.5s | ~2% |
| ⑮ 清理临时目录 | shutil.rmtree | <0.1s | ~0% |
| **总计** | | **约 10-27秒** | 100% |

#### 9.1.2 性能瓶颈识别

| 瓶颈类型 | 占比 | 说明 |
|---------|------|------|
| **数据库查询（④⑤⑥）** | ~55% | 最大瓶颈，每次查询约30万行 |
| **文件I/O（⑧⑨⑩）** | ~18% | 写入+读取约130MB临时文件 |
| **因子计算（⑪⑫）** | ~18% | Alpha158已优化，SOTA取决于因子复杂度 |
| **其他** | ~9% | 模型加载、预测、信号保存等 |

### 9.2 关键问题解答

#### Q1: 是否需要提前载入大量数据进行实时计算？

**当前实现**：每次选股时实时从数据库查询90天数据，不预加载。

**分析**：
- 优点：数据保证最新，无缓存一致性问题
- 缺点：每次选股都有3-8秒的SQL查询开销

#### Q2: 是直接载入90天数据还是按需载入？

**当前实现**：直接载入90天全量数据。

**原因**：
1. 因子代码通过 `pd.read_hdf("daily_pv.h5")` 读取**完整文件**，无法按需读取
2. Alpha158因子需要60个交易日的滚动窗口
3. 预计算字段（如5日/20日滚动）需要历史数据

#### Q3: 数据是否必须转换成H5格式？

**是的，必须转换**。

**原因**：RDAgent生成的因子代码使用固定接口：
```python
df = pd.read_hdf("daily_pv.h5", key="data")
static_df = pd.read_parquet("static_factors.parquet")
```

**约束**：在不修改因子代码的前提下，必须生成这两个临时文件。

#### Q4: 如果因子需要90天以上的数据，程序会如何处理？

**当前行为**：
1. 数据窗口固定为90自然日（约63个交易日）
2. 基本面/资金流数据有180天硬限制（`timescaledb_adapter.py:111-115`）
3. 数据不足时，因子计算返回NaN，不会报错
4. LightGBM模型原生支持NaN，预测仍可进行，但质量下降

**风险**：
- 90天窗口刚好覆盖60个交易日，遇长假可能不足
- ROC60因子需要61天数据，边界情况可能返回NaN

### 9.3 性能优化方案

#### 方案A：内存缓存（推荐，中期实施）

**核心思路**：缓存同一交易日的SQL查询结果，多次选股复用。

```python
# 缓存结构
_data_cache = {
    "trade_date": None,
    "universe_hash": None,
    "df_history": None,
    "df_fund_raw": None,
    "expire_at": None,
}

def get_cached_data(trade_date, universe):
    """获取缓存数据，缓存有效期为当日"""
    cache_key = (trade_date.date(), hash(tuple(sorted(universe))))

    if (_data_cache["trade_date"] == cache_key[0] and
        _data_cache["universe_hash"] == cache_key[1]):
        return _data_cache["df_history"], _data_cache["df_fund_raw"]

    # 缓存未命中，重新查询
    df_history = get_history_window(...)
    df_fund_raw = fetch_fundamental_data_ts(...)

    # 更新缓存
    _data_cache.update({
        "trade_date": cache_key[0],
        "universe_hash": cache_key[1],
        "df_history": df_history,
        "df_fund_raw": df_fund_raw,
    })

    return df_history, df_fund_raw
```

**预期收益**：
- 同日第2次及以后选股：SQL查询耗时从3-8s降至0s
- 总耗时从10-27s降至5-15s

**适用场景**：同一交易日内执行多个Task的选股

#### 方案B：RAM Disk文件桥接（可选，高频场景）

**核心思路**：将临时目录创建在内存文件系统上。

**Windows实现**：
```python
import tempfile
import os

# 配置RAM Disk路径（需要预先创建）
RAM_DISK_PATH = os.environ.get("AISTOCK_RAMDISK", None)

def get_temp_dir():
    if RAM_DISK_PATH and os.path.isdir(RAM_DISK_PATH):
        return tempfile.mkdtemp(prefix="aistock_inf_", dir=RAM_DISK_PATH)
    return tempfile.mkdtemp(prefix="aistock_inf_")
```

**预期收益**：
- 文件I/O耗时从1-2s降至<0.1s
- 总耗时减少约10-15%

**缺点**：需要额外配置RAM Disk（Windows需要ImDisk等工具）

#### 方案C：预计算字段数据库物化（长期，大规模部署）

**核心思路**：在数据入库时预计算派生字段，存入物化视图。

```sql
CREATE MATERIALIZED VIEW market.daily_basic_factors AS
SELECT
    trade_date,
    ts_code,
    -- 原始字段
    turnover_rate AS db_turnover_rate,
    pe_ttm AS db_pe_ttm,
    pb AS db_pb,
    circ_mv AS db_circ_mv,
    -- 预计算字段
    turnover_rate AS liquidity_turnover,
    volume_ratio AS liquidity_vol_ratio,
    1.0 / NULLIF(pe_ttm, 0) AS value_pe_inv,
    1.0 / NULLIF(pb, 0) AS value_pb_inv,
    LN(NULLIF(circ_mv, 0)) AS size_log_mv
FROM market.daily_basic;
```

**预期收益**：
- 预计算字段计算耗时从0.5-1s降至0s
- 减少推理引擎计算负担

**缺点**：
- 需要维护物化视图刷新逻辑
- 增加数据入库复杂度

### 9.4 方案对比

| 维度 | 当前方案 | 方案A（内存缓存） | 方案B（RAM Disk） | 方案C（DB物化） |
|------|---------|-----------------|-----------------|---------------|
| 单次选股耗时 | 10-27s | 10-27s（首次）/ 5-15s（后续） | 8-25s | 9-26s |
| 实现复杂度 | 低 | 低 | 中 | 高 |
| 运维复杂度 | 低 | 低 | 中 | 高 |
| 内存占用 | ~500MB | ~1GB（缓存数据） | ~500MB | ~500MB |
| 适用场景 | 通用 | 同日多次选股 | 高频选股 | 大规模部署 |

### 9.5 推荐实施路径

| 阶段 | 任务 | 优先级 | 预期收益 |
|-----|------|-------|---------|
| **立即** | 补全缺失的预计算字段 | P0 | 解决KeyError问题 |
| **短期** | 扩大数据窗口到120天 | P3 | 避免边界情况NaN |
| **中期** | 实施方案A（内存缓存） | P1 | 同日多次选股提速50% |
| **长期** | 评估方案B/C | P2 | 进一步优化 |

---

## 十、数据窗口不足的处理策略

### 10.1 当前窗口设置

| 数据类型 | 当前窗口 | 实际交易日 | 硬限制 |
|---------|---------|-----------|-------|
| OHLCV行情 | 90自然日 | ~63交易日 | 无 |
| 基本面/资金流 | 90自然日 | ~63交易日 | 180天 |

### 10.2 因子对历史数据的需求

| 因子类型 | 最大窗口需求 | 当前是否满足 |
|---------|------------|-------------|
| Alpha158（ROC60等） | 61交易日 | ⚠️ 刚好满足 |
| 预计算字段（20日滚动） | 20交易日 | ✅ 满足 |
| SOTA因子（当前） | 1-10交易日 | ✅ 满足 |

### 10.3 数据不足时的处理逻辑

**当前实现**（`inference_engine.py`）：

```python
# Alpha158因子：数据不足返回NaN
def _calc_roc60(group):
    arr = group.values
    if len(arr) < 61:
        return np.nan  # 数据不足，返回NaN
    return arr[-61] / arr[-1]

# 滚动聚合：min_periods控制
rolling(window=5, min_periods=5).sum()  # 不足5天返回NaN
```

**模型预测**：LightGBM原生支持NaN，不会报错，但预测质量下降。

### 10.4 建议：扩大数据窗口

将行情窗口从90天扩大到**120天**（约84个交易日）：

```python
# 修改 inference_engine.py:801
# 修改前
start_date = actual_date - timedelta(days=90)

# 修改后
start_date = actual_date - timedelta(days=120)
```

**收益**：
- 为60日窗口因子提供24个交易日的安全余量
- 覆盖春节+国庆等长假场景

**成本**：
- SQL查询数据量增加约33%
- 临时文件大小增加约33%
- 单次选股耗时增加约2-5秒

---

## 十一、资金流字段计算口径对齐

### 11.1 口径差异问题

当前推理引擎中 `mf_main_net_amt_ratio_5d` 的计算方式与RDAgent侧不一致：

| 计算方式 | 公式 | 数学含义 |
|---------|------|---------|
| **RDAgent侧** | `sum_5d(main_net_amt) / sum_5d(amount)` | 5日净流入总额 / 5日成交额总额 |
| **AIstock当前** | `sum_5d(main_net_amt / amount)` | 5日(净流入/成交额)之和 |

**数学上不等价**：`sum(a/b) ≠ sum(a)/sum(b)`

### 11.2 修复方案

对齐RDAgent侧 `generate_static_factors_bundle.py:200-208` 的计算逻辑：

```python
# 正确的计算方式
for w in [5, 20]:
    # 先计算滚动和
    main_net_amt_w = rolling_sum_by_instrument(df['mf_main_net_amt'], w)
    amount_w = rolling_sum_by_instrument(amount, w)

    # 再计算比率
    df[f'mf_main_net_amt_ratio_{w}d'] = safe_div(main_net_amt_w, amount_w)
```

---

## 十二、完整实施清单

### 12.1 P0：补全缺失的预计算字段

**修改文件**：`backend/inference_engine.py` 或新增 `backend/data_service/preprocessor.py`

**需要补充的字段**（共18个）：

| 类别 | 字段 | 计算公式 |
|-----|------|---------|
| 估值 | `value_pe_inv` | `1 / db_pe_ttm` |
| 估值 | `value_pb_inv` | `1 / db_pb` |
| 规模 | `size_log_mv` | `log(db_circ_mv)` |
| 流动性 | `liquidity_turnover` | `= db_turnover_rate` |
| 流动性 | `liquidity_vol_ratio` | `= db_volume_ratio` |
| 资金流净值 | `mf_total_net_vol` | `= mf_net_vol` |
| 资金流净值 | `mf_main_net_amt` | `(lg+elg)_buy - (lg+elg)_sell` |
| 资金流净值 | `mf_main_net_vol` | 同上（量） |
| 资金流净值 | `mf_elg_net_amt` | `elg_buy - elg_sell` |
| 资金流净值 | `mf_elg_net_vol` | 同上（量） |
| 资金流强度 | `mf_total_net_vol_ratio` | `mf_total_net_vol / volume` |
| 资金流强度 | `mf_main_net_amt_ratio` | `mf_main_net_amt / amount` |
| 资金流强度 | `mf_main_net_vol_ratio` | `mf_main_net_vol / volume` |
| 资金流强度 | `mf_elg_net_amt_ratio` | `mf_elg_net_amt / amount` |
| 资金流强度 | `mf_elg_net_vol_ratio` | `mf_elg_net_vol / volume` |
| 资金流强度 | `mf_elg_share_in_main_vol` | `mf_elg_net_vol / mf_main_net_vol` |
| 滚动聚合 | `mf_*_5d`, `mf_*_20d` | 见第四节 |

### 12.2 P1：对齐资金流计算口径

**修改文件**：`backend/inference_engine.py`

**修改内容**：将 `mf_*_ratio_5d/20d` 的计算方式从"先算比率再求和"改为"先求和再算比率"

### 12.3 P2：扩大数据窗口

**修改文件**：`backend/inference_engine.py:801`

**修改内容**：`timedelta(days=90)` → `timedelta(days=120)`

### 12.4 P3：实施内存缓存（中期）

**新增文件**：`backend/data_service/cache.py`

**修改文件**：`backend/inference_engine.py`

**功能**：缓存同一交易日的SQL查询结果

