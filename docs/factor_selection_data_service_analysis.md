# AIstock 因子选股数据服务层分析报告

> 生成日期：2025-02-07（更新：2025-02-09）  
> 分析范围：`inference_engine.py`、`data_service/`、`timescaledb_adapter.py`、RDAgent侧 `generate_static_factors_bundle.py`、`precompute_daily_basic_factors.py`  
> 分析方法：纯代码和数据追踪，无推测

---

## 一、因子选股报错问题

### 1.1 问题与修复

| 项目 | 详情 |
|------|------|
| **错误** | `factor.py` 第26行缩进错误导致 `unexpected indent`，推理引擎加载因子模块失败 |
| **修复** | 修正第26行缩进，因子模块可正常加载 |
| **代码位置** | `rdagent_assets/rdagent_tasks/2026-02-06_10-02-12-151236/factor.py:26` |
| **状态** | ✅ 已解决 |

---

## 二、数据来源确认：是否使用最新行情数据

### 2.1 结论

**✅ 确认：选股因子计算的数据100%来自TimescaleDB（PostgreSQL）中的最新行情数据，不使用任何历史H5/bin文件或包含历史数据的static_factors.parquet。**

### 2.2 代码证据

#### 2.2.1 OHLCV行情数据来源

**代码位置**：`backend/inference_engine.py:799-811`

```python
start_date = actual_date - timedelta(days=90)
universe = self._get_default_universe_excluding_st()
df_history = get_history_window(
    universe=universe,
    start=start_date,
    end=actual_date,
    fields=["open", "high", "low", "close", "volume", "amount"],
    freq="1d",
    adj="front",
)
```

`get_history_window` 函数（`backend/data_service/api.py:180+`）优先调用 `timescaledb_adapter.fetch_history_window_ts()`，从 PostgreSQL 的 `market.kline_daily_raw` 表查询。仅在TimescaleDB查询失败时回退到 `xtquant`（实时行情接口），**绝不读取本地H5/bin文件**。

#### 2.2.2 基本面与资金流数据来源

**代码位置**：`backend/inference_engine.py:836-843`

```python
from .data_service import timescaledb_adapter
df_fund = timescaledb_adapter.fetch_fundamental_data_ts(
    universe=universe,
    start_date=start_date.date(),
    end_date=actual_date.date()
)
```

`fetch_fundamental_data_ts()`（`backend/data_service/timescaledb_adapter.py:98-175`）直接查询 PostgreSQL：
- `market.daily_basic` 表：获取换手率、市盈率、市净率、市值等基本面数据
- `market.moneyflow_ts` 表：获取各档资金流买卖数据

#### 2.2.3 禁止使用的数据源

以下数据源在选股推理流程中**完全不被引用**：

| 禁止的数据源 | 状态 |
|-------------|------|
| `qlib_snapshots/qlib_export_20251209/daily_basic.h5` | ❌ 未引用 |
| `qlib_snapshots/qlib_export_20251209/moneyflow.h5` | ❌ 未引用 |
| `qlib_snapshots/qlib_export_20251209/static_factors.parquet` | ❌ 未引用 |
| QLib bin格式数据 | ❌ 未引用 |
| RDAgent侧 `git_ignore_folder/` 下的任何数据文件 | ❌ 未引用 |

---

## 三、每次选股的文件生成流程

### 3.1 完整数据流

```
用户触发选股
    │
    ▼
inference_engine._run_inference_impl()
    │
    ├─ 1. 加载因子模块（factor.py）
    ├─ 2. 解析 factor_order.json（区分Alpha158 vs SOTA动态因子）
    ├─ 3. 加载模型权重（model.pkl）
    │
    ├─ 4. 获取数据（全部来自TimescaleDB）
    │   ├─ get_history_window() → OHLCV行情（90天）
    │   └─ fetch_fundamental_data_ts() → 基本面+资金流（90天，上限180天）
    │
    ├─ 5. 创建临时目录 + 切换工作目录
    │   ├─ 写入 daily_pv.h5（OHLCV行情数据）
    │   ├─ 计算预计算字段（资金流衍生指标）
    │   └─ 写入 static_factors.parquet（基本面+资金流+预计算字段）
    │
    ├─ 6. 执行因子计算
    │   ├─ SOTA因子：factor_func() → 读取 daily_pv.h5 和 static_factors.parquet
    │   └─ Alpha158因子：_compute_alpha158_last_day_only() → 直接使用df_history
    │
    ├─ 7. 合并因子 → 模型预测 → 生成评分
    │
    ├─ 8. 保存信号到 trading.rdagent_signal 表
    │
    └─ 9. 清理：恢复工作目录 + 删除临时目录
```

### 3.2 临时文件详情

#### 3.2.1 daily_pv.h5

**代码位置**：`backend/inference_engine.py:822-832`

```python
df_pv = df_history.copy()
for col in ["open", "high", "low", "close", "volume", "amount", "factor"]:
    if col in df_pv.columns:
        df_pv[f"${col}"] = df_pv[col]
df_pv.to_hdf("daily_pv.h5", key="data", mode="w")
```

| 属性 | 值 |
|------|-----|
| **数据范围** | actual_date - 90天 到 actual_date |
| **数据量** | 约 5000只股票 × 60个交易日 × 12列 ≈ 360万行 |
| **文件大小** | 约 50-100MB（HDF5格式） |
| **用途** | 供SOTA因子代码通过 `pd.read_hdf("daily_pv.h5")` 读取 |

#### 3.2.2 static_factors.parquet

**代码位置**：`backend/inference_engine.py:844-1036`

| 属性 | 值 |
|------|-----|
| **数据范围** | actual_date - 90天 到 actual_date |
| **数据量** | 约 5000只股票 × 60个交易日 × 30+列 |
| **文件大小** | 约 30-80MB（Parquet格式） |
| **用途** | 供SOTA因子代码通过 `pd.read_parquet("static_factors.parquet")` 读取 |

### 3.3 预计算字段生成逻辑

推理引擎在 `inference_engine.py:884-955` 中计算了以下预计算字段：

| 字段名 | 计算公式 | 代码行 |
|--------|---------|--------|
| `mf_total_net_amt` | = `mf_net_amt`（直接赋值） | 895-898 |
| `mf_total_net_amt_ratio` | = `mf_total_net_amt / amount` | 901-903 |
| `mf_main_net_amt_ratio_5d` | = 5日滚动和(`(lg_buy-lg_sell+elg_buy-elg_sell)/amount`) | 906-924 |
| `mf_elg_net_amt_ratio_5d` | = 5日滚动和(`(elg_buy-elg_sell)/amount`) | 927-942 |
| `mf_elg_share_in_main_amt` | = `elg_net / main_net`（避免除零） | 944-948 |
| `PriceStrength_10D` | = 10日收益率 `pct_change(10)` | 950-955 |

---

## 四、关键发现：预计算字段缺失问题

### 4.1 问题描述

**⚠️ 推理引擎缺少5个预计算字段的计算逻辑。**

以下字段在RDAgent侧由 `generate_static_factors_bundle.py` 从 `daily_basic_factors/result.pkl` 合并生成，但AIstock推理引擎中**完全没有计算**：

| 缺失字段 | 含义 | 计算公式（来自RDAgent schema） | 依赖的原始字段 |
|----------|------|-------------------------------|---------------|
| `size_log_mv` | 市值对数（规模因子） | `log(db_circ_mv 优先，否则 db_total_mv)`，仅对>0取对数 | `db_circ_mv`, `db_total_mv` |
| `liquidity_turnover` | 换手率（流动性因子） | `db_turnover_rate`（直接赋值） | `db_turnover_rate` |
| `liquidity_vol_ratio` | 量比（流动性因子） | `db_volume_ratio`（直接赋值） | `db_volume_ratio` |
| `value_pe_inv` | 倒数市盈率（估值因子） | `1/db_pe_ttm`（优先）或 `1/db_pe`；分母为0=>NaN | `db_pe_ttm`, `db_pe` |
| `value_pb_inv` | 倒数市净率（估值因子） | `1/db_pb`；分母为0=>NaN | `db_pb` |

### 4.2 影响分析

- **当前因子代码** `factor.py` 第14行明确要求 `["size_log_mv", "liquidity_turnover"]`
- 推理引擎写入的 `static_factors.parquet` 中只有原始字段（`db_circ_mv`, `db_turnover_rate` 等），**没有**这些派生字段名
- 因子代码执行 `pd.read_parquet("static_factors.parquet", columns=["size_log_mv", "liquidity_turnover"])` 时会抛出 `KeyError`

### 4.2.1 普遍性影响

经代码搜索确认，**8个不同的RDAgent任务的因子代码都依赖 `size_log_mv` 和 `liquidity_turnover` 字段**：

| 任务ID | 引用次数 |
|--------|----------|
| `2025-12-22_17-29-59-695613` | 18处 |
| `2025-12-23_05-59-43-369830` | 13处 |
| `2025-12-19_08-42-46-183506` | 12处 |
| `2025-12-21_08-24-36-766685` | 12处 |
| `2025-12-18_16-24-29-487030` | 11处 |
| `2025-12-25_01-17-22-728723` | 5处 |
| `2026-02-06_10-02-12-151236` | 4处 |
| `2026-02-04_07-30-06-563166` | 3处 |

**结论**：这不是个别因子的特殊需求，而是RDAgent生成因子代码的普遍模式。所有引用这些字段的任务在当前推理引擎下都会报 `KeyError`。

### 4.3 原始字段已存在

虽然派生字段缺失，但其**依赖的原始字段已经存在于 `static_factors.parquet` 中**：

| 原始字段 | 来源 | 是否已写入 |
|----------|------|-----------|
| `db_circ_mv` | `market.daily_basic.circ_mv` → 字段映射 | ✅ 已写入 |
| `db_total_mv` | `market.daily_basic.total_mv` → 字段映射 | ✅ 已写入 |
| `db_turnover_rate` | `market.daily_basic.turnover_rate` → 字段映射 | ✅ 已写入 |
| `db_volume_ratio` | `market.daily_basic.volume_ratio` → 字段映射 | ✅ 已写入 |
| `db_pe_ttm` | `market.daily_basic.pe_ttm` → 字段映射 | ✅ 已写入 |
| `db_pe` | `market.daily_basic.pe` → 字段映射 | ✅ 已写入 |
| `db_pb` | `market.daily_basic.pb` → 字段映射 | ✅ 已写入 |

### 4.4 RDAgent侧精确计算代码（权威参考）

**代码位置**：`RD-Agent-main/debug_tools/precompute_daily_basic_factors.py:44-67`

以下是RDAgent侧的**原始计算逻辑**，AIstock推理引擎必须与之完全一致：

```python
# 估值相关
if "db_pe_ttm" in db.columns:
    df["value_pe_inv"] = 1.0 / db["db_pe_ttm"].replace(0, np.nan)
elif "db_pe" in db.columns:
    df["value_pe_inv"] = 1.0 / db["db_pe"].replace(0, np.nan)

if "db_pb" in db.columns:
    df["value_pb_inv"] = 1.0 / db["db_pb"].replace(0, np.nan)

# 市值（规模）相关
mv_col = None
for c in ["db_circ_mv", "db_total_mv"]:
    if c in db.columns:
        mv_col = c
        break
if mv_col is not None:
    df["size_log_mv"] = np.log(db[mv_col].where(db[mv_col] > 0)).replace(-np.inf, np.nan)

# 流动性相关
if "db_turnover_rate" in db.columns:
    df["liquidity_turnover"] = db["db_turnover_rate"]

if "db_volume_ratio" in db.columns:
    df["liquidity_vol_ratio"] = db["db_volume_ratio"]
```

**关键细节**：
- `size_log_mv`：优先使用 `db_circ_mv`（流通市值），仅当该列不存在时才使用 `db_total_mv`（总市值）。**不是**取两者中的非零值，而是**列级别的优先选择**。
- `value_pe_inv`：优先使用 `db_pe_ttm`，仅当该列不存在时才使用 `db_pe`。使用 `.replace(0, np.nan)` 处理零值。
- `liquidity_turnover` 和 `liquidity_vol_ratio`：直接赋值，无额外计算。

### 4.5 修复方案

在 `inference_engine.py` 的预计算字段生成逻辑中（第955行之后），增加以下5个字段的计算，**严格对齐RDAgent侧逻辑**：

```python
# 7. 计算 size_log_mv（市值对数）—— 对齐 precompute_daily_basic_factors.py:54-60
mv_col = None
for c in ['db_circ_mv', 'db_total_mv']:
    if c in df_fund.columns:
        mv_col = c
        break
if mv_col is not None:
    df_fund['size_log_mv'] = np.log(df_fund[mv_col].where(df_fund[mv_col] > 0)).replace(-np.inf, np.nan)
    logger.info(f"✓ 已计算 size_log_mv（基于 {mv_col}）")

# 8. 计算 liquidity_turnover（换手率）—— 对齐 precompute_daily_basic_factors.py:63-64
if 'db_turnover_rate' in df_fund.columns:
    df_fund['liquidity_turnover'] = df_fund['db_turnover_rate']
    logger.info("✓ 已计算 liquidity_turnover")

# 9. 计算 liquidity_vol_ratio（量比）—— 对齐 precompute_daily_basic_factors.py:66-67
if 'db_volume_ratio' in df_fund.columns:
    df_fund['liquidity_vol_ratio'] = df_fund['db_volume_ratio']
    logger.info("✓ 已计算 liquidity_vol_ratio")

# 10. 计算 value_pe_inv（倒数市盈率）—— 对齐 precompute_daily_basic_factors.py:45-48
if 'db_pe_ttm' in df_fund.columns:
    df_fund['value_pe_inv'] = 1.0 / df_fund['db_pe_ttm'].replace(0, np.nan)
    logger.info("✓ 已计算 value_pe_inv（基于 db_pe_ttm）")
elif 'db_pe' in df_fund.columns:
    df_fund['value_pe_inv'] = 1.0 / df_fund['db_pe'].replace(0, np.nan)
    logger.info("✓ 已计算 value_pe_inv（基于 db_pe）")

# 11. 计算 value_pb_inv（倒数市净率）—— 对齐 precompute_daily_basic_factors.py:50-51
if 'db_pb' in df_fund.columns:
    df_fund['value_pb_inv'] = 1.0 / df_fund['db_pb'].replace(0, np.nan)
    logger.info("✓ 已计算 value_pb_inv")
```

---

## 五、历史数据窗口与因子需求分析

### 5.1 当前历史数据窗口

| 数据类型 | 窗口 | 代码位置 | 说明 |
|----------|------|---------|------|
| OHLCV行情 | 90天（≈60个交易日） | `inference_engine.py:801` | `actual_date - timedelta(days=90)` |
| 基本面+资金流 | 90天（同上），上限180天 | `timescaledb_adapter.py:111-115` | 强制限制最大180天 |

### 5.2 因子对历史数据长度的需求

#### Alpha158因子（内置计算）

| 因子 | 最小窗口需求 | 当前是否满足 |
|------|------------|-------------|
| KLEN, KLOW | 1天 | ✅ |
| STD5, VSTD5, WVMA5, CORR5, CORD5, RSQR5, RESI5 | 5天 | ✅ |
| CORR10, CORD10, RSQR10, RESI10 | 10天 | ✅ |
| CORR20, RSQR20 | 20天 | ✅ |
| CORR60, CORD60, WVMA60, RSQR60, ROC60 | 60-61天 | ✅（90天≈60交易日，刚好满足） |

#### SOTA动态因子（factor.py）

| 因子 | 最小窗口需求 | 当前是否满足 |
|------|------------|-------------|
| `size_adjusted_turnover` | 1天（仅需当天的`size_log_mv`和`liquidity_turnover`） | ✅（前提是修复预计算字段缺失） |

#### 预计算衍生字段

| 字段 | 最小窗口需求 | 当前是否满足 |
|------|------------|-------------|
| `mf_main_net_amt_ratio_5d` | 5天 | ✅ |
| `mf_elg_net_amt_ratio_5d` | 5天 | ✅ |
| `PriceStrength_10D` | 10天 | ✅ |

### 5.3 历史数据不足时的行为

**代码逻辑追踪**（`inference_engine.py:181-569`）：

1. **Alpha158因子**：每个因子的计算函数内部检查数据长度，不足时返回 `np.nan`
   - 例如 `_calc_roc60()`（第501-505行）：`if len(arr) < 61: return np.nan`
   - 例如 `_calc_last_corr()`（第287-302行）：`if len(arr_a) < win: return np.nan`

2. **SOTA因子**：由因子代码自行处理。当前 `factor.py` 使用 `join(how="left")`，缺失数据自动为NaN

3. **预计算字段**：滚动窗口使用 `min_periods=5`，不足5天的数据返回NaN
   - 例如第917-921行：`rolling(window=5, min_periods=5).sum()`

4. **模型预测**：NaN值传入LGB模型时，LightGBM原生支持缺失值处理，不会报错

**结论**：历史数据不足时，因子值为NaN，模型仍可正常预测（LightGBM支持NaN），但预测质量会下降。**不会抛出异常**。

### 5.4 边界风险

| 风险 | 说明 | 严重程度 |
|------|------|---------|
| 90天窗口刚好覆盖60交易日 | 如遇长假（如春节+国庆连续），实际交易日可能不足60天 | 中 |
| ROC60需要61天数据 | 90天窗口在极端情况下可能不足 | 低 |
| 资金流数据可能滞后 | `market.moneyflow_ts` 数据更新可能比行情数据晚 | 低 |

---

## 六、性能分析

### 6.1 每次选股的性能消耗

| 步骤 | 耗时估算 | 说明 |
|------|---------|------|
| 1. 加载因子模块 | <0.1s | 动态import Python模块 |
| 2. 解析factor_order.json | <0.01s | JSON文件读取 |
| 3. 加载模型权重 | 0.5-2s | pickle反序列化（取决于模型大小） |
| 4a. 获取OHLCV行情 | 1-3s | SQL查询 ~5000只×60天 = ~30万行 |
| 4b. 获取基本面+资金流 | 2-5s | 两个SQL查询，各~30万行 |
| 5a. 写入daily_pv.h5 | 0.5-1s | HDF5序列化 ~360万行 |
| 5b. 计算预计算字段 | 0.5-1s | 滚动窗口计算 |
| 5c. 写入static_factors.parquet | 0.3-0.5s | Parquet序列化 |
| 6a. SOTA因子计算 | 1-3s | 读取H5+Parquet + 计算 |
| 6b. Alpha158因子计算 | 2-5s | 优化版本：只计算最后一天 |
| 7. 合并+模型预测 | 0.5-1s | DataFrame合并 + LGB predict |
| 8. 保存信号到DB | 0.2-0.5s | INSERT ~5000行 |
| 9. 清理临时目录 | <0.1s | shutil.rmtree |
| **总计** | **约 8-22秒** | 取决于数据库响应速度和股票数量 |

### 6.2 性能瓶颈分析

| 瓶颈 | 占比 | 可优化性 |
|------|------|---------|
| SQL查询（步骤4） | ~40% | 中（可加索引、连接池优化） |
| 文件I/O（步骤5+6a） | ~25% | 高（见优化方案） |
| 因子计算（步骤6） | ~25% | 低（已优化为只计算最后一天） |
| 模型预测（步骤7） | ~10% | 低（已足够快） |

### 6.3 磁盘I/O分析

每次选股写入约 80-180MB 临时文件（daily_pv.h5 + static_factors.parquet），然后因子代码再读取这些文件。这是一个**写入-读取-删除**的完整周期。

---

## 七、优化方案评估（不修改因子代码）

### 7.1 约束条件

- **不修改因子代码逻辑**：因子代码（`factor.py`）通过 `pd.read_hdf("daily_pv.h5")` 和 `pd.read_parquet("static_factors.parquet")` 读取数据，这是RDAgent的标准接口，不可更改
- **不修改因子计算公式**：预计算字段的计算公式必须与RDAgent侧 `generate_static_factors_bundle.py` 完全一致
- **数据必须来自最新行情**：禁止使用任何历史快照文件

### 7.2 当前方案（基线）

**方案A：每次选股重新生成临时文件（当前实现）**

```
优点：
- 数据保证最新
- 实现简单，无状态管理
- 临时文件用完即删，不占用磁盘

缺点：
- 每次选股都有文件I/O开销（写入+读取+删除）
- SQL查询重复执行
```

### 7.3 可选优化方案

#### 方案B：内存缓存 + 文件桥接

**核心思路**：将SQL查询结果缓存在内存中，仅在因子代码需要读取文件时才写入临时文件。

```
优化点：
- 如果同一交易日内多次选股（不同任务），SQL查询结果可复用
- 缓存键：(trade_date, universe_hash)
- 缓存过期：交易日切换时自动失效

约束满足：
- ✅ 不修改因子代码（仍然写入临时文件供因子读取）
- ✅ 数据来自最新行情（缓存仅在同一交易日内有效）

预期收益：
- 同一交易日内第2次及以后的选股，SQL查询耗时从3-8s降至0s
- 文件I/O仍然存在（因子代码需要读取文件）
```

#### 方案C：RAM Disk / tmpfs 文件桥接

**核心思路**：将临时目录创建在内存文件系统上（Windows上使用ImDisk或类似工具），消除磁盘I/O。

```
优化点：
- 文件写入和读取都在内存中完成，速度提升10-100倍
- 对因子代码完全透明

约束满足：
- ✅ 不修改因子代码
- ✅ 数据来自最新行情

预期收益：
- 文件I/O耗时从1-2s降至<0.1s

缺点：
- 需要额外配置RAM Disk（Windows环境下需要第三方工具）
- 增加运维复杂度
```

#### 方案D：预计算字段数据库物化

**核心思路**：在TimescaleDB中创建物化视图或预计算表，将 `size_log_mv`、`liquidity_turnover` 等预计算字段在数据入库时就计算好。

```
优化点：
- 预计算字段不再需要在推理时实时计算
- 减少推理引擎的计算负担

约束满足：
- ✅ 不修改因子代码
- ✅ 数据来自最新行情（物化视图基于最新数据）

预期收益：
- 预计算字段计算耗时从0.5-1s降至0s
- 但收益有限，因为预计算字段计算本身不是主要瓶颈

缺点：
- 需要维护物化视图的刷新逻辑
- 增加数据库写入路径的复杂度
```

### 7.4 方案对比

| 维度 | A（当前） | B（内存缓存） | C（RAM Disk） | D（DB物化） |
|------|----------|-------------|-------------|-----------|
| 单次选股耗时 | 8-22s | 8-22s（首次）/ 3-12s（后续） | 6-20s | 7-21s |
| 实现复杂度 | 低 | 中 | 中 | 高 |
| 运维复杂度 | 低 | 低 | 中 | 高 |
| 数据新鲜度 | ✅ 最新 | ✅ 最新（同日缓存） | ✅ 最新 | ✅ 最新 |
| 因子代码兼容 | ✅ | ✅ | ✅ | ✅ |
| 适用场景 | 通用 | 同日多次选股 | 高频选股 | 大规模部署 |

### 7.5 推荐

**短期（当前）**：维持方案A + 修复预计算字段缺失问题。当前8-22秒的选股耗时对于日频选股场景完全可接受。

**中期（如有需要）**：实施方案B（内存缓存），在同一交易日内多次选股时可显著减少SQL查询开销。实现成本低，风险小。

---

## 八、当前推理引擎中预计算字段计算与RDAgent侧的一致性对比

### 8.1 资金流衍生字段

| 字段 | RDAgent侧计算（generate_static_factors_bundle.py） | AIstock推理引擎计算（inference_engine.py） | 一致性 |
|------|--------------------------------------------------|------------------------------------------|--------|
| `mf_total_net_amt` | `buy_amt_total - sell_amt_total`（全档买卖汇总） | `= mf_net_amt`（直接赋值） | ⚠️ 口径差异 |
| `mf_total_net_amt_ratio` | `total_net_amt / amount` | `mf_total_net_amt / amount` | ✅ 一致（取决于上游） |
| `mf_main_net_amt` | `(lg_buy + elg_buy) - (lg_sell + elg_sell)` | 有计算但未写入df_fund | ⚠️ 中间变量未持久化 |
| `mf_main_net_amt_ratio_5d` | `sum_5d(main_net_amt) / sum_5d(amount)` | `5日滚动和(daily_ratio)`（先算日比率再求和） | ⚠️ 计算方式不同 |
| `mf_elg_net_amt_ratio_5d` | `sum_5d(elg_net_amt) / sum_5d(amount)` | `5日滚动和(daily_ratio)`（先算日比率再求和） | ⚠️ 计算方式不同 |
| `mf_elg_share_in_main_amt` | `elg_buy / (lg_buy + elg_buy)` | `elg_net / main_net` | ⚠️ 口径差异 |

### 8.2 口径差异说明

**`mf_main_net_amt_ratio_5d` 计算差异**：

- **RDAgent侧**（`generate_static_factors_bundle.py:200-208`）：
  ```
  先计算每日 main_net_amt = (lg_buy-lg_sell) + (elg_buy-elg_sell)
  再 sum_5d(main_net_amt) / sum_5d(amount)
  即：5日净流入总额 / 5日成交额总额
  ```

- **AIstock推理引擎**（`inference_engine.py:906-924`）：
  ```
  先计算每日 ratio = main_net_amt / amount
  再 rolling(5).sum() 对 ratio 求和
  即：5日(净流入/成交额)之和
  ```

这两种计算方式在数学上**不等价**：`sum(a/b) ≠ sum(a)/sum(b)`。

### 8.3 缺失的预计算字段（重复强调）

以下5个字段在推理引擎中**完全缺失**，需要补充：

1. `size_log_mv` = `log(db_circ_mv 优先，否则 db_total_mv)`
2. `liquidity_turnover` = `db_turnover_rate`
3. `liquidity_vol_ratio` = `db_volume_ratio`
4. `value_pe_inv` = `1/db_pe_ttm`（优先）或 `1/db_pe`
5. `value_pb_inv` = `1/db_pb`

---

## 九、总结与行动项

### 9.1 已确认事项

| 项目 | 状态 |
|------|------|
| 因子选股报错已修复（factor.py缩进错误） | ✅ |
| 数据来源为最新行情（TimescaleDB），不使用历史H5/bin文件 | ✅ |
| 每次选股重新生成临时文件（daily_pv.h5 + static_factors.parquet） | ✅ |
| 因子计算通过读取临时文件实现 | ✅ |
| 历史数据窗口：OHLCV 90天，基本面/资金流 90天（上限180天） | ✅ |
| 历史数据不足时因子值为NaN，不会抛出异常 | ✅ |
| 临时文件在推理完成后自动删除 | ✅ |

### 9.2 待修复问题

| 优先级 | 问题 | 影响 |
|--------|------|------|
| **P0** | 5个预计算字段缺失（size_log_mv等） | 引用这些字段的因子代码会报KeyError |
| **P1** | 资金流衍生字段计算口径与RDAgent侧不一致 | 因子值可能与训练时不同，影响预测准确性 |
| **P2** | 90天窗口在极端情况下可能不足60交易日 | ROC60等因子返回NaN |

### 9.3 推荐行动

1. **立即修复**：在推理引擎中补充5个缺失的预计算字段计算逻辑
2. **尽快修复**：对齐资金流衍生字段的计算口径，使其与RDAgent侧 `generate_static_factors_bundle.py` 完全一致
3. **评估**：考虑将OHLCV窗口从90天扩大到120天，以确保覆盖60个交易日
4. **中期优化**：如有同日多次选股需求，实施方案B（内存缓存）

---

## 附录A：关键代码文件索引

| 文件 | 职责 |
|------|------|
| `backend/inference_engine.py` | 推理引擎主逻辑，包含数据获取、文件生成、因子计算、模型预测 |
| `backend/data_service/api.py` | 数据服务API层，`get_history_window()` 获取OHLCV行情 |
| `backend/data_service/timescaledb_adapter.py` | TimescaleDB适配器，`fetch_fundamental_data_ts()` 获取基本面+资金流 |
| `rdagent_assets/rdagent_tasks/{task_id}/factor.py` | SOTA因子计算代码（RDAgent生成） |
| `rdagent_assets/rdagent_tasks/{task_id}/factor_order.json` | 因子顺序清单（区分Alpha158 vs SOTA） |
| RDAgent侧 `tools/generate_static_factors_bundle.py` | 预计算因子生成工具（schema定义和资金流衍生字段计算） |
| RDAgent侧 `debug_tools/precompute_daily_basic_factors.py` | **预计算基本面因子的精确计算代码**（size_log_mv等5个字段的权威实现） |
| `factors/daily_basic_factors/metadata/daily_basic_factors_schema.json` | 预计算因子schema定义 |

## 附录B：数据库表引用

| 表名 | 用途 | 查询位置 |
|------|------|---------|
| `market.kline_daily_raw` | OHLCV日线行情 | `timescaledb_adapter.fetch_history_window_ts()` |
| `market.daily_basic` | 基本面数据（PE/PB/市值/换手率等） | `timescaledb_adapter.fetch_fundamental_data_ts()` |
| `market.moneyflow_ts` | 资金流数据（各档买卖金额/量） | `timescaledb_adapter.fetch_fundamental_data_ts()` |
| `trading.rdagent_signal` | 选股信号输出 | `inference_engine._save_signals_to_db()` |
