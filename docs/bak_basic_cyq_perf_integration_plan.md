# bak_basic / cyq_perf 数据集接入 RD-Agent 实施文档

## 一、关键发现总结

### 1.1 static_factors.parquet 生成位置确认

**生成代码位置**: `backend/inference_engine.py` 第 1036 行

```python
@f:\Dev\AIstock\backend\inference_engine.py:1036
df_fund.to_parquet("static_factors.parquet")
```

**数据来源**: `backend/data_service/timescaledb_adapter.py` 中的 `fetch_fundamental_data_ts()` 函数

### 1.2 当前数据流向

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        选股/推理流程 (inference_engine.py)                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. 获取行情数据 (get_history_window)                                         │
│     └─→ df_history (daily_pv)                                               │
│                                                                              │
│  2. 获取基本面数据 (fetch_fundamental_data_ts)                                │
│     └─→ timescaledb_adapter.py 查询 PostgreSQL                              │
│         ├─→ market.daily_basic 表                                            │
│         └─→ market.moneyflow_ts 表                                           │
│     └─→ df_fund                                                              │
│         ├─→ 字段重命名 (mf_* / db_* 前缀)                                     │
│         └─→ 计算衍生字段 (mf_main_net_amt_ratio_5d 等)                         │
│                                                                              │
│  3. 生成 static_factors.parquet                                               │
│     └─→ df_fund.to_parquet("static_factors.parquet")                         │
│                                                                              │
│  4. 因子计算                                                                  │
│     └─→ 因子脚本读取 static_factors.parquet                                  │
│         └─→ pd.read_parquet("static_factors.parquet")                        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.3 数据集对比

| 数据集 | 数据库表 | 当前状态 | 字段前缀 |
|--------|----------|----------|----------|
| daily_basic | `market.daily_basic` | ✅ 已接入 | `db_*` |
| moneyflow | `market.moneyflow_ts` | ✅ 已接入 | `mf_*` |
| bak_basic | `market.bak_basic` | ⚠️ **待接入** | `bb_*` |
| cyq_perf | `market.cyq_perf` | ⚠️ **待接入** | `cp_*` |

---

## 二、需要修改的文件

### 2.1 文件清单

| 序号 | 文件路径 | 修改内容 | 优先级 |
|------|----------|----------|--------|
| 1 | `backend/data_service/timescaledb_adapter.py` | 添加 bak_basic 和 cyq_perf 数据查询 | P0 |
| 2 | `backend/inference_engine.py` | 添加新数据集的字段映射和衍生计算 | P0 |
| 3 | `rdagent/scenarios/qlib/experiment/prompts.yaml` | 更新 Prompt，添加 bb_* 和 cp_* 字段说明 | P1 |

---

## 三、详细修改步骤

### 步骤 1: 修改 timescaledb_adapter.py

**文件**: `f:/Dev/AIstock/backend/data_service/timescaledb_adapter.py`

**修改位置**: `fetch_fundamental_data_ts()` 函数（第 98-175 行）

**当前代码**:
```python
def fetch_fundamental_data_ts(
    universe: List[str],
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    # 1. 获取 daily_basic
    basic_sql = """
        SELECT trade_date as datetime, ts_code as instrument, ...
        FROM market.daily_basic
        ...
    """
    
    # 2. 获取 money_flow
    flow_sql = """
        SELECT trade_date as datetime, ts_code as instrument, ...
        FROM market.moneyflow_ts
        ...
    """
```

**需要添加的代码**（在第 136 行之后，合并之前）:

```python
    # 3. 获取 bak_basic（历史股票数据）
    bak_sql = """
        SELECT 
            b.trade_date as datetime, 
            b.ts_code as instrument,
            b.pe_dyn,
            b.total_assets,
            b.liquid_assets,
            b.fixed_assets,
            b.reserved,
            b.reserved_pershare,
            b.eps,
            b.bvps,
            b.undp,
            b.per_undp,
            b.rev_yoy,
            b.profit_yoy,
            b.gpr,
            b.npr,
            b.holder_num
        FROM market.bak_basic b
        INNER JOIN market.stock_basic s ON b.ts_code = s.ts_code
        WHERE b.ts_code = ANY(%s) 
          AND b.trade_date >= %s 
          AND b.trade_date <= %s
          AND s.list_status = 'L'
    """
    
    # 4. 获取 cyq_perf（筹码胜率数据）
    cyq_sql = """
        SELECT 
            c.trade_date as datetime, 
            c.ts_code as instrument,
            c.his_low,
            c.his_high,
            c.cost_5pct,
            c.cost_15pct,
            c.cost_50pct,
            c.cost_85pct,
            c.cost_95pct,
            c.weight_avg,
            c.winner_rate
        FROM market.cyq_perf c
        INNER JOIN market.stock_basic s ON c.ts_code = s.ts_code
        WHERE c.ts_code = ANY(%s) 
          AND c.trade_date >= %s 
          AND c.trade_date <= %s
          AND s.list_status = 'L'
    """
```

**修改合并逻辑**（第 160-171 行）:

```python
        # 合并并设置索引
        dfs_to_merge = []
        
        if not df_basic.empty:
            df_basic["datetime"] = pd.to_datetime(df_basic["datetime"])
            df_basic.set_index(["datetime", "instrument"], inplace=True)
            dfs_to_merge.append(df_basic)
            
        if not df_flow.empty:
            df_flow["datetime"] = pd.to_datetime(df_flow["datetime"])
            df_flow.set_index(["datetime", "instrument"], inplace=True)
            dfs_to_merge.append(df_flow)
        
        # 新增：加载 bak_basic
        df_bak = pd.read_sql(bak_sql, conn, params=(universe, start_date, end_date))
        if not df_bak.empty:
            df_bak["datetime"] = pd.to_datetime(df_bak["datetime"])
            df_bak.set_index(["datetime", "instrument"], inplace=True)
            dfs_to_merge.append(df_bak)
            
        # 新增：加载 cyq_perf
        df_cyq = pd.read_sql(cyq_sql, conn, params=(universe, start_date, end_date))
        if not df_cyq.empty:
            df_cyq["datetime"] = pd.to_datetime(df_cyq["datetime"])
            df_cyq.set_index(["datetime", "instrument"], inplace=True)
            dfs_to_merge.append(df_cyq)
        
        if not dfs_to_merge:
            return pd.DataFrame()
            
        # 合并所有数据
        result = dfs_to_merge[0]
        for df in dfs_to_merge[1:]:
            result = result.join(df, how='outer')
        
        return result.sort_index()
```

---

### 步骤 2: 修改 inference_engine.py

**文件**: `f:/Dev/AIstock/backend/inference_engine.py`

**修改位置**: `run_inference()` 方法中的字段映射部分（约第 848-882 行）

**当前字段映射**:
```python
field_mapping = {
    # 资金流字段
    'buy_lg_amount': 'mf_lg_buy_amt',
    ...
    # 基本面字段
    'turnover_rate': 'db_turnover_rate',
    ...
}
```

**需要添加的字段映射**:

```python
field_mapping = {
    # 资金流字段（已有）
    'buy_lg_amount': 'mf_lg_buy_amt',
    'sell_lg_amount': 'mf_lg_sell_amt',
    'buy_elg_amount': 'mf_elg_buy_amt',
    'sell_elg_amount': 'mf_elg_sell_amt',
    'buy_lg_vol': 'mf_lg_buy_vol',
    'sell_lg_vol': 'mf_lg_sell_vol',
    'buy_elg_vol': 'mf_elg_buy_vol',
    'sell_elg_vol': 'mf_elg_sell_vol',
    'buy_sm_amount': 'mf_sm_buy_amt',
    'sell_sm_amount': 'mf_sm_sell_amt',
    'buy_md_amount': 'mf_md_buy_amt',
    'sell_md_amount': 'mf_md_sell_amt',
    'net_mf_amount': 'mf_net_amt',
    'net_mf_vol': 'mf_net_vol',
    
    # 基本面字段（已有）
    'turnover_rate': 'db_turnover_rate',
    'turnover_rate_f': 'db_turnover_rate_f',
    'volume_ratio': 'db_volume_ratio',
    'pe': 'db_pe',
    'pe_ttm': 'db_pe_ttm',
    'pb': 'db_pb',
    'ps': 'db_ps',
    'ps_ttm': 'db_ps_ttm',
    'dv_ratio': 'db_dv_ratio',
    'dv_ttm': 'db_dv_ttm',
    'total_share': 'db_total_share',
    'float_share': 'db_float_share',
    'free_share': 'db_free_share',
    'total_mv': 'db_total_mv',
    'circ_mv': 'db_circ_mv',
    
    # ───────────────────────────────────────────────
    # 新增：bak_basic 字段（bb_* 前缀）
    # ───────────────────────────────────────────────
    'pe_dyn': 'bb_pe_dyn',
    'total_assets': 'bb_total_assets',
    'liquid_assets': 'bb_liquid_assets',
    'fixed_assets': 'bb_fixed_assets',
    'reserved': 'bb_reserved',
    'reserved_pershare': 'bb_reserved_pershare',
    'eps': 'bb_eps',
    'bvps': 'bb_bvps',
    'undp': 'bb_undp',
    'per_undp': 'bb_per_undp',
    'rev_yoy': 'bb_rev_yoy',
    'profit_yoy': 'bb_profit_yoy',
    'gpr': 'bb_gpr',
    'npr': 'bb_npr',
    'holder_num': 'bb_holder_num',
    
    # ───────────────────────────────────────────────
    # 新增：cyq_perf 字段（cp_* 前缀）
    # ───────────────────────────────────────────────
    'his_low': 'cp_his_low',
    'his_high': 'cp_his_high',
    'cost_5pct': 'cp_cost_5pct',
    'cost_15pct': 'cp_cost_15pct',
    'cost_50pct': 'cp_cost_50pct',
    'cost_85pct': 'cp_cost_85pct',
    'cost_95pct': 'cp_cost_95pct',
    'weight_avg': 'cp_weight_avg',
    'winner_rate': 'cp_winner_rate',
}
```

**可选：添加 cyq_perf 衍生字段计算**（在 PriceStrength_10D 计算之后，约第 955 行）:

```python
# 7. 计算筹码分布相关衍生字段（基于 cyq_perf）
if 'cp_cost_95pct' in df_temp.columns and 'cp_cost_5pct' in df_temp.columns:
    # 筹码分散度：95%分位成本 - 5%分位成本
    df_fund['cp_cost_spread'] = df_temp['cp_cost_95pct'] - df_temp['cp_cost_5pct']
    logger.info("✓ 已计算 cp_cost_spread（筹码分散度）")

if 'cp_winner_rate' in df_temp.columns:
    # 筹码胜率5日变化
    winner_change_5d = df_temp['cp_winner_rate'].groupby(level='instrument').diff(5)
    df_fund['cp_winner_rate_change_5d'] = winner_change_5d
    logger.info("✓ 已计算 cp_winner_rate_change_5d")

if 'cp_weight_avg' in df_temp.columns and 'close' in df_temp.columns:
    # 当前价格与平均成本比率
    df_fund['cp_price_to_cost_ratio'] = df_temp['close'] / df_temp['cp_weight_avg']
    logger.info("✓ 已计算 cp_price_to_cost_ratio")
```

---

### 步骤 3: 更新 RD-Agent Prompts

**文件**: `f:/Dev/RD-Agent-main/app_tpl/all/v4/rdagent/scenarios/qlib/experiment/prompts.yaml`

**修改位置**: `qlib_factor_interface` 部分的数据加载说明

**需要添加的内容**:

```yaml
  【数据加载与字段名规范（必须严格遵守）】
  - 因子实现只能从当前工作目录下的 `daily_pv.h5` 读取数据，该文件索引为 MultiIndex(`datetime`, `instrument`)，列名采用 Qlib 风格:
    `$open`, `$high`, `$low`, `$close`, `$volume`, `$amount`, `$factor`.
    
  - **可选静态字段（daily_basic / 资金流 / 历史股票 / 筹码胜率）**：当前工作目录下存在 `static_factors.parquet`，其索引同样为 MultiIndex(`datetime`, `instrument`)，包含以下字段：

    **1. daily_basic 字段**（以 `db_` 为前缀）：
    - db_circ_mv: 流通市值
    - db_turnover_rate: 换手率
    - db_pe_ttm: 市盈率TTM
    - db_pb: 市净率
    - db_dv_ttm: 股息率TTM
    - ...（其他原有字段）

    **2. moneyflow 字段**（以 `mf_` 为前缀）：
    - mf_lg_buy_amt: 大单买入金额
    - mf_elg_buy_amt: 特大单买入金额
    - mf_net_amt: 净流入金额
    - mf_main_net_amt_ratio_5d: 主力净流入5日累计（预计算字段）
    - mf_elg_net_amt_ratio_5d: 特大单净流入5日累计（预计算字段）
    - ...（其他原有字段）

    **3. bak_basic 字段（新增）**（以 `bb_` 为前缀）：
    - bb_pe_dyn: 动态市盈率
    - bb_total_assets: 总资产
    - bb_liquid_assets: 流动资产
    - bb_fixed_assets: 固定资产
    - bb_eps: 每股收益
    - bb_bvps: 每股净资产
    - bb_holder_num: 股东人数
    - bb_rev_yoy: 收入同比增长率
    - bb_profit_yoy: 利润同比增长率
    - bb_gpr: 毛利率
    - bb_npr: 净利率

    **4. cyq_perf 字段（新增）**（以 `cp_` 为前缀）：
    - cp_winner_rate: 筹码胜率（获利盘比例）
    - cp_weight_avg: 加权平均成本
    - cp_cost_5pct: 5%分位成本
    - cp_cost_15pct: 15%分位成本
    - cp_cost_50pct: 50%分位成本（中位成本）
    - cp_cost_85pct: 85%分位成本
    - cp_cost_95pct: 95%分位成本
    - cp_his_low: 历史低位
    - cp_his_high: 历史高位
    - cp_cost_spread: 筹码分散度（95%分位 - 5%分位，预计算字段）
    - cp_winner_rate_change_5d: 筹码胜率5日变化（预计算字段）
    - cp_price_to_cost_ratio: 当前价格与平均成本比率（预计算字段）

  - **列名白名单（硬约束）**：你只能使用上述列出的字段名，严禁凭经验"编造/猜测"字段名。
  
  - 使用示例：
    ```python
    # 按需读取所需列，避免 OOM
    required_cols = ["db_circ_mv", "mf_net_amt", "bb_pe_dyn", "cp_winner_rate"]
    static_df = pd.read_parquet("static_factors.parquet", columns=required_cols).sort_index()
    df = df.join(static_df, how="left")
    ```
```

**可选：在 factor_hypothesis_specification 中添加策略建议**:

```yaml
- **新数据集探索策略（bak_basic / cyq_perf）**：
  
  **bak_basic（历史股票数据）适用场景**：
  - 价值投资因子：使用 bb_pe_dyn（动态PE）、bb_pb（市净率）筛选低估值股票
  - 成长因子：使用 bb_eps（每股收益）、bb_rev_yoy（收入同比增长）、bb_profit_yoy（利润同比增长）构建成长性指标
  - 股东集中度因子：bb_holder_num（股东人数）的变化趋势，人数减少可能意味着筹码集中
  - 财务健康因子：bb_total_assets、bb_liquid_assets、bb_fixed_assets 构建资产结构指标
  
  **cyq_perf（筹码胜率数据）适用场景**：
  - 筹码集中度因子：使用 cp_cost_spread（成本分散度）衡量筹码集中程度，分散度越小筹码越集中
  - 获利盘比例因子：cp_winner_rate（筹码胜率）表示当前价格上方获利盘比例，可作为情绪指标
  - 成本支撑/阻力因子：cp_cost_5pct、cp_cost_50pct、cp_cost_95pct 形成成本分布区间
  - 价格与成本偏离因子：cp_price_to_cost_ratio 衡量当前价格相对于市场平均成本的偏离程度
  - 筹码变化动量：cp_winner_rate_change_5d 表示短期筹码获利比例的变化趋势
  
  **建议实验方向**：
  - 低动态PE + 高筹码胜率组合：基本面低估且市场情绪积极
  - 股东人数下降 + 筹码集中：机构建仓迹象
  - 价格低于加权平均成本 + 筹码集中：潜在反弹机会
```

---

## 四、数据验证步骤

### 4.1 验证数据库表存在且数据正常

```python
# debug_tools/verify_new_datasets.py
from backend.db.pg_pool import get_conn
import pandas as pd

def verify_bak_basic():
    """验证 bak_basic 表"""
    with get_conn() as conn:
        # 检查数据量
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM market.bak_basic")
        row = cur.fetchone()
        print(f"bak_basic: 总行数={row[0]}, 日期范围={row[1]} ~ {row[2]}")
        
        # 检查样例数据
        df = pd.read_sql("""
            SELECT * FROM market.bak_basic 
            WHERE trade_date = (SELECT MAX(trade_date) FROM market.bak_basic)
            LIMIT 5
        """, conn)
        print(f"\nbak_basic 样例数据:\n{df}")

def verify_cyq_perf():
    """验证 cyq_perf 表"""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM market.cyq_perf")
        row = cur.fetchone()
        print(f"\ncyq_perf: 总行数={row[0]}, 日期范围={row[1]} ~ {row[2]}")
        
        df = pd.read_sql("""
            SELECT * FROM market.cyq_perf 
            WHERE trade_date = (SELECT MAX(trade_date) FROM market.cyq_perf)
            LIMIT 5
        """, conn)
        print(f"\ncyq_perf 样例数据:\n{df}")

if __name__ == "__main__":
    verify_bak_basic()
    verify_cyq_perf()
```

### 4.2 验证修改后的数据合并

```python
# debug_tools/test_static_factors_merge.py
from datetime import date, timedelta
from backend.data_service.timescaledb_adapter import fetch_fundamental_data_ts

# 获取测试数据
end_date = date.today()
start_date = end_date - timedelta(days=30)
universe = ["600000.SH", "000001.SZ"]  # 测试股票

df = fetch_fundamental_data_ts(universe, start_date, end_date)

# 验证字段
print("所有字段:")
for col in sorted(df.columns):
    prefix = col.split('_')[0] if '_' in col else 'other'
    print(f"  - {col} ({prefix})")

# 按前缀统计
db_cols = [c for c in df.columns if c.startswith('db_')]
mf_cols = [c for c in df.columns if c.startswith('mf_')]
bb_cols = [c for c in df.columns if c.startswith('bb_')]
cp_cols = [c for c in df.columns if c.startswith('cp_')]

print(f"\n字段统计:")
print(f"  db_* (daily_basic): {len(db_cols)} 个")
print(f"  mf_* (moneyflow): {len(mf_cols)} 个")
print(f"  bb_* (bak_basic): {len(bb_cols)} 个")
print(f"  cp_* (cyq_perf): {len(cp_cols)} 个")
```

### 4.3 验证选股流程

```python
# debug_tools/test_inference_with_new_data.py
from backend.inference_engine import inference_engine
from datetime import datetime

# 执行选股
result = inference_engine.run_inference(
    strategy_id="test_strategy",
    version_tag="test",
    trade_date=datetime.now(),
    top_k=50
)

print(f"选股结果: {len(result)} 只股票")
print(result.head())
```

---

## 五、实施顺序建议

| 阶段 | 任务 | 预计时间 | 验证方式 |
|------|------|----------|----------|
| 1 | 修改 `timescaledb_adapter.py` 添加 SQL 查询 | 30分钟 | 运行 verify_new_datasets.py |
| 2 | 修改 `timescaledb_adapter.py` 合并逻辑 | 30分钟 | 运行 test_static_factors_merge.py |
| 3 | 修改 `inference_engine.py` 添加字段映射 | 20分钟 | 检查字段映射是否正确 |
| 4 | 可选：添加衍生字段计算 | 20分钟 | 验证衍生字段存在 |
| 5 | 更新 Prompts | 30分钟 | 检查 YAML 语法 |
| 6 | 端到端测试 | 1小时 | 运行完整选股流程 |

---

## 六、风险提示

### 6.1 数据覆盖范围
- **bak_basic** 和 **cyq_perf** 的数据起始日期可能与 `daily_basic`/`moneyflow` 不同
- 在合并时可能存在某些日期缺少新数据的情况，需要使用 `how='outer'` 确保不丢失主数据

### 6.2 性能影响
- 新增两个表的查询会增加数据库查询时间
- 建议监控 `fetch_fundamental_data_ts` 的执行时间，确保不会显著影响选股延迟

### 6.3 字段命名冲突
- 确保 `field_mapping` 中的字段名不会与现有字段冲突
- 特别是 `pe` 和 `pe_dyn` 是不同的字段，需要分别映射到 `db_pe` 和 `bb_pe_dyn`

---

## 七、文档历史

| 版本 | 日期 | 作者 | 变更内容 |
|------|------|------|----------|
| v1.0 | 2026-02-10 | Cascade | 初始版本，基于代码分析 |
