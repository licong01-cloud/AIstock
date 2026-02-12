# bak_basic / cyq_perf 数据集接入 RD-Agent 实施文档（完整版）

## 一、关键发现总结

### 1.1 static_factors.parquet 生成位置确认

**AIstock侧选股/推理时生成位置**: `backend/inference_engine.py` 第 1036 行
- 代码：`df_fund.to_parquet("static_factors.parquet")`
- 数据来源：`timescaledb_adapter.fetch_fundamental_data_ts()` 从 PostgreSQL 实时查询
- **结论**：这是选股时临时生成的，不是RD-Agent训练用的数据源

**RD-Agent侧训练时生成位置**: `tools/generate_static_factors_bundle.py`
- 输入：Snapshot目录中的 H5 文件
- 输出：`git_ignore_folder/factor_implementation_source_data/static_factors.parquet`
- **结论**：这是RD-Agent训练时使用的数据源，需要修改此脚本

### 1.2 当前数据流向（RD-Agent训练流程）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    RD-Agent 训练数据准备流程                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. AIstock Snapshot 导出（已支持）                                          │
│     ├─→ daily_basic.h5 (db_*)                                               │
│     ├─→ moneyflow.h5 (mf_*)                                                 │
│     ├─→ bak_basic.h5 (bb_*)  ⚠️ 已导出但未合并                                │
│     ├─→ cyq_perf.h5 (cp_*)   ⚠️ 已导出但未合并                                │
│     └─→ daily_pv.h5                                                         │
│                                                                              │
│  2. RD-Agent 合并生成 static_factors.parquet                                  │
│     └─→ tools/generate_static_factors_bundle.py                              │
│         ├─→ 读取 daily_basic.h5  ✅ 已支持                                    │
│         ├─→ 读取 moneyflow.h5   ✅ 已支持                                    │
│         ├─→ 读取 bak_basic.h5   ❌ 待添加                                    │
│         ├─→ 读取 cyq_perf.h5    ❌ 待添加                                    │
│         └─→ 合并生成 static_factors.parquet                                  │
│                                                                              │
│  3. RD-Agent 因子演进使用                                                     │
│     └─→ 因子脚本读取 static_factors.parquet                                   │
│         ├─→ db_* 字段（daily_basic）                                         │
│         ├─→ mf_* 字段（moneyflow）                                           │
│         ├─→ bb_* 字段（bak_basic）⚠️ 待支持                                    │
│         └─→ cp_* 字段（cyq_perf） ⚠️ 待支持                                    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.3 数据集导出状态（AIstock侧）

| 数据集 | H5文件 | 导出类 | 当前状态 |
|--------|--------|--------|----------|
| daily_basic | `daily_basic.h5` | `QlibDailyBasicExporter` | ✅ 已导出且已合并 |
| moneyflow | `moneyflow.h5` | `QlibMoneyflowExporter` | ✅ 已导出且已合并 |
| bak_basic | `bak_basic.h5` | `QlibBakBasicExporter` | ✅ 已导出 ⚠️ 未合并 |
| cyq_perf | `cyq_perf.h5` | `QlibCyqPerfExporter` | ✅ 已导出 ⚠️ 未合并 |

### 1.4 验证结果

```
combined_static_factors.parquet 检查：
- 总列数: 66
- bb_* 字段: []  (缺失 bak_basic)
- cp_* 字段: []  (缺失 cyq_perf)
```

**结论**：需要修改RD-Agent侧的合并脚本，将 bak_basic 和 cyq_perf 纳入 static_factors.parquet。

---

## 二、需要修改的文件清单

### 2.1 RD-Agent侧（F:/Dev/RD-Agent-main/）

| 序号 | 文件路径 | 修改内容 | 优先级 |
|------|----------|----------|--------|
| 1 | `tools/generate_static_factors_bundle.py` | 添加 bak_basic.h5 和 cyq_perf.h5 的读取与合并 | P0 |
| 2 | `tools/generate_static_factors_bundle.py` | 更新 _build_schema 添加 bb_* 和 cp_* 字段含义 | P1 |
| 3 | `rdagent/scenarios/qlib/experiment/prompts.yaml` | 更新 Prompt，添加 bb_* 和 cp_* 字段说明 | P1 |

### 2.2 AIstock侧（选股推理，可选增强）

| 序号 | 文件路径 | 修改内容 | 优先级 |
|------|----------|----------|--------|
| 4 | `backend/data_service/timescaledb_adapter.py` | 可选：添加 bak_basic/cyq_perf 查询支持选股使用 | P2 |

---

## 三、详细修改步骤

### 步骤 1: 修改 generate_static_factors_bundle.py（关键）

**文件**: `f:/Dev/RD-Agent-main/tools/generate_static_factors_bundle.py`

#### 1.1 添加读取 bak_basic 和 cyq_perf H5 文件的逻辑

**位置**: 第 701-724 行（读取 raw tables 部分）

**当前代码**:
```python
    daily_basic_path = snapshot_root / "daily_basic.h5"
    moneyflow_path = snapshot_root / "moneyflow.h5"
    daily_pv_path = snapshot_root / "daily_pv.h5"

    print("[INFO] snapshot_root:", snapshot_root)
    print("[INFO] factors_root :", aistock_factors_root)

    # Raw tables
    print("[INFO] Loading raw daily_basic.h5 ...")
    df_db_raw = _read_table(daily_basic_path, "daily_basic_raw")

    print("[INFO] Loading raw moneyflow.h5 ...")
    df_mf_raw = _read_table(moneyflow_path, "moneyflow_raw")

    df_pv = None
    if daily_pv_path.exists():
        print("[INFO] Loading raw daily_pv.h5 ...")
        df_pv = _read_table(daily_pv_path, "daily_pv")
    else:
        print(f"[WARN] daily_pv.h5 not found: {daily_pv_path} (mf_*_ratio derived features will be skipped)")
```

**需要添加的代码**（在 daily_pv_path 逻辑之后）:

```python
    # 新增：读取 bak_basic 和 cyq_perf
    bak_basic_path = snapshot_root / "bak_basic.h5"
    cyq_perf_path = snapshot_root / "cyq_perf.h5"
    
    df_bb_raw = None
    if bak_basic_path.exists():
        print("[INFO] Loading raw bak_basic.h5 ...")
        df_bb_raw = _read_table(bak_basic_path, "bak_basic_raw")
    else:
        print(f"[WARN] bak_basic.h5 not found: {bak_basic_path}")
    
    df_cp_raw = None
    if cyq_perf_path.exists():
        print("[INFO] Loading raw cyq_perf.h5 ...")
        df_cp_raw = _read_table(cyq_perf_path, "cyq_perf_raw")
    else:
        print(f"[WARN] cyq_perf.h5 not found: {cyq_perf_path}")
```

#### 1.2 将新数据添加到合并列表

**位置**: 第 726-730 行（添加 raw tables 到 dfs 列表）

**当前代码**:
```python
    # Optional precomputed factor tables
    dfs: list[pd.DataFrame] = []

    # Keep raw fields, but to avoid name collisions we keep their existing prefixes.
    dfs.append(df_db_raw)
    dfs.append(df_mf_raw)
```

**修改后的代码**:
```python
    # Optional precomputed factor tables
    dfs: list[pd.DataFrame] = []

    # Keep raw fields, but to avoid name collisions we keep their existing prefixes.
    dfs.append(df_db_raw)
    dfs.append(df_mf_raw)
    
    # 新增：添加 bak_basic 和 cyq_perf raw 数据
    if df_bb_raw is not None:
        dfs.append(df_bb_raw)
    if df_cp_raw is not None:
        dfs.append(df_cp_raw)
```

#### 1.3 更新 schema 构建函数添加新字段含义

**位置**: 第 222-275 行（_build_schema 函数中的 meaning_map）

**需要添加的字段含义**（在 meaning_map 字典中添加）:

```python
    meaning_map = {
        # daily_basic common (原有)
        "db_pe_ttm": "市盈率TTM",
        ...
        
        # moneyflow common (原有)
        "mf_net_amt": "资金净流入金额...",
        ...
        
        # ───────────────────────────────────────────────
        # 新增：bak_basic 字段（bb_* 前缀）
        # ───────────────────────────────────────────────
        "bb_pe_dyn": "动态市盈率",
        "bb_total_assets": "总资产",
        "bb_liquid_assets": "流动资产",
        "bb_fixed_assets": "固定资产",
        "bb_reserved": "公积金",
        "bb_reserved_pershare": "每股公积金",
        "bb_eps": "每股收益",
        "bb_bvps": "每股净资产",
        "bb_undp": "未分配利润",
        "bb_per_undp": "每股未分配利润",
        "bb_rev_yoy": "收入同比增长率",
        "bb_profit_yoy": "利润同比增长率",
        "bb_gpr": "毛利率",
        "bb_npr": "净利率",
        "bb_holder_num": "股东人数",
        
        # ───────────────────────────────────────────────
        # 新增：cyq_perf 字段（cp_* 前缀）
        # ───────────────────────────────────────────────
        "cp_his_low": "历史低位",
        "cp_his_high": "历史高位",
        "cp_cost_5pct": "5%分位成本",
        "cp_cost_15pct": "15%分位成本",
        "cp_cost_50pct": "50%分位成本（中位成本）",
        "cp_cost_85pct": "85%分位成本",
        "cp_cost_95pct": "95%分位成本",
        "cp_weight_avg": "加权平均成本",
        "cp_winner_rate": "筹码胜率（获利盘比例）",
    }
```

#### 1.4 更新 schema source 类型识别

**位置**: 第 277-288 行（_build_schema 函数中的 source 判断）

**当前代码**:
```python
    schema: list[dict[str, Any]] = []
    for col in df.columns:
        col_str = str(col)
        if col_str.startswith("db_"):
            source = "daily_basic_raw"
        elif col_str.startswith("mf_"):
            source = "moneyflow_raw_or_factor"
        elif col_str.startswith("ae_"):
            source = "ae_factor"
        else:
            source = "precomputed_or_other"
```

**修改后的代码**:
```python
    schema: list[dict[str, Any]] = []
    for col in df.columns:
        col_str = str(col)
        if col_str.startswith("db_"):
            source = "daily_basic_raw"
        elif col_str.startswith("mf_"):
            source = "moneyflow_raw_or_factor"
        elif col_str.startswith("bb_"):
            source = "bak_basic_raw"  # 新增
        elif col_str.startswith("cp_"):
            source = "cyq_perf_raw"   # 新增
        elif col_str.startswith("ae_"):
            source = "ae_factor"
        else:
            source = "precomputed_or_other"
```

#### 1.5 更新 _schema_cols_from_parquet_metadata 函数

**位置**: 第 391-424 行

**当前代码**:
```python
        for field in arrow_schema:
            name = str(field.name)
            dtype = str(field.type)
            if name.startswith("db_"):
                source = "daily_basic_raw"
            elif name.startswith("mf_"):
                source = "moneyflow_raw_or_factor"
            elif name.startswith("ae_"):
                source = "ae_factor"
            else:
                source = "precomputed_or_other"
```

**修改后的代码**:
```python
        for field in arrow_schema:
            name = str(field.name)
            dtype = str(field.type)
            if name.startswith("db_"):
                source = "daily_basic_raw"
            elif name.startswith("mf_"):
                source = "moneyflow_raw_or_factor"
            elif name.startswith("bb_"):
                source = "bak_basic_raw"  # 新增
            elif name.startswith("cp_"):
                source = "cyq_perf_raw"   # 新增
            elif name.startswith("ae_"):
                source = "ae_factor"
            else:
                source = "precomputed_or_other"
```

---

### 步骤 2: 更新 RD-Agent Prompts

**文件**: `f:/Dev/RD-Agent-main/app_tpl/all/v4/rdagent/scenarios/qlib/experiment/prompts.yaml`

在 `qlib_factor_interface` 的数据加载部分添加新字段说明：

```yaml
- **可选静态字段**：当前工作目录下存在 `static_factors.parquet`，包含以下字段：

  **1. daily_basic 字段**（以 `db_` 为前缀）：
  - db_circ_mv: 流通市值
  - db_turnover_rate: 换手率
  - db_pe_ttm: 市盈率TTM
  - db_pb: 市净率
  - ...（其他原有字段）

  **2. moneyflow 字段**（以 `mf_` 为前缀）：
  - mf_lg_buy_amt: 大单买入金额
  - mf_elg_buy_amt: 特大单买入金额
  - mf_net_amt: 净流入金额
  - mf_main_net_amt_ratio_5d: 主力净流入5日累计
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
```

---

## 四、实施验证步骤

### 4.1 重新生成 static_factors.parquet

```bash
# 在 RD-Agent 目录下运行
python tools/generate_static_factors_bundle.py \
  --snapshot-root /mnt/f/Dev/AIstock/qlib_snapshots/<snapshot_id> \
  --aistock-factors-root /mnt/f/Dev/AIstock/factors
```

### 4.2 验证新字段已合并

```python
import pandas as pd
df = pd.read_parquet("git_ignore_folder/factor_implementation_source_data/static_factors.parquet")

# 检查列数
print(f"总列数: {len(df.columns)}")

# 检查 bak_basic 字段
bb_cols = [c for c in df.columns if c.startswith('bb_')]
print(f"bb_* 字段 ({len(bb_cols)} 个): {bb_cols}")

# 检查 cyq_perf 字段
cp_cols = [c for c in df.columns if c.startswith('cp_')]
print(f"cp_* 字段 ({len(cp_cols)} 个): {cp_cols}")

# 检查 schema
import json
with open("git_ignore_folder/factor_implementation_source_data/static_factors_schema.json") as f:
    schema = json.load(f)
    
bb_schema = [c for c in schema['columns'] if c['name'].startswith('bb_')]
cp_schema = [c for c in schema['columns'] if c['name'].startswith('cp_')]
print(f"\nSchema 中 bb_* 字段数量: {len(bb_schema)}")
print(f"Schema 中 cp_* 字段数量: {len(cp_schema)}")
```

### 4.3 验证因子脚本可以读取新字段

```python
# 在因子脚本中测试
import pandas as pd

# 读取 static_factors.parquet 并选择新字段
required_cols = ["bb_pe_dyn", "cp_winner_rate", "db_circ_mv", "mf_net_amt"]
static_df = pd.read_parquet("static_factors.parquet", columns=required_cols)

print("bb_pe_dyn 统计:", static_df["bb_pe_dyn"].describe())
print("cp_winner_rate 统计:", static_df["cp_winner_rate"].describe())
```

---

## 五、风险提示

### 5.1 数据覆盖范围
- **bak_basic** 和 **cyq_perf** 的数据起始日期可能与 `daily_basic`/`moneyflow` 不同
- 合并时使用 `how='outer'` 确保不丢失主数据

### 5.2 字段命名冲突
- 确保 `bak_basic` 和 `cyq_perf` 的字段名使用不同的前缀（`bb_` 和 `cp_`）
- 与现有 `db_*` 和 `mf_*` 字段不冲突

### 5.3 内存占用
- 新增两个数据集会增加 static_factors.parquet 的文件大小
- 原文件约 3GB（66列），预计增加后约 4-5GB
- 确保系统有足够内存进行合并操作

---

## 六、文档历史

| 版本 | 日期 | 作者 | 变更内容 |
|------|------|------|----------|
| v1.0 | 2026-02-10 | Cascade | 初始版本，确认 RD-Agent 侧脚本位置 |
