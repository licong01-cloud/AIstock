# QuantEvolver (QE) 分析报告与功能设计文档

> 版本: v1.0  
> 日期: 2026-02-15  
> 范围: CUDA错误根因分析、因子失败分析、架构审查、实验数据展示/同步设计、实验选股设计

---

## 目录

1. [CUDA错误根因分析](#1-cuda错误根因分析)
2. [mf_lg_net_ratio因子失败分析](#2-mf_lg_net_ratio因子失败分析)
3. [QE整体架构审查报告](#3-qe整体架构审查报告)
4. [QE实验数据展示与同步功能设计](#4-qe实验数据展示与同步功能设计)
5. [QE实验选股功能设计](#5-qe实验选股功能设计)
6. [实施优先级与里程碑](#6-实施优先级与里程碑)

---

## 1. CUDA错误根因分析

### 1.1 错误现象

```
RuntimeError: CUDA error: invalid configuration argument
```

QE实验 `qe_exp_478c5025` 在模型训练阶段失败，使用的模型为RDAgent SOTA模型 `Transformer_TimeSeries_Model`。

### 1.2 根因：模型包装层差异

| 维度 | RDAgent原始执行 | QE当前执行 |
|------|----------------|-----------|
| **模型包装类** | `GeneralPTNN`（QLib内置） | QE自生成的 `Transformer_TimeSeries_ModelModel`（自定义QLib Model） |
| **配置文件** | `conf_sota_factors_model.yaml` | QE生成的 `conf.yaml` |
| **模型加载方式** | `pt_model_uri: "model.model_cls"` 动态加载 | 直接在 `custom_model.py` 中嵌入NN类和训练逻辑 |
| **GPU数据管理** | `GeneralPTNN.fit()` 使用 `DataLoader` 分批加载到GPU | QE包装类一次性将全部验证集送入GPU |
| **推理批处理** | `GeneralPTNN.predict()` 使用 `DataLoader` 分批推理 | QE包装类一次性推理全部数据 |
| **Flash Attention** | 无特殊处理（依赖PyTorch默认行为） | 需要显式禁用 `enable_nested_tensor` |

#### 核心差异详解

**RDAgent的 `GeneralPTNN`**（`qlib/contrib/model/pytorch_general_nn.py`）：
- `fit()` 方法中使用 `DataLoader(batch_size=self.batch_size)` 分批训练
- `test_epoch()` 方法中使用 `DataLoader` 分批验证
- `predict()` 方法中使用 `DataLoader` 分批推理
- 所有数据通过 `DataLoader` 按批次送入GPU，避免一次性占满显存

**QE的 `_write_custom_model`**（`config_composer.py`）：
- 自行实现了完整的 `fit()` / `predict()` 方法
- 原始实现中验证和预测阶段将全部数据一次性送入GPU
- RTX 2060 6GB显存不足以容纳完整的验证集（约1000万行 × 特征数）

### 1.3 Flash Attention影响评估

| 项目 | 说明 |
|------|------|
| **禁用影响** | Flash Attention是PyTorch Transformer的优化路径，禁用后使用标准attention计算，精度不变，速度略慢 |
| **RTX 2060兼容性** | RTX 2060（Turing架构，SM75）不支持原生Flash Attention v2（需要SM80+），PyTorch的`enable_nested_tensor`在该架构上可能触发不兼容的CUDA kernel |
| **结论** | 禁用Flash Attention对RTX 2060是**正确且必要的**，不影响模型训练效果 |

### 1.4 已实施修复

在 `config_composer.py` 的 `_write_custom_model` 模板中：
1. 添加 `_patch_transformer_compat()` 禁用Flash Attention
2. 添加 `_batched_inference()` 分批推理
3. 训练数据保留CPU按批次送GPU
4. 验证和预测改为分批推理

### 1.5 建议的根本性改进

**当前修复是补丁式的**。根本性改进方案是让QE也使用 `GeneralPTNN` 作为模型包装类，与RDAgent保持完全一致：

```yaml
# QE生成的conf.yaml应改为：
task:
  model:
    class: GeneralPTNN
    module_path: qlib.contrib.model.pytorch_general_nn
    kwargs:
      pt_model_uri: "custom_model.model_cls"
      pt_model_kwargs:
        input_dim: <num_features>
        # ... 其他模型参数
      n_epochs: 30
      lr: 1e-4
      batch_size: 256
      early_stop: 5
      optimizer: adam
      GPU: 0
```

这样 `custom_model.py` 只需包含纯PyTorch NN类（`model_cls`），无需自行实现QLib Model接口。

---

## 2. mf_lg_net_ratio因子失败分析

### 2.1 错误现象

因子 `mf_lg_net_ratio` 计算失败，报错缺少 `mf_lg_buy_amt` 字段。

### 2.2 数据源验证结果

通过实际检查数据文件内容：

| 数据文件 | 列数 | 包含的列 | 是否包含 `mf_lg_buy_amt` |
|----------|------|---------|------------------------|
| `daily_pv.h5` | **7** | `open, high, low, close, volume, amount, factor` | **否** |
| `static_factors.parquet` | **90** | `db_*`, `mf_*`, `bb_*`, `cp_*` 等 | **是** |

### 2.3 根因：因子代码未join静态数据

对比因子代码中的数据访问模式：

| 因子名称 | 是否join static_df | 访问的字段 | 字段来源 | 状态 |
|----------|-------------------|-----------|---------|------|
| `bb_pe_dyn_inv` | ✅ 有join | `bb_pe_dyn` | static_factors | ✅ 正常 |
| `chip_concentration_width` | ✅ 有join | `cp_cost_85pct`, `cp_cost_15pct` | static_factors | ✅ 正常 |
| `cost_deviation` | ✅ 有join | `close`, `cp_weight_avg` | daily_pv + static | ✅ 正常 |
| `net_inflow_intensity` | ✅ 有join | `mf_net_amt`, `db_circ_mv` | static_factors | ✅ 正常 |
| **`mf_lg_net_ratio`** | **❌ 无join** | `mf_lg_buy_amt`, `mf_lg_sell_amt` | **static_factors** | **❌ 失败** |
| `volume_price_divergence_5d` | 无需join | `close`, `volume` | daily_pv | ✅ 正常 |

**根本原因**：`mf_lg_net_ratio` 因子代码中直接从 `df` 读取 `mf_lg_buy_amt`，但 `df` 来自 `daily_pv.h5`（仅7列基础行情），`mf_lg_buy_amt` 实际存在于 `static_factors.parquet` 中。因子代码缺少 `if static_df is not None: df = df.join(static_df, how='left')` 这一步。

### 2.4 问题溯源

这个问题的根源在于**RDAgent生成因子代码时的数据访问模式不一致**：

1. **RDAgent原始执行环境**：因子代码在 `factor.py` 中执行时，`execute()` 方法会自动将 `daily_pv.h5` 和 `static_factors.parquet` 合并后传入。RDAgent的因子接口约定（见 `prompts_data_loading.yaml`）要求因子代码**显式join** `static_factors.parquet`。

2. **QE执行环境**：`prepare_factors.py` 脚本分别读取 `daily_pv.h5` 和 `static_factors.parquet`，然后调用 `calculate_xxx(df, static_df=None)`。如果因子代码没有显式join，就会缺少静态字段。

3. **代码来源**：`mf_lg_net_ratio` 的代码是从RDAgent SOTA因子同步过来的。在RDAgent侧，该因子可能是在较早版本的接口约定下生成的，当时数据加载方式不同。

### 2.5 修复方案

#### 方案A：修复QE侧 `prepare_factors.py` 生成逻辑（推荐）

在 `config_composer.py` 的 `_compose_prepare_factors` 方法中，修改因子调用方式：

```python
# 当前：
result = calculate_xxx(df, static_df=static_df)

# 改为：先合并再调用，确保df包含所有字段
merged_df = df.join(static_df, how='left') if static_df is not None else df
result = calculate_xxx(merged_df, static_df=static_df)
```

#### 方案B：修复因子代码（治标）

在同步因子代码时，自动检测并补充 `static_df` join逻辑。

#### 推荐方案

**方案A**，因为它从数据准备层面解决问题，无论因子代码是否显式join，都能获取到完整数据。同时保持 `static_df` 参数传递，允许因子代码按需选择性使用。

### 2.6 RDAgent侧因子执行机制对比

| 环节 | RDAgent因子流程 | QE因子流程 |
|------|---------------|-----------|
| **数据准备** | `process_factor_data()` → 调用每个因子实验的 `execute()` | `prepare_factors.py` → 读取h5/parquet后调用 `calculate_xxx()` |
| **因子执行** | `implementation.execute(data_type)` → 内部读取数据并计算 | `calculate_xxx(df, static_df)` → 外部传入数据 |
| **结果合并** | `pd.concat(factor_dfs, axis=1)` → `combined_factors_df.parquet` | 逐个计算后 `pd.concat` → `combined_factors_df.parquet` |
| **数据加载器** | `NestedDataLoader`（`Alpha158DL` + `StaticDataLoader`） | `CombinedAlpha158DynamicFactorsLoader` |

---

## 3. QE整体架构审查报告

### 3.1 设计目标回顾

> 通过RDAgent的task同步功能，把RDAgent的SOTA因子和模型同步到AIstock侧，要求因子代码、模型代码和策略代码都与RDAgent侧保持一致，之后通过QE模块在AIstock侧重新组合后，在RDAgent的WSL conda环境中使用QLib重新进行训练和测试。

### 3.2 一致性审查结果

#### 3.2.1 因子代码一致性

| 检查项 | 状态 | 说明 |
|--------|------|------|
| 因子源代码 | ✅ 一致 | 从 `aistock_factor_catalog` 读取，与RDAgent同步的代码一致 |
| 因子计算接口 | ⚠️ 差异 | RDAgent用 `execute()` 内部加载数据；QE用 `calculate_xxx(df, static_df)` 外部传入 |
| 因子数据源 | ⚠️ 差异 | 部分因子缺少 `static_df` join（如 `mf_lg_net_ratio`） |
| 预计算因子输出 | ✅ 一致 | 都生成 `combined_factors_df.parquet`，格式兼容 |

#### 3.2.2 模型代码一致性

| 检查项 | 状态 | 说明 |
|--------|------|------|
| NN模型源代码 | ✅ 一致 | 从 `aistock_model_catalog.code_text` 读取，与RDAgent同步的代码一致 |
| QLib Model包装 | ❌ **不一致** | RDAgent用 `GeneralPTNN`；QE自行生成包装类 |
| 训练超参数 | ✅ 一致 | 从 `model_training_hyperparameters` 读取 |
| GPU处理 | ❌ **不一致** | `GeneralPTNN` 内置分批处理；QE包装类需要手动补丁 |

#### 3.2.3 数据集配置一致性

| 检查项 | 状态 | 说明 |
|--------|------|------|
| QLib provider_uri | ✅ 一致 | 都指向 `/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_20251209` |
| Alpha158特征 | ✅ 一致 | 20个Alpha158因子配置相同 |
| 数据加载器 | ⚠️ 差异 | RDAgent用 `NestedDataLoader`；QE用 `CombinedAlpha158DynamicFactorsLoader` |
| 时间区间 | ✅ 一致 | train/valid/test分段配置相同 |
| 标签定义 | ✅ 一致 | `Ref($close, -2)/Ref($close, -1) - 1` |

#### 3.2.4 策略代码一致性

| 检查项 | 状态 | 说明 |
|--------|------|------|
| 策略源代码 | ✅ 一致 | 从 `aistock_strategy_catalog.source_code` 读取 |
| 策略参数 | ✅ 一致 | 从 `default_kwargs` 和用户自定义参数合并 |

### 3.3 关键差异总结

```
严重程度: ❌ 高  ⚠️ 中  ✅ 低/无

❌ 模型包装层不一致 → 导致CUDA错误、训练行为差异
⚠️ 因子数据加载接口不一致 → 导致部分因子计算失败
⚠️ 数据加载器类型不一致 → 可能导致数据对齐差异
✅ 因子/模型/策略源代码一致
✅ 数据集时间区间和特征配置一致
```

### 3.4 改进建议

1. **模型包装层统一**（优先级：高）
   - QE应使用 `GeneralPTNN` 作为模型包装类
   - `custom_model.py` 只包含纯NN类（`model_cls`）
   - `conf.yaml` 中 `model.class` 改为 `GeneralPTNN`

2. **因子数据加载统一**（优先级：高）
   - `prepare_factors.py` 中先合并 `daily_pv.h5` 和 `static_factors.parquet` 再调用因子函数
   - 或改用 RDAgent 的 `NestedDataLoader` + `StaticDataLoader` 方案

3. **数据加载器统一**（优先级：中）
   - 考虑将 `CombinedAlpha158DynamicFactorsLoader` 替换为 RDAgent 的 `NestedDataLoader` 配置
   - 或验证两者在相同数据下的输出完全一致

---

## 4. QE实验数据展示与同步功能设计

### 4.1 功能概述

在QE实验历史中，**同步前**即可通过RDAgent API获取每次实验的回测指标数据，用户根据指标判断是否需要同步。同步的主要数据包括：
- **模型权重数据**（`model.pkl` / `params.pkl`）
- **特征值序列数据**（`factor_order.json`，含Alpha158基线因子 + SOTA动态因子的完整顺序）

### 4.2 数据流架构

```
┌─────────────────────────────────────────────────────────┐
│                    RDAgent (WSL)                         │
│                                                         │
│  Results API (port 9000)                                │
│  ├── GET /tasks/{task_id}/loops                         │
│  │   → 返回每个loop的回测指标(IC, 年化收益, 最大回撤等) │
│  ├── GET /api/extractors/sota_factors/v2/{task_id}      │
│  │   → 返回SOTA因子列表和代码                           │
│  ├── GET /tasks/{task_id}/sota_factor_anchor             │
│  │   → 返回SOTA因子锚点(含模型权重定位)                 │
│  └── GET /artifacts/bundle/{asset_bundle_id}            │
│      → 下载资产包(含模型权重)                            │
│                                                         │
│  QE Workspace (qe_workspace/)                           │
│  ├── qe_exp_xxx/                                        │
│  │   ├── mlruns/          ← QLib训练产物                 │
│  │   ├── qlib_results.json ← 回测结果                   │
│  │   └── combined_factors_df.parquet                     │
│  └── ...                                                │
└─────────────┬───────────────────────────────────────────┘
              │ HTTP API / 文件系统
              ▼
┌─────────────────────────────────────────────────────────┐
│                  AIstock Backend                         │
│                                                         │
│  QE Experiment Service (新增)                            │
│  ├── list_experiments()     → 实验列表(含回测指标)       │
│  ├── get_experiment_metrics() → 单个实验详细指标         │
│  ├── sync_experiment()      → 同步模型权重+特征序列     │
│  └── get_sync_status()      → 同步状态查询              │
│                                                         │
│  数据存储                                                │
│  ├── PG: aistock_qe_experiment (实验元数据+指标)         │
│  ├── PG: aistock_qe_experiment_sync (同步记录)           │
│  └── 文件: rdagent_assets/qe_experiments/{exp_id}/       │
│       ├── model.pkl / params.pkl                         │
│       └── factor_order.json                              │
└─────────────┬───────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────┐
│                  AIstock Frontend                        │
│                                                         │
│  /quantevolver/experiments (增强)                        │
│  ├── 实验列表表格                                        │
│  │   ├── 实验名称、因子组合、模型、策略                   │
│  │   ├── 回测指标列: IC, 年化收益, 最大回撤, IR          │
│  │   ├── 同步状态: 未同步/已同步/同步中                   │
│  │   └── 操作: 查看详情 / 同步 / 选股                    │
│  └── 实验详情Drawer                                      │
│      ├── 完整回测指标                                    │
│      ├── 因子列表和代码                                  │
│      ├── 模型信息和超参数                                │
│      └── 同步操作面板                                    │
└─────────────────────────────────────────────────────────┘
```

### 4.3 数据库设计

#### 4.3.1 实验元数据表 `aistock_qe_experiment`

```sql
CREATE TABLE IF NOT EXISTS aistock_qe_experiment (
    id              SERIAL PRIMARY KEY,
    experiment_id   VARCHAR(64) NOT NULL UNIQUE,  -- qe_exp_xxx
    experiment_name VARCHAR(256),
    
    -- 组合信息
    factor_names    JSONB,          -- ["bb_pe_dyn_inv", "mf_lg_net_ratio", ...]
    model_id        VARCHAR(128),
    model_name      VARCHAR(128),
    strategy_id     VARCHAR(128),
    strategy_name   VARCHAR(128),
    
    -- 回测指标（从qlib_results.json或RDAgent API获取）
    metrics         JSONB,          -- {IC, ICIR, annualized_return, max_drawdown, ...}
    metrics_source  VARCHAR(32),    -- 'local' | 'rdagent_api'
    
    -- 执行信息
    status          VARCHAR(32) DEFAULT 'created',  -- created/running/completed/failed
    workspace_path  TEXT,
    wsl_command     TEXT,
    
    -- 同步信息
    is_synced       BOOLEAN DEFAULT FALSE,
    synced_at_utc   TIMESTAMPTZ,
    sync_assets     JSONB,          -- {model_weight_path, factor_order_path, ...}
    
    -- 选股启用
    is_enabled_for_selection BOOLEAN DEFAULT FALSE,
    
    created_at_utc  TIMESTAMPTZ DEFAULT NOW(),
    updated_at_utc  TIMESTAMPTZ DEFAULT NOW()
);
```

#### 4.3.2 同步记录表 `aistock_qe_experiment_sync`

```sql
CREATE TABLE IF NOT EXISTS aistock_qe_experiment_sync (
    id              SERIAL PRIMARY KEY,
    experiment_id   VARCHAR(64) NOT NULL REFERENCES aistock_qe_experiment(experiment_id),
    sync_type       VARCHAR(32) NOT NULL,  -- 'model_weight' | 'factor_order' | 'full'
    sync_status     VARCHAR(32) NOT NULL,  -- 'pending' | 'in_progress' | 'success' | 'failed'
    
    -- 同步的资产详情
    source_path     TEXT,           -- 源文件路径
    target_path     TEXT,           -- 目标文件路径
    file_size_bytes BIGINT,
    checksum        VARCHAR(64),
    
    error_message   TEXT,
    operator        VARCHAR(64) DEFAULT 'system',
    
    created_at_utc  TIMESTAMPTZ DEFAULT NOW(),
    completed_at_utc TIMESTAMPTZ
);
```

### 4.4 后端API设计

#### 4.4.1 实验列表（含回测指标）

```
GET /api/v1/quantevolver/experiments
Query: limit, offset, status, sort_by, order

Response:
{
  "ok": true,
  "total": 15,
  "items": [
    {
      "experiment_id": "qe_exp_478c5025",
      "experiment_name": "SOTA因子+Transformer模型实验",
      "factor_names": ["bb_pe_dyn_inv", "mf_lg_net_ratio", ...],
      "model_name": "Transformer_TimeSeries_Model",
      "strategy_name": "EnhancedTopkDropoutStrategy",
      "status": "completed",
      "metrics": {
        "IC": 0.0423,
        "ICIR": 1.234,
        "annualized_return": 0.156,
        "max_drawdown": -0.089,
        "information_ratio": 1.45,
        "sharpe": 1.82
      },
      "metrics_source": "local",
      "is_synced": false,
      "is_enabled_for_selection": false,
      "created_at_utc": "2026-02-14T16:53:29Z"
    }
  ]
}
```

#### 4.4.2 获取实验回测指标（优先从本地，回退到RDAgent API）

```
GET /api/v1/quantevolver/experiments/{experiment_id}/metrics

Response:
{
  "ok": true,
  "experiment_id": "qe_exp_478c5025",
  "metrics": { ... },
  "metrics_source": "local",  // or "rdagent_api"
  "raw_result": { ... }       // 完整的qlib_results.json内容
}
```

指标获取优先级：
1. 本地 `qe_workspace/{exp_id}/qlib_results.json`
2. 本地 `qe_workspace/{exp_id}/mlruns/` 中的MLflow记录
3. RDAgent Results API: `GET /tasks/{task_id}/loops`

#### 4.4.3 同步实验资产

```
POST /api/v1/quantevolver/experiments/{experiment_id}/sync

Request:
{
  "sync_types": ["model_weight", "factor_order"],  // 可选同步项
  "force": false  // 是否强制重新同步
}

Response:
{
  "ok": true,
  "experiment_id": "qe_exp_478c5025",
  "synced_assets": {
    "model_weight": {
      "status": "success",
      "source": "qe_workspace/qe_exp_478c5025/mlruns/.../model.pkl",
      "target": "rdagent_assets/qe_experiments/qe_exp_478c5025/model.pkl",
      "size_bytes": 4521984
    },
    "factor_order": {
      "status": "success",
      "source": "generated_from_conf.yaml",
      "target": "rdagent_assets/qe_experiments/qe_exp_478c5025/factor_order.json",
      "factor_count": 31
    }
  }
}
```

### 4.5 同步资产详细说明

#### 4.5.1 模型权重同步

模型权重文件位于QE实验workspace的MLflow artifacts中：

```
qe_workspace/qe_exp_xxx/mlruns/
  └── 1/
      └── <run_id>/
          └── artifacts/
              ├── model.pkl      ← 主要权重文件
              └── params.pkl     ← 备选权重文件
```

同步流程：
1. 扫描 `mlruns/` 目录，找到最新的run
2. 从artifacts中提取 `model.pkl` 或 `params.pkl`
3. 复制到 `rdagent_assets/qe_experiments/{exp_id}/`
4. 记录文件大小和校验和

#### 4.5.2 特征值序列同步（factor_order.json）

`factor_order.json` 定义了模型训练时的完整特征输入顺序，是推理时必须严格匹配的关键文件。

生成逻辑：
1. 从 `conf.yaml` 的 `alpha158_config.feature` 提取Alpha158因子名称列表
2. 从 `combined_factors_df.parquet` 的列名提取SOTA动态因子列表
3. 合并为完整的 `factor_order`

```json
{
  "experiment_id": "qe_exp_478c5025",
  "alpha158_factors": ["RESI5", "WVMA5", "RSQR5", ...],
  "dynamic_factors": ["bb_pe_dyn_inv", "chip_concentration_width", ...],
  "factor_order": ["RESI5", "WVMA5", ..., "bb_pe_dyn_inv", "chip_concentration_width", ...],
  "total_features": 31,
  "generated_at": "2026-02-15T10:00:00Z"
}
```

### 4.6 前端设计

#### 4.6.1 实验列表页增强（`/quantevolver/experiments`）

在现有实验列表表格中增加以下列：

| 列名 | 数据源 | 说明 |
|------|--------|------|
| IC | `metrics.IC` | 信息系数 |
| 年化收益 | `metrics.annualized_return` | 百分比显示 |
| 最大回撤 | `metrics.max_drawdown` | 百分比显示，红色 |
| IR | `metrics.information_ratio` | 信息比率 |
| 同步状态 | `is_synced` | Badge: 未同步(灰)/已同步(绿) |
| 操作 | - | 按钮: 详情/同步/选股 |

#### 4.6.2 实验详情Drawer

点击实验行展开Drawer，包含：

- **回测指标卡片**：IC、ICIR、年化收益、最大回撤、IR、Sharpe等
- **因子列表**：因子名称、来源、IC值
- **模型信息**：模型名称、类型、超参数
- **策略信息**：策略名称、参数
- **同步面板**：
  - 模型权重同步状态和操作
  - 特征序列同步状态和操作
  - 一键全量同步按钮

---

## 5. QE实验选股功能设计

### 5.1 设计原则

1. **严格隔离**：QE选股与现有Task选股在架构上完全独立，互不影响
2. **复用底座**：复用现有的 `InferenceEngine`、`data_service` 层和行情服务
3. **独立模块**：QE选股有独立的Service、Router、数据库表
4. **功能等价**：QE选股的输出格式与Task选股一致，便于前端统一展示

### 5.2 架构隔离设计

```
┌─────────────────────────────────────────────────────────────┐
│                     AIstock Backend                          │
│                                                             │
│  ┌──────────────────────┐    ┌──────────────────────────┐   │
│  │  Task选股模块 (现有)  │    │  QE实验选股模块 (新增)    │   │
│  │                      │    │                          │   │
│  │  Router:             │    │  Router:                 │   │
│  │  /rdagent/tasks/     │    │  /quantevolver/          │   │
│  │    {id}/selection    │    │    experiments/           │   │
│  │                      │    │    {id}/selection        │   │
│  │  Service:            │    │  Service:                │   │
│  │  rdagent_selection_  │    │  qe_selection_           │   │
│  │  service.py          │    │  service.py (新增)       │   │
│  │                      │    │                          │   │
│  │  DB Table:           │    │  DB Table:               │   │
│  │  aistock_task_catalog│    │  aistock_qe_experiment   │   │
│  │  trading.rdagent_    │    │  trading.qe_signal       │   │
│  │  signal              │    │  (新增)                   │   │
│  └──────────┬───────────┘    └──────────┬───────────────┘   │
│             │                           │                   │
│             ▼                           ▼                   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              共享底座 (不修改)                         │   │
│  │                                                      │   │
│  │  InferenceEngine (inference_engine.py)                │   │
│  │  ├── _load_task_manifest()  ← Task选股用              │   │
│  │  ├── _load_qe_manifest()   ← QE选股用 (新增方法)     │   │
│  │  ├── run_inference()        ← 核心推理逻辑(共用)      │   │
│  │  └── _save_signals_to_db() ← 信号持久化(共用)        │   │
│  │                                                      │   │
│  │  DataService (data_service/api.py)                    │   │
│  │  ├── get_realtime_snapshot()                          │   │
│  │  ├── get_history_window()                             │   │
│  │  └── ...                                              │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 5.3 QE选股Service设计

新增文件：`backend/services/qe_selection_service.py`

```python
class QESelectionService:
    """QE实验选股服务 - 与Task选股完全隔离"""
    
    def __init__(self):
        self.assets_root = Path("rdagent_assets/qe_experiments")
    
    def build_experiment_selection(
        self,
        *,
        experiment_id: str,
        trade_date: Optional[str] = None,
        cutoff_date: Optional[str] = None,
        top_k: int = 50,
    ) -> Dict[str, Any]:
        """基于QE实验资产执行选股
        
        流程：
        1. 从aistock_qe_experiment表获取实验信息
        2. 验证同步资产完整性(model_weight + factor_order)
        3. 构建QE专用manifest
        4. 调用InferenceEngine.run_inference()
        5. 加载TopK信号并附加行情数据
        6. 返回选股结果
        """
        ...
```

### 5.4 数据库设计

#### 5.4.1 QE信号表 `trading.qe_signal`

```sql
CREATE TABLE IF NOT EXISTS trading.qe_signal (
    id                  SERIAL PRIMARY KEY,
    experiment_id       VARCHAR(64) NOT NULL,
    strategy_version_id VARCHAR(128),
    trade_date          DATE NOT NULL,
    symbol              VARCHAR(20) NOT NULL,
    score               FLOAT,
    rank                INTEGER,
    output_mode         VARCHAR(32) DEFAULT 'topk',
    
    created_at_utc      TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(experiment_id, trade_date, symbol)
);

CREATE INDEX idx_qe_signal_exp_date ON trading.qe_signal(experiment_id, trade_date);
```

**关键隔离点**：QE信号写入 `trading.qe_signal`，Task信号写入 `trading.rdagent_signal`，两张表完全独立。

### 5.5 InferenceEngine扩展

在现有 `InferenceEngine` 中新增QE专用的manifest加载方法，**不修改**现有Task推理逻辑：

```python
class InferenceEngine:
    # 现有方法保持不变
    def _load_task_manifest(self, task_id: str) -> Optional[Dict]:
        """Task选股用 - 不修改"""
        ...
    
    # 新增方法
    def _load_qe_manifest(self, experiment_id: str) -> Optional[Dict]:
        """QE选股用 - 从qe_experiments目录加载manifest"""
        qe_dir = self.assets_root.parent / "qe_experiments" / experiment_id
        manifest_path = qe_dir / "manifest.json"
        if not manifest_path.exists():
            return None
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    
    def run_qe_inference(
        self,
        *,
        experiment_id: str,
        trade_date: Optional[datetime] = None,
        cutoff_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """QE实验推理 - 独立入口，不影响Task推理"""
        manifest = self._load_qe_manifest(experiment_id)
        if not manifest:
            raise ValueError(f"QE实验manifest未找到: {experiment_id}")
        
        # 复用核心推理逻辑
        return self._run_inference_impl(
            strategy_id=f"qe_{experiment_id}",
            version_tag="qe_replay",
            trade_date=trade_date,
            task_run_id=experiment_id,
            loop_id=0,
            cutoff_date=cutoff_date,
        )
```

### 5.6 QE选股manifest格式

QE实验同步后生成的manifest文件：

```json
{
  "manifest_version": "qe_v1",
  "experiment_id": "qe_exp_478c5025",
  "source": "quantevolver",
  
  "primary_assets": {
    "factor_entry_relpath": "factor.py",
    "model_weight_relpath": "model.pkl",
    "factor_order_relpath": "factor_order.json"
  },
  
  "experiment_config": {
    "model_name": "Transformer_TimeSeries_Model",
    "model_type": "TimeSeries",
    "factor_names": ["bb_pe_dyn_inv", "mf_lg_net_ratio", ...],
    "alpha158_count": 20,
    "dynamic_factor_count": 11,
    "total_features": 31
  },
  
  "metrics": {
    "IC": 0.0423,
    "annualized_return": 0.156,
    "max_drawdown": -0.089
  },
  
  "synced_at_utc": "2026-02-15T10:00:00Z"
}
```

### 5.7 Router设计

新增路由文件或在现有QE路由中扩展：

```python
# /api/v1/quantevolver/experiments/{experiment_id}/selection
@router.post("/experiments/{experiment_id}/selection")
def trigger_qe_selection(experiment_id: str, req: QESelectionRequest):
    """QE实验选股 - 独立于Task选股"""
    ...

# /api/v1/quantevolver/experiments/{experiment_id}/selection/stream
@router.get("/experiments/{experiment_id}/selection/stream")
async def trigger_qe_selection_stream(experiment_id: str, ...):
    """QE实验选股SSE流 - 独立于Task选股"""
    ...
```

### 5.8 前端设计

#### 5.8.1 实验列表中的选股入口

在实验列表的操作列中，已同步的实验显示"选股"按钮：

```
[实验名称] [IC] [年化] [回撤] [同步状态] [操作: 详情 | 同步 | 选股]
                                                          ↑
                                                    仅已同步实验可点击
```

#### 5.8.2 QE选股结果页

复用现有Task选股结果页的UI组件，但路由独立：

```
/quantevolver/experiments/{experiment_id}/selection
```

展示内容：
- 选股日期、Top-K数量
- 股票列表：代码、名称、评分、排名、现价、涨跌幅
- 行情来源标识
- 推理元信息（使用的因子数、模型类型等）

### 5.9 隔离验证清单

| 验证项 | 方法 | 预期结果 |
|--------|------|---------|
| Task选股不受影响 | 执行现有Task选股流程 | 结果与QE功能上线前完全一致 |
| 数据库隔离 | 检查 `trading.rdagent_signal` | QE选股不写入该表 |
| 路由隔离 | 检查 `/rdagent/tasks/` 路由 | 无任何修改 |
| Service隔离 | 检查 `rdagent_selection_service.py` | 无任何修改 |
| InferenceEngine兼容 | 检查现有 `run_inference` 方法 | 签名和行为不变 |

---

## 6. 实施优先级与里程碑

### Phase 1: 基础修复（1-2天）

| 任务 | 优先级 | 说明 |
|------|--------|------|
| 修复因子数据加载 | P0 | `prepare_factors.py` 中先合并数据再调用因子函数 |
| 验证CUDA修复 | P0 | 重新运行 `qe_exp_478c5025` 验证修复效果 |

### Phase 2: 实验数据展示（3-5天）

| 任务 | 优先级 | 说明 |
|------|--------|------|
| 创建DB表 | P1 | `aistock_qe_experiment` + `aistock_qe_experiment_sync` |
| 实验指标获取API | P1 | 从本地/RDAgent API获取回测指标 |
| 前端实验列表增强 | P1 | 增加指标列和同步状态 |
| 实验详情Drawer | P2 | 完整指标、因子、模型信息展示 |

### Phase 3: 实验同步（3-5天）

| 任务 | 优先级 | 说明 |
|------|--------|------|
| 模型权重同步 | P1 | 从mlruns提取并复制到assets目录 |
| factor_order.json生成 | P1 | 从conf.yaml和parquet生成 |
| QE manifest生成 | P1 | 同步完成后生成manifest.json |
| 同步状态管理 | P2 | 同步记录表和状态追踪 |

### Phase 4: QE选股（5-7天）

| 任务 | 优先级 | 说明 |
|------|--------|------|
| `qe_selection_service.py` | P1 | QE选股Service |
| InferenceEngine QE扩展 | P1 | 新增 `_load_qe_manifest` 和 `run_qe_inference` |
| QE信号表 | P1 | `trading.qe_signal` |
| QE选股Router | P1 | 独立路由 |
| 前端选股页面 | P2 | 复用Task选股UI组件 |
| 隔离验证 | P0 | 确保Task选股不受影响 |

### Phase 5: 模型包装层统一（可选，5-7天）

| 任务 | 优先级 | 说明 |
|------|--------|------|
| 改用GeneralPTNN | P2 | `config_composer.py` 生成GeneralPTNN配置 |
| custom_model.py简化 | P2 | 只包含纯NN类 |
| 数据加载器统一 | P3 | 评估是否改用NestedDataLoader |

---

## 附录

### A. 关键文件路径

| 文件 | 路径 | 说明 |
|------|------|------|
| QE配置组装器 | `f:/Dev/AIstock/backend/services/quantevolver/config_composer.py` | 生成实验文件 |
| RDAgent模型运行器 | `f:/Dev/RD-Agent-main/rdagent/scenarios/qlib/developer/model_runner.py` | 模型训练 |
| RDAgent因子运行器 | `f:/Dev/RD-Agent-main/rdagent/scenarios/qlib/developer/factor_runner.py` | 因子回测 |
| GeneralPTNN | `f:/Dev/qlib/contrib/model/pytorch_general_nn.py` | QLib模型包装 |
| 自定义加载器 | `f:/Dev/RD-Agent-main/rdagent/scenarios/qlib/experiment/custom_loaders.py` | 数据加载 |
| SOTA模型配置 | `f:/Dev/RD-Agent-main/rdagent/scenarios/qlib/experiment/model_template/conf_sota_factors_model.yaml` | 模型训练配置 |
| Task选股Service | `f:/Dev/AIstock/backend/services/rdagent_selection_service.py` | 现有选股 |
| 推理引擎 | `f:/Dev/AIstock/backend/inference_engine.py` | 核心推理 |
| 数据服务API | `f:/Dev/AIstock/backend/data_service/api.py` | 行情数据 |
| Results API | `f:/Dev/RD-Agent-main/rdagent/app/results_api_server.py` | RDAgent API |

### B. 数据源字段清单

**daily_pv.h5**（7列）：
`open, high, low, close, volume, amount, factor`

**static_factors.parquet**（90列）：
- `db_*`（19列）：每日基本面（PE、PB、换手率、市值等）
- `mf_*`（37列）：资金流向（大单/中单/小单/超大单买卖金额等）
- `bb_*`（16列）：历史基本面（EPS、BPS、利润增长等）
- `cp_*`（8列）：筹码分布（成本分位、胜率等）
- 其他（10列）：`PriceStrength_10D`, `liquidity_*`, `size_*`, `value_*`

---

## 7. 因子库详情增强与因子说明提示词优化

> 日期: 2026-02-15  
> 关联: 因子同步→QE组装→选股/实盘 全链路因子计算逻辑一致性保障

### 7.1 因子计算逻辑一致性保障

#### 7.1.1 全链路因子代码不变性验证

因子从RDAgent演进到AIstock侧QE实验执行，全程**不修改因子计算代码**：

```
RDAgent因子演进
  └── SOTA因子代码 (factor.py / aligned/{name}.py)
        │
        ▼ [rdagent_task_sync_service.py 步骤4]
        │  从aligned API获取每个因子独立的source_code
        │  原样存入 aistock_factor_catalog.code_text
        │
        ▼ [config_composer._compose_factor_file]
        │  从DB读取code_text，原样写入 factors/{name}.py
        │  不做任何包装、不添加函数壳
        │
        ▼ [prepare_factors.py]
        │  链接所有数据文件到工作目录
        │  对每个因子执行: python factors/{name}.py
        │  因子代码自行读取数据、计算、写入result.h5
        │
        ▼ [qrun conf.yaml]
           NestedDataLoader加载Alpha158 + combined_factors_df.parquet
           模型训练/回测使用完整特征集
```

**关键保障**：
- `_compose_factor_file` 直接读取 `code_text` 原样写入，不做任何代码变换
- `prepare_factors.py` 使用 `link_all_files_to_dir()` 链接RDAgent因子数据源目录的所有文件，与RDAgent `FactorFBWorkspace.execute()` 完全一致
- 每个因子独立执行 `python factor.py`，与RDAgent因子回测流程一致
- 合并后的 `combined_factors_df.parquet` 通过 `StaticDataLoader` 加载，与RDAgent `NestedDataLoader` 配置一致

#### 7.1.2 后续选股/实盘的因子复用

QE实验完成后，模型权重和特征序列（`factor_order.json`）同步到AIstock侧。后续选股或实盘交易时：
- 使用相同的因子代码重新计算实时因子值
- 按 `factor_order.json` 严格对齐特征顺序
- 确保推理时的特征与训练时完全一致

### 7.2 因子库详情页增强

#### 7.2.1 当前状态

因子库页面（`/quantevolver/factors`）展开详情区域目前显示：
- **因子说明**：来自 `qe_factor_classification.description`（规则或LLM生成）
- **分类原因**：来自 `qe_factor_classification.classification_reason`
- **因子来源**：仅显示 `source` 标识（如 `rdagent_task_sync`）

**缺失信息**：
- 不显示来源Task的具体ID和时间
- 不显示因子的截面/时序维度分类说明
- 不显示因子的完整代码
- 不显示因子的表达式/公式

#### 7.2.2 增强设计

因子详情展开区域增加以下信息模块：

**A. RDAgent Task来源信息**（仅 `source=rdagent_task_sync` 的因子）

| 字段 | 数据来源 | 说明 |
|------|---------|------|
| Task ID | `aistock_factor_catalog.source_task_id` | RDAgent任务ID，如 `2026-02-11_13-33-06-508190` |
| Loop轮次 | `aistock_factor_catalog.source_loop_tag` | 因子产生的训练轮次 |
| 首次SOTA Task | `aistock_factor_catalog.first_sota_task_id` | 因子首次成为SOTA的Task |
| 代码来源 | `aistock_factor_catalog.source_code_origin` | `aligned_api` / `factor_py` / `based_factor` |
| 代码路径 | `aistock_factor_catalog.source_code_relpath` | 如 `aligned/bb_pe_dyn_inv.py` |

**B. 因子维度分类说明**

在分类标签旁增加截面/时序因子的含义说明：

| 维度 | 标签 | 说明文字 |
|------|------|---------|
| `cross_sectional` | 截面因子 | 在同一时间点对不同股票进行横向比较排名 |
| `time_series` | 时序因子 | 对同一股票在不同时间点进行纵向分析 |

**C. 因子表达式/公式**

显示 `aistock_factor_catalog.expression` 字段，对于RDAgent SOTA因子通常包含LaTeX公式和文字描述。

**D. 因子完整代码**

显示 `aistock_factor_catalog.code_text` 完整内容（代码块格式），需要后端API返回完整 `code_text`。

#### 7.2.3 后端API修改

**修改1：因子详情API增加 `code_text` 完整字段**

`GET /api/v1/rdagent/catalogs/factors/{factor_name}` 当前已返回 `source_task_id`、`source_loop_tag` 等字段，但缺少完整 `code_text`。需要在SQL中增加 `code_text` 字段。

**修改2：前端因子库页面需要的数据**

前端因子库页面通过两个API获取数据：
1. `GET /api/v1/rdagent/catalogs/factors` — 因子列表（已有 `source_task_id` 等字段）
2. `GET /api/v1/quantevolver/factor-analyst/classifications` — 分类结果（已有 `factor_dimension`）

当用户点击展开详情时，需要调用因子详情API获取完整信息（`code_text`、`expression`、Task来源字段）。

#### 7.2.4 前端修改

修改 `frontend/src/app/quantevolver/factors/page.tsx`：

1. **展开详情时按需加载**：点击"展开"时调用 `GET /api/v1/rdagent/catalogs/factors/{factor_name}?source=xxx` 获取完整详情
2. **Task来源信息区域**：显示Task ID、Loop轮次、代码来源等
3. **截面/时序维度说明**：在维度标签旁增加tooltip或说明文字
4. **因子表达式**：显示expression字段
5. **因子代码**：可折叠的代码块，显示完整code_text

### 7.3 因子说明分析提示词优化

#### 7.3.1 当前提示词问题

当前LLM因子说明提示词（`_generate_description_with_llm`）要求：
- 因子原理、因子分类、因子特性
- 200-250字中文描述

**不足**：
- 缺少基于因子代码和表达式的**具体使用场景**分析
- 缺少与其他因子的**搭配建议**（基于代码分析而非通用模板）
- 描述偏向通用化，缺少针对具体因子的差异化分析结论

#### 7.3.2 优化后的提示词

```
你是一位资深量化因子研究员兼组合架构顾问。请基于因子的代码实现和表达式，从实战应用角度分析因子，内容必须包含以下要素：

1. 【因子逻辑】基于代码和表达式分析该因子捕捉什么市场信号，使用了什么数据源和计算方法（不要展示公式或代码）
2. 【因子维度】判断是截面因子（横向比较不同股票）还是时序因子（纵向分析同一股票历史），说明判断依据
3. 【使用场景】该因子适合什么市场环境（趋势市/震荡市/牛市/熊市）、什么投资风格（价值/成长/动量/低波）、什么持仓周期（日内/短线/中线/长线）
4. 【搭配建议】基于因子的数据源和计算逻辑，推荐与哪些类型的因子搭配使用可以形成互补（如动量+价值对冲、资金流+筹码共振等），说明搭配的金融逻辑

特别说明：
- 如果提供了QLib表达式（如 Resi($close, 5)/$close），请基于表达式的数学含义来理解因子逻辑
- 如果提供了Python代码，请基于代码中实际使用的数据列和计算逻辑来分析
- 分析结论必须基于代码和表达式的实际内容，不要泛泛而谈

要求：
- 200-250字中文描述
- 不要显示任何计算公式、表达式或代码
- 不要罗列代码中的变量名
- 用专业但易懂的金融语言描述
- 重点突出使用场景和搭配建议的实战价值

仅返回JSON格式：{"description": "因子描述"}
不要返回其他任何内容。
```

#### 7.3.3 规则生成描述优化

`_generate_description_by_rules` 函数当前已包含搭配建议（`_COMBO_ADVICE`），但缺少使用场景。增加使用场景模板：

```python
_USAGE_SCENARIO = {
    "MOM": "适合趋势明确的市场环境，中短线持仓效果较好",
    "VOL": "适合震荡市中的风险管理和择时，可用于构建低波动组合",
    "LIQ": "适合中长线价值投资，流动性溢价在小盘股中更显著",
    "VAL": "适合价值投资风格，在市场回归理性时表现突出",
    "QUAL": "适合保守型长线投资，在熊市中具有较好的防御性",
    "CORR": "适合短线量价分析，在量价背离时信号较强",
    "TECH": "适合短线交易和择时，在超买超卖区间信号较强",
    "SIZE": "适合小盘股策略，在流动性充裕的市场环境中效果更好",
    "STAT": "适合截面选股，通过相对排名识别强弱股",
    "MF": "适合短中线交易，主力资金信号在个股层面更有效",
    "CHIP": "适合中线波段操作，筹码结构变化预示支撑压力位",
    "ML": "适合多因子组合，作为非线性信号补充传统因子",
}
```

### 7.4 实施计划

| 步骤 | 文件 | 修改内容 |
|------|------|---------|
| 1 | `rdagent_catalog_admin.py` | 因子详情API增加 `code_text` 完整字段 |
| 2 | `factor_analyst.py` | 优化LLM提示词和规则描述生成（增加使用场景） |
| 3 | `factors/page.tsx` | 展开详情时按需加载完整信息，增加Task来源、维度说明、表达式、代码展示 |
| 4 | 验证 | 端到端检查因子详情展示效果 |

---

## 8. QE实验结果深度分析与因子实验表现追踪

### 8.1 背景与目标

QE实验通过QLib的`qrun`执行回测，生成丰富的结果数据。当前系统仅提取汇总级别的指标（IC、年化收益等），缺少：
1. **因子级别的实验表现追踪**：每个因子参与的每次实验中的IC、最大回撤、年化收益、夏普比等
2. **策略胜率统计**：买入股票盈利卖出vs亏损/止损卖出的比例、金额
3. **个股盈亏分析**：盈利股票平均盈利%、亏损股票平均亏损%
4. **交易明细数据**：每日持仓变动、换手率等

目标：
- 建立因子实验表现记录表，关联因子ID和实验ID
- 实验结束后自动采集因子级别指标和交易统计数据
- 在因子库详情中展示历史实验回测指标，作为因子选择依据
- 为未来AI Agent策略演进提供数据基础

### 8.2 QLib实验结果数据结构分析

QLib实验通过`SignalRecord`、`SigAnaRecord`、`PortAnaRecord`三个Record生成以下数据文件：

#### 8.2.1 mlflow metrics（文本文件）
```
IC                                          # 信息系数
ICIR                                        # 信息系数比率
Rank IC                                     # 排名IC
Rank ICIR                                   # 排名ICIR
1day.excess_return_without_cost.annualized_return   # 年化超额收益（不含成本）
1day.excess_return_without_cost.information_ratio   # 信息比率（不含成本）
1day.excess_return_without_cost.max_drawdown        # 最大回撤（不含成本）
1day.excess_return_with_cost.annualized_return      # 年化超额收益（含成本）
1day.excess_return_with_cost.information_ratio      # 信息比率（含成本）
1day.excess_return_with_cost.max_drawdown           # 最大回撤（含成本）
```

#### 8.2.2 pkl数据文件
| 文件 | 类型 | 内容 | 用途 |
|------|------|------|------|
| `pred.pkl` | DataFrame(datetime×instrument) | 预测信号值 | 信号质量分析 |
| `report_normal_1day.pkl` | DataFrame(datetime) | 每日return/bench/cost/turnover | 收益曲线、胜率统计 |
| `positions_normal_1day.pkl` | dict{date→{instrument→{amount,weight,price}}} | 每日持仓明细 | 个股盈亏分析 |
| `port_analysis_1day.pkl` | DataFrame | 风险分析汇总 | 回撤、波动率等 |
| `indicator_analysis_1day.pkl` | DataFrame | 交易指标 | 换手率等 |

#### 8.2.3 可计算的策略胜率指标
从`report_normal_1day.pkl`可计算：
- **日胜率**：每日超额收益>0的天数占比
- **周胜率**：每周累计超额收益>0的周数占比
- **最大连续盈利天数/亏损天数**

从`positions_normal_1day.pkl`可计算：
- **个股胜率**：持仓期间盈利卖出的股票数/总交易股票数
- **平均盈利幅度**：盈利股票的平均收益率
- **平均亏损幅度**：亏损股票的平均亏损率
- **盈亏比**：平均盈利/平均亏损
- **最大单笔盈利/亏损**

### 8.3 数据库设计

#### 8.3.1 因子实验表现记录表 `qe_factor_experiment_metrics`

```sql
CREATE TABLE IF NOT EXISTS qe_factor_experiment_metrics (
    id SERIAL PRIMARY KEY,
    factor_name TEXT NOT NULL,              -- 因子名称，关联 aistock_factor_catalog.factor_name
    factor_source TEXT NOT NULL,            -- 因子来源（rdagent_task_sync / alpha158 等）
    experiment_id TEXT NOT NULL,            -- 实验ID，关联 qe_experiments.experiment_id
    experiment_name TEXT,                   -- 实验名称（冗余，方便查询）

    -- 信号质量指标
    ic DOUBLE PRECISION,                    -- 信息系数
    icir DOUBLE PRECISION,                  -- 信息系数比率
    rank_ic DOUBLE PRECISION,               -- 排名IC
    rank_icir DOUBLE PRECISION,             -- 排名ICIR

    -- 超额收益指标（不含成本）
    ann_return_no_cost DOUBLE PRECISION,    -- 年化超额收益
    info_ratio_no_cost DOUBLE PRECISION,    -- 信息比率
    max_drawdown_no_cost DOUBLE PRECISION,  -- 最大回撤

    -- 超额收益指标（含成本）
    ann_return_with_cost DOUBLE PRECISION,  -- 年化超额收益
    info_ratio_with_cost DOUBLE PRECISION,  -- 信息比率
    max_drawdown_with_cost DOUBLE PRECISION,-- 最大回撤

    -- 策略胜率指标
    daily_win_rate DOUBLE PRECISION,        -- 日胜率
    weekly_win_rate DOUBLE PRECISION,       -- 周胜率
    max_consecutive_win INTEGER,            -- 最大连续盈利天数
    max_consecutive_loss INTEGER,           -- 最大连续亏损天数

    -- 个股交易统计
    total_trades INTEGER,                   -- 总交易股票数
    winning_trades INTEGER,                 -- 盈利股票数
    losing_trades INTEGER,                  -- 亏损股票数
    stock_win_rate DOUBLE PRECISION,        -- 个股胜率
    avg_profit_pct DOUBLE PRECISION,        -- 平均盈利幅度%
    avg_loss_pct DOUBLE PRECISION,          -- 平均亏损幅度%
    profit_loss_ratio DOUBLE PRECISION,     -- 盈亏比
    max_single_profit_pct DOUBLE PRECISION, -- 最大单笔盈利%
    max_single_loss_pct DOUBLE PRECISION,   -- 最大单笔亏损%

    -- 其他指标
    sharpe_ratio DOUBLE PRECISION,          -- 夏普比率
    calmar_ratio DOUBLE PRECISION,          -- 卡尔玛比率
    avg_turnover DOUBLE PRECISION,          -- 平均换手率
    total_trading_days INTEGER,             -- 总交易天数

    -- 实验上下文
    model_id TEXT,                          -- 使用的模型
    other_factors JSONB,                    -- 同实验中的其他因子
    data_split JSONB,                       -- 数据划分（训练/验证/测试）

    -- 元数据
    collected_at TIMESTAMPTZ DEFAULT NOW(), -- 采集时间
    raw_metrics JSONB,                      -- 原始完整指标（备份）

    UNIQUE(factor_name, factor_source, experiment_id)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_fexp_factor ON qe_factor_experiment_metrics(factor_name, factor_source);
CREATE INDEX IF NOT EXISTS idx_fexp_experiment ON qe_factor_experiment_metrics(experiment_id);
CREATE INDEX IF NOT EXISTS idx_fexp_ic ON qe_factor_experiment_metrics(ic DESC NULLS LAST);
```

### 8.4 后端API设计

#### 8.4.1 因子历史实验表现查询
```
GET /api/v1/quantevolver/factors/{factor_name}/experiment-metrics
    ?source=rdagent_task_sync
    &limit=20
    &order_by=collected_at

Response:
{
    "factor_name": "bb_pe_dyn_inv",
    "total": 5,
    "metrics": [
        {
            "experiment_id": "xxx",
            "experiment_name": "qe_exp_xxx",
            "ic": 0.045,
            "icir": 1.23,
            "ann_return_no_cost": 0.156,
            "max_drawdown_no_cost": -0.082,
            "daily_win_rate": 0.534,
            "stock_win_rate": 0.412,
            "avg_profit_pct": 3.2,
            "avg_loss_pct": -2.1,
            "profit_loss_ratio": 1.52,
            "model_id": "GeneralPTNN_xxx",
            "other_factors": ["factor_a", "factor_b"],
            "collected_at": "2026-02-15T12:00:00Z"
        }
    ],
    "summary": {
        "avg_ic": 0.042,
        "best_ic": 0.065,
        "worst_ic": 0.021,
        "avg_ann_return": 0.12,
        "avg_daily_win_rate": 0.52,
        "experiment_count": 5
    }
}
```

#### 8.4.2 实验交易统计查询
```
GET /api/v1/quantevolver/experiments/{experiment_id}/trade-stats

Response:
{
    "experiment_id": "xxx",
    "daily_win_rate": 0.534,
    "weekly_win_rate": 0.512,
    "max_consecutive_win": 8,
    "max_consecutive_loss": 5,
    "total_trades": 156,
    "winning_trades": 64,
    "losing_trades": 92,
    "stock_win_rate": 0.410,
    "avg_profit_pct": 3.2,
    "avg_loss_pct": -2.1,
    "profit_loss_ratio": 1.52,
    "max_single_profit_pct": 12.5,
    "max_single_loss_pct": -8.3,
    "avg_turnover": 0.15,
    "total_trading_days": 120
}
```

### 8.5 数据采集流程

实验结果同步时（`sync_experiment_results`），增强`read_exp_res.py`模板：

1. **读取mlflow metrics** → IC/ICIR/年化收益/最大回撤等
2. **读取report_normal_1day.pkl** → 计算日胜率/周胜率/连续盈亏
3. **读取positions_normal_1day.pkl** → 计算个股胜率/盈亏幅度/盈亏比
4. **保存到qlib_results.json** → 包含完整交易统计
5. **后端同步时** → 解析结果，为每个参与因子写入`qe_factor_experiment_metrics`

### 8.6 前端展示设计

在因子库详情展开区域增加"实验表现"板块：

```
┌─────────────────────────────────────────────────────┐
│ 📊 历史实验表现 (共5次实验)                          │
│                                                     │
│ 汇总: 平均IC=0.042 | 平均年化=12.0% | 平均日胜率=52%│
│                                                     │
│ ┌─────────┬────────┬────────┬────────┬────────┐     │
│ │ 实验     │ IC     │ 年化   │ 最大回撤│ 日胜率 │     │
│ ├─────────┼────────┼────────┼────────┼────────┤     │
│ │ exp_478c │ 0.045  │ 15.6%  │ -8.2%  │ 53.4%  │     │
│ │ exp_a9a4 │ 0.038  │ 11.2%  │ -6.5%  │ 51.2%  │     │
│ │ exp_44c6 │ 0.065  │ 18.3%  │ -9.1%  │ 55.1%  │     │
│ └─────────┴────────┴────────┴────────┴────────┘     │
│                                                     │
│ 📈 交易统计 (最近一次实验)                           │
│ 个股胜率: 41.0% | 盈亏比: 1.52                      │
│ 平均盈利: +3.2% | 平均亏损: -2.1%                   │
│ 最大单笔盈利: +12.5% | 最大单笔亏损: -8.3%          │
└─────────────────────────────────────────────────────┘
```

### 8.7 实施计划

| 步骤 | 文件 | 修改内容 |
|------|------|---------|
| 1 | `init_catalog_db.py` | 创建 `qe_factor_experiment_metrics` 表 |
| 2 | `config_composer.py` | 增强 `read_exp_res.py` 模板，增加交易统计计算 |
| 3 | `config_composer.py` | 增强 `sync_experiment_results`，解析完整结果并写入因子指标表 |
| 4 | `quantevolver.py` | 新增因子实验表现查询API和实验交易统计API |
| 5 | `factors/page.tsx` | 因子详情展开区域增加历史实验表现板块 |
| 6 | 验证 | 端到端检查数据采集和展示 |
