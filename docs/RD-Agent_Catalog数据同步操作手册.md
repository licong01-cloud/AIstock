# AIstock RD-Agent Catalog数据同步操作手册

## 文档概述

本文档描述了从RD-Agent系统导出数据到AIstock数据库的完整流程，包括数据导出、导入、验证等所有步骤，以及相关脚本、字段和数据获取方式的详细说明。

**最后更新时间**: 2026-01-10  
**适用版本**: AIstock v1.0, RD-Agent Phase2 Catalog

---

## 目录

1. [系统架构](#系统架构)
2. [数据流程概览](#数据流程概览)
3. [数据源说明](#数据源说明)
4. [数据导出步骤](#数据导出步骤)
5. [数据导入步骤](#数据导入步骤)
6. [数据验证步骤](#数据验证步骤)
7. [脚本详细说明](#脚本详细说明)
8. [数据表结构说明](#数据表结构说明)
9. [字段映射说明](#字段映射说明)
10. [常见问题与解决方案](#常见问题与解决方案)

---

## 系统架构

### 系统组件

```
RD-Agent系统
├── SQLite数据库 (registry.sqlite)
├── Workspace目录 (RD-Agent_workspace)
│   ├── factor_perf.json
│   ├── feedback.json
│   ├── model.py
│   ├── model_meta.json
│   └── ...
└── 数据导出工具
    ├── export_aistock_loop_catalog.py
    ├── export_aistock_model_catalog.py
    ├── export_aistock_factor_catalog.py
    └── export_aistock_strategy_catalog.py

AIstock系统
├── PostgreSQL数据库 (aistock)
├── Backend数据目录 (backend/data)
│   ├── aistock_loop_catalog.json
│   ├── aistock_model_catalog.json
│   ├── aistock_factor_catalog.json
│   └── aistock_strategy_catalog.json
└── 数据导入工具
    ├── reimport_all_catalogs.py
    ├── truncate_all_tables.py
    └── 各种验证脚本
```

### 数据流向

```
RD-Agent SQLite数据库
        ↓
RD-Agent导出工具
        ↓
JSON文件 (RDagentDB/aistock/)
        ↓
复制到AIstock backend/data/
        ↓
AIstock导入工具
        ↓
PostgreSQL数据库 (aistock)
```

---

## 数据流程概览

### 完整数据同步流程

```
┌─────────────────────────────────────────────────────────────┐
│ 步骤1: RD-Agent 全量初始化与资产固化（Phase3 必做）          │
│ - materialize-pending：补齐 workspace 的 factor_meta/perf 等  │
│ - solidify-all：生成 production_bundles 并写 manifest.json    │
│ - 产物位置: F:\Dev\RD-Agent-main\RDagentDB\production_bundles│
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 步骤2: RD-Agent 导出 Catalog JSON                            │
│ - 导出 loop/model/factor/strategy catalogs 到 RDagentDB/aistock│
│ - 文件位置: F:\Dev\RD-Agent-main\RDagentDB\aistock\          │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 步骤3: 复制JSON文件到AIstock                                 │
│ - 将导出的JSON文件复制到 AIstock backend/data/                │
│ - 文件位置: F:\Dev\AIstock\backend\data\                     │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 步骤4: 清空并重导入 AIstock PG                                │
│ - truncate_all_tables.py 清空 catalog 表                      │
│ - reimport_all_catalogs.py 导入所有 catalog                   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 步骤5: 验收（必须全通过，导出后不再修改）                     │
│ - bundle manifest/self_check 全量通过                         │
│ - catalogs 生成成功且字段符合设计要求                         │
│ - AIstock 页面与推理链路可稳定运行                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Phase3 全量初始化与资产固化（强制步骤）

本节用于确保 **AIstock 选股推理所需的核心物理资产** 在 RD-Agent 侧已固化并可被 manifest 确定性定位。

### 1) 前置检查

- RD-Agent Registry：`F:\Dev\RD-Agent-main\RDagentDB\registry.sqlite`
- 固化产物目录：`F:\Dev\RD-Agent-main\RDagentDB\production_bundles\`

### 2) 一键执行（推荐按顺序，不可跳步）

在 `F:\Dev\RD-Agent-main` 下执行：

```bash
python tools\audit_and_fix_registry_schema.py

python tools\backfill_registry_artifacts.py --db RDagentDB\registry.sqlite --mode materialize-pending

python tools\backfill_registry_artifacts.py --db RDagentDB\registry.sqlite --mode solidify-all

python tools\export_aistock_factor_catalog.py --registry-sqlite RDagentDB\registry.sqlite --output RDagentDB\aistock\factor_catalog.json
python tools\export_aistock_strategy_catalog.py --registry-sqlite RDagentDB\registry.sqlite --output RDagentDB\aistock\strategy_catalog.json
python tools\export_aistock_model_catalog.py --registry-sqlite RDagentDB\registry.sqlite --output RDagentDB\aistock\model_catalog.json
python tools\export_aistock_loop_catalog.py --registry-sqlite RDagentDB\registry.sqlite --output RDagentDB\aistock\loop_catalog.json

python tools\verify_production_bundles_manifest.py --run-self-check
```

### 3) 验收标准（必须 100% 通过）

- 每个 `production_bundles/{asset_bundle_id}` 目录：
  - 存在 `manifest.json`
  - `manifest.schema_version == 1`
  - `manifest.primary_assets.factor_entry_relpath` 指向的文件存在
  - `manifest.primary_assets.model_weight_relpath` 指向的文件存在
  - 存在 `self_check.py` 且运行返回码为 0
- `RDagentDB/aistock/*.json` 生成成功（4 个 catalogs）

---

---

## 数据源说明

### RD-Agent数据源

#### 1. SQLite数据库 (registry.sqlite)

**位置**: `F:\Dev\RD-Agent-main\RDagentDB\registry.sqlite`

**主要表结构**:
- `loops`: 存储RD-Agent实验循环信息
- `workspaces`: 存储工作空间信息
- `artifacts`: 存储模型相关的元数据
- `task_runs`: 存储任务运行信息

**数据获取方式**:
```python
# 通过SQL查询获取数据
sql = """
SELECT l.task_run_id, l.loop_id, w.workspace_id, w.workspace_path
FROM loops l
JOIN workspaces w ON l.task_run_id = w.task_run_id AND l.loop_id = w.loop_id
WHERE l.has_result = 1
"""
```

#### 2. Workspace目录

**位置**: `F:\Dev\RD-Agent-main\git_ignore_folder\RD-Agent_workspace\`

**关键文件**:
- `factor_perf.json`: 因子性能数据
- `feedback.json`: 反馈信息
- `model.py`: 模型定义文件
- `model_meta.json`: 模型元数据（Phase 3生成）
- 各种YAML配置文件

**数据获取方式**:
```python
# 读取JSON文件
with open(workspace_path / "factor_perf.json", 'r') as f:
    factor_perf = json.load(f)

# 读取model.py文件
with open(workspace_path / "model.py", 'r') as f:
    model_py_content = f.read()
```

### AIstock数据源

#### PostgreSQL数据库

**连接信息** (从.env文件加载):
```python
host = os.getenv("TDX_DB_HOST", "127.0.0.1")
port = int(os.getenv("TDX_DB_PORT", "5432"))
user = os.getenv("TDX_DB_USER", "postgres")
password = os.getenv("TDX_DB_PASSWORD", "lc78080808")
dbname = os.getenv("TDX_DB_NAME", "aistock")
```

**主要表结构**:
- `aistock_factor_catalog`: 因子目录表
- `aistock_model_catalog`: 模型目录表
- `aistock_strategy_catalog`: 策略目录表
- `aistock_loop_catalog`: 循环目录表

---

## 数据导出步骤

### 步骤1: 导出Loop Catalog

**脚本位置**: `F:\Dev\RD-Agent-main\tools\export_aistock_loop_catalog.py`

**执行命令**:
```bash
cd F:\Dev\RD-Agent-main\tools
python export_aistock_loop_catalog.py \
    --registry-sqlite F:\Dev\RD-Agent-main\RDagentDB\registry.sqlite \
    --output F:\Dev\RD-Agent-main\RDagentDB\aistock\loop_catalog.json
```

**执行内容**:
1. 从SQLite数据库查询有结果的loops
2. 遍历每个loop对应的workspace目录
3. 读取`factor_perf.json`提取因子列表和性能指标
4. 读取`feedback.json`提取决策和摘要信息
5. 生成稳定的strategy_id
6. 构造Loop entry并导出为JSON

**输出文件**: `F:\Dev\RD-Agent-main\RDagentDB\aistock\loop_catalog.json`

**关键提取逻辑**:
```python
# IC值提取（已修复）
for k, v in m.items():
    if not isinstance(k, str):
        continue
    lk = k.lower()
    # 优先匹配大写IC或小写ic
    if ic is None and (k == "IC" or k == "ic" or lk == "ic"):
        ic = v
```

### 步骤2: 导出Model Catalog

**脚本位置**: `F:\Dev\RD-Agent-main\tools\export_aistock_model_catalog.py`

**执行命令**:
```bash
cd F:\Dev\RD-Agent-main\tools
python export_aistock_model_catalog.py \
    --registry-sqlite F:\Dev\RD-Agent-main\RDagentDB\registry.sqlite \
    --output F:\Dev\RD-Agent-main\RDagentDB\aistock\model_catalog.json
```

**执行内容**:
1. 从SQLite数据库查询模型源数据
2. 遍历每个workspace目录
3. 按优先级提取模型类型：
   - **优先级1**: 从`model.py`文件中提取类定义
   - **优先级2**: 从`model_meta.json`读取
   - **优先级3**: 从模型权重文件（params.pkl）读取
   - **优先级4**: 从Registry Metadata读取
   - **优先级5**: 从YAML配置文件读取
4. 提取模型配置、数据集配置、特征schema
5. 构造Model entry并导出为JSON

**输出文件**: `F:\Dev\RD-Agent-main\RDagentDB\aistock\model_catalog.json`

**关键提取逻辑**:
```python
# 模型类型提取（已修复优先级）
# 1. 从model.py提取（最高优先级）
model_py = ws_root / "model.py"
if model_py.exists():
    # 提取类定义
    for line in content.split('\n'):
        if line.startswith('class ') and '(' in line and 'Model' in line:
            model_type = line[class_start:class_end].strip()
            break

# 2. 从model_meta.json读取
if model_type is None:
    model_meta = _load_json_if_exists(ws_root / "model_meta.json")
    if model_meta:
        model_type = model_meta.get("model_type")
```

### 步骤3: 导出Factor Catalog

**脚本位置**: `F:\Dev\RD-Agent-main\tools\export_aistock_factor_catalog.py`

**执行命令**:
```bash
cd F:\Dev\RD-Agent-main\tools
python export_aistock_factor_catalog.py \
    --registry-sqlite F:\Dev\RD-Agent-main\RDagentDB\registry.sqlite \
    --output F:\Dev\RD-Agent-main\RDagentDB\aistock\factor_catalog.json
```

**执行内容**:
1. 从SQLite数据库查询因子数据
2. 区分因子来源（qlib_alpha360, qlib_alpha158, rdagent_generated）
3. 提取因子名称、类型、描述等信息
4. 构造Factor entry并导出为JSON

**输出文件**: `F:\Dev\RD-Agent-main\RDagentDB\aistock\factor_catalog.json`

### 步骤4: 导出Strategy Catalog

**脚本位置**: `F:\Dev\RD-Agent-main\tools\export_aistock_strategy_catalog.py`

**执行命令**:
```bash
cd F:\Dev\RD-Agent-main\tools
python export_aistock_strategy_catalog.py \
    --registry-sqlite F:\Dev\RD-Agent-main\RDagentDB\registry.sqlite \
    --output F:\Dev\RD-Agent-main\RDagentDB\aistock\strategy_catalog.json
```

**执行内容**:
1. 从SQLite数据库查询策略数据
2. 提取策略ID、名称、描述等信息
3. 提取workspace示例信息
4. 构造Strategy entry并导出为JSON

**输出文件**: `F:\Dev\RD-Agent-main\RDagentDB\aistock\strategy_catalog.json`

---

## 数据导入步骤

### 步骤1: 复制JSON文件到AIstock

**执行命令**:
```bash
# 复制Loop Catalog
Copy-Item -Path "F:\Dev\RD-Agent-main\RDagentDB\aistock\loop_catalog.json" `
    -Destination "F:\Dev\AIstock\backend\data\aistock_loop_catalog.json" -Force

# 复制Model Catalog
Copy-Item -Path "F:\Dev\RD-Agent-main\RDagentDB\aistock\model_catalog.json" `
    -Destination "F:\Dev\AIstock\backend\data\aistock_model_catalog.json" -Force

# 复制Factor Catalog
Copy-Item -Path "F:\Dev\RD-Agent-main\RDagentDB\aistock\factor_catalog.json" `
    -Destination "F:\Dev\AIstock\backend\data\aistock_factor_catalog.json" -Force

# 复制Strategy Catalog
Copy-Item -Path "F:\Dev\RD-Agent-main\RDagentDB\aistock\strategy_catalog.json" `
    -Destination "F:\Dev\AIstock\backend\data\aistock_strategy_catalog.json" -Force
```

### 步骤2: 清空AIstock数据库表

**脚本位置**: `F:\Dev\AIstock\backend\truncate_all_tables.py`

**执行命令**:
```bash
cd F:\Dev\AIstock\backend
python truncate_all_tables.py
```

**执行内容**:
1. 连接到PostgreSQL数据库
2. 清空`aistock_loop_catalog`表
3. 清空`aistock_model_catalog`表
4. 清空`aistock_strategy_catalog`表
5. 清空`aistock_factor_catalog`表

**关键代码**:
```python
cur.execute("TRUNCATE TABLE aistock_loop_catalog CASCADE")
cur.execute("TRUNCATE TABLE aistock_model_catalog CASCADE")
cur.execute("TRUNCATE TABLE aistock_strategy_catalog CASCADE")
cur.execute("TRUNCATE TABLE aistock_factor_catalog CASCADE")
```

### 步骤3: 导入数据到AIstock数据库

**脚本位置**: `F:\Dev\AIstock\backend\reimport_all_catalogs.py`

**执行命令**:
```bash
cd F:\Dev\AIstock\backend
python reimport_all_catalogs.py
```

**执行内容**:
1. 导入Factor Catalog（778个因子）
2. 导入Model Catalog（501个模型）
3. 导入Strategy Catalog（155个策略）
4. 导入Loop Catalog（215个Loop）

**导入过程**:
- 每导入100条记录显示进度
- 使用`ON CONFLICT`进行upsert操作
- 支持JSONB字段的序列化
- 错误处理和日志记录

**关键代码**:
```python
# Factor Catalog导入
for i, factor in enumerate(factors, 1):
    if i % 100 == 0:
        print(f"  已导入 {i}/{len(factors)}")
    
    cur.execute("""
        INSERT INTO aistock_factor_catalog (
            factor_id, factor_name, factor_type, source, description,
            best_performance, best_loop_task_run_id, best_loop_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (factor_id) DO UPDATE SET
            factor_name = EXCLUDED.factor_name,
            factor_type = EXCLUDED.factor_type,
            source = EXCLUDED.source,
            description = EXCLUDED.description,
            best_performance = EXCLUDED.best_performance,
            best_loop_task_run_id = EXCLUDED.best_loop_task_run_id,
            best_loop_id = EXCLUDED.best_loop_id
    """, (...))
```

---

## 数据验证步骤

### 步骤1: 验证Loop IC值

**脚本位置**: `F:\Dev\AIstock\backend\verify_database_accuracy.py`

**执行命令**:
```bash
cd F:\Dev\AIstock\backend
python verify_database_accuracy.py
```

**验证内容**:
- Loop总数
- 有IC字段的Loop数量
- IC值不为0的Loop数量
- 前5个Loop的IC值示例

**预期结果**:
```
Loop总数: 136
有IC字段的Loop: 136
IC值不为0的Loop: 136
```

### 步骤2: 验证模型类型分布

**脚本位置**: `F:\Dev\AIstock\backend\verify_model_export.py`

**执行命令**:
```bash
cd F:\Dev\AIstock\backend
python verify_model_export.py
```

**验证内容**:
- 模型总数
- 有task_run_id的模型数量
- 有loop_id的模型数量
- 有workspace_path的模型数量
- 模型类型分布

**预期结果**:
```
模型总数: 501
有task_run_id的模型: 501
有loop_id的模型: 501
有workspace_path的模型: 501

模型类型分布:
  LGBModel: 458个
  GRU_Attention_Residual_Model: 12个
  Transformer_TimeSeries_Model: 9个
  ...
```

### 步骤3: 验证数据完整性

**脚本位置**: `F:\Dev\AIstock\backend\final_data_status.py`

**执行命令**:
```bash
cd F:\Dev\AIstock\backend
python final_data_status.py
```

**验证内容**:
- Factor Catalog状态（总数、按来源分布、关联Loop情况）
- Model Catalog状态（总数、按类型分布、关联Loop情况）
- Strategy Catalog状态（总数、关联Loop情况）
- Loop Catalog状态（总数、有结果数量、有策略ID数量）

---

## 脚本详细说明

### RD-Agent导出脚本

#### export_aistock_loop_catalog.py

**功能**: 从RD-Agent SQLite数据库和workspace目录导出Loop Catalog数据

**主要函数**:
- `_fetch_loop_sources(conn)`: 从SQLite查询loop源数据
- `_build_loop_entry(ws_root, registry_row)`: 构造单个Loop entry
- `_load_json_if_exists(path)`: 加载JSON文件（如果存在）
- `_load_yaml_if_exists(path)`: 加载YAML文件（如果存在）
- `_find_yaml_templates(ws_root)`: 查找YAML模板文件
- `run(registry_sqlite, output_path)`: 主执行函数

**关键数据提取**:
```python
# 从factor_perf.json提取性能指标
windows = first_combo.get("windows", [])
main_w = windows[0]
annualized_return = main_w.get("annual_return")
max_drawdown = main_w.get("max_drawdown")
sharpe = main_w.get("sharpe")

# 从metrics字典提取IC值
metrics = main_w.get("metrics", {})
for k, v in metrics.items():
    if k == "IC" or k == "ic":
        ic = v
```

**输出格式**:
```json
{
  "version": "v1",
  "generated_at_utc": "2026-01-08T16:58:39.597168+00:00",
  "source": "rdagent_tools",
  "loops": [
    {
      "task_run_id": "cb1079c43d744c669a91ff34a96b6a84",
      "loop_id": 7,
      "workspace_id": "064bf1b11f9240d8b9a0177baf26deb9",
      "workspace_path": "/mnt/f/Dev/RD-Agent-main/...",
      "strategy_id": "xxx",
      "factor_names": ["factor1", "factor2"],
      "annualized_return": 0.6204765269425842,
      "max_drawdown": -0.5420001744242837,
      "sharpe": 2.243377576342228,
      "ic": 0.0357339204968663,
      "ic_ir": 0.3704966493855416,
      "win_rate": null,
      "decision": "xxx",
      "summary": {},
      "metrics": {...},
      "artifacts": {...}
    }
  ]
}
```

#### export_aistock_model_catalog.py

**功能**: 从RD-Agent SQLite数据库和workspace目录导出Model Catalog数据

**主要函数**:
- `_fetch_model_sources(conn)`: 从SQLite查询模型源数据
- `_extract_model_struct(ws_root, registry_row)`: 提取模型结构信息
- `_load_json_if_exists(path)`: 加载JSON文件
- `_load_yaml_if_exists(path)`: 加载YAML文件
- `_to_native_path(p_str)`: 转换路径格式
- `run(registry_sqlite, output_path)`: 主执行函数

**模型类型提取优先级**:
1. **model.py文件**（最高优先级）
   - 查找`class XXXModel`定义
   - 查找`model_cls = XXX`赋值

2. **model_meta.json文件**
   - 读取`model_type`字段
   - 读取`model_conf`、`dataset_conf`、`feature_schema`

3. **模型权重文件**
   - 从`mlruns`目录查找`params.pkl`
   - 提取`model_type`信息

4. **Registry Metadata**
   - 从SQLite artifacts表读取
   - 提取`model_type`、`model_conf_json`等

5. **YAML配置文件**（最低优先级）
   - 扫描workspace中的所有YAML文件
   - 从`task.model.class`提取模型类型

**输出格式**:
```json
{
  "version": "v1",
  "generated_at_utc": "2026-01-08T16:58:39.597168+00:00",
  "source": "rdagent_tools",
  "models": [
    {
      "model_id": "f844d26acfaf564e84db8213ca47b528",
      "task_run_id": "2025-12-18_10-38-22-336632",
      "loop_id": 0,
      "workspace_id": "06b7d40d499b4bfa817e981fdad6f2b0",
      "workspace_path": "/mnt/f/Dev/RD-Agent-main/...",
      "model_type": "LGBModel",
      "model_config": {...},
      "dataset_config": {...},
      "feature_schema": [...],
      "flattened_feature_list": [...],
      "model_artifacts": {...}
    }
  ]
}
```

### AIstock导入脚本

#### reimport_all_catalogs.py

**功能**: 从JSON文件导入所有Catalog数据到PostgreSQL数据库

**主要函数**:
- `import_factor_catalog(conn, json_path)`: 导入Factor Catalog
- `import_model_catalog(conn, json_path)`: 导入Model Catalog
- `import_strategy_catalog(conn, json_path)`: 导入Strategy Catalog
- `import_loop_catalog(conn, json_path)`: 导入Loop Catalog

**导入特点**:
- 使用`ON CONFLICT`进行upsert操作
- 支持JSONB字段的序列化
- 每100条记录显示进度
- 完善的错误处理

**关键代码**:
```python
# JSONB字段处理
best_performance_json = json.dumps(best_performance) if best_performance else None

# Upsert操作
cur.execute("""
    INSERT INTO aistock_factor_catalog (
        factor_id, factor_name, factor_type, source, description,
        best_performance, best_loop_task_run_id, best_loop_id
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (factor_id) DO UPDATE SET
        factor_name = EXCLUDED.factor_name,
        ...
""", (...))
```

#### truncate_all_tables.py

**功能**: 清空所有Catalog数据表

**执行顺序**:
1. 清空`aistock_loop_catalog`
2. 清空`aistock_model_catalog`
3. 清空`aistock_strategy_catalog`
4. 清空`aistock_factor_catalog`

**注意事项**:
- 使用`TRUNCATE`而不是`DELETE`，效率更高
- 使用`CASCADE`确保外键约束正确处理
- 执行后需要提交事务

### 验证脚本

#### verify_database_accuracy.py

**功能**: 验证数据库中数据的准确性

**验证项目**:
- Loop IC值检查
- 模型类型分布检查
- 模型字段完整性检查

#### verify_model_export.py

**功能**: 验证导出的Model Catalog数据

**验证项目**:
- 模型总数
- 模型类型分布
- 字段完整性（task_run_id, loop_id, workspace_path）

#### verify_loop_export.py

**功能**: 验证导出的Loop Catalog数据

**验证项目**:
- Loop总数
- IC值完整性
- 性能指标完整性

#### final_data_status.py

**功能**: 提供完整的数据导入状态报告

**报告内容**:
- Factor Catalog状态
- Model Catalog状态
- Strategy Catalog状态
- Loop Catalog状态
- 数据完整性总结

---

## 数据表结构说明

### aistock_loop_catalog表

**表结构**:
```sql
CREATE TABLE aistock_loop_catalog (
    task_run_id VARCHAR(255) NOT NULL,
    loop_id INTEGER NOT NULL,
    workspace_id VARCHAR(255),
    workspace_path TEXT,
    strategy_id VARCHAR(255),
    factor_names JSONB,
    annualized_return NUMERIC,
    max_drawdown NUMERIC,
    sharpe NUMERIC,
    ic NUMERIC,
    ic_ir NUMERIC,
    win_rate NUMERIC,
    decision TEXT,
    summary JSONB,
    metrics JSONB,
    artifacts JSONB,
    PRIMARY KEY (task_run_id, loop_id)
);
```

**字段说明**:
| 字段名 | 类型 | 说明 | 数据来源 |
|--------|------|------|----------|
| task_run_id | VARCHAR(255) | 任务运行ID | SQLite loops表 |
| loop_id | INTEGER | 循环ID | SQLite loops表 |
| workspace_id | VARCHAR(255) | 工作空间ID | SQLite workspaces表 |
| workspace_path | TEXT | 工作空间路径 | SQLite workspaces表 |
| strategy_id | VARCHAR(255) | 策略ID | 根据模板文件生成 |
| factor_names | JSONB | 因子名称列表 | factor_perf.json |
| annualized_return | NUMERIC | 年化收益率 | factor_perf.json |
| max_drawdown | NUMERIC | 最大回撤 | factor_perf.json |
| sharpe | NUMERIC | 夏普比率 | factor_perf.json |
| ic | NUMERIC | 信息系数 | factor_perf.json |
| ic_ir | NUMERIC | IC信息比率 | factor_perf.json |
| win_rate | NUMERIC | 胜率 | factor_perf.json |
| decision | TEXT | 决策文本 | feedback.json |
| summary | JSONB | 摘要信息 | feedback.json |
| metrics | JSONB | 详细指标 | factor_perf.json |
| artifacts | JSONB | 文件路径 | workspace目录扫描 |

### aistock_model_catalog表

**表结构**:
```sql
CREATE TABLE aistock_model_catalog (
    model_id VARCHAR(255) PRIMARY KEY,
    task_run_id VARCHAR(255),
    loop_id INTEGER,
    workspace_id VARCHAR(255),
    workspace_path TEXT,
    model_type VARCHAR(255),
    model_config JSONB,
    dataset_config JSONB,
    feature_schema JSONB,
    model_artifacts JSONB
);
```

**字段说明**:
| 字段名 | 类型 | 说明 | 数据来源 |
|--------|------|------|----------|
| model_id | VARCHAR(255) | 模型ID（UUID） | 基于配置生成 |
| task_run_id | VARCHAR(255) | 任务运行ID | SQLite loops表 |
| loop_id | INTEGER | 循环ID | SQLite loops表 |
| workspace_id | VARCHAR(255) | 工作空间ID | SQLite workspaces表 |
| workspace_path | TEXT | 工作空间路径 | SQLite workspaces表 |
| model_type | VARCHAR(255) | 模型类型 | model.py/model_meta.json |
| model_config | JSONB | 模型配置 | model_meta.json/YAML |
| dataset_config | JSONB | 数据集配置 | model_meta.json/YAML |
| feature_schema | JSONB | 特征schema | model_meta.json/YAML |
| model_artifacts | JSONB | 模型文件 | workspace目录扫描 |

### aistock_factor_catalog表

**表结构**:
```sql
CREATE TABLE aistock_factor_catalog (
    factor_id VARCHAR(255) PRIMARY KEY,
    factor_name VARCHAR(255),
    factor_type VARCHAR(255),
    source VARCHAR(255),
    description TEXT,
    best_performance TEXT,
    best_loop_task_run_id VARCHAR(255),
    best_loop_id INTEGER
);
```

**字段说明**:
| 字段名 | 类型 | 说明 | 数据来源 |
|--------|------|------|----------|
| factor_id | VARCHAR(255) | 因子ID（UUID） | 基于名称生成 |
| factor_name | VARCHAR(255) | 因子名称 | SQLite/QLib |
| factor_type | VARCHAR(255) | 因子类型 | SQLite/QLib |
| source | VARCHAR(255) | 因子来源 | qlib_alpha360/qlib_alpha158/rdagent_generated |
| description | TEXT | 因子描述 | SQLite/QLib |
| best_performance | TEXT | 最佳性能 | Loop数据关联 |
| best_loop_task_run_id | VARCHAR(255) | 最佳Loop任务ID | Loop数据关联 |
| best_loop_id | INTEGER | 最佳Loop ID | Loop数据关联 |

### aistock_strategy_catalog表

**表结构**:
```sql
CREATE TABLE aistock_strategy_catalog (
    strategy_id VARCHAR(255) PRIMARY KEY,
    strategy_name VARCHAR(255),
    description TEXT,
    example_task_run_id VARCHAR(255),
    example_loop_id INTEGER,
    example_workspace_id VARCHAR(255),
    example_workspace_path TEXT
);
```

**字段说明**:
| 字段名 | 类型 | 说明 | 数据来源 |
|--------|------|------|----------|
| strategy_id | VARCHAR(255) | 策略ID（UUID） | 基于配置生成 |
| strategy_name | VARCHAR(255) | 策略名称 | SQLite |
| description | TEXT | 策略描述 | SQLite |
| example_task_run_id | VARCHAR(255) | 示例任务ID | workspace_example |
| example_loop_id | INTEGER | 示例Loop ID | workspace_example |
| example_workspace_id | VARCHAR(255) | 示例工作空间ID | workspace_example |
| example_workspace_path | TEXT | 示例工作空间路径 | workspace_example |

---

## 字段映射说明

### Loop Catalog字段映射

| JSON字段 | 数据库字段 | 数据来源 | 提取方式 |
|----------|------------|----------|----------|
| task_run_id | task_run_id | loops表 | SQL查询 |
| loop_id | loop_id | loops表 | SQL查询 |
| workspace_id | workspace_id | workspaces表 | SQL查询 |
| workspace_path | workspace_path | workspaces表 | SQL查询 |
| strategy_id | strategy_id | 模板文件 | UUID生成 |
| factor_names | factor_names | factor_perf.json | JSON解析 |
| annualized_return | annualized_return | factor_perf.json | JSON解析 |
| max_drawdown | max_drawdown | factor_perf.json | JSON解析 |
| sharpe | sharpe | factor_perf.json | JSON解析 |
| ic | ic | factor_perf.json | JSON解析 |
| ic_ir | ic_ir | factor_perf.json | JSON解析 |
| win_rate | win_rate | factor_perf.json | JSON解析 |
| decision | decision | feedback.json | JSON解析 |
| summary | summary | feedback.json | JSON解析 |
| metrics | metrics | factor_perf.json | JSON解析 |
| artifacts | artifacts | workspace目录 | 文件扫描 |

### Model Catalog字段映射

| JSON字段 | 数据库字段 | 数据来源 | 提取方式 |
|----------|------------|----------|----------|
| model_id | model_id | 配置哈希 | UUID生成 |
| task_run_id | task_run_id | loops表 | SQL查询 |
| loop_id | loop_id | loops表 | SQL查询 |
| workspace_id | workspace_id | workspaces表 | SQL查询 |
| workspace_path | workspace_path | workspaces表 | SQL查询 |
| model_type | model_type | model.py/model_meta.json | 文件解析 |
| model_config | model_config | model_meta.json/YAML | JSON/YAML解析 |
| dataset_config | dataset_config | model_meta.json/YAML | JSON/YAML解析 |
| feature_schema | feature_schema | model_meta.json/YAML | JSON/YAML解析 |
| model_artifacts | model_artifacts | workspace目录 | 文件扫描 |

---

## 常见问题与解决方案

### 问题1: Loop IC值全部为0

**原因**: IC值提取逻辑错误，匹配条件不正确

**解决方案**: 修改`export_aistock_loop_catalog.py`中的IC值提取逻辑
```python
# 修改前
if ic is None and ("ic" in lk and "mean" in lk or lk.endswith(".ic")):
    ic = v

# 修改后
if ic is None and (k == "IC" or k == "ic" or lk == "ic"):
    ic = v
```

**验证方法**:
```bash
cd F:\Dev\AIstock\backend
python verify_database_accuracy.py
```

### 问题2: 模型类型全部显示为LGBModel

**原因**: 模型类型获取优先级不正确，没有优先从model.py提取

**解决方案**: 修改`export_aistock_model_catalog.py`中的模型类型提取优先级
```python
# 调整优先级顺序
# 1. 优先从model.py提取
model_py = ws_root / "model.py"
if model_py.exists():
    # 提取类定义
    ...

# 2. 然后从model_meta.json读取
if model_type is None:
    model_meta = _load_json_if_exists(ws_root / "model_meta.json")
    ...
```

**验证方法**:
```bash
cd F:\Dev\AIstock\backend
python verify_model_export.py
```

### 问题3: 数据导入失败

**原因**: 可能是数据库连接问题、JSON文件格式问题或字段类型不匹配

**解决方案**:
1. 检查数据库连接配置（.env文件）
2. 验证JSON文件格式是否正确
3. 查看导入脚本输出的错误信息
4. 检查数据库表结构是否与JSON字段匹配

**验证方法**:
```bash
cd F:\Dev\AIstock\backend
python reimport_all_catalogs.py
```

### 问题4: 模型缺少task_run_id、loop_id、workspace_path字段

**原因**: 导出脚本没有正确提取这些字段

**解决方案**: 确认`export_aistock_model_catalog.py`中正确包含了这些字段
```python
entry: dict[str, Any] = {
    "task_run_id": src["task_run_id"],
    "loop_id": src["loop_id"],
    "workspace_id": src["workspace_id"],
    "workspace_path": src["workspace_path"],
}
```

**验证方法**:
```bash
cd F:\Dev\AIstock\backend
python verify_database_accuracy.py
```

### 问题5: 因子与Loop关联失败

**原因**: 因子的best_loop_task_run_id和best_loop_id字段没有正确更新

**解决方案**: 执行因子与Loop关联脚本
```bash
cd F:\Dev\AIstock\backend
python update_factor_loop_association.py
```

### 问题6: 策略与Loop关联失败

**原因**: 策略的best_loop_task_run_id和best_loop_id字段没有正确更新

**解决方案**: 执行策略与Loop关联脚本
```bash
cd F:\Dev\AIstock\backend
python update_strategy_loop_association.py
```

---

## 附录

### 完整数据同步脚本

创建一个批处理脚本`sync_rdagent_data.bat`：

```batch
@echo off
echo ========================================
echo AIstock RD-Agent数据同步
echo ========================================

echo.
echo 步骤1: 导出RD-Agent数据
cd F:\Dev\RD-Agent-main\tools
python export_aistock_loop_catalog.py --registry-sqlite F:\Dev\RD-Agent-main\RDagentDB\registry.sqlite --output F:\Dev\RD-Agent-main\RDagentDB\aistock\loop_catalog.json
python export_aistock_model_catalog.py --registry-sqlite F:\Dev\RD-Agent-main\RDagentDB\registry.sqlite --output F:\Dev\RD-Agent-main\RDagentDB\aistock\model_catalog.json
python export_aistock_factor_catalog.py --registry-sqlite F:\Dev\RD-Agent-main\RDagentDB\registry.sqlite --output F:\Dev\RD-Agent-main\RDagentDB\aistock\factor_catalog.json
python export_aistock_strategy_catalog.py --registry-sqlite F:\Dev\RD-Agent-main\RDagentDB\registry.sqlite --output F:\Dev\RD-Agent-main\RDagentDB\aistock\strategy_catalog.json

echo.
echo 步骤2: 复制JSON文件到AIstock
Copy-Item -Path "F:\Dev\RD-Agent-main\RDagentDB\aistock\loop_catalog.json" -Destination "F:\Dev\AIstock\backend\data\aistock_loop_catalog.json" -Force
Copy-Item -Path "F:\Dev\RD-Agent-main\RDagentDB\aistock\model_catalog.json" -Destination "F:\Dev\AIstock\backend\data\aistock_model_catalog.json" -Force
Copy-Item -Path "F:\Dev\RD-Agent-main\RDagentDB\aistock\factor_catalog.json" -Destination "F:\Dev\AIstock\backend\data\aistock_factor_catalog.json" -Force
Copy-Item -Path "F:\Dev\RD-Agent-main\RDagentDB\aistock\strategy_catalog.json" -Destination "F:\Dev\AIstock\backend\data\aistock_strategy_catalog.json" -Force

echo.
echo 步骤3: 清空AIstock数据库表
cd F:\Dev\AIstock\backend
python truncate_all_tables.py

echo.
echo 步骤4: 导入数据到AIstock数据库
python reimport_all_catalogs.py

echo.
echo 步骤5: 验证数据准确性
python verify_database_accuracy.py
python final_data_status.py

echo.
echo ========================================
echo 数据同步完成
echo ========================================
pause
```

### 数据统计参考

**当前数据统计**（2026-01-10）:
- Loop总数: 136个
- Model总数: 501个
- Factor总数: 778个
  - qlib_alpha360: 352个
  - rdagent_generated: 228个
  - qlib_alpha158: 158个
- Strategy总数: 155个

**模型类型分布**:
- LGBModel: 458个
- GRU_Attention_Residual_Model: 12个
- Transformer_TimeSeries_Model: 9个
- LSTM_Attention_Model: 4个
- 其他模型: 18个

### 联系方式

如有问题，请联系开发团队或查看项目文档。

---

**文档版本**: v1.0  
**最后更新**: 2026-01-10  
**维护者**: AIstock开发团队
