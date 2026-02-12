# SOTA 因子 Catalog 入库设计方案

## 1. 现状分析

### 1.1 当前问题

Task 同步流程（`sync_task_from_log`）完成后，SOTA 因子的代码文件被下载到 `rdagent_assets/rdagent_tasks/{task_id}/` 目录，但**没有将因子元数据写入 `aistock_factor_catalog` 表**。导致前端 `/rdagent/factors` 页面查询 `aistock_factor_catalog` 时返回空数据。

### 1.2 现有代码结构

| 模块 | 文件 | 职责 | 现状 |
|------|------|------|------|
| Task 同步 | `backend/services/rdagent_task_sync_service.py` | 从 RD-Agent API 下载因子代码、模型权重、生成 factor_order.json | ✅ 正常工作，但不写 factor_catalog |
| Catalog ETL | `backend/services/rdagent_catalog_etl_service.py` | 从 JSON 文件导入因子/策略/loop 到 PG | ⚠️ 仅支持从 JSON 文件导入，不与 Task 同步联动 |
| API 客户端 | `backend/services/rdagent_results_api_client.py` | 封装 RD-Agent HTTP API 调用 | ✅ 已有 `get_task_sota_factor_anchor`、`get_task_loops` 等 |
| Catalog Admin | `backend/routers/rdagent_catalog_admin.py` | 因子查询 API（`/rdagent/catalogs/factors`） | ✅ 正常，但表中无数据 |
| 前端因子页 | `frontend/src/app/rdagent/factors/page.tsx` | 展示因子列表 | ✅ 正常调用 API，但返回空 |

### 1.3 数据流断点

```
RD-Agent API ──(sota_factor_anchor)──> Task同步服务 ──(下载factor.py)──> 本地文件
                                                          │
                                                          ╳ ← 断点：未写入 aistock_factor_catalog
                                                          │
                                          前端 factors 页面 ──(查询空表)──> 无数据
```

### 1.4 可废弃的旧代码

以下函数基于 JSON 文件导入，在新方案中将被 Task 同步自动入库替代：

| 函数 | 文件 | 说明 |
|------|------|------|
| `import_factor_catalog_from_json()` | `rdagent_catalog_etl_service.py` | 从 `factor_catalog.json` 导入，依赖 RD-Agent 侧离线导出 |
| `import_all_catalogs_from_root()` | `rdagent_catalog_etl_service.py` | 批量从 JSON 根目录导入所有 catalog |
| `_build_sota_factor_payload_from_scan()` | `rdagent_catalog_etl_service.py` | 从 scan 结果构造因子 payload |
| `/catalogs/import` POST 端点 | `rdagent_catalog_admin.py` | 手动触发 JSON 导入的 API |

> 注意：`import_factor_catalog_from_payload()` 函数本身的 UPSERT 逻辑可复用，只是数据来源从 JSON 文件改为 Task 同步 API。

---

## 2. 设计目标

1. **Task 同步后自动入库**：`sync_task_from_log` 成功后，自动将 SOTA 因子写入 `aistock_factor_catalog`
2. **完整元数据**：入库因子名称、表达式/公式、回测指标（IC、年化收益、最大回撤、Sharpe 等）、因子代码路径
3. **因子去重**：相同逻辑的因子（即使名称不同）需要去重，采用代码相似度 + 计算结果相关性两阶段判定
4. **回测指标来源**：从 `sota_factor_anchor` API 获取 SOTA 因子所在 loop 的回测指标
5. **前端可见**：入库后前端 `/rdagent/factors` 页面能正常展示因子数据

---

## 3. 数据来源与字段映射（基于实际 API 验证）

### 3.1 API 实际返回结构（2026-02-12 验证）

经实际调用验证，三个 API 的数据分布如下：

| 数据 | API | 返回内容 |
|------|-----|---------|
| 因子代码 file key | `sota_factor_anchor` | `resolved_factor_entry_key`、`based_factor_entries[].resolved_factor_entry_key` |
| SOTA 因子名称列表 | `v2_alignment_preview` | `sota_factors: ["name1", "name2", ...]`（纯字符串列表） |
| 回测指标 | `/tasks/{tid}/loops` | 每个 loop 含 `annualized_return`、`max_drawdown`、`information_ratio`、`is_sota` |
| 每轮测试的因子名 | `/tasks/{tid}/loops` | `tested_factors: ["name1", "name2"]`（纯字符串列表） |
| 因子表达式/公式 | **无 API 提供** | 只能从下载的 factor.py 代码中提取 |

### 3.2 `sota_factor_anchor` 实际返回结构

```json
{
  "ok": true,
  "task_id": "2026-02-06_16-50-41-691996",
  "last_sota_factor_index": 13,
  "model_exp_index": null,
  "resolved_factor_entry_key": "factor.py",
  "based_factor_entries": [
    {
      "based_index": 0,
      "type": "QlibFactorExperiment",
      "resolved_factor_entry_key": "based_factor_0/factor.py"
    }
  ],
  "resolved_model_weight_key": "model.pkl",
  "resolved_model_weight_source": "factor_exp"
}
```

> 注意：`sota_factor_anchor` 不含因子名称、表达式、回测指标。

### 3.3 `/tasks/{tid}/loops` 实际返回结构

```json
{
  "loops": [
    {
      "loop_id": 0,
      "is_sota": true,
      "annualized_return": 0.48489,
      "max_drawdown": -0.283,
      "information_ratio": 2.61166,
      "tested_factors": ["size_log_mv", "mf_elg_net_amt_ratio_5d"],
      "all_factors": ["size_log_mv", "mf_elg_net_amt_ratio_5d"],
      "tested_count": 2,
      "total_count": 2,
      "hypothesis": "...",
      "feedback": "..."
    }
  ]
}
```

### 3.4 数据获取策略

由于回测指标是 loop 级别（非因子级别），需要建立因子→loop 的关联：

1. 从 `v2_alignment_preview` 获取完整 SOTA 因子名称列表
2. 从 `/tasks/{tid}/loops` 获取所有 loop 的 `tested_factors` 和回测指标
3. 对每个 SOTA 因子，找到它首次被测试的 SOTA loop，取该 loop 的回测指标
4. 因子表达式从下载的 factor.py 代码中用正则提取（docstring 或注释）

### 3.5 字段映射到 `aistock_factor_catalog`

| aistock_factor_catalog 字段 | 数据来源 | 说明 |
|----------------------------|---------|------|
| `factor_name` | `v2_alignment_preview.sota_factors[]` | 因子名称 |
| `source` | 固定 `"rdagent_task_sync"` | 标识来源为 Task 同步 |
| `expression` | 从 factor.py 代码提取 | 因子计算核心代码片段 |
| `source_task_id` | `task_id` | 来源 Task ID |
| `source_code_relpath` | `"factor.py"` 或 `"based_factors/based_factor_{i}.py"` | 因子代码在 AIstock 侧的相对路径 |
| `source_code_origin` | `resolved_factor_entry_key` | RD-Agent 侧原始 key |
| `source_loop_tag` | 因子首次出现的 loop_id | SOTA 因子所在 loop 编号 |
| `is_sota_factor` | `true` | 标记为 SOTA 因子 |
| `first_sota_task_id` | `task_id`（仅首次入库时写入） | 首次进入 SOTA 的 task |
| `performance_metrics` | loop 的完整指标 JSON | 完整回测指标 |
| `best_performance_sharpe` | `loop.information_ratio` | 信息比率（RD-Agent 用此代替 Sharpe） |
| `best_performance_ann_ret` | `loop.annualized_return` | 年化收益率 |
| `catalog_version` | `"task_sync_v1"` | 版本标识 |
| `generated_at_utc` | 同步时间 | UTC 时间戳 |
| `catalog_source` | `"rdagent_task_sync"` | 来源标识 |

### 3.4 需扩展的字段（ALTER TABLE）

当前 `aistock_factor_catalog` 表缺少以下字段，需要新增：

| 新字段 | 类型 | 说明 |
|--------|------|------|
| `source_task_id` | `TEXT` | 来源 Task ID |
| `source_code_relpath` | `TEXT` | 因子代码在 AIstock 侧的相对路径 |
| `source_code_origin` | `TEXT` | RD-Agent 侧原始 asset key |
| `source_loop_tag` | `TEXT` | SOTA 因子所在 loop 标签 |
| `source_index` | `INTEGER` | 因子在 SOTA 列表中的序号 |
| `ic` | `DOUBLE PRECISION` | IC 值 |
| `icir` | `DOUBLE PRECISION` | ICIR 值 |
| `max_drawdown` | `DOUBLE PRECISION` | 最大回撤 |
| `annualized_return` | `DOUBLE PRECISION` | 年化收益率 |
| `sharpe` | `DOUBLE PRECISION` | Sharpe 比率 |
| `dedup_hash` | `TEXT` | 去重哈希值（代码归一化后的 SHA256） |
| `dedup_group_id` | `TEXT` | 去重分组 ID（相同逻辑因子共享同一 group） |
| `is_dedup_primary` | `BOOLEAN` | 是否为去重组内的主因子 |
| `code_text` | `TEXT` | 因子计算核心代码（用于去重比较） |

> 注：`source_task_id`、`source_code_relpath`、`source_code_origin`、`source_loop_tag`、`source_index` 在 ETL 服务的 INSERT SQL 中已有引用，但 schema DDL 中未显式 CREATE。需在 `init_rdagent_catalog_schema.py` 中补充 ALTER TABLE。

---

## 4. 因子去重方案

### 4.1 两阶段去重策略

```
新因子 ──> 阶段1: 代码相似度快筛 ──(候选集)──> 阶段2: 计算结果相关性精判 ──> 入库/标记重复
```

#### 阶段 1：代码相似度快筛（静态分析）

1. **代码归一化**：提取因子 `factor.py` 中 `# BEGIN FACTOR COMPUTATION AREA` 到 `# END FACTOR COMPUTATION AREA` 之间的核心计算代码
2. **AST 归一化**：
   - 移除注释和空行
   - 变量名统一替换为占位符（`var_0`, `var_1`, ...）
   - 字符串常量统一替换
   - 数值常量保留（因为窗口大小等参数是因子逻辑的关键部分）
3. **生成 `dedup_hash`**：对归一化后的代码文本计算 SHA256
4. **快速匹配**：新因子的 `dedup_hash` 与已有因子比较，完全匹配则直接判定为重复

#### 阶段 2：计算结果相关性精判（动态分析）

当阶段 1 未命中精确匹配，但代码文本编辑距离相似度 > 0.7 时，进入阶段 2：

1. **触发条件**：代码归一化后的 Levenshtein 相似度 > 0.7
2. **相关性计算**：使用最近 N 个交易日的因子值，计算 Spearman 秩相关系数
3. **判定阈值**：Spearman ρ > 0.95 判定为相同逻辑因子
4. **延迟执行**：阶段 2 可异步执行，不阻塞 Task 同步流程

#### 去重结果处理

| 情况 | 处理方式 |
|------|---------|
| `dedup_hash` 完全匹配 | 不新增记录，更新已有记录的 `performance_metrics`（如果新指标更优） |
| Spearman ρ > 0.95 | 新增记录但标记 `is_dedup_primary = false`，设置相同 `dedup_group_id` |
| 无匹配 | 正常入库，`is_dedup_primary = true`，生成新 `dedup_group_id` |

### 4.2 去重数据结构

```sql
-- 去重组表（可选，也可直接在 factor_catalog 中用 dedup_group_id 关联）
CREATE TABLE IF NOT EXISTS aistock_factor_dedup_groups (
    group_id       TEXT PRIMARY KEY,
    primary_factor TEXT NOT NULL,       -- 组内主因子名称
    member_count   INTEGER DEFAULT 1,
    created_at_utc TIMESTAMPTZ DEFAULT NOW()
);
```

---

## 5. 实现方案

### 5.1 核心流程：Task 同步后自动入库

在 `RDAgentTaskSyncService.sync_task_from_log()` 的步骤 7（更新 task catalog 表）之后，新增步骤 8：

```
步骤 7: 更新 aistock_task_catalog（已有）
    │
    ▼
步骤 8: 因子 Catalog 入库（新增）
    ├── 8.1 从 anchor_resp 提取 SOTA 因子列表和回测指标
    ├── 8.2 对每个因子执行去重检查（阶段 1）
    ├── 8.3 构造 factor_catalog payload
    ├── 8.4 调用 import_factor_catalog_from_payload() 写入 PG
    └── 8.5 异步触发阶段 2 去重（如有候选）
```

### 5.2 新增服务函数

在 `backend/services/` 下新增 `rdagent_factor_catalog_sync.py`：

```python
def sync_factors_from_task_anchor(
    task_id: str,
    anchor_resp: dict,          # sota_factor_anchor API 返回
    v2_preview_data: dict,      # v2_alignment_preview API 返回
    task_dir: str,              # AIstock 侧 task 资产目录
) -> FactorSyncResult:
    """
    从 Task 同步数据中提取 SOTA 因子并入库到 aistock_factor_catalog。

    流程：
    1. 从 anchor_resp.sota_factors 提取因子名称、表达式、回测指标
    2. 从 task_dir 读取因子代码，提取核心计算逻辑
    3. 执行去重检查（阶段 1：代码哈希）
    4. 构造 payload 并调用 import_factor_catalog_from_payload()
    5. 返回入库结果
    """
```

```python
def compute_factor_dedup_hash(factor_code: str) -> str:
    """
    对因子代码进行归一化处理并生成去重哈希。

    1. 提取 BEGIN/END FACTOR COMPUTATION AREA 之间的代码
    2. AST 归一化（变量名替换、移除注释）
    3. SHA256 哈希
    """
```

```python
def check_factor_dedup(
    factor_name: str,
    dedup_hash: str,
    code_text: str,
) -> Optional[DedupResult]:
    """
    检查因子是否与已有因子重复。

    返回 None 表示无重复，返回 DedupResult 包含匹配的因子信息。
    """
```

### 5.3 回测指标提取逻辑

从 `sota_factor_anchor` 返回的 metrics 中提取标准化指标：

```python
METRICS_MAPPING = {
    "ic": ["IC", "Rank IC", "1day.Rank IC.mean", "1day.IC.mean"],
    "icir": ["ICIR", "Rank ICIR", "1day.Rank IC.std", "1day.IC.std"],
    "annualized_return": [
        "1day.excess_return_with_cost.annualized_return",
        "1day.excess_return_without_cost.annualized_return",
        "annual_return",
    ],
    "max_drawdown": [
        "1day.excess_return_with_cost.max_drawdown",
        "1day.excess_return_without_cost.max_drawdown",
    ],
    "sharpe": [
        "1day.excess_return_with_cost.information_ratio",
        "1day.excess_return_without_cost.information_ratio",
        "sharpe_ratio",
        "information_ratio",
    ],
}
```

> 注：此映射逻辑与 `rdagent_catalog_etl_service.py` 中 `_metric()` 函数一致，应抽取为共享工具函数。

### 5.4 修改点清单

| 文件 | 修改内容 |
|------|---------|
| `backend/services/rdagent_factor_catalog_sync.py` | **新增**：因子入库核心逻辑、去重逻辑 |
| `backend/services/rdagent_task_sync_service.py` | 在 `sync_task_from_log()` 步骤 7 后新增步骤 8 调用因子入库 |
| `scripts/init_rdagent_catalog_schema.py` | 新增 `source_task_id`、`dedup_hash` 等字段的 ALTER TABLE |
| `backend/services/rdagent_catalog_etl_service.py` | 抽取 `_metric()` 映射为共享工具；旧 JSON 导入函数标记 deprecated |
| `backend/routers/rdagent_catalog_admin.py` | 新增手动补录/重新入库 API 端点（作为补充） |

### 5.5 入库时序图

```
sync_task_from_log(task_id)
    │
    ├── 步骤1: sota_factor_anchor API ──> anchor_resp (含 sota_factors + metrics)
    ├── 步骤2: 下载模型权重
    ├── 步骤3: v2_alignment_preview API ──> v2_preview_data
    ├── 步骤4: 下载因子代码 ──> factor.py, based_factors/
    ├── 步骤5: 生成 factor_order.json
    ├── 步骤6: 生成 manifest.json
    ├── 步骤7: 更新 aistock_task_catalog
    │
    └── 步骤8 [新增]: sync_factors_from_task_anchor()
         ├── 遍历 anchor_resp.sota_factors
         │    ├── 提取 factor_name, factor_formulation, metrics
         │    ├── 读取 factor.py 提取核心代码
         │    ├── compute_factor_dedup_hash(code) ──> dedup_hash
         │    ├── check_factor_dedup(name, hash, code)
         │    │    ├── hash 匹配 ──> 更新已有记录指标（如更优）
         │    │    ├── 代码相似 > 0.7 ──> 标记候选，异步阶段2
         │    │    └── 无匹配 ──> 新增记录
         │    └── 构造 factor payload
         ├── 批量 UPSERT 到 aistock_factor_catalog
         └── 返回 FactorSyncResult
```

---

## 6. 数据库 Schema 变更

### 6.1 `aistock_factor_catalog` 新增字段

```sql
-- 回测指标独立字段（方便查询和排序）
ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS ic DOUBLE PRECISION;
ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS icir DOUBLE PRECISION;
ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS max_drawdown DOUBLE PRECISION;
ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS annualized_return DOUBLE PRECISION;
ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS sharpe DOUBLE PRECISION;

-- 去重相关字段
ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS dedup_hash TEXT;
ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS dedup_group_id TEXT;
ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS is_dedup_primary BOOLEAN DEFAULT TRUE;
ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS code_text TEXT;

-- 确保 source_task_id 等字段存在（ETL 代码已引用但 DDL 未显式创建）
ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS source_task_id TEXT;
ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS source_code_relpath TEXT;
ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS source_code_origin TEXT;
ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS source_loop_tag TEXT;
ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS source_index INTEGER;

-- 索引
CREATE INDEX IF NOT EXISTS idx_factor_catalog_dedup_hash ON aistock_factor_catalog(dedup_hash);
CREATE INDEX IF NOT EXISTS idx_factor_catalog_dedup_group ON aistock_factor_catalog(dedup_group_id);
CREATE INDEX IF NOT EXISTS idx_factor_catalog_source_task ON aistock_factor_catalog(source_task_id);
CREATE INDEX IF NOT EXISTS idx_factor_catalog_is_sota ON aistock_factor_catalog(is_sota_factor);
```

---

## 7. API 变更

### 7.1 现有 API 无需修改

- `GET /rdagent/catalogs/factors` — 已能查询 `aistock_factor_catalog`，入库后自动可见
- `GET /rdagent/catalogs/factors/{factor_name}` — 同上
- `GET /rdagent/catalogs/factors/{factor_name}/source-code` — 已支持通过 `source_task_id` + `source_code_relpath` 下载源码

### 7.2 新增 API

| 端点 | 方法 | 说明 |
|------|------|------|
| `POST /rdagent/catalogs/factors/resync-from-task` | POST | 手动触发从指定 Task 重新入库因子（补录用） |
| `GET /rdagent/catalogs/factors/dedup-groups` | GET | 查询去重分组列表 |
| `POST /rdagent/catalogs/factors/dedup-check` | POST | 手动触发全量去重检查 |

---

## 8. 前端影响

前端 `frontend/src/app/rdagent/factors/page.tsx` 已实现因子列表展示，包括：
- 因子名称、表达式、来源
- 回测指标（通过 `/factor-loop-best` API 聚合）
- 源码下载

入库后无需修改前端代码即可展示数据。后续可增强：
- 展示去重分组信息（`dedup_group_id`）
- 展示因子代码预览
- 因子组合选择器（用于自由组合选股）

---

## 9. 可废弃代码

入库逻辑改为 Task 同步自动触发后，以下旧代码可标记废弃或删除：

| 代码 | 文件 | 原因 |
|------|------|------|
| `import_factor_catalog_from_json()` | `rdagent_catalog_etl_service.py` | 不再需要从 JSON 文件导入因子 |
| `import_all_catalogs_from_root()` | `rdagent_catalog_etl_service.py` | 不再需要批量 JSON 导入 |
| `_build_sota_factor_payload_from_scan()` | `rdagent_catalog_etl_service.py` | scan 模式不再使用 |
| `import_alpha158_meta_from_json()` | `rdagent_catalog_etl_service.py` | Alpha158 元数据应通过 API 获取 |
| `import_alpha360_meta_from_json()` | `rdagent_catalog_etl_service.py` | Alpha360 元数据应通过 API 获取 |
| `POST /catalogs/import` 端点 | `rdagent_catalog_admin.py` | 手动 JSON 导入不再需要 |

> 保留 `import_factor_catalog_from_payload()` 函数，因为新方案复用其 UPSERT 逻辑。

---

## 10. 实施步骤

| 步骤 | 内容 | 依赖 |
|------|------|------|
| 1 | 执行 Schema 变更（ALTER TABLE 新增字段） | 无 |
| 2 | 实现 `rdagent_factor_catalog_sync.py`（入库 + 去重阶段 1） | 步骤 1 |
| 3 | 修改 `sync_task_from_log()` 在步骤 7 后调用因子入库 | 步骤 2 |
| 4 | 实现去重阶段 2（异步相关性计算） | 步骤 2 |
| 5 | 新增手动补录 API 端点 | 步骤 2 |
| 6 | 对已同步的 Task 执行一次全量补录 | 步骤 3 |
| 7 | 清理旧代码，标记废弃函数 | 步骤 6 验证通过后 |

---

## 11. 风险与注意事项

1. **`sota_factor_anchor` 返回的 metrics 结构不确定**：不同 Task 的 metrics key 可能不一致，需要健壮的多 key 探测逻辑（参考 `_metric()` 函数）
2. **去重阶段 2 的性能**：计算因子值的 Spearman 相关性需要实际运行因子代码，可能耗时较长，必须异步执行
3. **因子代码质量**：如前文分析的 `factor.py` 正则 bug，入库时应记录代码质量诊断信息
4. **幂等性**：同一 Task 多次同步不应产生重复记录，依赖 `ON CONFLICT (factor_name, source) DO UPDATE` 保证
5. **向后兼容**：新增字段使用 `ADD COLUMN IF NOT EXISTS`，不影响现有数据
