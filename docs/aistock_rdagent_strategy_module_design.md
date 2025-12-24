# AIstock 策略管理模块（RD-Agent 集成）全量设计方案

> 目标：将 RD-Agent 的实验产物（尤其 `signals.parquet`）以**人工确认**方式导入 AIstock，落盘到本地 Strategy Store，并将元数据与 signals 入库到 PostgreSQL（`trading` schema）。
>
> Phase 1：以导入的 `signals.parquet` 实现策略信号可视化/选股展示（Next.js UI + FastAPI）。
>
> Phase 2：AIstock 侧统一数据服务层 + 模型推理服务层，实现实时/定时推理与信号生成，并可接入模拟盘/实盘执行。

---

## 0. 文档维护约定

### 0.1 单一事实来源（SSOT）
- 本文档是“RD-Agent → AIstock 策略管理模块”的**单一事实来源**。
- 后续开发过程若出现：
  - **设计变更**
  - **实现偏差**
  - **字段/接口调整**

  必须在本文档的：
  - `变更记录`
  - `接口/表结构`

  同步更新。

### 0.2 进度跟踪
- 每个 Phase 的状态在 `15. 开发进度` 维护。

---

## 1. 背景与核心约束

### 1.1 角色边界
- **RD-Agent**：研究/产出 artifacts（回测、模型、配置、`signals.parquet` 等）+ 维护 registry.sqlite；不参与交易。
- **AIstock**：
  - Phase 1：导入、存储、入库、展示。
  - Phase 2：统一数据获取 + 模型推理 + 信号生成 +（可选）交易执行/模拟。

### 1.2 强约束
- 导入为**手动触发**，必须人工确认候选策略后才导入。
- AIstock 必须支持：
  - **策略形态**：`portfolio`（组合）/ `single_symbol`（单标的触发）
  - **输出模式**：`target_weight` / `topk`
- **signals 需要入库**（已确认）：采用方案 1
  - RD-Agent signals 入库到：`trading.rdagent_signal`

### 1.3 与 RD-Agent Registry 设计的对齐情况
- Registry 与 workspace 元信息完全以 RD-Agent 文档为权威：
  - 只读 SQLite：`task_runs` / `loops` / `workspaces` / `artifacts` / `artifact_files` 五张表。
  - 按 `task_run → loop → best_workspace_id → manifest.json` 链路定位成果与 artifacts。
  - `signals.parquet` / `signals.json` 作为**可执行信号主入口**，schema 以 RD-Agent 侧定义为准。
- AIstock 当前实现已遵循上述契约：
  - `backend/services/rdagent_registry_service.py` 按 RD-Agent 设计读取 SQLite。
  - `backend/services/rdagent_signals_service.py` 使用 `pyarrow` 读取 `signals.parquet`，并按约定字段做规范化/入库。
  - 不回写、不修改 RD-Agent registry，仅作为只读消费者。

---

## 2. 术语与对象模型

### 2.1 术语
- **Source**：策略来源（`rdagent` / 未来其它来源）。
- **Strategy**：AIstock 统一策略实体。
- **Strategy Version**：策略版本（一次导入对应一个版本）。
- **Artifact**：策略产物文件集合（signals、schema、模型、配置、summary、manifest 等）。
- **Signal**：策略输出（TopK 或目标权重）。

### 2.2 策略维度（必须固定）
- `strategy_kind`：`portfolio` | `single_symbol`
- `output_mode`：`target_weight` | `topk`

---

## 3. 配置（.env）

建议新增/确认以下环境变量：

- `RDAGENT_REGISTRY_SQLITE_PATH`：RD-Agent registry.sqlite 路径
- `AISTOCK_STRATEGY_STORE_ROOT`：本地策略仓库根目录
- （可选）`RDAGENT_WORKSPACE_ROOT`：用于定位 workspace（若 registry 中存相对路径）

---

## 4. 本地策略仓库（Strategy Store）

根目录：`{AISTOCK_STRATEGY_STORE_ROOT}`

建议结构：

```
{AISTOCK_STRATEGY_STORE_ROOT}/
  sources/
    rdagent/
      {source_strategy_key}/
        versions/
          {version_tag}/
            artifacts/
              signals.parquet
              signals.json
              ret_schema.parquet
              ret_schema.json
              qlib_res.csv
              manifest.json
              summary.json
              conf*.yaml
              mlruns/...
            import/
              import_meta.json
  inference_runs/
    {strategy_id}/
      {run_id}/
        inputs_snapshot.json
        signals.parquet
        logs.txt
```

说明：
- `source_strategy_key`：用于唯一定位 RD-Agent 的策略候选（建议由 workspace_id/task_run_id 等组合生成）。
- `version_tag`：用于区分导入版本（建议使用 task_run_id + loop_id/step 等稳定信息）。

---

## 5. 数据契约：signals.parquet（规范化）

### 5.1 权威来源
- 以 RD-Agent 集成文档对 `signals.parquet/json` 的约定为“权威契约”。
- AIstock 导入时额外做 `schema 探测 + 容错映射`。

### 5.2 AIstock 内部统一信号逻辑模型
最小字段集合（必须能得到）：
- `trade_date`（date 或 timestamptz）
- `symbol`（统一为 `000001.SZ` 风格）
- `output_mode`（`topk` / `target_weight`）

可选字段：
- `rank`（TopK）
- `score`（TopK 或其它评分）
- `target_weight`（目标权重）
- `action`（buy/sell/hold/long/short 等）
- `meta`（jsonb：保留原始字段）

### 5.3 规范化组件
- `SignalNormalizer`：
  - 输入：parquet DataFrame
  - 输出：统一字段 DataFrame
  - 能力：列名映射、类型转换、symbol 格式规范、缺失字段校验

- `SymbolMapper`：
  - 统一 market 后缀与代码格式

---

## 6. PostgreSQL 设计（trading schema）

> 目标：
> - 存储策略元数据、版本、artifact 文件信息
> - signals 明细入库（可按 `strategy_version_id` 快速查询）

### 6.1 建议新增表

#### 6.1.1 `trading.strategy_source`
用于支持多策略来源（RD-Agent 只是其一）。

字段建议：
- `source_id` (bigserial pk)
- `source_type` (text) 例如：`rdagent`
- `name` (text)
- `description` (text)
- `created_at` (timestamptz)

#### 6.1.2 `trading.strategy`
AIstock 统一策略实体。

字段建议：
- `strategy_id` (uuid pk)
- `source_id` (fk)
- `source_strategy_key` (text)（与 source_id 组合唯一）
- `strategy_name` (text)
- `strategy_kind` (text) CHECK in (`portfolio`,`single_symbol`)
- `output_mode` (text) CHECK in (`target_weight`,`topk`)
- `universe_spec` (jsonb, nullable)
- `enabled` (bool)
- `created_at`, `updated_at`

#### 6.1.3 `trading.strategy_version`
字段建议：
- `strategy_version_id` (uuid pk)
- `strategy_id` (fk)
- `version_tag` (text)
- `manifest_json` (jsonb)
- `artifact_root_path` (text)
- `import_status` (text)
- `created_at`

#### 6.1.4 `trading.strategy_artifact_file`
字段建议：
- `file_id` (uuid pk)
- `strategy_version_id` (fk)
- `kind` (text)
- `path_rel` (text)
- `sha256` (text, nullable)
- `size_bytes` (bigint)
- `created_at`

#### 6.1.5 `trading.rdagent_signal`（signals 入库主表）
字段建议：
- `id` bigserial pk
- `strategy_id` uuid
- `strategy_version_id` uuid
- `trade_date` date (或 timestamptz)
- `symbol` text
- `output_mode` text
- `rank` int null
- `score` double precision null
- `target_weight` numeric(10,6) null
- `action` text null
- `meta` jsonb null
- `created_at` timestamptz default now()

建议约束：
- `UNIQUE(strategy_version_id, trade_date, symbol)`（若存在同日多条，可增加 `signal_key`）

建议索引：
- `(strategy_version_id, trade_date)`
- `(strategy_version_id, symbol)`
- `(strategy_version_id, trade_date, rank)`

### 6.2 SQL 迁移策略
- 当前工程可能没有统一的 migration 框架（需确认）。
- Phase 1 建议先提供一份可重复执行的 SQL：
  - `CREATE TABLE IF NOT EXISTS ...`
  - `CREATE INDEX IF NOT EXISTS ...`

---

## 7. 后端（FastAPI）设计

### 7.1 现有工程适配点（已确认）
- FastAPI 入口：`backend/main.py`
- DB：`backend/db/pg_pool.py -> get_conn()`
- 已存在入库范式：`backend/services/quant_analyst_service.py`（可参考其 upsert/查询风格）

### 7.2 新增模块建议

#### 7.2.1 Router
- `backend/routers/rdagent.py`
  - RD-Agent 相关 API 的统一入口，路由前缀 `/api/v1/rdagent`：
    - 候选浏览与导入（registry.sqlite + workspace + Strategy Store）
    - RD-Agent 策略/版本浏览（仅 RDagent 视角）
    - signals 查询（从 PostgreSQL `trading.rdagent_signal`）

#### 7.2.2 Service
- `backend/services/rdagent_registry_service.py`
  - 读取 registry.sqlite
  - 抽取 candidate 列表 + 详情（manifest/summary/artifacts）

- `backend/services/rdagent_store_service.py`
  - 复制 artifacts 到 Strategy Store
  - 计算 size/hash

- `backend/services/rdagent_signals_service.py`
  - 读取 parquet
  - `SignalNormalizer` 规范化
  - 批量写入 `trading.rdagent_signal`

### 7.3 API 设计（Phase 1 必须）

#### 7.3.1 候选浏览与导入
- `GET /api/v1/rdagent/candidates`
  - 支持过滤：是否含 signals、日期范围、指标（若有）

- `GET /api/v1/rdagent/candidates/{candidate_id}`
  - 返回：manifest/summary 关键信息 + artifact 文件清单

- `POST /api/v1/rdagent/import`
  - 入参建议：
    - `candidate_id`
    - `strategy_name`
    - `strategy_kind`
    - `output_mode`
    - `enabled`
  - 返回：`strategy_id`, `strategy_version_id`

#### 7.3.2 signals 查询（从 DB）
- `GET /api/v1/rdagent/strategies/{strategy_id}/versions`

- `GET /api/v1/rdagent/signals/overview?strategy_version_id=...`
  - 返回：日期范围、标的数、每期信号数量统计等

- `GET /api/v1/rdagent/signals/dates?strategy_version_id=...&start=...&end=...`

- `GET /api/v1/rdagent/signals/topk?strategy_version_id=...&trade_date=...&k=...`

- `GET /api/v1/rdagent/signals/portfolio?strategy_version_id=...&trade_date=...`

- `GET /api/v1/rdagent/signals/symbol_series?strategy_version_id=...&symbol=...&limit=...`

---

## 8. 前端（Next.js）设计（Phase 1）

前端目录：`F:/Dev/AIstock/frontend`（Next.js）

### 8.1 页面信息架构（IA）

#### 8.1.1 RDagent 策略管理主页面（独立于现有策略管理）
- 路径建议：`/rdagent/strategies`
- 作用：
  - **只面向 RD-Agent 实验成果**，不与现有 AIstock 策略管理合并。
  - 展示来自 registry.sqlite 的 task_run / loop / workspace / 指标信息，以及是否已导入 AIstock 主策略库的状态标记。
- 组件：
  - RDStrategyList（按 task_run/loop 聚合的 RD-Agent 策略候选列表）
  - RDStrategyMetricsBadge（ic_mean/ann_return/mdd/multi_score 等关键指标）
  - RDStrategyDetailDrawer（manifest/summary/artifacts + signals 概览）
  - ImportToAIstockButton（“导入到 AIstock 主策略管理”——显式人工操作）

#### 8.1.2 候选导入入口（RDagent 视角）
- 路径建议：`/rdagent/import`
- 功能与 8.1.1 可复用同一数据源，强调“从 registry 挑选某个 workspace/candidate 并执行导入动作”：
  - CandidateList（筛选/分页）
  - CandidateDetailDrawer（manifest/summary/artifacts）
  - ImportDialog（填写 strategy_name/kind/output_mode/enabled）

#### 8.1.3 AIstock 主策略管理页（与 RDagent 分离）
- 路径建议：`/strategies`
- 展示：**仅展示已批准导入的策略**（包含 RD-Agent 来源，但已经过人工确认），字段包括：来源、kind、output_mode、最新版本、状态等。

#### 8.1.4 信号可视化页（核心）
- 路径建议：`/strategies/[strategyId]/versions/[versionId]/signals`
- 按 output_mode 渲染：
  - TopK：日期选择 + TopK 表格 + 统计图
  - Target Weight：日期选择 + 权重表 + 权重分布/变动图
  - Single Symbol：symbol 选择 + 信号时间序列

### 8.2 前端数据访问
- 建议在 `frontend/src` 内实现：
  - `lib/api.ts`：封装 fetch
  - `services/rdagent.ts`：rdagent API client

---

## 9. Phase 1 导入流程（端到端）

1. UI 调用 `GET /api/v1/rdagent/candidates` 展示候选
2. 用户打开候选详情 `GET /api/v1/rdagent/candidates/{id}`
3. 用户确认导入 -> `POST /api/v1/rdagent/import`
4. 后端执行：
   - 从 registry 定位 artifacts 路径
   - 复制到 Strategy Store
   - 写入：`strategy` / `strategy_version` / `strategy_artifact_file`
   - 解析 `signals.parquet` -> 规范化 -> 写入 `trading.rdagent_signal`
5. UI 跳转 signals 页面，通过 DB 查询接口展示

---

## 10. Phase 2 预留设计：统一数据服务层与推理服务层

### 10.1 统一数据服务层（Data Service）
- 多数据源：miniQMT（优先）/TDX/本地DB/tushare
- 标准化：字段名、精度、单位、交易日对齐
- dataset 概念：bar/tick/l2/financial/index_weight/sector/...（参考 `docs/xtquant_dataset_catalog.md`）

### 10.2 推理服务层（Inference）
- 读取数据（Data Service）
- 加载模型（AIstock 侧）
- 输出 signals（统一结构）
- 写入 inference_runs（本地）+（可选）入库 strategy_execution

---

## 11. 安全与权限（建议）
- 导入接口默认只允许内网或管理员访问
- registry.sqlite 路径与 Strategy Store Root 必须可配置，不硬编码

---

## 12. 性能与一致性

- Phase 1：signals 入库后，查询全部走 DB，避免频繁读 parquet。
- 大批量写入：建议 `execute_values` 或分批提交（需实现细节确认）。
- 保持幂等：
  - 同一 candidate + version_tag 再次导入时：应检测并拒绝或覆盖（按产品需求定）。

---

## 13. 错误处理与可观测性

- 导入失败必须返回可读错误：缺少文件、parquet schema 不符、DB 写入失败等
- 导入任务建议记录到 `strategy_import_job`（可选）

---

## 14. 验收标准

### 14.1 Phase 1
- 能浏览候选并手动导入
- 导入后 signals 进入 `trading.rdagent_signal`
- Next.js 页面能：
  - 按日期查看 TopK/权重
  - 按 symbol 查看时间序列

### 14.2 Phase 2（预留）
- Data Service 能按统一 dataset API 拉取数据
- 推理服务能生成 signals 并展示/执行

---

## 15. 开发进度（开发过程中持续更新）

- **Phase 1 - signals 可视化 + 入库**：`后端主要功能已完成，前端页面开发中`
  - [x] DB 建表（`trading.strategy_*` / `trading.rdagent_signal`）
  - [x] 导入候选浏览 API（`backend/routers/rdagent.py` + `RDRegistryReader`）
  - [x] 导入执行（复制 artifacts 到 Strategy Store，写入 strategy/strategy_version/strategy_artifact_file）
  - [x] signals 解析 + 规范化 + 入库（`rdagent_signals_service.py`）
  - [x] signals 查询 API（overview/topk/portfolio/symbol_series 等）
  - [ ] Next.js 页面：RDagent 独立策略管理页（`/rdagent/strategies`）
  - [ ] Next.js 页面：RDagent 候选导入页（`/rdagent/import`）
  - [ ] Next.js 页面：signals 可视化页（`/strategies/[strategyId]/versions/[versionId]/signals`）

- **统一数据服务层设计**：`待开始`
- **Phase 2 推理服务层设计**：`待开始`

---

## 16. 变更记录（开发过程中持续更新）

| 日期 | 变更内容 | 原因 | 影响范围 |
|------|----------|------|----------|
| 2025-12-23 | 初版设计文档建立；确认 signals 入库到 `trading.rdagent_signal`；前端为 Next.js | 需求确认 | 全局 |
| 2025-12-23 (晚) | 对齐 RD-Agent registry & signals 设计文档；补充 RDagent 独立策略管理页面与人工导入流程；更新 Phase 1 后端实现进度 | 与 RD-Agent 思路保持一致，并反映最新产品决策 | 后端服务、前端 IA、策略生命周期 |
