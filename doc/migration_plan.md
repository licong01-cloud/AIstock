# AI 股票分析系统新架构设计与迁移计划

> 目标：在不影响现有 Streamlit 应用运行的前提下，**全新构建** FastAPI + React 架构的新应用，逐一移植旧功能，复用现有数据获取/数据库/TDX 数据，最终新应用可独立运行，旧应用彻底退出，只共享后台数据库。

---

## 一、总体原则与约束

- **旧应用（Streamlit + tdx_backend）在整个迁移过程必须可用**  
  不直接修改其核心逻辑，只做必要的 Bugfix。所有大规模重构只发生在 `next_app/` 下。

- **新架构从 0 开始搭建**  
  以 `next_app/` 为新应用根目录，完全基于 FastAPI（后端）+ React/Next.js（前端）。

- **数据层统一复用**  
  复用现有 PostgreSQL/TimescaleDB，以及需要保留的 SQLite 库。  
  复用 TDX 数据获取链路（包括 `tdx_backend` 和 `tdx_scheduler`）作为数据生产通道。

- **数据集 CSV 作为统一元数据缓存**  
  以 `docs/data_schema_fields.csv` 和 `docs/data_schema_source_mapping.csv` 为主数据字典。  
  新应用启动时加载到内存缓存，用于：
  - 字段命名/单位统一
  - 自动数据源选择
  - 通用数据接口定义

- **新旧功能共存一段时间**  
  所有新功能仅在新应用实现。旧功能按模块逐一迁移至新应用，期间两端都可用。

- **最终状态**  
  新应用独立仓库（以 `next_app` 为原型整理）。旧代码与旧 UI 不再被新应用 import，仅共享数据库。

---

## 二、现有应用架构概览（只读，不修改）

### 1. 运行与进程结构

- 启动入口：`run.py`
  - 启动 **TDX 调度后端（FastAPI）**：`uvicorn tdx_backend:app --port 9000`
  - 启动 **Streamlit 主应用**：`streamlit run app.py --server.port 8503`
- 用户入口：浏览器访问 `http://localhost:8503`（Streamlit UI）。

### 2. 前端/UI 层（旧架构）

- `app.py` 单页面 Streamlit 应用，聚合：
  - 多智能体股票/组合分析（`ai_agents`）
  - 监控管理与定时分析（`monitor_*`）
  - 主力选股与历史报告（`main_force_*`）
  - 行业/板块策略（`sector_strategy_*`）
  - 龙虎榜分析（`longhubang_*`）
  - 智能监控（`smart_monitor_*`）
  - 指标筛选、云筛选（`indicator_screening_*`, `cloud_screening_*`）
  - 自选股与组合管理（`watchlist_*`, `portfolio_*`）
  - 热点板块与本地数据管理（`hotboard_ui`, `tdx_ui`）

- 多个 `*_ui.py` 模块封装具体页面 UI 与交互，但大量直接调用数据与业务逻辑。

- `monitor_service.py` 等服务类中直接 `import streamlit as st`，UI 与后台线程强耦合。

### 3. 后端/服务与调度

- `tdx_backend.py`：已有 FastAPI 应用，提供 TDX 测试/ingestion 调度与管理接口，访问 TimescaleDB 的 `market.*` 表。
- `tdx_scheduler.py`：TDX 数据调度框架（`schedule + ThreadPoolExecutor`）。
- 其它调度脚本（`monitor_scheduler.py`, `portfolio_scheduler.py`, `sector_strategy_scheduler.py` 等）负责各自业务的定时任务。

### 4. 业务逻辑与数据访问

- 统一数据访问：`unified_data_access.py` 提供对 Tushare / TDX / TimescaleDB 等多源访问，广泛被 UI 模块调用。
- 业务模块：
  - 指标与筛选：`indicator_screening_*`, `cloud_screening_*`, `strategy_indicators.py` 等
  - 主力资金与筹码：`main_force_*`
  - 行业/板块策略：`sector_strategy_*`
  - 龙虎榜：`longhubang_*`
  - 智能监控：`smart_monitor_*`
  - 自选股与组合：`watchlist_*`, `portfolio_*`
  - 新闻公告与情绪：`news_announcement_data.py`, `market_sentiment_data.py` 等

- 存储：
  - PostgreSQL/TimescaleDB：通过 `pg_*_repo.py` 与 `market.*` 表。
  - 多个 SQLite 本地库：`stock_analysis.db`, `main_force_batch.db`, `longhubang.db`, `sector_strategy.db`, `smart_monitor.db`, `portfolio_stocks.db` 等。
  - 外部数据源：Tushare、TDX、本地 tdx_api、Akshare、YFinance、CNInfo、LLM API 等。

---

## 三、新架构总体设计（以 next_app 为根）

### 1. 目标目录结构

```text
next_app/
  backend/                 # FastAPI 后端
    main.py                # app 实例、路由挂载
    config.py              # 配置加载（env/ini）
    deps.py                # 依赖注入（DB 会话、缓存等）
    db/
      connection.py        # PostgreSQL/TimescaleDB 连接封装
      repositories/        # 数据访问对象（DAO）
    schema_registry/
      loader.py            # 从 CSV 加载 schema 元数据到缓存
      models.py            # 内存中的 Dataset/Field/Source 模型
    services/              # 业务服务层（完全不依赖 streamlit）
      kline_service.py
      reference_service.py
      analysis_service.py          # 股票分析（优先实现）
      monitor_service.py
      sector_strategy_service.py
      longhubang_service.py
      watchlist_service.py
      ...
    routers/               # API 路由层
      health.py
      datasets.py          # 通用数据访问（基于 schema CSV）
      kline.py
      analysis.py          # 股票分析 API（优先实现）
      monitor.py
      sector_strategy.py
      longhubang.py
      watchlist.py
      ...
    utils/
      caching.py           # 内存缓存（如 LRU/TTL）
      logging.py           # 日志封装
      auth.py              # （如未来有用户体系）

  frontend/                # React / Next.js 前端
    src/
      pages/
        index.tsx          # 总控制台
        analysis/          # 股票分析相关页面（优先实现）
        kline/
        monitor/
        sector/
        longhubang/
        watchlist/
        ...
      components/
      api/                 # 封装对 backend API 的调用
      styles/

  doc/
    migration_plan.md      # 迁移与新架构总计划（本文件）
    architecture.md        # （后续）新架构细节说明
    api_contracts.md       # （后续）主要 API 约定

  tools/
    ...                    # 新架构专用脚本
```

### 2. 后端启动与数据流

1. **启动流程**

- 读取 `.env` / `config.py` → 初始化 settings。
- 初始化 DB 连接池（PostgreSQL/TimescaleDB）。
- 从 CSV 加载 schema 元数据至内存：
  - `data_schema_fields.csv` → 字段定义、类型、单位。
  - `data_schema_source_mapping.csv` → 数据源映射、单位转换系数。
- 提供全局 `SchemaRegistry`（通过依赖注入暴露给 services 与 routers）。

2. **统一 Dataset API 流程**

以请求：

```http
GET /api/v1/datasets/kline_daily_qfq?ts_code=000001.SZ&start=2024-01-01&end=2024-06-30
```

为例：

- `routers/datasets.py` 解析 dataset 和查询条件。
- `SchemaRegistry`：
  - 查 dataset 的字段列表（含最终单位/类型）。
  - 查 dataset 的源列表（`db_aistock`, `tushare_*`, `tdx_*` 等）。
- `services/kline_service` 或通用 `dataset_service`：
  - 按策略选择 source（优先本地 `db_aistock.market.*`，再考虑 TDX/Tushare）。
  - 通过 `db/repositories` 调用 DB/接口获取原始数据。
  - 按 CSV 映射和转换系数统一为规范格式。
- 返回 JSON/CSV 等格式给前端。

3. **缓存策略**

- 元数据缓存：
  - CSV → 内存对象，只在文件更新时重载。
- 数据缓存：
  - 对指数行情、板块数据等高频只读场景使用短 TTL 内存缓存。
- 不改动旧应用缓存逻辑，完全在 `next_app/backend` 内部实现。

---

## 四、迁移范围与优先级（按功能域）

主要功能域（基于当前 app.py 及模块）：

1. 行情/基础数据（统一数据访问能力）
2. **股票分析模块（AI 多智能体分析）← 优先**
3. 持仓/自选股监控与告警（`monitor_*`）
4. 主力选股与历史报告（`main_force_*`）
5. 行业/板块策略（`sector_strategy_*`）
6. 龙虎榜分析与报告（`longhubang_*`）
7. 智能监控（`smart_monitor_*`）
8. 指标筛选 & 云筛选（`indicator_screening_*`, `cloud_screening_*`）
9. 自选股与组合管理（`watchlist_*`, `portfolio_*`）
10. 热点板块 & 本地数据管理（`hotboard_ui`, `tdx_ui` 等）

迁移优先级：

1. 基础数据层（Dataset API + 行情/K 线）
2. **股票分析模块（AI 分析）**
3. 简单页面（指标筛选、云筛选等）
4. 复杂策略/监控（主力、行业策略、智能监控、龙虎榜等）
5. 历史报告与导出功能

---

## 五、阶段化执行计划

### 阶段 1：备份与基线锁定（不改任何旧代码）

- **全面备份**：
  - Git 打 tag：如 `legacy-stable-YYYYMMDD`。
  - 备份 PostgreSQL/TimescaleDB：`pg_dump` 全量；
  - 备份关键 SQLite：将 `*.db` 拷贝至 `backups/` 目录。
  - 备份 `.env` 与 `config.py`。

- **基线用例记录**：
  - 选取若干代表性股票和功能（行情、AI 分析、主力选股、监控告警等）；
  - 在旧应用界面导出/截图结果，作为后续对比基线。

> 阶段 1 只涉及运维层面操作，不修改任何应用代码。

---

### 阶段 2：新架构骨架（FastAPI + React）

**目标**：`next_app/backend` 与 `next_app/frontend` 能独立启动，并提供健康检查 API/页面。

- **后端**：
  - `backend/main.py`：创建 FastAPI app，挂载 `/health`；
  - `backend/config.py`：加载基础配置（DB 连接、日志级别等，先可简化）；
  - `backend/deps.py`：定义 `get_settings`、`get_db` 等依赖；
  - `backend/routers/health.py`：返回应用状态。

- **前端**：
  - 在 `next_app/frontend` 初始化 React/Next.js 项目；
  - 首页调用 `/health` 并展示状态。

- **运行方式**：
  - 旧应用继续使用 `python run.py` 启动；
  - 新应用通过单独命令启动，不影响旧进程：
    - `uvicorn next_app.backend.main:app --reload --port 8001`
    - `npm run dev`（或 `next dev`）。

---

### 阶段 3：统一数据访问 & Schema CSV 内存缓存

**目标**：新后端能基于 CSV 元数据和现有 DB/TDX，提供统一 dataset API。

- **实现 SchemaRegistry**：
  - `schema_registry/models.py`：定义 `Dataset`, `Field`, `SourceMapping` 等类；
  - `schema_registry/loader.py`：从 `docs/data_schema_fields.csv` 和 `docs/data_schema_source_mapping.csv` 加载数据，构建：
    - `dataset_id -> [Field]`
    - `dataset_id -> [SourceMapping]`。

- **通用 Dataset API**：
  - `routers/datasets.py`：
    - 路由：`GET /api/v1/datasets/{dataset_id}`；
    - 入参：dataset 标识 + 通用过滤参数；
    - 调用通用 `dataset_service`：
      - 选择 source（本地 DB 优先，必要时调用 Tushare/TDX）；
      - 应用字段映射与单位转换；
      - 返回统一 JSON。

- **首批支持的数据集**：
  - `kline_daily_qfq`, `index_kline_daily_qfq`, `daily_basic`, `moneyflow_ind_dc` 等。

---

### 阶段 4：股票分析模块优先迁移

> 本阶段针对“股票分析模块”（当前由 `ai_agents.py` + `app.py` 中相关部分驱动），是新架构的首要业务目标。

#### 4.1 分析现状（只读）

- 确认当前股票分析模块的：
  - 输入参数：股票代码、日期、持仓信息、用户偏好等；
  - 依赖数据：行情、财报、资金流、公告、情绪等；
  - 使用的 LLM 模型与 prompt 结构；
  - 输出：多智能体观点、综合结论、风险提示、操作建议等。

#### 4.2 设计新后端 API 合约

- 新增路由文件：`backend/routers/analysis.py`：
  - `POST /api/v1/analysis/stock`：
    - Request：
      - `ts_code` / `symbol`；
      - 分析日期范围；
      - 可选：持仓成本、仓位、风险偏好等；
    - Response：
      - 多智能体结果列表：每个 agent 的观点、评分、证据；
      - 综合结论与建议；
      - 内部使用的关键数据概览（行情、基本面、资金流摘要）。

#### 4.3 在新后端实现分析服务

- 新增：`backend/services/analysis_service.py`：
  - 不再 import 旧 `ai_agents`，而是：
    - 参考其业务逻辑，重新实现适合服务化的分析管线；
    - 严格通过 Dataset API 获取数据（不直接访问旧 `unified_data_access`），便于未来完全独立；
    - 封装 LLM 调用（DeepSeek / OpenAI）与 prompt 构建。

- 使用从 CSV 加载的 schema 确保：
  - 数据字段与单位统一；
  - 能对接新前端的表格/图表显示需求。

#### 4.4 新前端股票分析页面

- 在 `frontend/src/pages/analysis/` 下创建：
  - `index.tsx` 或 `stock.tsx`：
    - 股票代码输入组件；
    - 分析范围/参数设置；
    - 分析进度指示；
    - 多智能体卡片展示区域；
    - 结论/建议卡片（视觉风格尽量贴近当前 Streamlit 的卡片设计）。

- 使用统一 API 封装模块（如 `frontend/src/api/analysis.ts`）调用 `/api/v1/analysis/stock`。

#### 4.5 验证与调优

- 对同一股票在旧应用与新应用分别执行分析：
  - 对比：
    - 关键结论与风险提示是否一致；
    - 证据数据是否对应相同区间/指标；
  - 如有差异，优先保证新版本逻辑更合理，再修订旧版或接受有控制的差异。

#### 4.6 渐进切换

- 在旧 Streamlit 界面中增加“使用新版分析（推荐）”跳转按钮，指向新前端分析页面；
- 当内部验证稳定后，将主入口（例如首页分析 Tab）在前端导航中偏向新应用；
- 但保留旧分析入口一段时间，以便用户回退。

---

### 阶段 5：其它功能模块按模板迁移

对下列每个模块，都按“**分析现状 → 设计 API → 实现服务 → 实现前端页面 → 新旧对比 → 渐进下线旧入口**”的模板执行：

1. 持仓/自选股监控与告警（`monitor_*`）
2. 主力选股与历史报告（`main_force_*`）
3. 行业/板块策略（`sector_strategy_*`）
4. 龙虎榜分析与报告（`longhubang_*`）
5. 智能监控（`smart_monitor_*`）
6. 指标筛选 & 云筛选（`indicator_screening_*`, `cloud_screening_*`）
7. 自选股与组合管理（`watchlist_*`, `portfolio_*`）
8. 热点板块 & 本地数据管理（`hotboard_ui`, `tdx_ui` 等）

每个模块迁移时，都遵守：

- 新代码全部写在 `next_app/backend` 与 `next_app/frontend` 中；
- 不在新应用中 import 旧应用的 UI 或业务模块；
- 仅通过：
  - 新的 Dataset API；
  - PostgreSQL/TimescaleDB 及必要 SQLite 数据库；
  - TDX 后端或其它稳定外部 API；
- 旧应用保持原状运行，直至确认新模块稳定后，才考虑移除旧入口。

---

### 阶段 6：切流、下线旧应用与新仓库独立

1. **功能覆盖与稳定性验收**：
   - 确认新应用已覆盖所有旧应用核心功能；
   - 新应用在一段时间内稳定运行，无严重问题。

2. **下线旧 UI**：
   - 移除旧应用部署中的 Streamlit 前端；
   - 或将旧仓库部署完全停用，仅保留数据库与少量脚本。

3. **新 GitHub 仓库创建**：
   - 新建仓库（如 `aistock-next`），以 `next_app/` 为基础精简后导入：
     - 调整目录为：`backend/`, `frontend/`, `doc/`, `tools/`；
     - 保留 `.env.example`、`Dockerfile`、`README` 等。

4. **去耦旧仓库**：
   - 确保新仓库中不存在对旧仓库 Python 模块的 import；
   - 新应用仅通过 DB 或外部 API 使用历史数据资源。

---

## 七、当前决策与下一步

- **当前决策**：
  - 方案无关键遗漏；
  - **优先从“股票分析模块”开始迁移**；
  - 先形成本地文档，再按上述阶段逐步实施。

- **下一步执行建议（代码层面）**：
  1. 在 `next_app/backend` 中落地 FastAPI 骨架与 `health` 路由；
  2. 在 `next_app/backend/schema_registry` 中实现 CSV 加载与内存缓存（先支持少量 dataset，必要时只做 stub）；
  3. 在 `next_app/backend/services/analysis_service.py` 与 `routers/analysis.py` 中实现股票分析 API 雏形（可以先返回 mock 数据，逐步引入真实数据访问与 LLM 调用）；
  4. 在 `next_app/frontend` 中创建股票分析页面，调试前后端交互；
  5. 之后再逐步增强分析逻辑，并按阶段 4 的流程做新旧对比与引导迁移。

> 整个改造过程中，所有对旧应用的影响都应控制在“只读访问数据库”和“并行运行”层面，严禁在未验证新应用稳定前停止或破坏旧应用的正常使用。
