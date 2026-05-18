# AIstock Research Pipeline + MCP 统一网关详细设计

> 版本：v2.2（最终分阶段方案，补充只读 UI 与 Python DB 初始化）
> 日期：2026-05-18
> 状态：架构定稿；Phase 0-5 为首期实施范围，含只读 Research Pipeline Inspector；Phase 6-8 为后续独立迁移范围
> 范围：Research Pipeline、统一 MCP 平台骨架、Research MCP 独立入口、只读研究任务进度 UI；首期不迁移现有 MCP，不纳入 Paper Trading MCP

---

## 1. 最终架构结论

本方案采用折中路线：**统一平台骨架 + Research MCP 独立入口 + 旧 MCP 并存过渡**。

核心判断：

- 长期最终形态仍应是统一 MCP 平台，但不应在首期一次性合并所有 MCP。
- 新建的 Research MCP 从第一天就在未来统一平台 `backend/mcp/` 上开发，避免未来统一平台稳定后再迁移一次。
- 首期只新增 `aistock-research`，现有 `aistock-validation`、`aistock-qe-experiment`、`aistock-qe-archive` 继续使用现有脚本入口。
- Research Pipeline 是研究编排层和元数据层，不是新的资产 registry、QE 执行引擎或远程任务调度器。
- 需要新增统一的只读 Research Pipeline Inspector UI，用于跨研究任务查看进度、状态、历史和验证结论。
- 科研结果必须通过现有 backend API、`qe_templates`、QE backend、`qe_archive` 完成验证闭环。
- 旧 MCP 迁移放在 Phase 6-8，以独立 issue/branch 执行；迁移顺序为 `qe_archive -> qe_experiment -> validation`。
- 现有 `/quantevolver/model-training` 是模型训练专项页面，不应重做；新 UI 只聚合状态并链接过去。
- Paper Trading MCP 不属于 Phase 0-5，也不默认进入 Phase 6-8；需要独立设计和审批。

### 1.1 决策表

| 决策 | 最终选择 | 说明 |
|---|---|---|
| MCP 平台 | 建设 `backend/mcp/` 统一平台骨架 | 新模块先在未来平台上开发，降低二次迁移成本 |
| 首期入口 | 新增 `aistock-research` | 使用 `scripts/aistock_mcp_gateway.py --modules=research` |
| 现有 MCP | Phase 0-5 不迁移 | 避免把新平台风险传导给已稳定入口 |
| 未来统一入口 | Phase 8+ 再推荐 `aistock --profile=...` | 必须等旧 MCP parity 通过 |
| Research Pipeline | 编排层 + 元数据层 | 只记录过程、attempt、外部链接和引用 |
| 资产处理 | 使用 `artifact_ref` | 指向既有 factor/model/strategy/QE/event/HMM 资产，不抢主数据归属 |
| QE 验证 | 通过现有 QE 体系 | 使用 `qe_templates`、QE backend 和 `qe_archive` |
| 统一只读 UI | 新增 Research Pipeline Inspector | 跨 HMM/event_signal/QE/model-training 查看状态、历史、链接和 verdict；不做工作台 |
| 既有模型训练页 | 不重新开发 | `/quantevolver/model-training` 保留为专项详情页，Research UI 只链接引用 |
| DB 初始化 | Python schema bootstrap | 使用 `backend/db/init_research_pipeline_schema.py`，风格对齐现有 DB init 脚本，不新增裸 `.sql` 文件 |
| 生产影响 | 默认 dev/shadow 验证 | 生产 `8001` 启用或重启必须另行确认 |
| Paper Trading | 不纳入首期 | Paper v2/trading_core/vn.py/MiniQMT 边界敏感，独立 issue |

### 1.2 Phase 0-5 首期运行拓扑

```text
Claude Code / Codex App
  |
  +-- aistock-research
  |     scripts/aistock_mcp_gateway.py --modules=research
  |       -> backend/mcp/gateway.py
  |       -> backend/mcp/modules/research.py       (12 tools)
  |       -> loopback HTTP /api/v1/research-pipeline/*
  |       -> backend services, qe_templates, qe_archive
  |
  +-- Frontend read-only Inspector
  |     /research-pipeline                         (list/detail, no workbench)
  |       -> loopback HTTP /api/v1/research-pipeline/*
  |       -> links to /quantevolver/model-training, /qe-archive, etc.
  |
  +-- aistock-validation
  |     scripts/aistock_mcp_server.py              (existing, unchanged)
  |
  +-- aistock-qe-experiment
  |     scripts/aistock_qe_experiment_mcp_server.py(existing, unchanged)
  |
  +-- aistock-qe-archive
        scripts/aistock_qe_archive_mcp_server.py   (existing, unchanged)
```

首期只有 `aistock-research` 经过新 gateway。旧 MCP 不经过 gateway，因此 gateway 故障时只影响 Research MCP，不影响现有 validation/QE/archive MCP。

### 1.3 Phase 8+ 最终目标拓扑

```text
Claude Code / Codex App
  |
  +-- aistock
        scripts/aistock_mcp_gateway.py --profile=<profile>
          -> research       (Phase 0-5)
          -> qe_archive     (Phase 6 migrated)
          -> qe_experiment  (Phase 7 migrated)
          -> validation     (Phase 8 migrated)
          -> optional future modules, for example paper_trading by separate issue
```

统一入口成为推荐默认入口的前提：

- Research MCP 独立运行已稳定。
- `qe_archive`、`qe_experiment`、`validation` 都完成新旧 parity。
- 旧入口至少保留一个版本周期作为 fallback。
- `.mcp.json` 默认入口切换经过用户确认。
- 生产 `8001` 配置、重启、验证均有单独批准。

---

## 2. 文件结构与写入范围

### 2.1 Phase 0-5 必须新增或修改

```text
backend/mcp/
  __init__.py
  common.py                         # AIstockApiClient, sanitize, confirm, error handling
  gateway.py                        # dynamic module loading + FastMCP entry factory
  profiles.py                       # first-phase profile only loads research
  registry.py                       # shared module registration context
  modules/
    __init__.py
    research.py                     # new Research MCP module, 12 tools

backend/services/research_pipeline/
  __init__.py
  models.py                         # Pydantic models and status enums
  experiment_registry.py            # experiment CRUD + state transitions
  stage_executor.py                 # orchestration only, calls existing backend services
  artifact_ref_manager.py           # references only, no production asset ownership
  validation_gate.py                # criteria and verdict calculation
  archive_handler.py                # qe_archive / event linkage
  pipelines/
    __init__.py
    base.py
    hmm_research.py
    event_signal_research.py
  constants.py

backend/routers/research_pipeline.py
backend/routers/__init__.py               # export/include research_pipeline router only
backend/db/init_research_pipeline_schema.py
scripts/aistock_mcp_gateway.py
backend/tests/research_pipeline/
backend/tests/mcp/
frontend/src/app/research-pipeline/
  page.tsx                                # read-only experiment list
  [experimentId]/page.tsx                 # read-only experiment detail
frontend/src/lib/research-pipeline/
  api.ts                                  # typed client for /api/v1/research-pipeline
frontend/tests/research-pipeline/
```

### 2.2 Phase 0-5 必须保留且不迁移

```text
scripts/aistock_mcp_server.py
scripts/aistock_qe_experiment_mcp_server.py
scripts/aistock_qe_archive_mcp_server.py
scripts/aistock_mcp_common.py
```

### 2.3 Phase 6-8 才新增的迁移模块

```text
backend/mcp/modules/qe_archive.py       # Phase 6
backend/mcp/modules/qe_experiment.py    # Phase 7
backend/mcp/modules/validation.py       # Phase 8
backend/mcp/modules/validation_helpers.py
```

### 2.4 首期明确不新增

```text
backend/mcp/modules/paper_trading.py    # future independent issue only
frontend/src/app/quantevolver/model-training/ # keep existing page; do not redevelop
```

---

## 3. MCP 平台设计

### 3.1 Gateway

`backend/mcp/gateway.py` 只负责：

- 解析 `--profile` 或 `--modules`。
- 动态导入 `backend.mcp.modules.<module_name>`。
- 调用模块的 `register(registry)`。
- 创建并运行 `FastMCP`。

它不负责：

- 业务状态机。
- DB session。
- QE scheduler。
- RD-Agent。
- 远程 workspace。
- 生产运行时写入。

### 3.2 Profiles

Phase 0-5 的 profile 必须保守：

```python
INITIAL_PROFILES = {
    "research": ["research"],
}
```

Phase 6-8 迁移完成后才允许启用：

```python
FUTURE_PROFILES = {
    "research_with_archive": ["research", "qe_archive"],
    "research_with_qe": ["research", "qe_archive", "qe_experiment"],
    "operations": ["validation", "qe_archive", "qe_experiment"],
    "full": ["research", "qe_archive", "qe_experiment", "validation"],
}
```

约束：

- Phase 0-5 默认只允许 `--modules=research` 或 `--profile=research`。
- `resolve_modules(profile="research")` 必须返回 `["research"]`。
- `full`、`operations`、`research_with_qe` 在 Phase 0-5 必须不可用或明确标记 future-only。
- Paper Trading 不出现在 Phase 0-8 默认 profile 中。

### 3.3 ModuleRegistry

`ModuleRegistry` 是模块注册上下文，不是通用 service locator。

必须提供：

- `mcp`: 当前 `FastMCP`。
- `client(path_prefix="")`: 返回统一 `AIstockApiClient`。
- `sanitize(value, name)`: 统一 ID 校验。
- `confirm(actual, expected, field)`: 统一确认机制。
- `register_tool_count(module_name, count)`: 记录 tool 数，供日志和测试使用。

不得提供：

- 直接 DB session。
- 远程节点客户端。
- RD-Agent client。
- QE scheduler client。
- 生产运行时写入能力。

### 3.4 统一 API Client

`backend/mcp/common.py` 可以从 `scripts/aistock_mcp_common.py` 抽取稳定逻辑并增强，但不能破坏旧脚本。

建议接口：

```python
class AIstockApiClient:
    def __init__(
        self,
        base_url: str,
        *,
        env_name: str = "dev",
        timeout: float | None = None,
        unwrap_data: bool = False,
        transport: httpx.BaseTransport | None = None,
    ) -> None: ...
```

要求：

- `base_url` 只允许 loopback：`127.0.0.1`、`localhost`、`::1`。
- 使用 `httpx.Client(..., trust_env=False)`，避免代理污染本地 MCP。
- `transport` 可注入，便于单元测试。
- 默认 `unwrap_data=False`，因为 AIstock 现有 backend API 不保证统一 envelope。
- HTTP 错误必须包含 method、path、status code、response body excerpt。
- 禁止静默 fallback。

### 3.5 Research MCP

`backend/mcp/modules/research.py` 是 Phase 0-5 唯一新 MCP module。

```python
TOOL_COUNT = 12

def register(registry: ModuleRegistry) -> None:
    client = registry.client("research-pipeline")
    # register 12 tools
    registry.register_tool_count("research", TOOL_COUNT)
```

Research MCP 只做参数校验、confirm 校验和 backend API 调用。实验逻辑必须在 `/api/v1/research-pipeline/*` 后端服务中实现。

---

## 4. 现有 MCP 并存与迁移

### 4.1 Phase 0-5：并存，不迁移

| MCP server | Phase 0-5 状态 | 说明 |
|---|---|---|
| `aistock-validation` | 保持旧脚本 | 不改变 bug registry / GitHub sync |
| `aistock-qe-experiment` | 保持旧脚本 | 不改变 QE template/custom evo |
| `aistock-qe-archive` | 保持旧脚本 | 不改变 archive 查询/worker |
| `aistock-research` | 新增 gateway 入口 | 唯一首期新 MCP |

这种并存不能消除所有风险，但可以把新平台风险隔离在 Research MCP 内。旧 MCP 在首期仍可作为稳定 fallback。

### 4.2 Phase 6：迁移 `qe_archive`

`qe_archive` 以查询和归档状态读取为主，适合作为迁移试点。

必须满足：

- 新旧 tool 名称一致。
- 新旧 input schema 一致。
- confirm 行为一致。
- 同输入下返回结构等价。
- 旧入口保留，不立即删除。

### 4.3 Phase 7：迁移 `qe_experiment`

迁移 `qe_experiment` 前，`qe_archive` parity 必须已通过。

额外要求：

- run/stop/delete/materialize confirm token 完全一致。
- custom evo retry/rerun 行为逐项对比。
- 错误信息不退化。
- 不改变现有 QE UI/backend 任务行为。

### 4.4 Phase 8：迁移 `validation`

`validation` 最后迁移，因为它涉及本地 bug JSON、GitHub issue mirror、validation execution 和文件写入。

完成 Phase 8 后才能评估：

- 推荐 `aistock --profile=research|operations|full`。
- 旧入口进入保留期。
- `.mcp.json` 默认入口切换。

---

## 5. Research Pipeline 服务设计

### 5.1 服务边界

Research Pipeline 可以做：

- 创建研究实验并记录假设、pipeline 类型、criteria、issue 链接。
- 编排 HMM、event_signal 等离线 stage。
- 通过 backend API 提交 QE dev/shadow 验证。
- 记录 stage attempt、external run link、artifact reference、comparison、verdict。
- 给出 `validated`、`rejected`、`blocked`、`inconclusive` 等可解释状态。
- 生成 issue 或 promotion request，供人工决定是否进入生产链路。

Research Pipeline 不能做：

- 直接写生产策略包、实盘/仿真运行时表、Paper Trading v2 状态。
- 直接调用 RD-Agent、QE scheduler 或远程 workspace。
- 新建平行 factor/model/strategy/QE 资产主库。
- 绕过 Validation Center 或 QE archive。

### 5.2 状态模型

Experiment 状态：

```text
draft -> running -> stage_failed -> running
running -> validated
running -> rejected
running -> blocked
validated -> promotion_requested -> promoted
validated -> rejected
```

Stage attempt 状态：

```text
queued -> running -> passed
queued -> running -> failed
queued -> running -> cancelled
queued -> running -> timeout
```

设计原则：

- `stage_plan` 表示实验应该有哪些 stage 和当前 stage 状态。
- `stage_attempt` 记录每次执行尝试。
- retry/rerun 必须新增 attempt，不覆盖历史。
- 不允许只用 `(experiment_id, stage_name)` 唯一行记录所有执行历史。

### 5.3 DB Schema

Schema 使用 `research_pipeline` schema。初始化必须采用 Python 脚本 `backend/db/init_research_pipeline_schema.py`，风格对齐现有 `backend/db/init_qe_archive_schema.py` 和 `backend/db/init_qe_execution_templates_schema.py`。

要求：

- Python 脚本内维护 `BASE_DDL`、`TABLE_COMMENTS`、`COLUMN_COMMENTS`、`COMMENT_DDL` 和 `init_research_pipeline_schema()`。
- DDL 可以作为 Python 字符串嵌入，但不新增独立 `.sql` 文件作为首期交付物。
- 所有表和关键列必须生成 PostgreSQL `COMMENT ON`，并通过 `pg_description` 验证。
- 初始化脚本只能显式执行，业务服务不得在运行时隐式建表。

| 表 | 用途 | 关键约束 |
|---|---|---|
| `research_pipeline.experiment` | 实验主记录 | status 枚举；记录 criteria、baseline_ref、issue_url、promoted/rejected/blocked 时间 |
| `research_pipeline.stage_plan` | 实验 stage 计划和当前状态 | `UNIQUE (experiment_id, stage_name)` |
| `research_pipeline.stage_attempt` | stage 执行历史 | `UNIQUE (experiment_id, stage_name, attempt_no)`；status 包含 `cancelled`、`timeout` |
| `research_pipeline.external_run_link` | 外部运行引用 | `run_type` 包含 `qe_template`、`qe_task`、`qe_loop`、`qe_archive_run`、`validation_run`、`event_signal_validation`、`hmm_job` |
| `research_pipeline.artifact_ref` | 资产引用 | `domain_type` 包含 `factor`、`model`、`strategy_pkg`、`qe_archive`、`event_signal`、`hmm_artifact`、`file`；status 为 `candidate`、`validated`、`superseded`、`deleted` |
| `research_pipeline.comparison` | baseline/candidate 对比 | verdict 为 `pass`、`fail`、`inconclusive`、`blocked` |
| `research_pipeline.pipeline_event` | 事件和审计日志 | 记录 payload、experiment、stage_attempt |

必建索引：

- `experiment(pipeline_type, status)`
- `stage_plan(experiment_id, stage_order)`
- `stage_attempt(stage_id, attempt_no DESC)`
- `external_run_link(experiment_id, run_type)`
- `artifact_ref(experiment_id, domain_type, status)`
- `comparison(experiment_id, created_at DESC)`
- `pipeline_event(experiment_id, created_at DESC)`

### 5.4 Backend API

首期 API 挂载在 `/api/v1/research-pipeline`。

| Method | Path | 用途 |
|---|---|---|
| `POST` | `/experiments` | 创建实验 |
| `GET` | `/experiments` | 列表查询 |
| `GET` | `/experiments/{experiment_id}` | 实验详情，含 stages、attempts、artifact_refs、comparisons |
| `POST` | `/experiments/{experiment_id}/stages/{stage_name}/run` | 执行 stage，需要 confirm |
| `POST` | `/experiments/{experiment_id}/stages/{stage_name}/retry` | 重试 stage，需要 confirm |
| `GET` | `/experiments/{experiment_id}/stages/{stage_name}` | stage 当前状态和 attempt 历史 |
| `POST` | `/experiments/{experiment_id}/compare` | baseline comparison |
| `GET` | `/experiments/{experiment_id}/artifact-refs` | 列出 artifact references |
| `POST` | `/experiments/{experiment_id}/promote` | 记录 promotion request，需要 issue_url + confirm |
| `POST` | `/experiments/{experiment_id}/reject` | 拒绝实验 |
| `POST` | `/issues` | 根据发现创建 issue 或本地 bug record |
| `GET` | `/pipeline-types` | 支持的 pipeline 类型和默认 criteria |

### 5.5 初始 pipeline

Phase 0-5 只实现两个 dogfooding pipeline：

| Pipeline | Stage | 说明 |
|---|---|---|
| `hmm_research` | `artifact_gen` | 生成或引用 HMM 候选 artifact |
| `hmm_research` | `offline_validation` | 计算离线指标 |
| `hmm_research` | `portfolio_simulation` | 调用现有模拟/回测路径并记录 comparison |
| `hmm_research` | `qe_shadow` | 通过 `qe_templates` 和 QE dev/shadow 验证 |
| `event_signal_research` | `signal_compute` | 调用现有 event signal 逻辑 |
| `event_signal_research` | `ic_validation` | 计算 IC/RankIC/稳定性 |
| `event_signal_research` | `qe_shadow` | 符合条件时进入 QE dev/shadow |

后续 factor、portfolio、execution 相关 pipeline 需要独立扩展。

### 5.6 Research MCP Tools

Phase 0-5 共 12 个 tools：

```text
research_create_experiment
research_list_experiments
research_get_experiment
research_run_stage
research_retry_stage
research_get_stage_result
research_compare_baseline
research_list_artifact_refs
research_get_pipeline_types
research_create_issue
research_promote
research_reject
```

约束：

- `research_run_stage`、`research_retry_stage`、`research_promote` 必须要求 confirm。
- `research_promote` 只能记录 promotion request，不能直接写生产资产表。
- `research_list_artifact_refs` 只返回引用，不声明资产所有权。

### 5.7 统一研究任务只读 UI / Research Pipeline Inspector

AIstock 已有 `http://localhost:3000/quantevolver/model-training`，该页面是模型训练专项页面，未来不应重新开发或复制其训练配置、snapshot、job 管理能力。Research Pipeline 需要的是一个更上层的只读 Inspector，用于把不同研究线索串成可审计的过程视图。

定位：

- 统一查看 HMM、event_signal、QE shadow、模型训练、QE archive、validation 等研究任务的进度、状态、历史和结论。
- 聚合 Research Pipeline 自己记录的 experiment、stage_plan、stage_attempt、artifact_ref、external_run_link、comparison、pipeline_event。
- 链接到既有专项页面查看细节，例如 `/quantevolver/model-training`、`/quantevolver/experiments`、`/quantevolver/templates`、`/qe-archive`、`/validation-center`。
- 作为 dogfooding 和人工复核入口，帮助判断某个研究结论是否已完成 QE 验证、是否 blocked、是否需要创建 issue 或 promotion request。

首期页面建议：

| 路由 | 内容 | 写能力 |
|---|---|---|
| `/research-pipeline` | 实验列表、pipeline_type、status、latest_stage、updated_at、verdict、blocked reason、外部链接摘要 | 无 |
| `/research-pipeline/[experimentId]` | 实验详情、stage plan、attempt timeline、artifact refs、external run links、comparison、pipeline events | 无 |

明确不做：

- 不做拖拽式 pipeline 编排。
- 不做创建实验、运行 stage、retry、promote、reject 等写操作。
- 不复制模型训练、QE template、QE archive、Validation Center 的详情 UI。
- 不直接访问 DB，只通过 `/api/v1/research-pipeline/*` 读取。
- 不替代 MCP；MCP 仍是创建、运行、重试和 promotion request 的主要自动化入口。

因此，未来不需要重做 `/quantevolver/model-training`，但需要新增只读的统一研究任务 UI。它的价值是“跨系统过程审计”，不是“新的研究工作台”。

---

## 6. 分阶段实施计划

### 6.1 Phase 0-5：首期可合入范围

| Phase | 目标 | 范围 | 明确不做 |
|---|---|---|---|
| 0 | 基线确认 | 确认 `origin/main`、allowed write scope、dev backend 端口、DB 迁移方式 | 不写业务代码，不碰生产 `8001` |
| 1 | MCP 平台骨架 | `common.py`、`registry.py`、`gateway.py`、`profiles.py`、`aistock_mcp_gateway.py` | 不迁移旧 MCP |
| 2 | Research MCP 独立入口 | `modules/research.py`、`.mcp.json` 新增 `aistock-research` 示例 | 不替换现有 MCP |
| 3 | Research Pipeline 后端 | schema、service、router、状态机、artifact_ref、comparison、pipeline_event | 不自建平行资产库 |
| 4 | Offline dogfooding | HMM/event_signal 离线 stage、criteria、comparison、verdict | 不要求收益为正，按 criteria 判定 |
| 5 | QE shadow 闭环 | 调 `qe_templates`、运行 QE dev/shadow、读取 `qe_archive`、写 external_run_link 和 verdict | 不直接调 RD-Agent，不直接操作远程 workspace |
| 5b | 只读 Inspector UI | `/research-pipeline` 列表和详情页，展示状态、attempt、artifact、external link、comparison、verdict | 不做工作台，不重做 `/quantevolver/model-training` |

Phase 0-5/5b 完成后，Research MCP 已经在未来统一平台上可用，并以独立入口对外暴露；人类复核通过只读 Inspector 完成，专项细节仍跳转到既有页面。

### 6.2 Phase 6-8：后续独立迁移

| Phase | 目标 | 范围 | Gate |
|---|---|---|---|
| 6 | 迁移 `qe_archive` | 新增 `backend/mcp/modules/qe_archive.py`，保留旧入口 | 新旧 archive 查询 parity |
| 7 | 迁移 `qe_experiment` | 新增 `backend/mcp/modules/qe_experiment.py` | run/stop/materialize/custom evo parity |
| 8 | 迁移 `validation` | 新增 `backend/mcp/modules/validation.py`，评估默认统一入口 | bug registry、GitHub sync、validation run parity |

Phase 6-8 必须开独立 issue/branch，不能作为 Phase 0-5 的隐藏任务或 merge gate。

---

## 7. 分支策略

Phase 0-5 建议分支：

```text
branch: feature/mcp-gateway-research-pipeline-20260518
base: implementation-time latest origin/main
current design snapshot: 20e20e8
```

实施前必须重新确认：

```powershell
git fetch origin
git status --short --branch
git rev-parse HEAD
git rev-parse origin/main
```

注意：当前仓库可能存在与本方案无关的 untracked/dirty 文件。实施时必须先分类并确认不纳入本分支，不能假设“工作目录干净”。

### 7.1 Phase 0-5 allowed write scope

允许：

```text
backend/mcp/
backend/services/research_pipeline/
backend/routers/research_pipeline.py
backend/db/init_research_pipeline_schema.py
scripts/aistock_mcp_gateway.py
backend/tests/research_pipeline/
backend/tests/mcp/
frontend/src/app/research-pipeline/
frontend/src/lib/research-pipeline/
frontend/tests/research-pipeline/
docs/architecture/research_pipeline_and_mcp_gateway_design_v2.md
```

谨慎：

```text
.mcp.json                         # 只新增 aistock-research 示例/配置，不删除旧 server
backend/main.py                    # 只挂载 research_pipeline router
backend/routers/__init__.py        # 只导出 research_pipeline router
frontend/src/lib/navigation/nav-groups.ts # 只新增 Research Pipeline 只读入口
noxfile.py                         # 只新增 research_pipeline backend/mcp/ui 验证 session
```

禁止纳入 Phase 0-5：

```text
scripts/aistock_mcp_server.py
scripts/aistock_qe_experiment_mcp_server.py
scripts/aistock_qe_archive_mcp_server.py
backend/mcp/modules/qe_archive.py
backend/mcp/modules/qe_experiment.py
backend/mcp/modules/validation.py
backend/mcp/modules/paper_trading.py
Paper v2 / trading_core / vn.py / MiniQMT runtime paths
```

---

## 8. 验收标准

### 8.1 Phase 0-5 功能验收

| ID | 验收项 | 通过标准 |
|---|---|---|
| F1 | Gateway 启动 | `--modules=research` 正常加载，只注册 Research tools |
| F2 | Research MCP 可调用 | stdio contract 能 list/call 12 个 tools |
| F3 | 旧 MCP 不受影响 | 旧 3 个脚本未迁移、未删除、未改行为 |
| F4 | Backend route 可用 | dev backend 上 `/api/v1/research-pipeline/*` 正常 |
| F5 | 状态机正确 | experiment/stage 状态转换符合规则 |
| F6 | Attempt 历史完整 | retry/rerun 不覆盖历史 |
| F7 | Artifact reference 正确 | 只记录 `artifact_ref`，不写生产资产主表 |
| F8 | External run link 正确 | qe_template/qe_task/qe_archive_run 可追踪 |
| F9 | Comparison/verdict 正确 | criteria 判定可解释 |
| F10 | QE shadow 闭环 | 通过 backend API 提交 dev/shadow QE 并读取 `qe_archive` |
| F11 | Confirm 生效 | run/retry/promote 缺 confirm 时拒绝 |
| F12 | Profile 行为正确 | `research` profile 只加载 `research` |
| F13 | 只读 Inspector 可用 | `/research-pipeline` 列表和详情可读取状态、历史、artifact、external link、comparison、verdict |
| F14 | 既有模型训练页未重做 | `/quantevolver/model-training` 保持专项页面，新 UI 只链接引用 |

### 8.2 Dogfooding 验收

Dogfooding 验证流程可追踪、可重试、可解释，不要求研究结论一定为正收益。

| ID | 验收项 | 通过标准 |
|---|---|---|
| D1 | HMM 实验跑通 | 覆盖 offline stage、comparison、verdict、artifact_ref |
| D2 | Event signal 实验跑通 | 覆盖 signal_compute、ic_validation、verdict |
| D3 | Retry 可追踪 | 失败 stage retry 后保留多个 attempt |
| D4 | QE shadow 可追踪 | 有 qe_template/qe_task/qe_archive_run 或明确 blocked reason |
| D5 | MCP 可驱动 | 通过 Research MCP 完成创建、运行、查询、比较、拒绝或 promotion request |
| D6 | UI 可复核 | 通过只读 Inspector 查看 D1-D5 产生的完整历史和结论 |

### 8.3 回归保护

| ID | 验收项 | 通过标准 |
|---|---|---|
| R1 | 旧 MCP 可用 | 旧 validation/qe_experiment/qe_archive 仍可 initialize/list_tools |
| R2 | QE 不受影响 | 标准 QE backend API 在 dev/shadow 环境可用 |
| R3 | HMM 不受影响 | 既有 HMM API smoke 正常 |
| R4 | Validation Center 不受影响 | validation plan/run/bug API 基本 smoke 正常 |
| R5 | Frontend 不受影响 | 只新增 Research Pipeline 只读页和导航；frontend build/smoke 通过 |
| R6 | 生产未误触碰 | 报告明确说明是否触碰、重启或配置生产 `8001`；默认应为未触碰 |

### 8.4 统一只读 UI 验收

| ID | 验收项 | 通过标准 |
|---|---|---|
| UI-01 | 列表页可打开 | 本地 dev frontend 打开 `/research-pipeline`，能加载 experiment 列表或空状态 |
| UI-02 | 详情页可打开 | `/research-pipeline/[experimentId]` 展示 stage plan、attempt timeline、artifact refs、external links、comparison 和 verdict |
| UI-03 | 外部链接正确 | QE template、QE archive、model training、validation 等链接跳到既有页面，不复制详情 UI |
| UI-04 | 只读约束 | 页面不提供创建、运行、retry、promote、reject 等写操作入口 |
| UI-05 | 错误态可解释 | API 失败、无数据、blocked reason 均有可读提示，不静默吞错 |

### 8.5 Phase 6-8 迁移验收（非首期 merge gate）

| ID | 验收项 | 适用阶段 |
|---|---|---|
| M1 | 新旧 tool 名称集合一致 | Phase 6-8 |
| M2 | 新旧 input schema 一致 | Phase 6-8 |
| M3 | confirm token 和拒绝行为一致 | Phase 6-8 |
| M4 | 错误信息不退化 | Phase 6-8 |
| M5 | 旧入口 fallback 可用 | Phase 6-8 |
| M6 | `qe_archive` 查询 parity | Phase 6 |
| M7 | `qe_experiment` run/stop/materialize parity | Phase 7 |
| M8 | validation bug registry/GitHub sync parity | Phase 8 |

---

## 9. 测试方案

### 9.1 测试分层

```text
Layer 1: Unit tests, no external backend
Layer 2: Backend integration tests, test DB or transactional DB
Layer 3: MCP contract tests, stdio/mock transport
Layer 4: Frontend read-only smoke, dev backend + dev frontend
Layer 5: Dogfooding tests, dev backend + optional QE shadow
Layer 6: Migration parity tests, Phase 6-8 only
```

### 9.2 关键单元测试

| ID | 用例 | 预期 |
|---|---|---|
| U-GW-01 | `create_gateway(["research"])` | `tool_count("research") == 12` |
| U-GW-02 | `resolve_modules(profile="research")` | `["research"]` |
| U-GW-03 | `resolve_modules(profile="full")` in Phase 0-5 | 报错或明确 future-only |
| U-CM-01 | loopback base_url | 允许 |
| U-CM-02 | non-loopback base_url | 拒绝 |
| U-CM-03 | injected `transport` | 不访问真实网络 |
| U-CM-04 | `unwrap_data=False` | 返回原始 JSON |
| U-CM-05 | HTTP 4xx/5xx | 错误包含 method/path/status/body excerpt |
| U-RP-01 | 创建实验 | status=draft |
| U-RP-02 | retry failed stage | 新增 attempt_no，不覆盖旧 attempt |
| U-RP-03 | criteria pass/fail | verdict 和 reason 可解释 |
| U-RP-04 | artifact_ref duplicate sha256 | 复用引用，不创建生产资产 |

### 9.3 集成测试

| ID | 用例 | 预期 |
|---|---|---|
| I-DB-01 | Schema 创建 | 7 张表存在 |
| I-DB-02 | 表和关键列 comment | `pg_description` 可查 |
| I-API-01 | 创建实验 | DB 有 experiment |
| I-API-02 | 运行 stage | DB 有 stage_attempt |
| I-API-03 | external link | DB 有 external_run_link |
| I-API-04 | artifact reference | DB 有 artifact_ref，不写生产资产表 |
| I-API-05 | comparison | DB 有 comparison 和 verdict |
| I-API-06 | reject/promote request | 状态和时间字段正确 |

### 9.4 MCP 合约测试

| ID | 用例 | 预期 |
|---|---|---|
| M-RES-01 | list tools | 恰好 12 个 Research tools |
| M-RES-02 | `research_create_experiment` 缺必填 | 参数错误 |
| M-RES-03 | `research_run_stage` 缺 confirm | 拒绝 |
| M-RES-04 | `research_run_stage` confirm 正确 | 调 backend API |
| M-RES-05 | `research_retry_stage` confirm 正确 | 新增 attempt |
| M-RES-06 | `research_promote` 缺 issue_url | 拒绝 |
| M-RES-07 | `research_list_artifact_refs` | 返回引用列表 |
| M-RES-08 | backend 500 | 返回可诊断错误，不静默 fallback |

### 9.5 Dogfooding

| ID | 流程 | 预期 |
|---|---|---|
| D-HMM-01 | MCP 创建 HMM 实验 | DB 有 experiment/stage_plan |
| D-HMM-02 | 运行 offline stages | attempt 记录完整，metrics 非空 |
| D-HMM-03 | 运行 comparison | verdict 按 criteria 生成 |
| D-HMM-04 | 运行 qe_shadow | 有 QE 外部链接或明确 blocked reason |
| D-EVT-01 | MCP 创建 event_signal 实验 | DB 有 experiment/stage_plan |
| D-EVT-02 | signal_compute/ic_validation | metrics 非空，verdict 可解释 |

### 9.6 Frontend 只读 UI 测试

| ID | 用例 | 预期 |
|---|---|---|
| UI-T-01 | `/research-pipeline` 列表页 | 能渲染实验列表、状态、latest stage、verdict 或空状态 |
| UI-T-02 | `/research-pipeline/[experimentId]` 详情页 | 能渲染 stage attempt timeline、artifact_ref、external_run_link、comparison、pipeline_event |
| UI-T-03 | 外部链接 | 模型训练、QE archive、QE template、Validation Center 链接指向既有页面 |
| UI-T-04 | 只读检查 | 页面无写操作按钮；无直接 DB/API mutation 调用 |
| UI-T-05 | API 错误 | 展示可诊断错误，不白屏、不静默 fallback |

### 9.7 迁移 parity 测试（Phase 6-8）

这些测试不是 Phase 0-5 合入条件。

| ID | 用例 | 阶段 |
|---|---|---|
| R-MIG-01 | 新旧 tool names parity | Phase 6-8 |
| R-MIG-02 | 新旧 input schema parity | Phase 6-8 |
| R-MIG-03 | 新旧 confirm error parity | Phase 6-8 |
| R-MIG-04 | `qe_archive` output parity | Phase 6 |
| R-MIG-05 | `qe_experiment` run/stop parity | Phase 7 |
| R-MIG-06 | `report_bug` path/format parity | Phase 8 |

---

## 10. Token 预算与 Profile

| 阶段 | Profile / Server | 加载模块 | Tools | 说明 |
|---|---|---|---|---|
| Phase 0-5 | `aistock-research` | research | 12 | 新 gateway 唯一首期模块 |
| Phase 0-5 | `aistock-validation` | legacy validation | existing | 独立旧 MCP，不计入新 gateway |
| Phase 0-5 | `aistock-qe-experiment` | legacy qe_experiment | existing | 独立旧 MCP，不计入新 gateway |
| Phase 0-5 | `aistock-qe-archive` | legacy qe_archive | existing | 独立旧 MCP，不计入新 gateway |
| Phase 6 | future `research_with_archive` | research + qe_archive | TBD | 迁移后重新测量 |
| Phase 7 | future `research_with_qe` | research + qe_archive + qe_experiment | TBD | 迁移后重新测量 |
| Phase 8 | future `full` | research + qe_archive + qe_experiment + validation | TBD | Phase 8 后才可推荐 |

文档不再沿用旧版 operations 组合预算，因为 Paper Trading 不在范围内，旧 MCP 也不通过新 gateway 加载。

---

## 11. 合入 main 前提

### 11.1 代码质量 Gate

| ID | 条件 | 推荐命令 | 通过标准 |
|---|---|---|---|
| CQ-01 | Python 编译通过 | `python -m compileall backend/mcp backend/services/research_pipeline backend/db/init_research_pipeline_schema.py; python -m py_compile backend/routers/research_pipeline.py scripts/aistock_mcp_gateway.py` | 0 errors |
| CQ-02 | diff 无空白错误 | `git diff --check` | 0 errors |
| CQ-03 | 改动在 scope 内 | `git diff --name-only origin/main...HEAD` | 无越界文件 |
| CQ-04 | 无静默异常 | `rg -n "except\s*:\s*pass|except\s+Exception\s*:\s*pass" backend/mcp backend/services/research_pipeline` | 0 matches 或逐项解释 |
| CQ-05 | 无硬编码空密码 | `rg -n "password" backend/mcp backend/services/research_pipeline` | 所有命中均确认不是空密码、明文密码或测试外泄 |
| CQ-06 | Python schema comments 完整 | 检查 `backend/db/init_research_pipeline_schema.py` 的 `TABLE_COMMENTS`、`COLUMN_COMMENTS`、`COMMENT_DDL`，并查询 `pg_description` | 表和关键列均有 COMMENT |
| CQ-07 | 旧 MCP 未被误改 | `git diff --name-only origin/main...HEAD -- scripts/aistock_mcp_server.py scripts/aistock_qe_experiment_mcp_server.py scripts/aistock_qe_archive_mcp_server.py` | Phase 0-5 应为空 |

### 11.2 测试 Gate

| ID | 条件 | 推荐命令 | 通过标准 |
|---|---|---|---|
| TG-01 | Gateway/Research MCP unit | `python -m pytest backend/tests/mcp -q` | 0 failures |
| TG-02 | Research Pipeline backend | `python -m pytest backend/tests/research_pipeline -q` | 0 failures |
| TG-03 | 旧 MCP startup regression | `python -m pytest backend/tests/test_aistock_mcp_server.py backend/tests/test_aistock_qe_mcp_servers.py -q` | 0 failures；如路径不同按实际测试名调整 |
| TG-04 | nox 聚合 | `python -m nox -s research_pipeline_backend research_mcp_contract research_pipeline_ui` | 0 failures；如 session 尚未存在，需在同 PR 增加或说明替代命令 |
| TG-05 | 迁移 parity | Phase 6-8 only | 不作为 Phase 0-5 gate |

### 11.3 生产安全 Gate

| ID | 条件 | 通过标准 |
|---|---|---|
| RP-01 | Dev backend 启动 | 非生产端口启动无 ERROR/CRITICAL |
| RP-02 | 旧 MCP 可用 | 旧入口 initialize/list_tools smoke 通过 |
| RP-03 | 生产 `8001` 未误触碰 | PR 报告明确说明未重启/未写入；如触碰必须有用户批准 |
| RP-04 | DB 迁移可回滚 | schema 变更有创建顺序和禁用/回滚策略 |
| RP-05 | Frontend | 只读 Inspector 触碰 frontend 时，build 和 `/research-pipeline` smoke 通过 |

### 11.4 合入执行清单

```text
[ ] 确认 branch 基于最新 origin/main
[ ] 确认 unrelated dirty/untracked 文件未纳入提交
[ ] CQ-01 ~ CQ-07 通过
[ ] TG-01 ~ TG-04 通过；UI-T-01 ~ UI-T-05 通过或有明确 blocked reason；TG-05 标记为 Phase 6-8 only
[ ] Dogfooding 通过或有明确 blocked reason
[ ] RP-01 ~ RP-05 通过
[ ] 用户确认允许合入 main
[ ] 合入 main 并 push
[ ] 合入后在 dev/shadow backend 验证 API + MCP
[ ] 如需要启用生产 8001 或切换默认 .mcp.json，另行请求用户确认
[ ] 旧 MCP 入口保留并完成 smoke
```

---

## 12. 风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| 新 gateway 不稳定 | 影响 Research MCP | 首期只有 `aistock-research` 使用 gateway，旧 MCP 不受影响 |
| 文档暗示旧 MCP 已迁移 | 实施范围膨胀 | 明确 Phase 6-8 才迁移，迁移验收不作为首期 gate |
| Research Pipeline 变成平行资产库 | 与既有系统冲突 | 只存 `artifact_ref`，主数据仍归 factor_catalog/model_registry/strategy_pkg/qe_archive |
| QE shadow 绕过治理 | 结果不可追溯 | 只能通过 backend API、`qe_templates`、`qe_archive` 闭环 |
| retry 覆盖历史 | 无法复盘失败 | `stage_attempt` 使用 `(experiment_id, stage_name, attempt_no)` 保留历史 |
| 生产端口被误用 | 影响现有服务 | 默认 dev/shadow 验证；生产 `8001` 需要用户确认 |
| Paper Trading 边界混入 | 触碰敏感运行时 | Paper Trading MCP 独立 issue，不在本方案首期 |
| 只读 UI 变成工作台 | 引入额外写路径和权限风险 | Phase 0-5/5b UI 只读，写操作继续走 MCP/backend confirm |
| 重做模型训练页 | 造成 UI 分叉和维护成本 | `/quantevolver/model-training` 保留为专项页面，Inspector 只外链 |
| DB 初始化方式不一致 | 与仓库现有 bootstrap 风格冲突 | 使用 Python init 脚本和 DDL/comment 列表，不新增裸 `.sql` |

---

## 13. 最终形态

最合理的长期形态仍然是统一 MCP 平台，但实施路径必须分阶段：

1. **现在**：Research MCP 直接在未来统一平台 `backend/mcp/` 上开发，以 `aistock-research` 独立入口上线；同时提供只读 `/research-pipeline` Inspector 作为人工复核视图。
2. **平台稳定后**：按风险从低到高迁移旧 MCP，先 `qe_archive`，再 `qe_experiment`，最后 `validation`。
3. **迁移完成后**：`aistock --profile=research|operations|full` 成为推荐统一入口，旧入口进入保留期。
4. **UI 形态**：统一研究 UI 长期保持只读聚合层，专项详情继续由 `/quantevolver/model-training`、QE archive、Validation Center 等现有页面承担。
5. **更远期**：如需 Paper Trading MCP，必须单独设计 profile、confirm、生产隔离和运行时边界，不能默认并入 `full`。

这条路线的优势：

- Research MCP 不会在未来统一平台完成后再迁移一次。
- 旧 MCP 不会因为新平台未成熟而承担首期回归风险。
- QE 验证仍由现有 QE backend 和 `qe_archive` 负责，Research Pipeline 只做研究治理。
- 统一只读 UI 提供跨系统历史视图，但不替代既有专项页面，也不引入新的写入口。
- 后续统一入口有清晰迁移顺序和 parity gate，可以逐步收敛而不是一次性切换。
