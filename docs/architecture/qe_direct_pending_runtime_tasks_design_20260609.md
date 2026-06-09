# QE Runtime-First 待启动任务设计方案

更新时间：2026-06-09

## 1. 背景与目标

当前 QE MCP 已经可以通过 `qe_execution_templates` 创建“待执行模板”，再由 UI 或 MCP
进行校验、审批、物化和执行。该链路适合保留历史提案、审批元数据和模板 diff，但用户现在明确要求：

1. 不再把 MCP 新建实验的主路径停留在“待执行模板”分支。
2. MCP 应直接创建真实 QE runtime 对象，但默认不启动执行。
3. 单次实验和自定义演进都必须支持“创建但不启动、UI 编辑、UI 启动”。
4. UI 的“启动”必须与已有“恢复”语义区分。
5. Codex 和 Claude Code 都必须通过统一 gateway 调用同一套后端能力。

本设计将 QE MCP 主路径调整为 **Runtime-First Pending Task**：

```text
MCP / UI -> QE 后端共用 API -> 创建真实 runtime 记录 -> status=pending/created 且未启动
          -> UI 打开同源编辑器审查和修改全部配置
          -> 用户点击“启动” -> 进入现有 QE 执行链路
```

`qe_execution_templates` 不在本设计中立即删除；它进入兼容和历史审计角色，不再作为新 MCP
创建任务的首选主路径。

## 2. 已核验事实

### 2.1 单次实验

- `POST /api/v1/quantevolver/config/generate` 由 `GenerateConfigRequest` 创建单次实验配置，当前不会执行实验。
- 单次实验落库到 `qe_experiments`，已有状态 `created`，执行入口为
  `POST /api/v1/quantevolver/experiments/{experiment_id}/run`。
- `run_experiment` 读取已有 `qe_experiments` 配置，经 unified executor 提交 RD-Agent，并把实验状态更新为
  `running`，同时写入 `qe_task_id`、`qe_loop_id`、`started_at` 等运行字段。
- 前端实验历史页已有“执行回测”按钮，但目前缺少“编辑未启动单次实验配置”的完整 UI 和对应保存 API。

参考文件：

- `backend/routers/quantevolver.py`：`generate_config`、`run_experiment`、`_run_experiment_unified`
- `frontend/src/app/quantevolver/experiments/page.tsx`：实验历史列表、执行按钮

### 2.2 自定义演进

- `POST /api/v1/quantevolver/evolution/custom-tasks` 已有 `auto_start` 字段。
- 当 `auto_start=false` 时，后端创建 `qe_evolution_tasks` 记录，但不调用
  `submit_custom_evo_all_loops`。
- `GET /api/v1/quantevolver/evolution/tasks/{task_id}/custom-evo-config` 已可读取可编辑配置。
- `POST /api/v1/quantevolver/evolution/tasks/{task_id}/custom-evo/run` 已可启动 materialized custom_evo。
- 前端 `/quantevolver/evolution` 已有创建自定义演进任务的完整表单，但创建时当前默认不传
  `auto_start=false`；任务列表有详情、恢复、继续、克隆、删除等按钮，缺少“编辑未启动任务”和“启动未启动任务”。

参考文件：

- `backend/routers/quantevolver_evolution.py`：`CustomEvolutionCreateRequest`、`create_custom_evolution_task`、
  `get_custom_evo_config`、`run_custom_evo_task`
- `backend/services/quantevolver/qe_evolution_service.py`：`create_custom_evo_task`、`get_custom_evo_editable_config`
- `frontend/src/app/quantevolver/evolution/page.tsx`：自定义演进创建表单和任务列表

### 2.3 MCP/Gateway

- 当前 QE MCP 模块已有 `qe_template_create`、`qe_template_materialize_confirmed`、
  `qe_template_run_confirmed`、`qe_template_create_and_run_confirmed`。
- 当前已有 `qe_experiment_run_confirmed` 和 `qe_custom_evo_run_confirmed`，但缺少直接创建“真实 runtime
  待启动任务”的 MCP 工具。
- 既有 QE MCP 设计明确要求：MCP 不直接调用 RD-Agent，不绕过 QE 后端，不维护一套与 UI 不同的执行逻辑；如后端缺共用 API，应先补 UI/MCP 共用 API。

参考文件：

- `backend/mcp/modules/qe_experiment.py`
- `docs/architecture/qe_mcp_template_archive_research_design_20260515.md`

## 3. 设计原则

1. **Runtime-first**：MCP 创建的对象直接出现在真实 QE 运行域中，不再默认只创建模板。
2. **创建不等于启动**：创建 runtime 记录不提交 RD-Agent、不启动 loop、不占用执行节点。
3. **UI/MCP 同源**：MCP 只调用 UI 也能调用的 FastAPI endpoint；禁止 MCP 专用执行路径。
4. **启动与恢复分离**：未启动任务只能“启动”；已运行过的暂停、失败或完成任务才允许“恢复”。
5. **编辑边界明确**：只有未启动 runtime 允许全量编辑；启动后只能走 append、rerun、retry、resume 等既有受控变更。
6. **兼容优先**：`qe_execution_templates` 保留历史入口和兼容工具，但新工具和新 UI 主路径使用 runtime-first。
7. **无静默兜底**：校验失败必须 fail-fast；不得用默认模型、默认因子或默认节点掩盖 MCP 传入配置错误。

## 4. 目标状态模型

### 4.1 单次实验

单次实验以 `qe_experiments` 为 runtime 记录：

```text
created / pending_start
  -> running
  -> completed | failed | interrupted | cancelled
```

V1 不强制新增状态值；建议使用现有 `status='created'` 表示未启动。后端和 UI 通过以下条件计算
`startable=true`：

```text
status in ('created', 'pending')
and qe_task_id is null
and qe_loop_id is null
and started_at is null
and completed_at is null
```

如果后续实现发现 `pending` 在其他单次实验路径中语义混杂，可再新增一等状态
`pending_start`，但该状态不是本设计第一阶段的必要 DDL。

### 4.2 自定义演进

自定义演进以 `qe_evolution_tasks` 为 runtime 记录：

```text
pending_start
  -> running
  -> paused | completed | failed | cancelled
  -> resume
```

V1 可沿用现有 `status='pending'`，并通过以下条件计算 `startable=true`：

```text
task_type = 'custom_evo'
and status = 'pending'
and current_loop = 0
and no qe_evolution_loops row has been submitted/running/processing/completed/failed/cancelled
```

为了 UI 清晰，API response 应额外返回：

```json
{
  "startable": true,
  "editable": true,
  "resume_allowed": false,
  "start_reason": "custom_evo task has not been submitted"
}
```

### 4.3 启动与恢复的差异

| 操作 | 适用对象 | 前置条件 | 行为 | UI 文案 |
|---|---|---|---|---|
| 启动 | 单次实验 / custom_evo | 真实 runtime 已创建但从未提交 | 首次提交执行 | 启动 |
| 恢复 | 演进任务 | 任务已运行过，处于 paused/failed/completed/stopped 等可继续状态 | 从 current_loop 或失败 loop 状态继续 | 恢复 |
| 继续 | custom_evo | 已有任务未运行中，追加新 loop | append 新 loop 并提交新增 loop | 继续 |
| 重试 | loop | loop failed/cancelled | 保留配置，重试失败 loop | 重试 |
| 重跑 | loop | 需显式删除旧结果 | 替换 loop 配置并删除旧结果后重跑 | 重跑 |

未启动对象不显示“恢复”；已启动过的对象不显示“编辑全部配置”。

## 5. 后端 API 设计

### 5.1 单次实验 API

新增 UI/MCP 共用 API：

```text
POST /api/v1/quantevolver/experiments/pending
GET  /api/v1/quantevolver/experiments/{experiment_id}/editable-config
PUT  /api/v1/quantevolver/experiments/{experiment_id}/editable-config
POST /api/v1/quantevolver/experiments/{experiment_id}/run
```

`POST /experiments/pending` 语义：

- 复用 `GenerateConfigRequest` 或提取为 `SingleExperimentPendingCreateRequest`。
- 复用 `generate_config` 现有校验、seed 规范化、HMM 校验、风险策略校验和 `ConfigComposer.compose_experiment`。
- 创建 `qe_experiments` 记录，返回 `experiment_id`、`status`、`editable=true`、`startable=true`。
- 不提交 RD-Agent，不写 `qe_task_id`、`qe_loop_id`、`started_at`。
- 可携带 `source_context_json`、`created_by_type`、`created_by_name`、`provenance` 等信息，优先放入
  `custom_params.qe_mcp_provenance`，避免第一阶段新增 DDL。

`GET /editable-config` 返回单次实验完整可编辑配置：

```json
{
  "experiment_id": "qe_...",
  "experiment_name": "...",
  "status": "created",
  "factor_names": [],
  "factor_sources": {},
  "model_id": "...",
  "strategy_id": "...",
  "data_split": {},
  "custom_params": {},
  "dispatch_mode": null,
  "evolution_params": null,
  "editable": true,
  "startable": true,
  "config_source": "qe_experiments"
}
```

`PUT /editable-config` 语义：

- 只允许 `editable=true` 的单次实验。
- 更新 `experiment_name`、`factor_names`、`factor_sources`、`model_id`、`strategy_id`、`data_split`、`custom_params`。
- 复用 `GenerateConfigRequest` 的轻量校验逻辑；不得跳过 fixed seed、label_horizon、HMM、执行算法等契约。
- 如果 `qe_task_id`、`qe_loop_id`、`started_at` 任一非空，返回 409，提示“实验已启动，不允许全量编辑”。
- 不删除任何 workspace，因为未启动对象理论上没有远端运行 workspace。

`POST /run` 继续使用现有 `run_experiment`，但实现时应把按钮文案和错误提示从“执行回测”调整为“启动”。

### 5.2 自定义演进 API

保留现有创建和启动入口，补齐全量保存：

```text
POST /api/v1/quantevolver/evolution/custom-tasks
GET  /api/v1/quantevolver/evolution/tasks/{task_id}/custom-evo-config
PUT  /api/v1/quantevolver/evolution/tasks/{task_id}/custom-evo-config
POST /api/v1/quantevolver/evolution/tasks/{task_id}/custom-evo/run
POST /api/v1/quantevolver/evolution/tasks/{task_id}/resume
```

`POST /custom-tasks`：

- UI 新建“保存待启动任务”时传 `auto_start=false`。
- MCP 创建待启动 custom_evo 时也传 `auto_start=false`。
- 现有“创建并立即运行”行为可保留，但必须在 UI 上区分按钮。

`PUT /custom-evo-config`：

- 只允许 `task_type='custom_evo'`。
- 只允许未启动任务：`status='pending'`、`current_loop=0`、未产生任何已提交 loop 记录。
- 复用 `_prepare_custom_evo_loop_configs` 进行 loop、node、node_parallelism、label_horizon、backtest_only、fixed seed 等校验。
- 更新 `qe_evolution_tasks.task_name`、`target_desc`、`max_loops`、`node_id`、`strategy_evo_config`。
- 不允许用该接口编辑已运行任务；已运行任务继续走 append、rerun、retry、resume。

`POST /custom-evo/run`：

- 已存在，继续作为“启动”入口。
- 对从未启动任务显示为“启动”；对已运行过但状态不是 running 的任务，不应复用为“恢复”。
- 后端可在 response 中返回 `submitted=true`、`operation='start'`。

`POST /resume`：

- 只用于已运行过任务。
- 对 `current_loop=0` 且无 loop 记录的未启动 custom_evo，应返回 400/409，提示“该任务尚未启动，请使用启动按钮”。

## 6. MCP/Gateway 工具设计

新增工具必须注册在统一 QE gateway 下，Codex 和 Claude Code 通过同一个 gateway profile 获取相同工具列表。

### 6.1 单次实验工具

```text
qe_single_experiment_create_pending
qe_single_experiment_get_config
qe_single_experiment_update_config_confirmed
qe_experiment_run_confirmed               # 已有，继续作为启动入口
```

建议确认 token：

- `qe_single_experiment_create_pending`：可不要求执行确认，但必须记录 `created_by_type=mcp_gateway`；
  如团队希望所有 runtime 写入都显式确认，则使用 `QE_SINGLE_EXPERIMENT_CREATE_PENDING`。
- `qe_single_experiment_update_config_confirmed`：要求 `QE_SINGLE_EXPERIMENT_UPDATE_CONFIG`。
- `qe_experiment_run_confirmed`：继续要求 `QE_EXPERIMENT_RUN`。

### 6.2 自定义演进工具

```text
qe_custom_evo_create_pending
qe_custom_evo_get_config                  # 已有
qe_custom_evo_update_config_confirmed
qe_custom_evo_run_confirmed               # 已有，作为启动入口
qe_custom_evo_resume_confirmed            # 仅恢复已运行任务
```

建议确认 token：

- `qe_custom_evo_create_pending`：可不要求执行确认，但必须显式设置 `auto_start=false` 并记录来源；
  如团队希望所有 runtime 写入都显式确认，则使用 `QE_CUSTOM_EVO_CREATE_PENDING`。
- `qe_custom_evo_update_config_confirmed`：要求 `QE_CUSTOM_EVO_UPDATE_CONFIG`。
- `qe_custom_evo_run_confirmed`：继续要求 `QE_CUSTOM_EVO_RUN`。
- `qe_custom_evo_resume_confirmed`：如新增 MCP 包装，要求 `QE_CUSTOM_EVO_RESUME`，且后端必须拒绝未启动任务。

### 6.3 旧模板工具

保留但降级：

- `qe_template_create`
- `qe_template_validate`
- `qe_template_materialize_confirmed`
- `qe_template_run_confirmed`
- `qe_template_create_and_run_confirmed`

文档和 MCP tool description 中标注：

```text
Deprecated for new QE runtime creation. Prefer qe_single_experiment_create_pending
or qe_custom_evo_create_pending when the user wants a UI-editable task that is
created but not started.
```

旧模板工具仍服务于历史提案、审批差异和旧调用兼容，不在本需求中删除。

## 7. 前端设计

### 7.1 单次实验 UI

目标页面：

- `/quantevolver/experiments`：实验历史/单次任务列表。
- `/quantevolver/experiments/{experiment_id}`：详情页。
- 新增或复用 modal/page：单次实验编辑器。

列表按钮：

| 条件 | 按钮 |
|---|---|
| `startable=true` | `编辑`、`启动` |
| `status='running'` | `查看日志`、`刷新状态` |
| terminal 或已启动过 | `查看详情`、`同步结果`、`重新生成`、`删除` 等既有按钮 |

编辑器字段必须覆盖创建单次 QE 实验时能看到的完整配置：

- 标题/实验名。
- 因子列表与来源。
- 模型、策略、策略参数。
- data_split。
- label_type、label_horizon、random_seed。
- HMM、停牌过滤、ST/PIT 风险过滤、尾盘未成交处理。
- execution_algo、execution_algo_params、node_id。
- archive_policy 和 MCP provenance 展示。

保存时调用 `PUT /experiments/{experiment_id}/editable-config`，不启动。

### 7.2 自定义演进 UI

目标页面：

- `/quantevolver/evolution` 任务列表。
- 现有“新建任务”弹窗中的 custom_evo 表单。

改造：

1. 把现有 custom_evo 创建表单抽成可复用组件，例如 `CustomEvoConfigEditor`。
2. 表单支持模式：
   - `create_and_start`
   - `create_pending`
   - `edit_pending`
   - `clone`
   - `append`
   - `rerun`
3. 任务列表对未启动 custom_evo 显示：
   - `编辑`：加载 `GET /custom-evo-config`，打开同一个编辑器，保存到 `PUT /custom-evo-config`。
   - `启动`：调用 `POST /custom-evo/run`。
4. 未启动 custom_evo 不显示 `恢复`；已经运行过的 custom_evo 保留 `恢复`、`继续`、`重试`、`重跑`。

按钮文案：

- `保存待启动任务`：创建但不启动。
- `保存修改`：编辑未启动任务。
- `启动`：首次提交。
- `恢复`：从暂停/失败/完成状态继续。

### 7.3 待执行模板 UI

`/quantevolver/templates` 短期保留，但导航建议降级为“历史待执行模板”。

页面顶部增加说明：

```text
新的 MCP QE 任务默认直接创建为真实 QE runtime 待启动任务，请到实验历史或演进任务页面编辑和启动。
本页面仅保留历史模板、审批 diff 和兼容入口。
```

## 8. 数据与审计设计

V1 尽量不新增 DDL，优先复用现有 JSON 字段：

### 8.1 单次实验 provenance

写入 `qe_experiments.custom_params.qe_mcp_provenance`：

```json
{
  "created_by_type": "mcp_gateway",
  "created_by_name": "codex|claude_code|ui",
  "gateway_profile": "qe",
  "source_context": {},
  "analysis_summary": "...",
  "risk_summary": "...",
  "created_as": "pending_runtime_task"
}
```

### 8.2 自定义演进 provenance

写入 `qe_evolution_tasks.strategy_evo_config.provenance`：

```json
{
  "created_by_type": "mcp_gateway",
  "created_by_name": "codex|claude_code|ui",
  "gateway_profile": "qe",
  "source_context": {},
  "analysis_summary": "...",
  "risk_summary": "...",
  "created_as": "pending_runtime_task"
}
```

### 8.3 未来可选 DDL

如果后续审计要求需要一等字段，可新增：

- `qe_experiments.created_by_type`
- `qe_experiments.created_by_name`
- `qe_experiments.pending_runtime_source_json`
- `qe_evolution_tasks.pending_runtime_source_json`

新增 DDL 必须按生产 DDL gate 执行；本设计第一阶段不要求 DDL。

## 9. 兼容和迁移策略

1. 不删除 `qe_execution_templates` 表、API 和前端页面。
2. 不自动迁移历史模板；历史模板继续可查看、物化、执行。
3. 新 MCP 文档、tool description 和 UI 主入口都指向 runtime-first。
4. 如果已有模板已物化成 `submitted_experiment_id` 或 `submitted_task_id`，UI 可提示跳转到对应 runtime 页面继续编辑/启动。
5. `qe_template_create_and_run_confirmed` 继续保留，用于明确“一键创建并执行”的特殊场景。

## 10. 实施阶段

### Phase 1：后端共用 API

- 新增单次实验 pending create/get editable/update API。
- 新增 custom_evo full editable update API。
- 为实验列表和任务列表 response 增加 `startable`、`editable`、`resume_allowed` 计算字段。
- 强化 resume guard：未启动 custom_evo 不能 resume。

验收：

- 后端单元测试覆盖 single pending create/update/start。
- 后端单元测试覆盖 custom pending create/update/start。
- 后端单元测试覆盖 resume 未启动任务返回 400/409。

### Phase 2：MCP/Gateway

- 新增 runtime-first pending create/update tools。
- tool description 标注旧模板工具 deprecated。
- Codex 和 Claude Code 配置均通过统一 gateway profile 暴露同一工具集。

验收：

- MCP smoke：列出工具，确认 Codex/Claude Code 均能看到新工具。
- MCP create pending smoke：创建单次实验和 custom_evo，验证 DB/API 状态未启动。
- MCP update config smoke：修改未启动配置，验证后端返回新 config。

### Phase 3：前端 UI

- 单次实验列表增加 `编辑`、`启动`。
- 单次实验详情或 modal 支持完整配置编辑。
- custom_evo 任务列表增加 `编辑`、`启动`。
- custom_evo 创建表单抽取复用，确保“编辑看到的 UI 与创建 QE 自定义任务一致”。
- 恢复按钮仅对已运行过的演进任务显示。

验收：

- Playwright 或 frontend test 覆盖单次 pending 编辑、启动。
- Playwright 或 frontend test 覆盖 custom_evo pending 编辑、启动。
- UI 截图或 E2E 证据证明启动和恢复按钮不会同时误导未启动任务。

### Phase 4：模板路径降级

- `/quantevolver/templates` 增加历史兼容说明。
- 导航降级或移到二级入口。
- 文档更新 MCP 推荐路径。

验收：

- 旧模板 API 测试不回归。
- 新 runtime-first 流程成为文档和 UI 的默认入口。

## 11. 设计验收矩阵

| 用户要求 | 设计落点 | 验收证据 |
|---|---|---|
| 支持创建单次任务但不启动 | `POST /experiments/pending`，状态 `created/startable` | API 测试 + DB/API smoke |
| 支持创建自定义任务但不启动 | `POST /custom-tasks` with `auto_start=false` | API 测试 + DB/API smoke |
| UI 编辑单次任务完整配置 | 单次实验编辑器 + `PUT /editable-config` | UI/E2E + API 测试 |
| UI 编辑自定义任务完整配置且与创建 UI 一致 | 抽取 `CustomEvoConfigEditor` 复用 | UI/E2E + 组件测试 |
| UI 提供启动功能 | 单次 `POST /experiments/{id}/run`，custom `POST /custom-evo/run` | API smoke + UI/E2E |
| 启动与恢复区分 | startable/resume_allowed 计算字段 + UI 条件按钮 + resume guard | API negative test + UI test |
| Codex 和 Claude Code 统一 gateway | 新 MCP tools 注册到统一 QE gateway profile | MCP tool list smoke |
| 旧模板路径可替代但不破坏历史 | 模板路径 deprecated，不删除 | 旧模板测试不回归 |

## 12. 风险与开放问题

1. 单次实验当前 `config/generate` 已经可创建 `status='created'` 记录，但编辑 API 需要补齐，否则 UI 只能查看不能安全修改。
2. 自定义演进当前 `pending` 同时可表示“未启动”和“恢复后待提交”，需要通过 `current_loop` 与 loop 记录计算 startable；如未来语义继续混杂，建议新增 `pending_start`。
3. 旧模板表保存了分析摘要、风险摘要、审批信息和 diff；runtime-first 需要把关键 provenance 写入 JSON，否则审计信息会变薄。
4. 如果单次任务未来包含 multi-alpha，本设计需扩展 `editable-config`；本期先覆盖普通 single experiment。
5. UI 复用现有 custom_evo 表单需要前端结构性重构，不能只复制一份表单，否则后续字段会漂移。

## 13. 明确不做

- 不让 MCP 直接调用 RD-Agent。
- 不让 MCP 直接读写 worker workspace。
- 不新增一套 MCP 专用执行器。
- 不在本阶段删除 `qe_execution_templates`。
- 不在第一阶段强制生产 DDL。
- 不把未启动任务称为“恢复”。
