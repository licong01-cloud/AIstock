# QE MCP、实验模板与数仓自动归档设计方案

日期：2026-05-15
状态：草案，等待审阅
范围：仅设计方案；本文档不包含任何代码实现

## 1. 目标

本文档定义 QE 实验体系的三项目标能力：

1. 新增与现有 Validation/流水线 MCP 平行的 QE MCP 能力。
2. 新增可审阅的 QE 实验模板流程，让 Codex 可以配置单次实验或自定义演进实验，但不会在配置完成后立即执行。
3. 补齐 QE 实验完成后自动进入 `qe_archive` 数仓的流程，同时提供历史实验一次性补齐能力，以及配置期间选择是否进入数仓的能力。

最终目标是：智能体可以基于历史实验证据分析 QE 结果、生成可控实验模板、在用户确认后提交执行，并在实验完成后自动归档，为下一轮优化提供可靠历史依据。

## 2. 当前事实与约束

### 2.1 现有 MCP 基线

`scripts/aistock_mcp_server.py` 已经实现了 Validation Center MCP 模式，使用 FastMCP + stdio。可以复用的基础原则包括：

- MCP 工具通过 HTTP 调用本地后端 API。
- 后端 base URL 必须是 loopback 地址，禁止非本机地址。
- 路径标识符必须做白名单校验，避免路径注入或额外 path segment。
- HTTP / 文件系统错误必须 fail-fast，不能静默返回空结果。
- `.mcp.json` 已经有项目级 MCP 注册模式。

QE MCP 可以复用这些基础设施设计，但不应该和现有 Validation MCP 合并职责。

### 2.2 现有 QE 执行行为

单次实验目前通过 `/api/v1/quantevolver/experiments/{experiment_id}/run` 执行；该接口读取已有实验配置，并提交给 RD-Agent/QE。

自定义演进实验目前在创建 `qe_evolution_tasks` 后，会立即异步调用 `submit_custom_evo_all_loops(new_task_id)`。虽然任务初始状态是 `pending`，但这不是可长期停留、可人工审阅的“待执行”状态，因为创建后会立刻进入调度流程。

因此，真正的“配置完成但待审阅/待确认执行”能力，应该新增独立的模板/提案层，而不是简单复用现有 runtime status。

### 2.3 现有 QE Archive 状态

当前数仓基础已经存在：

- `qe_archive` schema 与 repository 基础已经存在。
- 手动历史 backfill 和 API backfill 基础已经存在。
- realtime ingestion facade 已经存在，但默认由 `QE_ARCHIVE_REALTIME_ENABLED` 关闭。
- realtime mode 支持 outbox 或 direct 写入。
- Archive 失败不会改变 QE 源实验状态，这是正确的隔离设计。

缺失的是生产级默认行为：符合条件的 QE 实验完成后应自动入仓，无需人工干预；同时，特殊实验应能在配置时选择不入仓。

### 2.4 安全约束

- 实现和验证阶段不应依赖重启生产 `8001`；验证应优先使用 dev port。
- Windows 侧后端不得直接读取或修改远端 QE/RD-Agent worker workspace；worker artifact 必须通过 node API、缓存摘要、显式同步/下载或 archive 自有 artifact store 获取。
- 没有权威涨跌停/停牌处理的日频回测必须归档为 `research_valid=false`，并默认排除出排行榜、优化器 warm-start 和有效研究证据。
- 智能体工具不得获得任意 SQL 权限，只能使用白名单 API、repository 方法或受控聚合视图。

### 2.5 本设计依据的现有文件

| 领域 | 当前来源 |
|---|---|
| Validation MCP 模式 | `scripts/aistock_mcp_server.py`、`.mcp.json` |
| 单次实验执行 | `backend/routers/quantevolver.py` |
| 自定义演进请求 schema 与 API | `backend/routers/quantevolver_evolution.py` |
| 自定义演进创建后立即执行行为 | `backend/services/quantevolver/qe_evolution_service.py` |
| 统一 QE 配置构造 | `backend/services/quantevolver/experiment_config.py`、`backend/services/quantevolver/experiment_config_builders.py` |
| Archive schema 与注释规范 | `backend/db/init_qe_archive_schema.py` |
| Archive API | `backend/routers/qe_archive.py` |
| Archive realtime hook 基础 | `backend/services/qe_archive/realtime_ingestion.py` |
| Archive backfill 与 source assembler | `backend/services/qe_archive/source_assembler.py`、`backend/services/qe_archive/backfill_service.py`、`scripts/qe_archive_backfill.py` |
| 数仓目标设计 | `docs/architecture/qe_realtime_experiment_warehouse_top_level_design_20260502.md`、`docs/architecture/qe_realtime_experiment_warehouse_detailed_design_20260502.md` |
| 项目约束与长期记忆 | `docs/codex_project_memory.md` |

## 3. 目标架构

### 3.1 平行 MCP 服务器

目标 MCP 形态应是平行模式，而不是互相嵌套：

```text
aistock-validation MCP
  - validation plans
  - validation runs
  - bug lifecycle
  - GitHub issue sync
  - 不具备 QE 执行权限

aistock-qe-archive MCP
  - 只读 QE 数仓证据
  - archive health 与 backfill preview
  - 因子 / 模型 / seed / 超参统计
  - 只允许写入审计记录，不允许发起 QE 实验

aistock-qe-research MCP
  - 当前 QE 实验分析
  - 实验模板 / 提案创建
  - dry-run 校验
  - 用户确认后的模板提交
  - 状态、日志、结果轮询
```

三者可以共享一个很小的 Python support package，用于 HTTP client、loopback 校验、identifier 校验、错误处理和 response normalization。但三者不应该共享执行权限，也不应该共享工具命名空间。

### 3.2 后端服务分层

建议新增或扩展以下模块：

```text
backend/services/qe_templates/
  repository.py
  schemas.py
  validation.py
  materializer.py
  diff.py

backend/services/qe_archive/
  realtime_ingestion.py        # 已存在，扩展
  worker_service.py            # 已存在，扩展自动运行
  backfill_service.py          # 已存在，扩展 UX/status
  aggregate_queries.py         # 新增，只读数仓证据层

scripts/
  aistock_qe_research_mcp_server.py
  aistock_qe_archive_mcp_server.py
```

MCP server 应调用后端 API，而不是绕过服务层直接写数据库。唯一例外可以是明确设计并测试过的本地 MCP audit fallback。

## 4. QE 实验模板层

### 4.1 为什么需要独立模板层

独立模板/提案层优于简单新增一个 QE runtime status，原因如下：

- 单次实验和自定义演进实验使用不同 runtime 表和执行流程。
- 当前 custom_evo 的 `pending` 会立即进入执行流程，不是人工审阅状态。
- 模板需要保存分析摘要、风险摘要、配置差异、数仓策略、审批元数据，这些不适合塞入核心 runtime 表。
- 模板可以同时供 MCP 和 UI 检查，而不影响现有实验列表、调度器和运行状态同步。

### 4.2 模板表

建议新增表：

```text
qe_execution_templates
```

也可以命名为 `qe_experiment_templates`，但 `qe_execution_templates` 更能覆盖单次实验和 custom_evo 两类执行对象。

核心字段：

| 字段 | 语义 |
|---|---|
| `template_id` | 稳定模板 ID。 |
| `template_kind` | `single_experiment` 或 `custom_evo`。 |
| `status` | 模板生命周期状态。 |
| `title` | 人类可读标题。 |
| `description` | 人类/智能体说明。 |
| `config_json` | 规范化模板配置。 |
| `config_sha256` | 配置 hash，用于去重和审计。 |
| `archive_policy` | 该模板生成 run 的默认入仓策略。 |
| `created_by` | `user`、`ui`、`codex_mcp` 或其他 agent 名称。 |
| `source_context_json` | 生成模板时参考的实验、任务、archive runs。 |
| `analysis_summary_md` | 为什么提出这个模板。 |
| `risk_summary_md` | 已知风险与约束。 |
| `validation_json` | 最近一次 dry-run 校验结果。 |
| `approval_json` | 审批人、审批时间、确认文本等。 |
| `submitted_experiment_id` | 提交后绑定的单次实验 ID。 |
| `submitted_task_id` | 提交后绑定的演进任务 ID。 |
| `created_at` / `updated_at` | 审计时间。 |

所有新增表和字段必须有 PostgreSQL `COMMENT ON TABLE` / `COMMENT ON COLUMN`，并加 schema 测试，保持现有数据库注释标准。

### 4.3 模板状态机

建议生命周期：

```text
draft
  -> ready_for_review
  -> approved
  -> submitted
  -> running
  -> completed
  -> failed
  -> cancelled
  -> superseded
  -> expired
```

状态语义：

- `draft`：配置未完成或正在编辑。
- `ready_for_review`：配置完整，且 dry-run 校验通过。
- `approved`：用户已明确审批，但尚未提交运行。
- `submitted`：已经物化为 `qe_experiments` 或 `qe_evolution_tasks`。
- `running` / `completed` / `failed` / `cancelled`：镜像运行对象状态。
- `superseded`：被新模板替代。
- `expired`：因因子、模型、数据契约变化而失效。

### 4.4 单次实验模板 API

新增稳定 API 前缀，例如：

```text
GET  /api/v1/qe-templates
POST /api/v1/qe-templates
GET  /api/v1/qe-templates/{template_id}
PUT  /api/v1/qe-templates/{template_id}
POST /api/v1/qe-templates/{template_id}/validate
POST /api/v1/qe-templates/{template_id}/approve
POST /api/v1/qe-templates/{template_id}/submit
POST /api/v1/qe-templates/{template_id}/supersede
```

当 `template_kind=single_experiment` 时，`submit` 才创建或绑定 `qe_experiments` 运行记录，然后复用现有执行路径。现有 UI 的直接创建/直接执行行为不需要删除。

### 4.5 自定义演进模板 API

同一模板 API 支持 `template_kind=custom_evo`。配置结构应对齐当前 `CustomEvoLoopConfig` 语义。

提交时才物化为 `qe_evolution_tasks`，再调用现有 custom_evo 执行逻辑。当前 `/custom-tasks` 可以继续保持“创建即运行”的兼容行为。

模板校验器在提交前必须检查：

- `factor_keys` 是否有效且可用。
- `disable_alpha158` 是否显式。
- `model_id` 是否存在且兼容。
- `strategy_params` 是否符合策略 schema。
- `label_horizon` 是否规范化。
- `backtest_only` loop 是否具有 source task/loop，且 source label horizon 一致。
- node 分配和 `node_parallelism` 是否在预算内。
- archive policy 是否有效。
- execution algo 字段是否满足 QE/Paper 执行契约。

### 4.6 UI 行为

第一阶段不需要改变现有 UI 行为。

后续可添加：

- 模板列表页或侧边栏。
- 模板详情页：配置 JSON/diff、分析摘要、风险摘要、dry-run 结果、入仓策略、资源预算。
- 操作按钮：`Validate`、`Approve`、`Submit`、`Clone`、`Supersede`。
- 入仓策略选择器：`自动入仓`、`不入仓`、`仅手动入仓`。

## 5. 入仓策略与特殊实验跳过

### 5.1 策略模型

每个模板和每个运行对象都应携带明确入仓策略：

```text
AUTO
SKIP
MANUAL_ONLY
```

语义：

- `AUTO`：实验完成后自动入仓。默认值应为 `AUTO`。
- `SKIP`：不自动入仓，也不被宽泛历史 backfill 导入，除非使用特权 override。
- `MANUAL_ONLY`：完成时不自动入仓，但允许按 id 做定向手动 backfill。

根据你的要求，普通实验默认 `AUTO`；特殊实验可以在配置期间选择 `SKIP`。

### 5.2 策略存储位置

策略必须同时保存在模板和运行上下文中：

- 模板：`qe_execution_templates.archive_policy`。
- 单次实验 runtime：`qe_experiments.custom_params.archive_policy`，或新增一列一等字段。
- 自定义演进 task：`qe_evolution_tasks.strategy_evo_config.archive_policy` 作为 task 默认值。
- 自定义演进 loop：可选 per-loop `archive_policy`，覆盖 task 默认值。

Archive source assembler 和 realtime ingestion 必须读取 runtime 记录上的有效策略。对于历史记录，如果没有策略字段，默认按 `AUTO` 处理，除非 backfill 请求明确过滤。

### 5.3 `SKIP` 语义

`SKIP` 必须被以下流程尊重：

- 完成时 realtime ingestion。
- backfill candidate 列表。
- 宽泛历史 backfill。
- MCP archive 建议。
- 默认 archive health gap 报告。

可以后续支持特权 override，但必须指定具体 source id，并要求确认 token，例如：

```text
QE_ARCHIVE_OVERRIDE_SKIP
```

建议新增：

```text
qe_archive.skip_registry
```

或使用现有审计表记录 source id、跳过原因、操作者和时间。这样可以避免未来排查时误以为该实验“漏归档”。

## 6. QE 完成后自动入仓

### 6.1 目标行为

当一个符合条件的 QE source 完成时：

1. QE 源 DB 事务成功提交。
2. completion hook 读取有效 `archive_policy`。
3. 如果策略是 `AUTO`，hook 写入 durable outbox event。
4. archive worker 无需人工干预处理 outbox。
5. archive service 幂等写入或更新 `qe_archive`。
6. archive health 展示成功、等待、跳过或失败状态。
7. archive 失败绝不改变 QE 源状态。

### 6.2 默认使用 outbox

默认 realtime 模式应是 durable outbox，而不是在 QE 完成路径里直接写 archive：

```text
QE completion path -> outbox event -> archive worker -> qe_archive tables
```

原因：

- 保持 QE runtime 快速返回。
- 支持失败重试。
- artifact enrichment 暂时不可用时不会丢任务。
- 保持 archive 失败与 QE 状态隔离。

Direct mode 可以保留给诊断或测试，但不应作为生产默认路径。

### 6.3 Worker 自动化

当前缺失的关键点是 worker 自动运行。建议新增由后端管理的 archive worker loop，并通过显式环境变量控制。

建议变量：

```text
QE_ARCHIVE_REALTIME_ENABLED=true
QE_ARCHIVE_REALTIME_MODE=outbox
QE_ARCHIVE_WORKER_AUTOSTART=true
QE_ARCHIVE_WORKER_INTERVAL_SEC=30
QE_ARCHIVE_WORKER_BATCH_SIZE=10
QE_ARCHIVE_WORKER_MAX_RETRIES=5
```

目标部署行为：

- `QE_ARCHIVE_REALTIME_ENABLED=true`
- `QE_ARCHIVE_WORKER_AUTOSTART=true`
- 完成 hook 自动 enqueue
- worker 自动消费 outbox
- 普通实验无需人工操作即可入仓

开发和测试阶段可以继续默认关闭，直到 dev port 验证通过并得到部署批准。

### 6.4 Completion hook 位置

复用现有 hook 基础：

- 单次实验完成路径调用 `safe_archive_experiment_completed(experiment_id=...)`。
- 演进 loop 完成路径调用 `safe_archive_loop_completed(task_id=..., loop_id=..., loop_index=...)`。

需要扩展这些 hook：

- 计算有效 `archive_policy`。
- 如果是 `SKIP`，写 skip audit。
- 如果是 `AUTO`，写 outbox。
- 使用 idempotency key 避免重复事件。
- 返回结构化 hook 状态，方便日志和诊断。

### 6.5 幂等性

每个入仓 source 应有稳定 logical key：

- 单次实验：source type + `experiment_id`
- 演进 loop：source type + `task_id` + `loop_index`

重复 completion hook、worker retry、历史 backfill 应更新同一个 `qe_archive.run`，不能生成重复 logical run。现有 backfill 的幂等替换行为需要保留并扩展。

### 6.6 失败行为

Archive 失败不能失败 QE，只能进入 archive 状态体系：

- outbox event 状态：`pending`、`processing`、`failed`、`dead_letter`、`completed`、`skipped`
- archive job 记录错误摘要和 retry 次数
- health endpoint 展示 backlog 和失败
- UI/MCP 可以看到是否需要手动 retry

在 `max_retries` 前自动重试；超过后进入 `dead_letter`，需要人工检查。

## 7. 历史实验一次性补齐

### 7.1 要求

系统应提供历史 QE 实验一次性补齐功能：

- 先 preview，再 execute。
- 展示 source type、有效性、已入仓、跳过、提取失败、策略跳过等统计。
- 支持宽泛 backfill，不需要逐个 id 选择。
- 默认尊重 `archive_policy=SKIP`。
- 幂等、可恢复、可重试。

### 7.2 API 流程

扩展现有 `/api/v1/qe-archive/backfill` 和 `/backfill-candidates`，形成更清晰的 backfill run 模型：

```text
POST /api/v1/qe-archive/backfill/preview
POST /api/v1/qe-archive/backfill/execute
GET  /api/v1/qe-archive/backfill/runs
GET  /api/v1/qe-archive/backfill/runs/{backfill_run_id}
POST /api/v1/qe-archive/backfill/runs/{backfill_run_id}/resume
```

当前 `/api/v1/qe-archive/backfill` 可以保留兼容，但新接口更适合 UI 和 MCP。

### 7.3 Backfill 模式

建议模式：

```text
all_completed
single_experiments
custom_evo_loops
task_id
experiment_ids
loop_ids
missing_only
failed_retry
```

默认应是：

```text
missing_only + all_completed + respect_skip=true
```

### 7.4 确认行为

UI 上执行 broad backfill 需要明显确认步骤。

MCP 上执行 broad backfill 需要确认 token，例如：

```text
confirm_write=QE_ARCHIVE_WRITE
```

这与现有 backfill API 设计一致。区别是：历史宽泛写入需要确认；但 realtime completion 入仓一旦启用，`archive_policy=AUTO` 的普通实验不需要每次人工确认。

## 8. QE Archive MCP

### 8.1 角色

`aistock-qe-archive` 是智能体的数仓证据服务器，不负责发起实验。

主要工具：

```text
qe_archive_health
qe_archive_list_runs
qe_archive_get_run_quality
qe_archive_query_factor_summary
qe_archive_query_factor_pair_synergy
qe_archive_query_model_trials
qe_archive_query_seed_trials
qe_archive_query_hyperparam_history
qe_archive_query_priority_queue
qe_archive_backfill_preview
qe_archive_backfill_status
```

写类工具只允许：

```text
qe_archive_record_agent_query_audit
qe_archive_backfill_execute_confirmed
qe_archive_retry_failed_event_confirmed
```

执行/重试类工具必须要求显式确认 token。

### 8.2 查询设计

查询必须调用白名单后端 API，这些 API 由 view 或 repository function 支撑。禁止向智能体暴露任意 SQL。

每个查询都应强制：

- limit/page 上限
- source type 过滤
- 默认 `research_valid=true`
- 只有显式请求时才包含 excluded runs
- 审计 agent name、tool name、filters、result count

### 8.3 证据质量分层

Archive MCP 必须明确区分：

- 有效研究证据：`research_valid=true`
- 无效/排除实验
- capture 不完整实验
- failed runs
- 主动跳过的实验

这对防止因子选择泄漏、优化器 warm-start 污染非常关键。

## 9. QE Research MCP

### 9.1 角色

`aistock-qe-research` 负责当前 QE 实验分析和模板生命周期，并查询 `aistock-qe-archive` 获取历史证据。

主要只读工具：

```text
qe_list_experiments
qe_get_experiment
qe_get_experiment_metrics
qe_list_evolution_tasks
qe_get_evolution_task
qe_get_loop_metrics
qe_get_task_trajectory
qe_get_logs_tail
```

模板工具：

```text
qe_template_create
qe_template_validate
qe_template_get
qe_template_list
qe_template_update
qe_template_approve_confirmed
qe_template_submit_confirmed
qe_template_supersede
```

执行状态工具：

```text
qe_get_submitted_status
qe_get_submitted_logs_tail
qe_collect_submitted_result
qe_compare_template_result
```

### 9.2 执行门禁

`qe_template_submit_confirmed` 必须要求：

- template status 是 `ready_for_review` 或 `approved`
- 最近一次 validation 通过
- `confirm_run` 符合要求
- loop 数和 node parallelism 在 MCP 配置预算内
- archive policy 显式存在

建议确认文本：

```text
confirm_run=QE_TEMPLATE_SUBMIT
```

这样用户既可以在聊天里确认，也可以在 UI 里通过同一后端 submit API 确认。

## 10. 数仓与模板的联动

模板配置应在创建时包含 archive policy：

```json
{
  "archive_policy": "AUTO",
  "archive_reason": "normal research experiment",
  "template_kind": "custom_evo",
  "loops": []
}
```

提交时：

- materializer 将 `archive_policy` 复制到 runtime QE 记录。
- completion hook 读取 runtime policy，而不是只看模板。
- archive run 记录 template provenance。

Archive provenance 应包含：

- `template_id`
- `created_by`
- 用于生成建议的 source archive runs
- config hash
- submitted runtime id

这支持 post-run learning：系统可以比较模板预期价值和实际归档结果，为下一轮建议提供反馈。

## 11. 实施阶段

### Phase 0：设计审阅与最终确认

任务：

- 审阅本文档。
- 确认 QE MCP 最终拆分：两个 QE MCP，还是一个 QE MCP 内部分 archive/research namespace。
- 确认 archive policy 值。
- 确认模板 UI 是否必须进入第一批实现。

验证：

- 设计已审阅并确认。
- 无 runtime 行为变更。

### Phase 1：补齐 Archive 自动入仓

实现：

- effective archive policy resolver。
- completion hook policy check。
- outbox idempotency key。
- archive worker autostart，带显式环境变量。
- health/backlog 状态增强。
- skip audit tracking。

验证：

- policy resolver 单测覆盖 `AUTO`、`SKIP`、`MANUAL_ONLY`、legacy default。
- outbox 幂等 enqueue 单测。
- 后端测试证明 archive 失败不会改变 QE 状态。
- dev-port smoke：合成完成 QE source 自动 enqueue，worker 自动入仓，无人工操作。
- data-quality smoke 显示 archive run count 增加，pending outbox 回到 0。

反模式保护：

- 不允许通过 Windows worker workspace path 直接归档。
- 不允许 archive 失败影响 QE 状态。
- 不允许默认把无效日频回测作为优化证据。

### Phase 2：历史 backfill UX/API 强化

实现：

- 明确的 preview / execute / status / resume endpoint。
- backfill run 记录。
- 默认尊重 skip policy。
- failed/dead-letter archive job retry。

验证：

- 宽泛预览历史完成 QE，不写入。
- 使用确认 token 执行有限 backfill batch。
- 重跑同一 batch，证明幂等。
- skipped experiments 不入仓，但在 skip audit/status 中可见。

### Phase 3：模板/提案后端

实现：

- 模板 schema 与 repository。
- 单次实验模板 payload 支持。
- custom_evo 模板 payload 支持。
- dry-run validator。
- 仅在 submit 时 materialize 到 runtime QE 记录。
- 模板提交后的状态镜像。

验证：

- 创建模板不会触发 runtime QE task/experiment 执行。
- 模板 validation 通过。
- 带确认提交后才创建 runtime 记录。
- custom_evo 模板不会在 submit 前自动运行。
- archive policy 正确复制到 runtime 记录。

### Phase 4：QE MCP servers

实现：

- `scripts/aistock_qe_archive_mcp_server.py`
- `scripts/aistock_qe_research_mcp_server.py`
- `.mcp.json` opt-in 配置或文档说明。
- 如有必要，抽取 shared MCP support utilities。

验证：

- MCP tool list 可加载。
- loopback base URL 强制生效。
- identifier sanitize 拒绝非法 id。
- 只读工具能返回当前 QE 数据。
- template submit 和 backfill execute 工具必须带确认 token。

### Phase 5：UI 增量入口

实现：

- 模板列表/详情 UI。
- 模板 validation 和 submit 按钮。
- 实验/模板配置中的 archive policy selector。
- Archive health/backfill status 页面或面板。

验证：

- 现有 QE direct-run UI 仍然工作。
- 新模板流程支持保存、查看、校验、提交。
- 用户可以为特殊实验选择不入仓。
- Archive health 显示当前 run/backlog/skips。

### Phase 6：面向优化的 Archive 证据 API

实现：

- 因子 summary API/view。
- 因子 pair synergy API/view。
- 模型 trial summary API/view。
- seed 与超参历史 API/view。
- priority queue API/view。
- agent audit query logging。

验证：

- 查询默认只返回 `research_valid=true`。
- excluded/invalid/skipped runs 只有显式过滤才展示。
- MCP 证据工具可以在无任意 SQL 的情况下生成候选分析。

## 12. 待审阅决策

1. QE Archive MCP 和 QE Research MCP 是否拆成两个 server？建议：拆成两个，权限边界更清楚。
2. v1 是否保留 `MANUAL_ONLY`？建议：保留，支持敏感实验后续按需定向入仓。
3. Archive autostart 是否只在生产配置打开？建议：实现和验证阶段默认关闭，部署配置确认后打开。
4. 模板 UI 是否进入首批实现？建议：后端 + MCP 先做，UI 第二阶段；除非你要求 UI 审阅为 v1 必须项。
5. 跳过实验记录是否用独立 `skip_registry` 表？建议：如果 archive health 要清楚展示跳过原因，则用独立表。

## 13. 非目标

- 不自动提升 StrategyPackage。
- 不自动准入 Paper v2。
- 不暴露任意 SQL MCP 权限。
- 不直接修改 RD-Agent worker workspace。
- 不改变现有 QE direct-run UI 行为，除非明确批准。
- 不基于回顾性 OOS 指标宣称无偏自动因子选择。

## 14. 预期最终流程

实施完成后，普通流程为：

```text
Codex 或用户创建 QE 模板
  -> 模板 dry-run 校验
  -> 用户在聊天或 UI 中审阅
  -> 用户确认 submit
  -> QE run 执行
  -> 如果 archive_policy=AUTO，completion hook 写入 outbox
  -> archive worker 无需人工干预持久化结果
  -> archive MCP 将结果暴露为未来证据
  -> research MCP 比较实际结果并提出下一轮受控模板
```

特殊实验可以选择 `archive_policy=SKIP`，宽泛历史 backfill 默认不会导入这些实验。

## 15. Claude Code 建议（基于澄清后修订）

本章节是在用户提供以下关键约束后，对前文设计的补充与修订。这些约束应作为后续实现的硬假设，先于一切其他建议生效。

### 15.1 运行假设

- 回测使用固定时间段，不依赖实时行情，无论当天/隔天均无影响。
- 仅使用因子缓存值，不消费任何实盘数据。
- ST pit 数据、涨跌停字段、因子缓存在 task 创建前已就绪。
- 验证、审阅、提交期间不存在数据漂移面。
- WSL 当前并发上限 = 2 loops，是软限制，未来扩内存后可手工调节。
- 历史实验入仓的回补只做一次，不应在每次新任务执行时重复触发。
- QE 前端目前已可保证实验执行成功。
- MCP 必须复用与 UI 一致的执行路径，不允许独立写一套校验或执行逻辑。
- MCP 生成的实验模板必须保持可编辑：用户在执行前后均可修改并发度或其他配置后继续执行，不得在 submit 后锁死。

### 15.2 修订后的设计原则

#### 15.2.1 MCP 是 UI 的程序化入口，不是新世界

- 单次实验：MCP `qe_template_submit_confirmed` 必须调用 `/api/v1/quantevolver/experiments/{id}/run`，与 UI 完全相同。
- 自定义演进：MCP submit 必须调用现有 `/api/v1/quantevolver/evolution/...` 入口，复用 `submit_custom_evo_all_loops` 链路。
- 禁止 MCP 自行拼装 RD-Agent 调用、绕过 service 层、或重写一份"MCP 专用执行流"。
- MCP `validate` 只做 schema/类型/枚举级校验（沿用后端 pydantic schema），不做数据完整性扫描，不做并发预算硬校验。

#### 15.2.2 模板是可编辑草稿，不是不可变契约

- 模板 submit 后 materialize 到 runtime（`qe_experiments` 或 `qe_evolution_tasks`），用户可继续在 UI 编辑 runtime 配置（并发度、loop 数、factor_keys、strategy_params 等）后再次执行。
- runtime 编辑不强制 supersede 模板，也不锁定模板。
- `config_sha256` 仅用于创建期去重，不用于执行期一致性校验。
- 取消前文若干"submit 后转 read-only / 修改必须 supersede"类设想。

#### 15.2.3 并发=2 是软限制，不写死

- 不在 MCP/template 层做硬拒绝。
- 后端配置项 `AISTOCK_QE_BACKTEST_CONCURRENCY` 默认 2，运维可手工调高。
- 调度器层按当前配额排队，不影响模板可提交性。
- MCP 只读工具可返回"当前 running loops 数 + 配置上限"，让 agent 自行决定是否排队，但不在 submit 路径上强制拒绝。

### 15.3 取消的早期建议

| 取消项 | 理由 |
|---|---|
| validate 强制 ≤ 2 loops 并发 | 软限制 + 调度器已处理；UI 不做 MCP 也不做 |
| validate 期间扫描因子缓存/ST pit 完整性 | 数据前置条件保证就绪；UI 不扫 MCP 也不扫 |
| 模板 submit 后转 read-only | 与"运行时可编辑"约束直接冲突 |
| 修改 runtime 必须走 supersede | 同上 |
| validate 阶段拒绝 research_valid=false 配置 | 数据前置条件已保证；该标识由 archive 阶段填，不作为 validate 门禁 |
| validate→submit 之间冻结 data snapshot | 数据无漂移面 |

### 15.4 保留并强化的建议

#### 15.4.1 一次性历史 backfill 协议（Phase 1 上线前必做）

- 新增 `qe_archive.bootstrap_marker(source_type, completed_at, ingested_count, mode, operator)`。
- **per source_type 一行**：`single_experiment` 与 `custom_evo_loop` 各自独立标记，便于程序根据不同实验类型选择合理处理方式（如不同的 payload extractor、不同的有效性规则）。
- 同一 source_type 已存在 marker 时，broad backfill 直接拒绝；只有显式 `force_rebackfill=QE_ARCHIVE_REBOOTSTRAP` token 才允许重做。
- 稳态路径完全依赖 realtime hook + outbox worker，新任务永不触发宽泛 backfill。

#### 15.4.2 outbox latest-wins + ingest_history 子表

- `qe_archive.run` 表对同一 logical key 采用 latest-wins 覆盖。
- 新增 `qe_archive.ingest_history` 子表，保留每次入仓快照（trigger_reason: realtime / backfill / retry / manual / rebootstrap）。
- 由于回测时间段固定 + 因子缓存确定性 + 数据无漂移，多次入仓在正常情况下结果应一致；任何差异本身就是诊断信号。

#### 15.4.3 archive worker 单 leader

- outbox 表新增 `claimed_by`、`claimed_at`、`claim_token` 字段。
- worker 使用 `SELECT ... FOR UPDATE SKIP LOCKED` 或 PG advisory lock。
- uvicorn 多 worker 部署下不会重复消费同一 outbox event。

#### 15.4.4 模板 lineage 与 runtime diff（支持 post-run learning）

由于 runtime 可编辑，必须同时记录"提案配置"与"实测配置"两份指纹：

新增字段：
- `parent_template_id`：上一版模板（agent 迭代提案时填）。
- `proposed_metrics_json`：模板创建时 agent 给出的预期 IC/Sharpe/PA 区间。
- `actual_metrics_json`：archive worker 入仓时回填的实测指标。
- `metric_delta_json`：自动计算差异。
- `runtime_config_sha256`：实测时 runtime config 的 hash（可与 `config_sha256` 不同）。
- `runtime_diff_json`：runtime 配置相对模板 `config_json` 的差异（用户/agent 改了什么）。

这套字段同时支持："agent 提案命中率统计"、"用户对模板的常见修改模式"、"修改后是否更优"三类分析。

#### 15.4.5 created_by 结构化

- `created_by_kind` enum：`user` / `codex` / `claude` / `other_agent` / `ui`。
- `created_by_id`：agent session id 或用户名。
- `created_by_version`：agent/客户端版本，便于评估不同 agent 版本提案质量。

#### 15.4.6 数据版本归档字段（仅 audit，不作门禁）

模板与 runtime 都记录引用的数据版本：

```
data_versions = {
  "factor_lib_version":  "...",
  "st_pit_version":      "...",
  "limit_price_version": "..."
}
```

archive 入仓时一并归档，支持"同一模板在不同因子库版本上的效果对比"。submit 阶段不做版本一致性校验，因约束保证一致。

### 15.5 待审阅决策（修订与确认）

1. 模板 submit 后是否锁配置：**已确认不锁**。runtime 保持可编辑，模板记录 runtime diff。
2. bootstrap_marker 是否区分 source_type：**已确认区分**。`single_experiment` 与 `custom_evo_loop` 各自一行 marker，程序按类型选择处理路径。
3. WSL 并发上限：**已确认为软限制**。`AISTOCK_QE_BACKTEST_CONCURRENCY=2` 作为后端配置项，运维手工调，不在 MCP/template 层强制。
4. MCP 与 UI 是否同源：**已确认必须同源**。MCP submit/validate 必须复用 UI 同一后端 endpoint。

### 15.6 修订后的 Phase 实施清单

**Phase 1 必做（Archive 自动入仓）**
- bootstrap_marker per source_type + broad backfill 拒重复（15.4.1）
- outbox latest-wins + ingest_history 子表（15.4.2）
- archive worker 单 leader 锁（15.4.3）
- effective archive policy resolver（前文 §6.4 不变）

**Phase 3 必做（模板/提案后端）**
- 模板 lineage 字段：parent_template_id / proposed_metrics / actual_metrics / runtime_config_sha256 / runtime_diff（15.4.4）
- created_by 结构化（15.4.5）
- 数据版本归档字段（15.4.6）
- 明确"materialize 后 runtime 仍可编辑"，不在代码或 UI 中加锁

**Phase 4 必做（MCP servers）**
- MCP submit 工具直接调用 UI 同一后端 endpoint（15.2.1）
- MCP validate 工具仅做 schema 级校验
- MCP 只读工具暴露当前 running loops 数与 `AISTOCK_QE_BACKTEST_CONCURRENCY`，作为信息返回，不作硬拒绝
- MCP 之间不互调；共享路径只能是后端 API

**Phase 5 增量（UI）**
- UI 文案三态对齐：AUTO / MANUAL_ONLY / SKIP
- 模板详情页展示 `runtime_diff_json`，让用户直观看到自己改了什么

**取消项（不再实现）**
- validate 阶段强制并发上限校验
- validate 阶段数据完整性扫描
- 模板提交后 read-only 锁定
- runtime 修改必须 supersede

### 15.7 文档头部建议追加的"运行假设"段

建议在 §1 目标之后、§2 当前事实与约束之前，加一段简短的"运行假设"，把 15.1 的关键三条压缩为读者第一眼能看到的前置条件：

> 本设计依赖以下运行假设：回测时间段固定且仅消费因子缓存（无实时行情面）；ST pit/涨跌停/因子缓存在 task 前置条件中已就绪；WSL 并发为软限制（默认 2 loops，可手工调）；历史 backfill 仅做一次；MCP 必须复用 UI 同源后端 endpoint；模板生成的配置在 submit 后仍可由用户编辑并继续执行。

把这段话前置可以让后续 reviewer 立即对齐边界，避免重复讨论已经被排除的早期设想。
