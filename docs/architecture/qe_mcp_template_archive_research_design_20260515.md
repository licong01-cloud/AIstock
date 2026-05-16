# QE MCP、实验模板与数仓自动归档设计方案

更新时间：2026-05-16

## 1. 目标

本方案面向 QuantEvolver（以下简称 QE）实验体系，目标是在不改变现有 QE UI 直接执行能力的前提下，新增与现有流水线 MCP 平行且独立的 QE MCP 能力，让智能体可以通过受控接口完成实验分析、模板生成、用户确认后的 QE 实验调度、实验结果入仓、历史数仓查询和下一轮实验优化建议。

最终目标是：智能体可以基于当前 QE 实验和历史数仓证据，分析因子组合、模型回测、seed、超参和执行参数，生成可审阅的 QE 实验模板；用户确认后，MCP 只能调用现有 QE 后端程序来执行单次实验或自定义演进实验；实验完成后自动进入 QE 数仓，为下一轮优化提供可靠历史依据。

本方案不让 MCP 调度 RD-Agent，也不让 MCP 新建任何独立执行链路。QE 后端内部如果按现有实现调用 RD-Agent、Qlib、WSL 或远端节点，那仍属于 QE 后端的既有实现；MCP 只允许作为 QE 后端 API 的程序化入口。

## 2. 运行假设与硬约束

### 2.1 运行假设

- QE 回测使用固定时间段，主要消费已经准备好的因子缓存，不依赖实时行情面。
- ST pit 数据、涨跌停字段、因子缓存和基础数据版本应在 QE task 创建前置条件中就绪。
- 实验配置审阅和提交期间默认不存在需要 MCP 额外冻结的数据漂移面。
- WSL 当前默认并发可按 2 loops 处理，但这是软限制；未来内存或资源扩展后可由运维手工调高。
- 历史实验宽泛 backfill 只做一次性补齐，不应在每次新任务执行时重复触发。
- MCP 生成或保存的模板必须保持可编辑；用户可以在真正执行前继续检查和修改配置。
- 模板物化到 runtime 后，runtime 配置仍可由 UI 或后端既有编辑接口继续调整；是否再次执行由用户确认。

### 2.2 硬约束

- MCP 调度对象是 QE 实验，不是 RD-Agent 任务。
- MCP 禁止直接拼装 RD-Agent 调用、禁止直接读写 RD-Agent worker workspace、禁止绕过 QE 后端 service/route 执行实验。
- MCP 禁止新建执行链路；所有执行请求必须进入 QE 后端现有或 UI/MCP 共用的受控程序入口。
- MCP 的执行类工具必须调用现有 QE 后端程序，优先调用与 UI 同源的 FastAPI endpoint。
- 如果后端确实缺少“保存但不执行”或“已保存后再启动”的公共入口，应在 QE 后端新增 UI 与 MCP 共用的 API；禁止只给 MCP 写一套专用执行流程。
- MCP validate 只能复用后端 schema 和轻量语义校验，不做远端工作区扫描，不做因子缓存/ST pit 全量完整性扫描，不做并发预算硬拒绝。
- MCP 执行类工具必须要求显式确认 token；只读查询工具不需要确认。
- QE archive 失败不能改变 QE 实验、任务或 loop 的源状态。
- 新增 DB 表和字段必须有 PostgreSQL `COMMENT ON TABLE` / `COMMENT ON COLUMN`，并纳入 schema 注释检查。
- 开发前必须同时设计验证方案、预期效果、自动化流水线入口和验收门禁；不得开发完成后再临时补测试。
- 开发完成后必须通过现有自动化流水线/Validation MCP 执行完整验收，保留 run record 和证据路径。
- 开发分支完成验证后只提交 feature 分支；未经用户明确确认不得合入 `main`。
- 不触碰生产 `8001`/`3000` runtime，开发和验证使用 dev 端口。

## 3. 当前事实与约束

### 3.1 现有 MCP 基线

仓库已有 `scripts/aistock_mcp_server.py`，主要服务 Validation Center。它具备以下可复用基础：

- stdio FastMCP server 模式。
- loopback-only backend URL 校验。
- 工具参数校验和受控 HTTP 调用。
- 与 `.mcp.json` 配置集成的基础形态。

QE MCP 可以复用这些基础设施模式，但必须保持产品边界独立，不把 QE 工具混入 Validation MCP。

### 3.2 现有 QE 执行行为

单次实验当前通过 `/api/v1/quantevolver/experiments/{experiment_id}/run` 执行。该接口读取已有 QE 实验配置，并交给 QE 后端统一执行路径处理。

自定义演进实验当前通过 `/api/v1/quantevolver/evolution/custom-tasks` 创建。当前创建后会立即进入 `submit_custom_evo_all_loops` 调度流程，因此现有 runtime 的 `pending` 不是可长期停留、可人工审阅的“待执行模板”状态。

QE 演进任务中已经存在任务查询、详情、停止、删除、恢复、loop retry、fork、custom_evo config 查询、custom loop rerun、append loops、日志、enhanced metrics、trajectory 等后端能力。MCP 应覆盖这些现有 UI 能力，但只能通过 QE 后端已有或新增的共用 API 调用。

### 3.3 现有 QE Archive 状态

当前 QE Archive 已有基础能力：

- `qe_archive` schema 与 repository 基础已经存在。
- 手动历史 backfill、dry-run preview、quality smoke 和 API backfill 基础已经存在。
- realtime ingestion hook 基础已经存在，但默认关闭。
- outbox event、worker service、worker run-once 基础已经存在。
- 仍缺少“QE 完成后默认自动进入数仓”的完整产品闭环。
- 仍缺少历史 backfill 一次性 bootstrap marker、skip registry、ingest history、运行时模板 lineage 等面向优化的审计信息。

因此，不能因为有 hook 和 backfill 基础就宣称数仓已经自动收集所有 QE 实验。必须补齐 completion hook、policy resolver、outbox worker 自动化、一次性 backfill 和状态可观测能力。

### 3.4 本设计依据的现有文件

| 领域 | 现有文件 |
|---|---|
| Validation MCP 基线 | `scripts/aistock_mcp_server.py` |
| MCP 配置 | `.mcp.json` |
| 单次 QE API | `backend/routers/quantevolver.py` |
| QE 演进 API | `backend/routers/quantevolver_evolution.py` |
| QE 自定义演进服务 | `backend/services/quantevolver/qe_evolution_service.py` |
| Archive schema 与注释规范 | `backend/db/init_qe_archive_schema.py` |
| Archive API | `backend/routers/qe_archive.py` |
| Archive realtime hook | `backend/services/qe_archive/realtime_ingestion.py` |
| Archive outbox capture | `backend/services/qe_archive/event_capture.py` |
| Archive worker | `backend/services/qe_archive/worker.py`、`backend/services/qe_archive/worker_service.py` |
| Archive backfill 与 source assembler | `backend/services/qe_archive/source_assembler.py`、`backend/services/qe_archive/backfill_service.py`、`scripts/qe_archive_backfill.py` |
| 项目约束 | `docs/codex_project_memory.md` |

## 4. 目标架构

### 4.1 平行 MCP 服务器

建议新增两个 QE 相关 MCP server，与现有 Validation MCP 平行：

```text
现有：
aistock-validation MCP
  - validation center
  - bug / run / report 工具

新增：
aistock-qe-experiment MCP
  - QE 单次实验查询、保存模板、物化、确认执行
  - QE 自定义演进任务查询、模板、创建、启动、恢复、停止、删除
  - custom_evo loop retry / rerun / append / fork / clone 类操作
  - QE 日志、状态、enhanced metrics、trajectory 查询
  - 基于数仓证据生成下一轮实验模板

新增：
aistock-qe-archive MCP
  - archive health 与 backfill preview/status
  - 历史 run、因子、模型、seed、超参证据查询
  - archive quality 与 skip/invalid 状态查询
  - 受控 backfill execute / retry
```

两个 QE MCP server 可以共享 loopback HTTP client、确认 token 校验、ID sanitize、错误包装等基础工具，但工具命名、权限、环境变量和执行边界必须独立。MCP server 之间不要求互相调用；智能体可以分别调用两个 MCP，底层共享的事实来源只能是后端 API 和数据库服务。

### 4.2 后端服务分层

建议后端新增或扩展以下服务层：

```text
backend/services/qe_templates/
  models.py
  repository.py
  validator.py
  materializer.py
  runtime_diff.py

backend/services/qe_archive/
  policy.py                  # 新增：archive_policy resolver
  bootstrap_marker.py        # 新增：历史 backfill 一次性标记
  skip_registry.py           # 新增：跳过入仓审计
  ingest_history.py          # 新增：每次入仓快照
  realtime_ingestion.py      # 已存在，扩展 policy check
  worker_service.py          # 已存在，扩展自动 worker loop
  backfill_service.py        # 已存在，扩展 preview/status/resume

scripts/
  aistock_qe_experiment_mcp_server.py
  aistock_qe_archive_mcp_server.py
```

如果某个功能现有 UI 已经使用后端 endpoint，则 MCP 必须调用同一个 endpoint。如果某个功能目前只有 service 内部方法，没有稳定 endpoint，则应先补一个 UI 和 MCP 共用的后端 endpoint，再由 MCP 调用。

### 4.3 禁止的新链路

以下做法明确禁止：

- MCP 直接调用 RD-Agent worker、直接构造 RD-Agent task、直接操作 RD-Agent workspace。
- MCP 直接导入 QE scheduler 并绕过 FastAPI route 做执行。
- MCP 维护一套与 UI 不同的 validate、materialize、run 逻辑。
- MCP 为远端节点单独实现并发调度器。
- MCP 用任意 SQL 修改 QE runtime 表或 archive 表。
- MCP 读取 Windows 或远端 worker workspace path 来归档实验结果。

## 5. QE 实验模板层

### 5.1 为什么需要独立模板层

独立模板/提案层优于简单新增 QE runtime status，原因如下：

- 单次实验和自定义演进实验使用不同 runtime 表和执行流程。
- 当前 custom_evo 创建后会立即进入执行流程，不是人工审阅状态。
- 模板需要保存分析摘要、风险摘要、配置差异、数仓策略、审批元数据和 agent 提案信息，这些不适合塞入核心 runtime 表。
- 模板可以同时供 MCP 和 UI 检查，而不影响现有实验列表、调度器和运行状态同步。
- 模板是可编辑草稿，不是不可变契约；真正执行的 runtime 配置可能被用户继续修改，因此必须记录 runtime diff。

### 5.2 模板表

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
| `config_sha256` | 创建期配置 hash，用于去重和审计，不作为执行期锁定。 |
| `archive_policy` | 该模板生成 runtime run 的默认入仓策略。 |
| `archive_reason` | 选择入仓策略的原因。 |
| `source_context_json` | 生成模板时参考的实验、任务、archive runs。 |
| `analysis_summary_md` | 为什么提出这个模板。 |
| `risk_summary_md` | 已知风险与约束。 |
| `validation_json` | 最近一次轻量校验结果。 |
| `approval_json` | 审批人、审批时间、确认文本等。 |
| `parent_template_id` | 上一版模板 ID，用于 agent 迭代提案 lineage。 |
| `proposed_metrics_json` | 模板创建时预期 IC、RankIC、Sharpe、PA、回撤等区间。 |
| `created_by_kind` | `user`、`ui`、`codex`、`claude`、`other_agent`、`service`。 |
| `created_by_id` | 用户名、agent session id 或服务标识。 |
| `created_by_version` | agent 或客户端版本。 |
| `data_versions_json` | 因子库、ST pit、涨跌停、训练数据等版本引用，仅审计，不作 submit 门禁。 |
| `submitted_experiment_id` | 物化后绑定的单次实验 ID。 |
| `submitted_task_id` | 物化后绑定的演进任务 ID。 |
| `runtime_config_sha256` | 最近一次实测 runtime config hash。 |
| `runtime_diff_json` | runtime 配置相对模板配置的差异。 |
| `actual_metrics_json` | 入仓后回填的实测指标摘要。 |
| `metric_delta_json` | 预期指标与实际指标差异。 |
| `created_at` / `updated_at` | 审计时间。 |

所有新增表和字段必须有 PostgreSQL 注释，并加 schema 测试。

### 5.3 模板状态机

建议生命周期：

```text
draft
  -> ready_for_review
  -> approved
  -> materialized
  -> run_requested
  -> running
  -> completed
  -> failed
  -> cancelled
  -> superseded
  -> expired
```

状态语义：

- `draft`：配置未完成或正在编辑。
- `ready_for_review`：配置完整，轻量校验通过，可供用户检查。
- `approved`：用户已明确审批，但尚未物化 runtime。
- `materialized`：已经创建或绑定 `qe_experiments` / `qe_evolution_tasks` runtime 记录，但尚未真正启动执行。
- `run_requested`：用户或 MCP 已通过 QE 后端同源 endpoint 请求执行。
- `running` / `completed` / `failed` / `cancelled`：镜像 runtime 执行状态。
- `superseded`：被新模板替代；仅表示提案版本更新，不强制影响已经物化的 runtime。
- `expired`：因模型、因子、数据契约或执行契约变化而失效。

`materialized` 与 `run_requested` 必须分开，避免把“创建待执行 runtime”误解为“已经提交执行”。

### 5.4 单次实验模板 API

新增稳定 API 前缀，例如：

```text
GET  /api/v1/qe-templates
POST /api/v1/qe-templates
GET  /api/v1/qe-templates/{template_id}
PUT  /api/v1/qe-templates/{template_id}
POST /api/v1/qe-templates/{template_id}/validate
POST /api/v1/qe-templates/{template_id}/approve
POST /api/v1/qe-templates/{template_id}/materialize
POST /api/v1/qe-templates/{template_id}/run
POST /api/v1/qe-templates/{template_id}/supersede
```

当 `template_kind=single_experiment` 时：

1. `materialize` 创建或绑定 `qe_experiments` runtime 记录，不启动执行。
2. 用户可以在 UI 中打开 runtime 配置继续检查或修改。
3. `run` 必须调用与 UI 同源的 `/api/v1/quantevolver/experiments/{experiment_id}/run`。
4. 现有 UI 的直接创建/直接执行行为保留，不被模板流程替代。

### 5.5 自定义演进模板 API

同一模板 API 支持 `template_kind=custom_evo`。配置结构应对齐当前 `CustomEvoLoopConfig` 语义。

当 `template_kind=custom_evo` 时：

1. `materialize` 创建或绑定 `qe_evolution_tasks` runtime 记录，但必须支持 `auto_start=false`，不立即调用 `submit_custom_evo_all_loops`。
2. 用户可以在 UI 中打开 custom_evo 配置继续检查或修改 loop、并发、节点、因子、模型、策略参数。
3. `run` 必须调用 QE 后端共用启动入口，再由后端复用现有 `submit_custom_evo_all_loops` 链路。
4. 当前 `/custom-tasks` 可以继续保持 UI 兼容的“创建即运行”行为，但应补充共用后端能力支持“创建但不启动”。
5. MCP 不能直接调用 service 内部 scheduler 作为替代路径。

轻量校验应覆盖：

- `factor_keys` 是否格式正确、是否在 catalog 中可识别。
- `disable_alpha158` 是否显式。
- `model_id` 是否存在且与实验类型兼容。
- `strategy_params` 是否符合策略 schema。
- `label_horizon` 是否规范化。
- `backtest_only` loop 是否具有 source task/loop，且 source label horizon 一致。
- archive policy 是否有效。
- execution algo 字段是否满足 QE/Paper 执行契约。
- node_id 和 node_parallelism 是否格式有效。

不在 validate 阶段做：

- 远端工作区扫描。
- 因子缓存/ST pit/涨跌停全量完整性扫描。
- WSL 并发预算硬拒绝。
- `research_valid=false` 的硬门禁。
- validate 到 submit 之间的数据 snapshot 冻结。

### 5.6 UI 行为

第一阶段不需要改变现有 QE direct-run UI 行为。

后续可添加：

- 模板列表页或侧边栏。
- 模板详情页：配置 JSON/diff、分析摘要、风险摘要、轻量校验结果、入仓策略、资源提示。
- 操作按钮：`Validate`、`Approve`、`Materialize`、`Run`、`Clone`、`Supersede`。
- 入仓策略选择器：`自动入仓`、`不入仓`、`仅手动入仓`。
- runtime diff 展示：让用户看到模板物化后自己或 agent 修改了什么。

## 6. MCP 覆盖的 QE 实验能力

### 6.1 覆盖原则

QE MCP 的目标是覆盖现有 QE UI 的实验能力，但不是复制 UI 页面。每个 MCP tool 必须映射到一个明确的 QE 后端 allowlist endpoint 或后端新增的 UI/MCP 共用 endpoint。

实现时必须维护一份“UI 功能到 MCP 工具再到后端 endpoint”的 parity matrix。任何无法找到后端 endpoint 的 UI 功能，不允许由 MCP 自行实现执行逻辑；应先补后端共用 API。

### 6.2 单次实验工具

建议 MCP 工具：

```text
qe_experiment_list
qe_experiment_get
qe_experiment_get_config
qe_experiment_validate_config
qe_experiment_run_confirmed
qe_experiment_stop_confirmed
qe_experiment_get_status
qe_experiment_get_logs_tail
qe_experiment_get_enhanced_metrics
qe_experiment_get_trade_stats
qe_experiment_sync_results_confirmed
qe_experiment_regenerate_confirmed
qe_experiment_analyze
qe_experiment_delete_confirmed
```

其中执行和破坏性工具必须要求确认 token。`qe_experiment_run_confirmed` 必须调用 `/api/v1/quantevolver/experiments/{experiment_id}/run` 或后端同源替代 endpoint。

### 6.3 自定义演进工具

建议 MCP 工具：

```text
qe_custom_evo_create_template
qe_custom_evo_materialize_confirmed
qe_custom_evo_run_confirmed
qe_custom_evo_list_tasks
qe_custom_evo_get_task
qe_custom_evo_get_config
qe_custom_evo_update_config
qe_custom_evo_stop_confirmed
qe_custom_evo_resume_confirmed
qe_custom_evo_delete_confirmed
qe_custom_evo_retry_loop_confirmed
qe_custom_evo_rerun_loop_confirmed
qe_custom_evo_append_loops_confirmed
qe_custom_evo_fork_task_confirmed
qe_custom_evo_clone_task_confirmed
qe_custom_evo_get_loop_metrics
qe_custom_evo_get_trajectory
qe_custom_evo_get_logs_tail
```

这些工具覆盖用户明确要求的自定义演进、loop 重试、克隆、重跑等能力。实现时应优先映射以下现有 QE 后端能力：

- `GET /api/v1/quantevolver/evolution/tasks`
- `GET /api/v1/quantevolver/evolution/tasks/{task_id}`
- `POST /api/v1/quantevolver/evolution/custom-tasks`
- `GET /api/v1/quantevolver/evolution/tasks/{task_id}/custom-evo-config`
- `POST /api/v1/quantevolver/evolution/tasks/{task_id}/resume`
- `POST /api/v1/quantevolver/evolution/tasks/{task_id}/stop`
- `DELETE /api/v1/quantevolver/evolution/tasks/{task_id}`
- `POST /api/v1/quantevolver/evolution/tasks/{task_id}/loops/{loop_index}/retry`
- `POST /api/v1/quantevolver/evolution/tasks/{task_id}/loops/{loop_index}/rerun`
- `POST /api/v1/quantevolver/evolution/tasks/{task_id}/custom-loops/append`
- `POST /api/v1/quantevolver/evolution/tasks/{task_id}/fork`
- `GET /api/v1/quantevolver/evolution/tasks/{task_id}/trajectory`
- `GET /api/v1/quantevolver/evolution/tasks/{task_id}/logs/tail`
- `GET /api/v1/quantevolver/evolution/tasks/{task_id}/loops/{loop_id}/enhanced-metrics`

如果“克隆”当前只是通过 `clone_from_task_id` 或 fork 语义间接支持，则应在后端增加 UI/MCP 共用的 clone endpoint 或明确将 clone 定义为“从已有 custom_evo config 复制生成新模板/新任务”。

### 6.4 本期不覆盖的 QE 能力

以下能力本期不纳入 MCP 执行调度：

- 自动演进中由 LLM 决定演进方向的任务创建和推进。
- 多 alpha 架构实验调度。

原因：自动演进的 LLM 决策需要额外治理和审计；多 alpha 架构仍需在 QE 实验研发中继续完善。MCP 可以查询相关只读状态或文档，但不能在本期发起这些执行。

## 7. 节点与并行执行边界

### 7.1 支持节点

MCP 需要支持通过 QE 后端在以下节点类型执行实验：

- 本地节点：支持现有 QE 后端已经支持的单次实验和自定义演进实验。
- WSL 节点：支持所有类型 QE 实验，包括单次实验、自定义演进、训练和回测。
- 远端机节点：软限制为只运行 CPU 训练模型相关实验。

### 7.2 软限制语义

远端机“只能运行 CPU 训练模型实验”应作为软限制处理：

- MCP 可在提交前返回 warning，提示该节点类型不适合 GPU、特殊依赖或未确认类型实验。
- MCP 不在 submit 路径硬拒绝，除非后端已有明确校验失败。
- QE 后端 scheduler 和 node preflight 是最终执行权威。
- 后续可在后端增加节点能力 catalog，让 MCP 查询节点支持矩阵并给出更准确建议。

### 7.3 并发策略

- WSL 默认并发上限可按 `AISTOCK_QE_BACKTEST_CONCURRENCY=2` 处理，但只是运行配置默认值，不写死在模板或 MCP 中。
- MCP 只读工具可以返回当前 running loops 数、节点队列、配置上限和资源提示。
- 调度器层按当前配额排队或执行，不影响模板保存和物化。
- `node_parallelism` 必须通过现有 QE 后端配置传递，MCP 不实现自己的并发调度器。

## 8. 入仓策略与特殊实验跳过

### 8.1 策略模型

每个模板或 runtime 实验都应显式携带 `archive_policy`：

```text
AUTO
SKIP
MANUAL_ONLY
```

语义：

- `AUTO`：实验完成后自动入仓，默认普通研究实验使用。
- `SKIP`：不自动入仓，也不被宽泛历史 backfill 导入，除非使用特权 override。
- `MANUAL_ONLY`：完成时不自动入仓，但允许按 id 做定向手动 backfill。

### 8.2 策略存储位置

建议存储位置：

- 模板：`qe_execution_templates.archive_policy`。
- 单次实验 runtime：`qe_experiments.custom_params.archive_policy`，或新增一列一等字段。
- 自定义演进 task：`qe_evolution_tasks.strategy_evo_config.archive_policy` 作为 task 默认值。
- 自定义演进 loop：可选 per-loop `archive_policy`，覆盖 task 默认值。

Archive source assembler 和 realtime ingestion 必须读取 runtime 记录上的有效策略。对于历史记录，如果没有策略字段，默认按 `AUTO` 处理，除非 backfill 请求明确过滤。

### 8.3 SKIP 审计

`SKIP` 不应变成“完全无痕迹”。建议新增：

```text
qe_archive.skip_registry
```

记录内容：

- source type / source id / source sub id。
- skip reason。
- created_by。
- created_at。
- 是否允许特权 override。

这样可以同时满足“特殊实验不进入数仓”和“系统可以解释为什么没入仓”。

## 9. QE 完成后自动入仓

### 9.1 目标行为

当 QE 单次实验或 custom_evo loop 完成后：

1. QE 源 DB 事务成功提交。
2. completion hook 读取有效 `archive_policy`。
3. 如果策略是 `AUTO`，hook 写入 durable outbox event。
4. 如果策略是 `SKIP`，写 skip registry，不写 outbox。
5. 如果策略是 `MANUAL_ONLY`，记录可手动补录状态，不自动入仓。
6. archive worker 无需人工干预处理 outbox。
7. archive service 幂等写入或更新 `qe_archive`。
8. archive health 展示成功、等待、跳过或失败状态。
9. archive 失败绝不改变 QE 源状态。

### 9.2 默认使用 outbox

默认 realtime 模式应是 durable outbox，而不是在 QE 完成路径里直接写 archive：

```text
QE completion path -> outbox event -> archive worker -> qe_archive tables
```

理由：

- 避免 QE 完成事务被 archive 解析拖慢。
- 保持 archive 失败与 QE 状态隔离。
- 支持重试、dead letter、状态可观测。
- 支持多个 backend worker 下的单次消费。

### 9.3 Worker 自动化

当前已有 worker run-once 能力，但产品闭环需要自动 worker loop。建议新增显式环境变量：

```text
QE_ARCHIVE_REALTIME_ENABLED=true
QE_ARCHIVE_REALTIME_MODE=outbox
QE_ARCHIVE_WORKER_ENABLED=true
QE_ARCHIVE_WORKER_AUTOSTART=true
QE_ARCHIVE_WORKER_INTERVAL_SECONDS=30
QE_ARCHIVE_WORKER_BATCH_SIZE=10
```

默认策略：

- 开发和测试阶段默认关闭 autostart。
- 明确验证后由部署配置打开。
- 只在后端 lifespan 或独立受控 worker 进程中启动。
- 多 uvicorn worker 部署时必须依赖 row lock 或 advisory lock 防重复消费。

### 9.4 Completion hook 位置

建议 hook 放在：

- 单次实验完成路径调用 `safe_archive_experiment_completed(experiment_id=...)`。
- 演进 loop 完成路径调用 `safe_archive_loop_completed(task_id=..., loop_id=..., loop_index=...)`。

hook 内部必须：

- 读取 source runtime 记录。
- 计算有效 `archive_policy`。
- 如果是 `AUTO`，写 outbox。
- 如果不是 `AUTO`，写 skip/manual audit。
- 捕获所有异常，不向 QE runtime 抛出。

### 9.5 幂等性与历史快照

重复 completion hook、worker retry、历史 backfill 应更新同一个 logical run，不能生成重复 logical run。建议采用：

- `qe_archive.run` 对同一 logical key 采用 latest-wins 语义。
- 新增 `qe_archive.ingest_history` 子表，记录每次入仓快照。
- 记录 `trigger_reason`：`realtime`、`backfill`、`retry`、`manual`、`rebootstrap`。
- 记录 `payload_sha256`、`runtime_config_sha256`、`result_fingerprint`。
- 如果同一 logical key 多次入仓结果不一致，不静默覆盖，应写 anomaly 标记，作为数据诊断信号。

### 9.6 Worker 单 leader

现有 outbox 已有 `locked_by`、`locked_at` 和 `FOR UPDATE SKIP LOCKED` 基础。实现时可以沿用现有字段，不必强制改名为 `claimed_by`。

可选增强：

- 增加 `lock_token` 或 `claim_token`，用于更强的 worker ownership 审计。
- 增加过期 processing 回收机制。
- 增加 dead-letter retry endpoint。

## 10. 历史实验一次性补齐

### 10.1 要求

用户要求历史实验可以一次性补齐入仓，不需要人工逐个处理。设计要求：

- 支持宽泛 backfill，不需要逐个 id 选择。
- 默认尊重 `archive_policy=SKIP`。
- 支持 preview，不写入。
- 支持 execute，需要确认 token。
- 支持 status/resume/retry。
- broad backfill 对每个 source_type 默认只允许成功执行一次。

### 10.2 Bootstrap marker

建议新增：

```text
qe_archive.bootstrap_marker
```

核心字段：

| 字段 | 语义 |
|---|---|
| `source_type` | `single_experiment` 或 `custom_evo_loop`。 |
| `completed_at` | 本 source_type 一次性补齐完成时间。 |
| `ingested_count` | 入仓数量。 |
| `skipped_count` | 跳过数量。 |
| `failed_count` | 失败数量。 |
| `mode` | preview / execute / rebootstrap。 |
| `operator` | 操作者或 agent。 |
| `run_id` | backfill run id。 |

同一 `source_type` 已存在完成 marker 时，broad backfill 直接拒绝；只有显式 `force_rebackfill=QE_ARCHIVE_REBOOTSTRAP` token 才允许重做。

### 10.3 API 流程

扩展现有 `/api/v1/qe-archive/backfill` 和 `/backfill-candidates`，形成更清晰的 backfill run 模型：

```text
POST /api/v1/qe-archive/backfill/preview
POST /api/v1/qe-archive/backfill/execute
GET  /api/v1/qe-archive/backfill/runs
GET  /api/v1/qe-archive/backfill/runs/{backfill_run_id}
POST /api/v1/qe-archive/backfill/runs/{backfill_run_id}/resume
```

当前 `/api/v1/qe-archive/backfill` 可以保留兼容，但新接口更适合 UI 和 MCP。

### 10.4 Backfill 模式

建议支持：

```text
completed_single_experiments
completed_custom_evo_loops
all_completed_qe_sources
specific_ids
```

其中：

- `all_completed_qe_sources` 默认尊重 `SKIP` 和 bootstrap marker。
- `specific_ids` 可用于 `MANUAL_ONLY` 或定向修复。
- 特权 override 必须指定 source ids，不允许无条件扫全量。

### 10.5 确认行为

UI 上执行 broad backfill 需要明显确认步骤。

MCP 上执行 broad backfill 需要确认 token，例如：

```text
confirm_backfill=QE_ARCHIVE_BACKFILL
```

强制 rebootstrap 需要更强确认 token：

```text
force_rebackfill=QE_ARCHIVE_REBOOTSTRAP
```

## 11. QE Archive MCP

### 11.1 角色

`aistock-qe-archive` 是智能体的数仓证据服务器，不负责发起 QE 实验。

它负责：

- 查询历史 archive runs。
- 查询 archive health、outbox、jobs、skip、invalid、quality。
- 提供因子、模型、seed、超参、组合效果等证据查询。
- 执行受控 backfill preview、execute、retry。
- 记录 agent 查询审计。

### 11.2 初期工具

初期先具备查询和基础分析能力：

```text
qe_archive_health
qe_archive_list_runs
qe_archive_get_run
qe_archive_get_run_quality
qe_archive_list_outbox
qe_archive_list_jobs
qe_archive_backfill_preview
qe_archive_backfill_status
qe_archive_query_recent_experiment_metrics
qe_archive_query_factor_usage
qe_archive_query_model_trials
qe_archive_query_seed_trials
qe_archive_query_hyperparam_history
qe_archive_query_invalid_or_skipped
qe_archive_record_agent_query_audit
```

执行类工具：

```text
qe_archive_backfill_execute_confirmed
qe_archive_retry_failed_event_confirmed
```

执行/重试类工具必须要求显式确认 token。

### 11.3 后续增强查询

后续在数仓中设计更完善的查询分析统计能力，再补充给 MCP 调用：

- 因子 summary：出现频率、有效样本数、平均 IC、RankIC、ICIR、收益贡献、回撤暴露。
- 因子 pair synergy：组合提升、相关性、互补度、同簇冗余。
- 模型 trial summary：模型族、参数、训练稳定性、回测表现。
- seed stability：同配置不同 seed 的方差、尾部风险、稳健性。
- hyperparam history：超参区间、最优区间、失败区间。
- priority queue：下一轮建议实验候选。
- agent proposal hit-rate：agent 预期指标与实际入仓指标差异。

### 11.4 查询安全

默认查询必须带安全过滤：

- 默认 `research_valid=true`。
- 默认排除 `archive_policy=SKIP`。
- 默认排除没有权威涨跌停/停牌处理的日频回测。
- 默认限制 row count。
- 禁止任意 SQL MCP 权限。
- 每次 agent 查询可写入 `qe_archive.agent_query_audit`。

如果用户明确要求查看 invalid/skipped/excluded，MCP 可以通过显式过滤参数展示，但结果必须带原因字段。

## 12. QE Experiment MCP

### 12.1 角色

`aistock-qe-experiment` 负责 QE 当前实验、实验模板和可控执行。它可以查询 `qe_archive` 后端 API 或配合 `aistock-qe-archive` 工具获取历史证据，但不在 server 内部直接调用另一个 MCP server。

主要职责：

- 查询当前 QE 单次实验和 QE 演进任务。
- 查询日志、状态、metrics、trajectory。
- 基于历史数仓证据生成新模板。
- 保存模板，等待用户审阅。
- 用户确认后物化 runtime。
- 用户确认后调用 QE 后端同源执行入口。
- 支持 custom_evo loop retry、rerun、append、fork、clone 等现有 UI 能力。

### 12.2 只读工具

```text
qe_list_experiments
qe_get_experiment
qe_get_experiment_config
qe_get_experiment_metrics
qe_get_experiment_logs_tail
qe_list_evolution_tasks
qe_get_evolution_task
qe_get_custom_evo_config
qe_get_loop_metrics
qe_get_task_trajectory
qe_get_task_logs_tail
qe_get_node_capacity_snapshot
```

### 12.3 模板工具

```text
qe_template_create
qe_template_validate
qe_template_get
qe_template_list
qe_template_update
qe_template_approve_confirmed
qe_template_materialize_confirmed
qe_template_run_confirmed
qe_template_supersede
qe_template_compare_result
```

### 12.4 执行工具

```text
qe_experiment_run_confirmed
qe_experiment_stop_confirmed
qe_custom_evo_run_confirmed
qe_custom_evo_stop_confirmed
qe_custom_evo_resume_confirmed
qe_custom_evo_retry_loop_confirmed
qe_custom_evo_rerun_loop_confirmed
qe_custom_evo_append_loops_confirmed
qe_custom_evo_fork_task_confirmed
qe_custom_evo_clone_task_confirmed
```

执行工具必须要求：

- 显式确认 token。
- template 或 runtime id 明确。
- archive policy 显式存在。
- 后端轻量 validation 最近一次通过，或由后端在执行前同步校验。
- node_id / node_parallelism 参数格式有效。
- 对远端节点 CPU-only 软限制返回 warning。

建议确认文本：

```text
confirm_run=QE_EXPERIMENT_RUN
confirm_template=QE_TEMPLATE_MATERIALIZE
confirm_custom_evo=QE_CUSTOM_EVO_RUN
```

### 12.5 数仓驱动的优化提案

MCP 生成新实验模板时，应至少记录：

- 参考的 archive run ids。
- 参考的因子、模型、seed、超参证据。
- 预期改进方向。
- 预期指标区间。
- 风险和排除项。
- 是否继承上一版模板。
- 推荐节点类型和并发提示。
- archive policy。

初期可以基于基础查询结果做分析；后续数仓补充更完善的统计 API 后，MCP 再调用新的 summary/synergy/priority 工具生成更可靠的提案。

## 13. 数仓与模板联动

模板配置应在创建时包含 archive policy：

```json
{
  "archive_policy": "AUTO",
  "archive_reason": "normal research experiment",
  "template_kind": "custom_evo",
  "loops": []
}
```

物化时：

- materializer 将 `archive_policy` 复制到 runtime QE 记录。
- materializer 记录 template provenance。
- materializer 记录 `data_versions_json`，仅用于 audit。
- runtime 可继续编辑，编辑后通过 `runtime_diff_json` 记录差异。

执行完成并入仓时：

- archive run 记录 `template_id`。
- archive run 记录 submitted runtime id。
- archive run 回填 `actual_metrics_json`。
- archive run 计算 `metric_delta_json`。
- archive run 记录 `runtime_config_sha256`。

这支持 post-run learning：系统可以比较模板预期值和实际归档结果，为下一轮建议提供反馈。

## 14. 实施阶段

### Phase 0：设计审阅与最终确认

任务：

- 审阅本文档。
- 确认 QE MCP 分拆为 `aistock-qe-experiment` 与 `aistock-qe-archive`。
- 确认 archive policy 值：`AUTO`、`SKIP`、`MANUAL_ONLY`。
- 确认本期不覆盖自动演进 LLM 决策和多 alpha 调度。
- 确认所有执行类 MCP 工具必须调用 QE 后端同源 API。

验证：

- 设计已审阅并确认。
- 无 runtime 行为变更。

### Phase 1：补齐 Archive 自动入仓

实现：

- effective archive policy resolver。
- completion hook policy check。
- outbox idempotency key。
- skip registry。
- ingest history。
- bootstrap marker per source_type。
- archive worker autostart，带显式环境变量。
- health/backlog/outbox/jobs/skips 状态增强。

验证：

- policy resolver 单测覆盖 `AUTO`、`SKIP`、`MANUAL_ONLY`、legacy default。
- outbox 幂等 enqueue 单测。
- worker row lock 测试证明不会重复消费同一 outbox event。
- 后端测试证明 archive 失败不会改变 QE 状态。
- dev-port smoke：合成完成 QE source 自动 enqueue，worker 自动入仓，无人工操作。
- data-quality smoke 显示 archive run count 增加，pending outbox 回到 0。
- `SKIP` 实验不入仓但可在 skip registry/status 中解释。

反模式保护：

- 不允许通过 Windows worker workspace path 直接归档。
- 不允许 archive 失败影响 QE 状态。
- 不允许默认把无效日频回测作为优化证据。

### Phase 2：历史 backfill UX/API 强化

实现：

- preview / execute / status / resume endpoint。
- backfill run 记录。
- bootstrap marker per source_type。
- 默认尊重 skip policy。
- failed/dead-letter archive job retry。
- rebootstrap 强确认 token。

验证：

- 宽泛预览历史完成 QE，不写入。
- 使用确认 token 执行有限 backfill batch。
- broad backfill 成功后写 marker。
- 重跑同一 source_type broad backfill 被拒绝。
- 使用 `QE_ARCHIVE_REBOOTSTRAP` 才允许强制重做。
- skipped experiments 不入仓，但在 skip audit/status 中可见。

### Phase 3：模板/提案后端

实现：

- 模板 schema 与 repository。
- 单次实验模板 payload 支持。
- custom_evo 模板 payload 支持。
- 轻量 validator。
- materializer。
- runtime diff。
- lineage 字段：parent_template_id / proposed_metrics / actual_metrics / runtime_config_sha256 / runtime_diff。
- created_by 结构化。
- data_versions audit 字段。
- custom_evo `auto_start=false` 共用后端能力。
- custom_evo 已物化任务的启动 endpoint。

验证：

- 创建模板不会触发 runtime QE task/experiment 执行。
- 模板 validation 通过。
- materialize 后创建 runtime 记录但不执行。
- 带确认 run 后才调用现有 QE 执行入口。
- custom_evo 模板不会在 run 前自动运行。
- runtime 可编辑，不被模板锁死。
- runtime diff 正确记录用户/agent 修改。
- archive policy 正确复制到 runtime 记录。

### Phase 4：QE MCP servers

实现：

- `scripts/aistock_qe_experiment_mcp_server.py`
- `scripts/aistock_qe_archive_mcp_server.py`
- `.mcp.json` opt-in 配置或文档说明。
- shared MCP support utilities：loopback URL、confirm token、ID sanitize、HTTP client、错误包装。
- UI 功能 parity matrix。

验证：

- MCP tool list 可加载。
- loopback base URL 强制生效。
- identifier sanitize 拒绝非法 id。
- 只读工具能返回当前 QE 数据。
- archive 查询工具能返回数仓数据。
- template materialize、template run、custom_evo retry/rerun/append/fork/clone 工具必须带确认 token。
- MCP submit 工具直接调用 QE 后端同源 endpoint。
- MCP 不直接 import scheduler，不直接访问 RD-Agent，不直接读写 DB 执行实验。

### Phase 5：UI 增量入口

实现：

- 模板列表/详情 UI。
- 模板 validation、materialize、run 按钮。
- 实验/模板配置中的 archive policy selector。
- Archive health/backfill status 页面或面板。
- runtime diff 展示。

验证：

- 现有 QE direct-run UI 仍然工作。
- 新模板流程支持保存、查看、校验、物化、执行。
- 用户可以为特殊实验选择不入仓。
- Archive health 显示当前 run/backlog/skips/jobs。

### Phase 6：面向优化的 Archive 证据 API

实现：

- 因子 summary API/view。
- 因子 pair synergy API/view。
- 模型 trial summary API/view。
- seed 与超参历史 API/view。
- priority queue API/view。
- agent proposal hit-rate API/view。
- agent audit query logging。

验证：

- 查询默认只返回 `research_valid=true`。
- excluded/invalid/skipped runs 只有显式过滤才展示。
- MCP 证据工具可以在无任意 SQL 的情况下生成候选分析。
- 基于历史数仓生成的新模板能记录 source archive runs、预期指标和风险。

## 15. 开发验证与交付门禁

### 15.1 验证方案必须前置

本项目属于 QE 执行、数仓、MCP、UI/API parity 和自动化流水线联动的高风险功能，验证方案必须在开发阶段同步设计，而不是开发完成后临时补测试。每个开发 slice 在开始编码前必须明确：

- 业务目标：该 slice 证明哪一个 QE/MCP/Archive 能力真实可用。
- 预期效果：API 返回、DB 状态、outbox/job 状态、MCP tool 响应、UI 状态或日志应该出现什么确定结果。
- 失败判定：哪些现象必须视为失败，例如静默 fallback、空结果伪成功、MCP 绕过后端、archive 失败影响 QE 状态、skip 实验被错误入仓。
- 自动化入口：对应 nox、pytest、Playwright、validation MCP 或 smoke script。
- 证据记录：必须落地到 `tests/aistock_validation/history/` 的 run record，包含命令、端口、环境变量、样本 id、API/DB/UI/log 证据和残余风险。

### 15.2 验证分层

建议按 L0-L5 分层设计并执行：

| 层级 | 目标 | 本方案中的验收重点 |
|---|---|---|
| L0 静态门禁 | 防止明显越界和低级错误 | 无 secret、无任意 SQL MCP、无 worker workspace 直读、无生产端口依赖、schema 注释完整。 |
| L1 单元测试 | 验证核心服务函数 | archive policy resolver、template validator、runtime diff、bootstrap marker、skip registry、outbox 幂等、MCP 参数校验。 |
| L2 后端/API 集成 | 验证后端业务状态 | 模板保存/物化/执行、custom_evo `auto_start=false`、run endpoint 同源调用、archive hook/outbox/worker/backfill/status。 |
| L3 UI/MCP 端到端 | 验证用户和智能体入口 | 现有 QE UI 功能不回归；MCP tool 调用与 UI endpoint parity；确认 token 生效；错误信息可读。 |
| L4 跨模块链路 | 验证 QE -> Archive -> Evidence -> Template 闭环 | 实验完成自动入仓、数仓查询生成新模板、模板再次物化和执行前可审阅。 |
| L5 发布候选验收 | 验证分支可交付 | 自动化流水线通过、run record 完整、残余风险可接受、feature 分支提交完成但不合入 `main`。 |

### 15.3 自动化流水线与 Validation MCP

开发完成后必须调用现有自动化流水线能力完成验收。推荐顺序：

1. 使用 Validation MCP 或仓库脚本创建本次验证 run record，记录模块为 `qe_mcp` / `qe_archive` / `qe_templates`。
2. 执行 L0 静态门禁：diff 范围、secret、worker path、任意 SQL、生产端口、schema comment 检查。
3. 执行 QE Archive 既有入口：
   - `python -m nox -s qe_archive_backend`
   - `python -m nox -s qe_archive_data_quality`
4. 执行 QE 既有 read/mutation 边界测试，覆盖 custom_evo retry/rerun/append/fork/clone 不直读 worker workspace。
5. 对新增模板/API/MCP server 执行新增单元和 API 测试。
6. 如 UI scope 进入本期，使用 dev backend `8011`/`8012` 和 dev frontend `3011`/`3012` 执行 Playwright L3；禁止重启生产 `8001`。
7. 使用 Validation MCP 汇总自动化流水线结果，生成或更新 run record。
8. 所有失败必须修复后重跑失败项及相邻链路；不得用人工解释替代失败测试。

如果 Validation MCP 当时缺少某个专用工具，则允许通过 `scripts/aistock_validate.py`、`nox` 或显式 pytest/Playwright 命令完成同等验证，但 run record 仍必须由自动化流水线能力归档。

### 15.4 功能级验收矩阵

| 功能组 | 必须验证的预期效果 |
|---|---|
| QE MCP 基础 | tool list 可加载；loopback backend URL 强制生效；非法 id 被拒绝；错误响应保留后端上下文。 |
| 执行链路同源 | MCP 执行单次实验时调用 QE 后端 `/experiments/{id}/run` 或后端同源入口；MCP 不 import scheduler、不直调 RD-Agent。 |
| 自定义演进 | MCP 可查询、物化、启动 custom_evo；支持 loop retry、rerun、append、fork、clone；执行类工具必须带确认 token。 |
| 模板层 | 创建模板不创建 runtime；materialize 创建 runtime 但不执行；run 才执行；runtime 可编辑；runtime diff 可追踪。 |
| 节点能力 | 本地/WSL/远端 node_id 和 node_parallelism 可传递；WSL 支持全类型实验；远端 CPU-only 只作为 warning/软限制。 |
| 入仓策略 | `AUTO` 自动 outbox；`SKIP` 不入仓但写 skip registry；`MANUAL_ONLY` 不自动入仓但支持定向 backfill。 |
| 自动入仓 | completion hook 写 outbox；worker 自动消费；archive 失败不改变 QE source 状态；pending outbox 可回到 0。 |
| 历史 backfill | preview 不写入；execute 需要确认；per source_type marker 防重复；rebootstrap 需要强确认 token。 |
| 数仓查询 | 默认 `research_valid=true`；invalid/skipped 需显式过滤；因子/模型/seed/超参基础查询可用于生成模板建议。 |
| UI 不回归 | 现有 QE direct-run UI、自定义演进 UI、archive health/backfill UI 仍能执行原有流程。 |

### 15.5 必须产出的验证证据

开发完成后的交付包必须包含：

- feature 分支名、提交 hash、变更文件清单。
- 自动化流水线 run record 路径。
- 执行过的 exact commands 和环境变量。
- dev backend/frontend 端口，明确说明未触碰生产 `8001` / `3000`。
- API 样本：模板创建、materialize、run、custom_evo 操作、archive health/backfill/status。
- DB 样本：template、runtime provenance、outbox、archive run、skip registry、bootstrap marker、ingest history。
- MCP 样本：tool list、只读查询、确认执行、错误输入拒绝。
- UI 证据：如本期包含 UI，则保存 Playwright trace/report/screenshot。
- 失败、修复、重跑记录。
- 未实现能力和残余风险，特别是自动演进 LLM 决策、多 alpha 调度、远端 CPU-only 软限制精细化。

### 15.6 分支与合入规则

- 开发必须从干净的 feature 分支或独立 worktree 开始，不在污染的共享 `main` 上直接开发。
- 每个并行开发 worker 必须有明确 write scope，避免多人同时修改同一文件族。
- 集成者负责合并各 worker 结果、解决冲突、运行自动化流水线和提交最终 feature 分支。
- 完成开发和验证后，只提交 feature 分支；未经用户明确确认，不合入 `main`，不 push 生产同步分支，不重启生产服务。

## 16. 待审阅决策

1. QE MCP server 拆分：建议确认拆成 `aistock-qe-experiment` 与 `aistock-qe-archive`，权限边界更清楚。
2. `MANUAL_ONLY` 是否保留：建议保留，支持敏感实验后续按需定向入仓。
3. Archive autostart 是否只在部署配置打开：建议实现和验证阶段默认关闭，部署配置确认后打开。
4. 模板 UI 是否进入首批实现：建议后端 + MCP 先做，UI 第二阶段；除非明确要求 UI 审阅为 v1 必须项。
5. custom_evo 是否新增 `auto_start=false` 共用后端入口：建议必须新增，否则无法满足“保存待执行模板”。
6. clone 语义：建议定义为“从已有 custom_evo config 复制生成新模板/新任务”，并补 UI/MCP 共用 endpoint。
7. 远端机 CPU-only 软限制：建议先 warning，不做 MCP 硬拒绝；后续由节点能力 catalog 精细化。

## 17. 非目标

- 不让 MCP 调度 RD-Agent。
- 不新增 MCP 专用执行链路。
- 不自动提升 StrategyPackage。
- 不自动准入 Paper v2。
- 不暴露任意 SQL MCP 权限。
- 不直接修改 RD-Agent worker workspace。
- 不改变现有 QE direct-run UI 行为，除非明确批准。
- 不实现自动演进 LLM 决策调度。
- 不实现多 alpha 架构实验调度。
- 不基于回顾性 OOS 指标宣称无偏自动因子选择。

## 18. 预期最终流程

实施完成后，普通流程为：

```text
Codex 或用户查询 QE archive 历史证据
  -> 分析因子、模型、seed、超参和历史回测结果
  -> 生成 QE 实验模板
  -> 模板轻量校验
  -> 用户在聊天或 UI 中审阅
  -> 用户确认 materialize
  -> QE 后端创建 runtime 实验或 custom_evo task，但不自动执行
  -> 用户可在 UI 或 MCP 中继续检查/修改 runtime 配置
  -> 用户确认 run
  -> MCP 调用 QE 后端同源执行 endpoint
  -> QE 后端按既有本地/WSL/远端节点路径执行实验
  -> 如果 archive_policy=AUTO，completion hook 写入 outbox
  -> archive worker 无需人工干预持久化结果
  -> archive MCP 将结果暴露为未来证据
  -> experiment MCP 比较实际结果并提出下一轮受控模板
```

特殊实验可以选择 `archive_policy=SKIP`，宽泛历史 backfill 默认不会导入这些实验；但 skip registry 会保留原因，便于后续解释。

## 17. QE 待执行实验 UI 管理台补充设计（2026-05-16）

### 17.1 补充背景

QE MCP v1 已经具备通过数据库模板保存单次实验和自定义演进实验配置的能力，但如果只通过 MCP 或 API 操作，人工审查入口不够清晰。为满足“由 MCP 创建实验方案、人工在 UI 中检查和修改、确认后再正式执行”的要求，需要新增一个统一 UI 管理台。该 UI 不创建新的执行链路，只管理 `qe_execution_templates` 中的待执行模板。

### 17.2 本期必须支持的实验类型

本期 UI 与 MCP 只支持两类 QE 实验：

1. QE 单次实验：模板配置必须与现有人工单次实验配置兼容，物化时继续复用现有 `GenerateConfigRequest` / `generate_config` 路径，执行时复用现有 `run_experiment` 统一执行层。
2. 自定义演进：模板配置必须与现有 `CustomEvolutionCreateRequest` 兼容，物化时强制 `auto_start=false`，人工点击执行后复用现有 `run_custom_evo_task` 统一执行层。

本期明确不实现：

1. 多 alpha 架构实验调度。已有多 alpha 相关研发和页面可以保留，但不接入本次 QE MCP UI 执行链路。
2. 自动演进 LLM 决策调度。已有自动演进页面可以继续使用，但 MCP 暂不具备让 LLM 自动决定演进方向并直接调度的能力。

未来当多 alpha 架构和自动演进模块完成统一配置层、统一执行层和完整验证后，再通过同一模板管理框架接入 MCP。

### 17.3 产品目标

新增 `QE 待执行实验管理台`，让操作者可以：

1. 查看所有 MCP 创建的 QE 单次实验和自定义演进待执行模板。
2. 打开模板详情，查看 MCP 给出的分析摘要、风险说明、数仓策略和完整配置。
3. 像人工创建实验一样修改所有实验配置：页面提供关键字段表单，同时提供完整 `config_json` 编辑区覆盖所有配置字段。
4. 点击“保存配置”时只更新数据库模板，不创建实验、不启动训练、不写运行任务。
5. 点击“保存并执行”时，先保存当前配置，再校验、审批、物化，最后调用现有 QE 执行层。
6. 执行后的单次实验进入现有实验历史页面，自定义演进进入现有自动演进任务页面，与人工配置实验显示一致。

### 17.4 数据和状态约束

MCP 创建的实验模板必须存储在 `qe_execution_templates` 表。UI 只通过 `/api/v1/qe-templates` API 读取和修改模板，不直接写数据库，不直接调用 scheduler。

模板状态机保持如下边界：

```text
draft -> ready_for_review -> approved -> materialized -> run_requested
```

编辑规则：

1. `draft`、`ready_for_review`、`approved` 允许人工编辑。
2. 修改 `config_json`、`archive_policy`、`archive_reason` 或数据版本后，必须重置为 `draft`，清空旧校验、旧审批、旧物化结果和旧运行关联。
3. `materialized`、`run_requested`、`running`、`completed`、`failed`、`cancelled`、`superseded`、`expired` 不允许原地修改配置。
4. 已执行或已物化模板如需修改，应创建新的待执行模板，不能改写历史运行配置。
5. 前端保存配置时不执行，执行必须由显式按钮触发。

### 17.5 UI 页面设计

新增页面：

1. `/quantevolver/templates`：QE 待执行实验列表。
2. `/quantevolver/templates/[templateId]`：模板详情与配置编辑。

列表页能力：

1. 按状态、实验类型、来源、关键字筛选模板。
2. 展示模板 ID、标题、描述、类型、状态、数仓策略、创建来源、更新时间、已关联的实验 ID 或演进 task ID。
3. 对已执行模板提供跳转到现有实验历史或自动演进详情的入口。

详情页能力：

1. 显示人工审查信息：标题、描述、MCP 分析摘要、风险说明、数仓策略和来源。
2. 对 QE 单次实验显示实验名、模型 ID、策略 ID、label horizon、节点、因子列表等关键字段。
3. 对自定义演进显示任务名、基础实验、节点并行度、目标描述和 loop 列表。
4. 提供完整 `config_json` 编辑区，确保任何人工实验配置字段都可以修改。
5. 提供“保存配置”“校验模板”“保存并执行”“废弃模板”操作。
6. 执行成功后提供跳转现有运行详情页面的入口。

### 17.6 后端接口补强

`/api/v1/qe-templates` 需要支持 UI 管理台使用：

1. `GET /qe-templates` 支持 `status`、`template_kind`、`created_by_type`、`search`、`limit`、`offset` 筛选。
2. `PUT /qe-templates/{template_id}` 只允许编辑未物化、未执行的模板。
3. 配置修改后必须自动撤销旧校验、旧审批和旧物化结果。
4. `POST /qe-templates/{template_id}/approve` 必须基于当前配置重新校验，通过后才能审批。
5. `POST /qe-templates/{template_id}/materialize` 必须要求模板已审批。
6. `POST /qe-templates/{template_id}/run` 必须继续使用现有 QE 单次实验或自定义演进执行层。

### 17.7 验证方案

本次新增功能必须在提交前完成以下验证。所有验证只允许使用开发端口 `8011/3011` 或 `8012/3012`，禁止触碰生产 `8001/3000`。

#### 17.7.1 后端单元与合约验证

命令：

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_mcp_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/qe_templates -q -p no:cacheprovider
```

验收标准：

1. MCP 创建模板后只保存到数据库，不直接执行。
2. 配置更新会重置 `status= draft`、清空 `validation_json`、`approval_json`、`submitted_experiment_id`、`submitted_task_id` 和 runtime diff。
3. 已物化或已执行模板不可原地修改。
4. 直接修改状态必须被拒绝，状态只能通过 validate/approve/materialize/run/supersede 端点变化。
5. 未审批模板不能物化。
6. 多 alpha 模板继续被拒绝，自定义演进空 loop 继续被拒绝。

#### 17.7.2 前端类型与 UI E2E 验证

命令：

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_template_ui -- 8011 3011
```

验收标准：

1. `/quantevolver/templates` 可以展示 MCP 创建的 QE 单次实验和自定义演进模板。
2. 列表页可以按状态、类型、来源、搜索关键字筛选。
3. `/quantevolver/templates/[templateId]` 可以打开完整详情。
4. 单次实验模板可以修改模型、因子、策略、label horizon、节点和完整 JSON 配置。
5. 自定义演进模板可以查看 loop 列表，并通过完整 JSON 修改所有 loop 配置。
6. 点击“保存配置”只调用模板更新接口，不调用执行接口。
7. 点击“保存并执行”按顺序调用保存、校验、审批、物化、执行。
8. 单次实验执行请求使用 `QE_EXPERIMENT_RUN` 确认 token。
9. 自定义演进执行请求使用 `QE_CUSTOM_EVO_RUN` 确认 token。
10. 页面无 console error、page error 和意外 4xx/5xx。

#### 17.7.3 L3 流水线验证

命令：

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_mcp_l3 -- 8011 3011
```

验收标准：

1. guardrail 扫描新增后端、MCP、UI、测试和文档，无 HIGH 级问题。
2. `qe_mcp_backend` 通过。
3. `qe_archive_backend` 通过。
4. `qe_template_ui` 通过。
5. 验证记录写入 `tests/aistock_validation/history/qe_mcp/`。
6. 验证期间不重启、不停止、不调用生产 `8001/3000`。

#### 17.7.4 提交和远端一致性验证

提交前必须执行：

```powershell
git diff --check
git status -sb
```

提交和推送后必须执行：

```powershell
git fetch origin codex/qe-mcp-template-archive-20260516
git rev-parse HEAD
git rev-parse origin/codex/qe-mcp-template-archive-20260516
git status -sb
```

验收标准：

1. 本地工作区干净。
2. 本地 feature 分支与 GitHub 远端 feature 分支 HEAD 完全一致。
3. 不合入 main。
4. 不修改 root 工作区或其他分支的非本任务文件。
5. 中文设计方案、验证矩阵和验证记录随代码一起提交。
