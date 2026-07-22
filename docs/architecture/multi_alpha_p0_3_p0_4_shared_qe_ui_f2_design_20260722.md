# 多 Alpha P0-3/P0-4 共享 QE UI、正式创建器与运行明细 F2 设计

- 文档类型：F2 跨模块从属实现设计
- 父级权威：`docs/architecture/multi_alpha_qe_evolution_foundation_f2_design_20260718.md`
- 日期：2026-07-22
- 状态：`SOURCE_MERGED_VERIFIED_RUNTIME_PENDING`
- 范围：QuantEvolver / Multi-Alpha combine-backtest / QE UI / PostgreSQL durable read model
- 唯一隔离边界：仅在 QE 研究环境运行，不读写 Selection、Advisory、Paper、模拟盘、QMT 或生产交易链

本设计不增加研究准入、淘汰、审批、确认或指标门禁。数据、制品或执行证据不完整时，页面展示真实缺口、已存在的交叉证据和补取路径；缺口不删除研究方向。创建、控制和恢复操作沿用现有 QE 技术状态机与幂等合同，它们不是研究审批。

## 0. Source Implementation Record / 源码实施记录

- 已实现规范 `/quantevolver/evolution` task-type 路由、layout-transparent shared shell、旧列表/详情 URL query-preserving redirect、正式全字段多场景创建器、场景级 submission idempotency、typed API adapter、run-scoped child/attempt grid、技术动作连接、durable event cursor API/SSE、Last-Event-ID/visibility 续传、DB events/workspace logs/commands/Archive 分区展示。
- child/attempt 读取已在审核中从逐 child N+1 调整为单个 run-scoped join query，并以 `include_attempts=true` 一次返回；未改变 child/attempt 数据模型或控制语义。
- 后端完整 Multi-Alpha 定向矩阵 `214 passed, 9 skipped`（9 项为未配置显式 PostgreSQL 测试 DSN 的可选集成用例），直接改动矩阵 `51 passed`，QE read nox `14 passed`；Ruff、Python compile、catalog integrity 与 CI ownership classifier 通过。新旧 Playwright 合并矩阵 `8 passed`，TypeScript 与 Next.js production build 通过。视觉基线已生成并由 Playwright 对照。
- 本阶段没有新 DDL、依赖或非 QE 调用；没有启动实验、修改研究结果或添加科研门禁。源码已通过 PR #2593 合入，merge commit 为 `faaad2376ed381618aa66a60aa6d2740a4f42069`，并已同步到本地 `main`；运行时尚未重启验收，重启由用户执行。

## 1. 背景与现有代码事实

P0-1/P0-2 已提供耐久 `task/run/child/attempt/event/command`、QE Workspace submission/kill receipt、pause/resume/cancel/reconcile、三类恢复、日志、Archive 和组合结果接口。当前缺口集中在 UI 与只读事件投影：

1. `/quantevolver/evolution` 是单 Alpha 规范入口，但组合回测仍有独立列表和详情 DOM，形成两套导航、刷新状态和视觉实现。
2. 后端 `CombineBacktestRunRequest` 已完整接受 roster、OOS、融合方法、walk-forward、rank-fusion、节点、并行度、回测参数、TopK、覆盖率和超时，但 UI 没有正式创建器。
3. `CombineRunOperationsPanel` 已能调用 P0-2 控制 API，但 child 类型只投影少数字段，attempt 仅为控制下拉输入，不能完整审计节点、远端 ID、阶段、heartbeat、错误和制品。
4. `MultiAlphaDurableRepository.list_events()` 已有稳定 `event_id` 游标，但 HTTP 没有耐久事件分页/SSE；现有 logs 接口主要读取 workspace 日志，不能替代数据库事件权威。
5. 旧 URL 必须继续可访问，但只做规范页面映射，不再拥有业务状态机或第二套页面实现。

## 2. Scope / 目标、范围与非目标

### 2.1 目标

- `/quantevolver/evolution` 同时承载 single-alpha 与 `multi_alpha_combine`，使用稳定 task-type adapter 切换数据源。
- 抽取可复用的 QE workspace shell/navigation；不改变单 Alpha 现有操作语义和视觉输出。
- 提供完整多 Alpha 创建器：至少两条腿、每腿多个 seed run、全部现有 request 字段、多场景 run；场景只复用已存在 prediction，不触发模型训练。
- 提供可排序、过滤、展开的 child/attempt grid；展示所有权威身份、状态、阶段、时间、heartbeat、错误、制品、lineage 和允许的技术动作。
- 提供数据库耐久事件的 cursor API 与 SSE，断线以 `Last-Event-ID`/`after_event_id` 续传；页面隐藏时停止持续连接，恢复可见后补齐。
- 将 workspace logs、durable events、commands、Archive 和 recovery evidence 分区呈现，禁止以其中一种来源冒充另一种来源。
- 旧列表/详情 URL 保留查询参数并重定向到规范页面。

### 2.2 非目标

- 不修改组合公式、LOO、训练、标签、模型、因子、回测指标或研究结论。
- 不新建第二套任务表、组合结果表或前端状态平台。
- 不触发新的 GPU/显存/资源遥测，不调用 `nvidia-smi`。
- 不把缺数据、缺制品、failed/partial loop 或 evidence unknown 转换为研究淘汰、禁止创建或方向停止。
- 不新增 RBAC、审批、发布确认或科研晋级状态。
- 不自动启动、停止或重启服务；运行时重启由用户单独执行。

## 3. Architecture / 规范路由与共享页面结构

### 3.1 规范 URL

- 列表：`/quantevolver/evolution?task_type=multi_alpha_combine`
- 详情：`/quantevolver/evolution?task_type=multi_alpha_combine&task_id=<task_key>`
- 可选 scheme/tab 等参数继续保留。
- `/quantevolver/multi-alpha/combine-backtest` 与其 `[taskKey]` 页面只执行客户端兼容重定向，原样透传 query；不 fetch、不持有列表/详情状态、不渲染独立业务 DOM。

### 3.2 组件边界

- `EvolutionWorkspaceShell.tsx`：共享标题、任务类型切换、刷新状态、主内容容器和统一视觉 token。
- `multiAlphaEvolutionAdapter.ts`：唯一负责 combine task list/detail/trajectory/config/run/events 的 typed HTTP 映射和结构化错误展开。
- `MultiAlphaEvolutionWorkspace.tsx`：在规范 route 下组合共享 shell、列表、详情、创建器和运行明细；不复制后端状态机。
- `MultiAlphaCreateComposer.tsx`：管理显式草稿、逐场景 payload preview 和提交结果。
- `MultiAlphaChildGrid.tsx`：child/attempt/event/command 的只读投影与现有 P0-2 动作连接。
- 现有 `LoopDetailPanel`、`TopologyPanel`、`EvolutionTrajectory`、`LoopMetricsComparison`、`CombineDiagnosticsPanel` 和 `CombineRunOperationsPanel` 继续复用，不复制指标解释。

单 Alpha 页面只抽取壳层，不改变其 fetch、SSE、创建、停止、恢复、fork、日志或归档语义。共享壳不得依赖 multi-alpha 字段。

## 4. Contracts / 正式创建器合同

### 4.1 Roster

每条腿包含：

- `leg_id`：非空且在 roster 内唯一；
- `seed_run_ids[]`：至少一个非空 QE prediction-store run id，去除空白后保持用户顺序；
- `metadata`：JSON 对象，原样提交并在 preview 中展示。

至少两条腿。`baseline_leg_id` 必须引用当前 roster；UI 发现不一致时展示字段错误，不自动替换用户选择。

### 4.2 全量请求字段

创建器必须覆盖现有 `CombineBacktestRunRequest`：`task_id`、`roster`、`oos_start/end`、`weighting_schemes`、`normalize_method`、`walk_forward`、`rank_fusion`、`backtest_config`、`baseline_leg_id`、`topk`、`min_date_coverage`、`run_async`、`scheme_timeout_seconds`、`run_timeout_seconds`、`wait_timeout_seconds`。

`backtest_config` 的正式表单覆盖 `node_id`、`node_parallelism`、`initial_cash`、`topk`、`strategy_kwargs`（`n_drop/max_n_drop/min_n_drop/hold_thresh`）和 runtime template/path 等高级 JSON。高级 JSON 与结构化字段合并时，结构化字段为当前可见的最终值；冲突在 preview 中列出，不静默覆盖。

### 4.3 多场景 runs

场景只改变 run 级字段（例如 initial cash、TopK、调仓/持仓参数、node、timeouts、OOS）；roster/prediction identity 保持冻结。提交器为每个场景生成完整独立 payload，并逐一返回：场景名、HTTP 状态、run/task identity、结构化错误。某个场景失败不伪造整批失败或成功，其余场景结果继续保留；用户可只重试失败场景。

场景提交不调用任何训练 API。每个场景持有稳定 `Idempotency-Key`；相同 key/相同 payload 重放返回同一 durable run，相同 key/不同 payload 显式 409，避免响应丢失或双击产生未知重复 run。节点容量由既有共享 reservation/coordinator 管理；UI 不自行绕过 WSL 2、远端 4 的既有执行限制，也不增加科研门禁。

## 5. Child/Attempt 明细合同

### 5.1 Child 行

至少展示：`ordinal`、`child_id/key/kind`、`status/phase`、`selected_attempt_id`、`execution_disposition`、`source_child_id`、`dependency_json`、`artifact_manifest_json/hash`、`result_manifest_json/hash`、`row_version`、`created_at/updated_at/finished_at`、`error_code/error_json`。

### 5.2 Attempt 展开行

至少展示：`attempt_id/no`、`status/phase`、`retry_mode`、`execution_kind`、`retry_of_attempt_id/source_attempt_id`、`node_id`、`qe_task_id/qe_loop_id`、`submission_intent_hash`、`remote_submission_json`、`process_identity_json/hash`、`environment_identity_json/hash`、`dataset_identity_json/hash`、`heartbeat_at/lease_expires_at/started_at/finished_at/updated_at`、`artifact_manifest_json`、`result_manifest_json/hash`、`error_code/error_json`、是否为 selected attempt。

排序对缺失值使用稳定、类型安全的比较器；任何字符串/数字混合字段先规范为显式 sort key，禁止把原始异构值直接交给 `<`。筛选覆盖 child status/kind、attempt status/node、错误存在性和文本搜索。

### 5.3 动作

- run pause/resume/cancel/reconcile、attempt cancel、child recovery 和 whole-run retry 只调用现有 P0-2 API。
- capability state 决定按钮的技术可执行状态，并展示 reason/evidence；不隐藏研究方向。
- recovery 必须先展示 frozen scope、dependency plan、mode 和 idempotency identity，再调用现有执行接口；这是防重复技术合同，不是审批。
- 动作后刷新 run/child/attempt/event/command，页面刷新或后端重启后从数据库重新发现，不依赖浏览器内存。

## 6. Durable Event API 与 SSE

### 6.1 分页 API

`GET /multi-alpha/combine-backtest/runs/{run_id}/events?after_event_id=0&limit=500`

返回 `events`、`count`、`after_event_id`、`next_event_id`、`has_more`。先验证 run 存在；无事件返回真实空数组。非法 cursor 返回结构化 400，run 不存在返回结构化 404，schema/DB 错误不得变成空结果。

### 6.2 SSE

`GET /multi-alpha/combine-backtest/runs/{run_id}/events/stream?after_event_id=0`

- 每条消息 `id=<event_id>`、`event=durable_event`、`data=<完整事件 JSON>`；
- 优先读取 `Last-Event-ID`，若同时提供 query 则使用较大的非负值，避免重复倒退；
- 按游标批量补齐，空闲时发送 comment heartbeat；
- run 终态且游标已追平后发送 `event=stream_end` 并结束；
- 客户端断开立即停止，不创建后台线程、不占用 QE worker；
- 序列化或数据库错误发送结构化 `event=stream_error` 后结束，禁止静默重连成空流。

前端维护 last event id，visibility hidden 时关闭 EventSource；visible 时用 cursor 重连并补齐。SSE 只通知 durable event，workspace 文件日志继续走既有 logs API。

## 7. API、数据源与错误语义

`multiAlphaEvolutionAdapter` 统一展开 `{status,data}` 与 FastAPI `{detail}`，保留 `reason_code/message/context`。列表和详情 API 继续使用现有 `combine/tasks` 投影；run 级详情使用现有 `combine-backtest/runs/{run_id}`，child/attempt/command/archive/logs 使用现有 P0-2 route，新增 events route 只暴露已存在 repository 权威。

任一分区失败时，仅该分区显示错误及重试入口，其他已成功获取的事实继续展示；不得清空整页、伪造默认值或把 API 失败解释为“暂无数据”。

## 8. Implementation Plan / 文件改动计划

### 8.1 后端

- `backend/routers/multi_alpha.py`：events page + SSE；复用 durable repository/error envelope。
- `backend/tests/multi_alpha/test_durable_router.py`：cursor、404、结构化错误、SSE replay/terminal/end。

无需新表或 DDL；事件表和索引已由 P0-1A/P0-2 部署。

### 8.2 前端

- 新增共享 shell、typed adapter、create composer、child grid、multi-alpha workspace。
- 修改 `evolution/page.tsx`：按 query 选择 task adapter/workspace，single-alpha 默认行为不变。
- 旧 combine list/detail page 改为薄重定向。
- `CombineRunOperationsPanel.tsx`：复用新 grid/adapter 的完整类型和刷新回调，不保留第二份 child 摘要表。
- 新增 Playwright/组件合同测试覆盖 F-211/F-212/F-213。

## 9. 验证方案

### 9.1 后端

- event cursor 分页无丢失、无倒退、稳定 next cursor；
- invalid cursor、missing run、DB/schema failure 返回结构化错误；
- SSE backlog replay、Last-Event-ID、heartbeat、terminal `stream_end`、disconnect cleanup；
- 现有 P0-2 router/control/recovery 回归。

### 9.2 前端

- 旧列表/详情 URL 映射到规范 route 并保留 query；
- `/evolution` 默认仍为 single-alpha，multi-alpha query 使用共享 shell；
- 创建器 payload snapshot 覆盖全部字段、两腿/多 seed、多个场景、逐场景错误和只重试失败项；
- child grid 排序/筛选/展开、string/int 混合值、attempt identity、错误/制品和动作；
- refresh 与 backend restart fixture 后从 DB read model 恢复；
- hidden/visible SSE cursor 补齐；
- screenshot/golden 证明共享壳未引入第二套视觉语言。

### 9.3 静态与流程

- changed-file TypeScript/ESLint、Python Ruff/compile；
- 定向 pytest 与 Playwright；
- `git diff --check`；
- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/multi_alpha_p0_3_p0_4_shared_qe_ui_f2_design_20260722.md --tier F2`。

## 10. Design Acceptance Index

| ID | 验收合同 |
|---|---|
| F-501 | `/quantevolver/evolution` 是唯一规范入口，旧 URL 仅透传 query 后重定向，不持有第二套业务 DOM/状态。 |
| F-502 | single-alpha 与 multi-alpha 使用共享 workspace shell；single-alpha 原行为与视觉不变。 |
| F-503 | typed adapter 完整映射 task/detail/trajectory/config/run/children/attempts/events/commands/logs/archive，并保留结构化错误。 |
| F-504 | 创建器覆盖现有 `CombineBacktestRunRequest` 全字段、至少两腿/多 seed、baseline 引用和完整 payload preview。 |
| F-505 | 多场景只创建独立 combine runs、不重新训练；逐场景保存成功/失败证据、稳定提交幂等身份并支持仅重试失败场景。 |
| F-506 | child grid 完整展示 child 权威字段，排序/筛选对异构与缺失值类型安全。 |
| F-507 | attempt 展开完整展示节点、远端 ID、状态、阶段、heartbeat、lineage、环境/数据/进程身份、错误和制品。 |
| F-508 | 现有 pause/resume/cancel/reconcile/attempt cancel/recovery/retry 能从 grid 执行并在刷新/重启后重新发现。 |
| F-509 | durable event page API 提供稳定 cursor、has_more 和结构化 400/404/5xx，不以空数组掩盖错误。 |
| F-510 | durable event SSE 支持 backlog、Last-Event-ID、heartbeat、终态收口、断线释放和显式 stream error。 |
| F-511 | UI 分开显示 DB events、workspace logs、commands、Archive/recovery evidence，不交叉冒充。 |
| F-512 | QE-only；无 GPU telemetry、无科研门禁/审批、无缺数据淘汰、无非 QE 调用。 |
| F-513 | parent F-211/F-212/F-213 的进度和实现/测试证据同步更新。 |

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-211 | F-501/F-502 shared canonical QE page | `frontend/tests/quantevolver/evolution-shared-shell.spec.ts`; `frontend/tests/quantevolver/evolution-visual-parity.spec.ts` | SOURCE_VERIFIED | none |
| F-212 | F-504/F-505 formal create composer | `frontend/tests/quantevolver/multi-alpha-create.spec.ts` | SOURCE_VERIFIED | none |
| F-213 | F-506/F-507/F-508 child/attempt runtime view | `frontend/tests/quantevolver/multi-alpha-child-grid.spec.ts` | SOURCE_VERIFIED | none |
| F-501 | canonical route + legacy redirect pages | `frontend/tests/quantevolver/evolution-shared-shell.spec.ts` | SOURCE_VERIFIED | none |
| F-502 | `EvolutionWorkspaceShell.tsx`; evolution page | `frontend/tests/quantevolver/evolution-visual-parity.spec.ts`; golden PNG | SOURCE_VERIFIED | none |
| F-503 | `multiAlphaEvolutionAdapter.ts` | `frontend/tests/quantevolver/multi-alpha-adapter.spec.ts` | SOURCE_VERIFIED | none |
| F-504 | `MultiAlphaCreateComposer.tsx` | `frontend/tests/quantevolver/multi-alpha-create.spec.ts` | SOURCE_VERIFIED | none |
| F-505 | composer multi-scenario submit ledger | `frontend/tests/quantevolver/multi-alpha-create.spec.ts` | SOURCE_VERIFIED | none |
| F-506 | `MultiAlphaChildGrid.tsx` | `frontend/tests/quantevolver/multi-alpha-child-grid.spec.ts` | SOURCE_VERIFIED | none |
| F-507 | `MultiAlphaChildGrid.tsx` | `frontend/tests/quantevolver/multi-alpha-child-grid.spec.ts` | SOURCE_VERIFIED | none |
| F-508 | existing P0-2 operations APIs + grid actions | `frontend/tests/quantevolver/multi-alpha-child-grid.spec.ts`; `backend/tests/multi_alpha/test_durable_router.py` | SOURCE_VERIFIED | none |
| F-509 | multi-alpha event page route | `backend/tests/multi_alpha/test_durable_router.py` | SOURCE_VERIFIED | none |
| F-510 | multi-alpha event SSE route | `backend/tests/multi_alpha/test_durable_router.py` | SOURCE_VERIFIED | none |
| F-511 | workspace evidence tabs | `frontend/tests/quantevolver/multi-alpha-child-grid.spec.ts`; `frontend/tests/multi-alpha-combine-backtest.spec.ts` | SOURCE_VERIFIED | none |
| F-512 | scope/code search | `backend/tests/scripts/test_aistock_feature_workflow.py`; F2 validator receipt | SOURCE_VERIFIED | none |
| F-513 | parent blueprint progress ledger | `frontend/tests/quantevolver/evolution-shared-shell.spec.ts`; design diff receipt | SOURCE_VERIFIED | none |

## 12. DESIGN-COMPLIANCE-001 预审

- 禁止简化版：不以旧独立页面换皮冒充共享页，不以 JSON textarea 冒充全字段创建器，不以 child 下拉框冒充 child/attempt grid，不以轮询 workspace log 冒充 durable event SSE。
- 禁止静默错误：API、SSE、场景提交和证据分区均保留 reason/context；失败不清空为“暂无数据”，缺失不填 0。
- 禁止业务逻辑偏移：不改组合、训练、回测、LOO、Archive、control/recovery 语义；多场景只创建已有 prediction 的 run。
- 禁止私增门禁审批：无科研准入、淘汰、晋级、人工审批或数据完整性停止规则；仅保留既有 QE 技术状态机与用户已规定的 QE-only 边界。

## 13. Risks / 风险与处置

| 风险 | 处置 |
|---|---|
| 单 Alpha 页面抽壳引入视觉或行为回归 | 保留原 fetch/action 状态机，使用 screenshot/golden 与现有流程用例逐项对照。 |
| legacy redirect 丢失 scheme/tab/task query | 使用 URLSearchParams 原样合并，仅替换 pathname 与规范 task 参数。 |
| 多场景双击重复提交 | 一次提交持有稳定场景请求身份并禁用重复点击；后端 task/run request identity 继续作为权威，结果逐场景回显。 |
| SSE 连接泄漏或后端重启后丢事件 | disconnect 检测、visibility close、Last-Event-ID cursor replay；数据库 event_id 为唯一进度事实。 |
| DB events 与 workspace logs 被混为一类 | 独立数据源、标题、时间线与错误区域，禁止相互 fallback。 |
| child/attempt 异构字段排序再次触发类型异常 | 显式 typed sort key、null 排序策略和 string/int 混合 fixture。 |
| UI capability 被误解为研究准入 | 文案固定为技术执行状态并展示 evidence，不隐藏候选和研究方向。 |

## 14. Production Gates / 发布、回滚与运行边界

- 本阶段无新 DDL，`production_ddl_gate=noop`。
- 预计无新前后端依赖，两个 dependency gate 均为 `noop`；若实现中发现确需依赖，必须先报告，不能静默安装。
- 代码合入、根目录同步与运行时激活分开记录。前后端代码部署后如需重启，先通知用户执行。
- 回滚只回滚 route/UI/API 代码；不删除 durable task/run/child/attempt/event/command、workspace 或 Archive 证据。
