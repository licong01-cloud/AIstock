# 多 Alpha QE 演进底座 P0 F2 详细设计

- 文档类型：F2 跨模块架构与实现级详细设计
- 模块：QuantEvolver / Multi-Alpha combine-backtest / QE Workspace / QE UI
- 日期：2026-07-18
- 状态：`P0_1A_PRODUCTION_VERIFIED_P0_1B_REVIEWED_DESIGN_READY_CODE_PENDING`
- 审计修订：P0-1A 已完成源码合入、生产 DDL、历史 task/run/result-child 回填与 readback；P0-1B 从属设计已根据现有 AIstock、生产 schema 与 QE Workspace 服务合同完成二次审核，补齐服务端幂等 receipt、共享 reservation、task/run identity 分离、Archive 和 timeout 语义；运行路径仍未切换
- 用户授权本次设计范围：P0-1 持久化编排、P0-2 生命周期与子任务恢复、P0-3 QE 同风格创建器、P0-4 子任务运行网格与重启恢复可见性；不代表用户已逐项批准本文全部实现细节或未来偏差
- 唯一运行边界：QE-only；不得影响 Selection、Advisory、Paper、模拟盘、QMT、StrategyPackage 运行语义或其他非 QE 模块
- 设计约束：遵循 `DESIGN-COMPLIANCE-001` 四项约束，基于现有架构增量开发，不另建“多 Alpha v2”
- 研究蓝图：`docs/analysis/sector_rotation_factors_develop_spec_20260710.md` v5.6

关联基线：

- `docs/architecture/multi_alpha_combine_backtest_ui_reuse_design_20260626.md`
- `docs/architecture/multi_alpha_combine_backtest_remote_dispatch_design_20260627.md`
- `docs/architecture/multi_alpha_combine_backtest_qe_ui_parity_design_20260718.md`
- `docs/architecture/multi_alpha_p0_1b_durable_execution_orchestrator_f2_design_20260719.md`
- `docs/architecture/multi_alpha_warehouse_archive_f2_design_20260628.md`
- `backend/services/multi_alpha/combine_backtest.py`
- `backend/services/multi_alpha/remote_dispatch.py`
- `backend/services/multi_alpha/combine_ui_adapter.py`
- `backend/services/quantevolver/qe_workspace_client.py`
- `backend/routers/multi_alpha.py`
- `frontend/src/app/quantevolver/evolution/page.tsx`
- `frontend/src/app/quantevolver/evolution/components/*`
- `frontend/src/app/quantevolver/multi-alpha/combine-backtest/*`

---

## 1. Background / 背景与结论

AIstock 当前已经具备可用的多 Alpha 组合研究能力：已有单腿 prediction 可以进入组合器，执行标准化、walk-forward 权重、baseline、LOO、TopK/资金量/持仓参数场景回放，并通过 WSL 或远端 QE Workspace 完成 pred-backtest；scheme/LOO 指标、Prediction Store、QE Archive、运行日志和 StrategyPackage 输出均已有实现。前端已经通过 `combine_ui_adapter.py` 把多 Alpha run 映射为单 Alpha QE task/loop 形状，并复用了 `LoopDetailPanel` 与 `EvolutionTrajectory`。

因此本设计不创建新平台、不复制一套 QE、不重新实现组合算法。目标是在现有 Tier-1 combine-backtest 上补齐完整演进平台所需的可靠任务生命周期。

当前不能宣称“以后所有策略只需要开发模型和因子，不再需要程序研发”，原因不是组合公式不足，而是以下基础能力仍不完整：

1. `MultiAlphaCombineBacktestService.submit_run()` 使用 FastAPI 进程内 `threading.Thread(..., daemon=True)`；父 run 和 `ThreadPoolExecutor` 子任务由当前 Python 进程持有。
2. 远端 `qe_task_id/qe_loop_id` 只在子任务完成后进入 metrics，提交后到完成前没有数据库权威映射；后端重启后无法可靠判断“未提交、已提交、仍运行、已完成但未回收结果”。
3. `_NODE_RESERVATIONS` 是进程内字典；重启、多进程或多个后端实例之间不能形成统一节点占用事实。
4. 当前 retry 是整组新 run；没有暂停、恢复、取消、单子任务 retry 和 results-only 回收。
5. 当前多 Alpha 页面主要用于查看；没有沿用单 Alpha自动演进页面的完整创建器和子任务级运行面板。
6. 当前 QE Workspace create API 每次 POST 都注册 background task，request 没有 submission intent/idempotency 字段；确定性 `task_id/Loop1` 只能帮助定位，不能独立保证响应丢失后的 at-most-once execution。
7. `qe_experiments` 没有统一 node reservation 字段，部分路径又在 POST 成功后才写 node/loop；从多个业务表事后统计 active rows 不能关闭容量超卖窗口。

2026-07-18 重启后的 live evidence 进一步说明这一缺口：R12G 单 Loop 已正常完成；R12P 最新 run 在 baseline child 运行 3600 秒后以 `combine_backtest_scheme_timeout` 终止，父 run 无法保留“远端是否仍运行、是否已有可回收结果”的一等状态，也不能只恢复该 baseline child。该事实是平台编排问题，不是 Alpha 效果结论。

## 2. Scope / 本轮范围

### 2.1 P0-1：持久化父子任务编排

- 把现有 synthetic task、run、baseline/scheme/LOO child、每次 attempt 和事件持久化为明确层级。
- 使用 PostgreSQL lease、fencing token、row-version CAS 确保同一 run/attempt 只有一个有效 worker 所有者。
- 持久化远端 QE `task_id/loop_id`，后端重启后自动重新核对和接管。
- QE Workspace 服务端以原子 submission receipt 接收 `submission_intent_hash`；相同 identity/hash 重放不再次注册 execution，不同 hash 显式 409。
- WSL 与远端节点统一通过现有 `QEWorkspaceClient` 执行和查询，不再由本地 subprocess 与远端 client 分别拥有不同生命周期。
- 所有生产 QE Workspace submit 在 POST 前写统一 `infra.qe_execution_reservation`，reservation INSERT 与 source claim 同一 advisory transaction。
- 节点容量暂时不足时保持 `queued/waiting_capacity`，不把实验标记为失败。

### 2.2 P0-2：任务控制与子任务恢复

- run 级：pause、resume、cancel；兼容 `stop` endpoint 必须保持现有单 Alpha 的终止语义并委托 cancel，禁止把 stop 静默改成 pause。
- child 级：查看 attempts、取消在途 attempt、按明确模式重试失败/取消 child。
- retry 模式：`backtest_only`、`results_only`、`rematerialize_and_backtest`。
- 完成 child 的结果保持不可覆盖；父 run 失败不删除或隐藏成功 child。
- 网络中断、后端重启或 heartbeat 过期进入 reconciliation，不自动伪造成 Alpha 失败。

### 2.3 P0-3：正式创建器

- UI 必须沿用现有 QE 自动演进页面的信息架构、颜色、间距、卡片、表格、状态和操作风格。
- 复用单 Alpha 页面已有任务列表、创建 Dialog、节点选择、状态 Badge、结果/轨迹/日志组件；不得再设计一套独立视觉语言。
- 覆盖当前 `CombineBacktestRunRequest` 和 `backtest_config` 的完整业务字段，不只实现最小子集。
- 支持同一 task 下创建一个或多个场景 run；复用 prediction，不重新训练模型。

### 2.4 P0-4：子任务运行网格和恢复可见性

- 展示 baseline、scheme、LOO child 及每个 attempt 的节点、远端 task/loop、状态、阶段、耗时、heartbeat、错误、制品和恢复动作。
- DB event 是权威事件流；workspace/远端日志作为详细证据。
- 后端重启后明确展示 `reconciling`、远端状态核对结果和是否重新接管。
- 复用 `LogsPanel`、`LoopDetailPanel`、`EvolutionTrajectory`、`CombineDiagnosticsPanel` 和现有 adapter。

## 3. Non-Goals / 非目标

- 不开发新的组合算法、权重公式、Alpha 因子或模型。
- 不把多 Alpha 改造成新的模型训练系统；多 Alpha child 仍是基于已有 prediction 的组合和回测。
- 不修改单腿 QE 的标签、模型、因子、训练、Recorder 或数仓业务语义。
- 不触碰 Selection、Advisory、Paper、模拟盘、QMT、实时荐股或生产交易。
- 不新增 GPU/显存/桌面资源轮询，不调用 `nvidia-smi`、NVML 或其他资源遥测。
- 不新增研究方向的 PASS/KILL/GO/STOP、数据就绪门槛、人工审批、发布审批或 promotion 审批。
- 不用新的表替代现有 scheme/LOO 结果表，不复制已有 metrics 和 Archive 数据。
- 允许新增最小的跨来源 QE execution reservation ledger；它只记录 node/source/remote identity/slot 生命周期，不替代业务表、不存储 Alpha 指标。
- 不以静态 mock 页面、仅前端按钮、仅 API 声明或仅内存队列作为完成交付。

## 4. DESIGN-COMPLIANCE-001

### 4.1 禁止简化版

P0-1～P0-4 是一个完整基础能力包。以下任一情况都不能报告为“多 Alpha 演进底座已完成”：

- 只把 daemon thread 换成另一个进程内 queue，没有 DB parent/child/attempt 状态；
- 只有父 run heartbeat，没有远端 task/loop 映射；
- 只有整组 retry，没有 child retry/results-only；
- 只有后端 API，没有创建 UI 或 child grid；
- 新建一套风格不同的多 Alpha 页面，没有复用单 Alpha QE 组件；
- 仅对新 run 生效，历史 run 完全无法关联或查看；
- 只在单进程 happy path 测试，没有后端重启和重复派发验证。

### 4.2 禁止静默错误

- 捕获异常后必须写入 run/child/attempt/event 的稳定 `reason_code` 和上下文，并由 API/UI 展示。
- 禁止 `except Exception: pass`、只写 server log、返回空成功、把缺失指标写 0、把 unknown remote state 写 failed/succeeded。
- retry mode 不得自动互换；`results_only` 缺制品时必须返回 `results_only_artifact_missing`，不能静默改成完整回测。
- node/path/artifact/Recorder 身份缺失时不得回退到 localhost、默认节点、默认 workspace 或其他 run。
- 已成功 child 不得因其他 child 失败而从 UI、Archive 或聚合结果中消失。

### 4.3 禁止业务逻辑偏移

以下实现继续作为唯一业务计算路径：

- `MultiAlphaPanelBuilder`：腿数据装配和覆盖率；
- `MultiAlphaCombiner`：标准化、权重和 rank fusion；
- `seed_ensemble_prediction_only`：种子 prediction ensemble；
- `apply_pred_backtest_overrides`：TopK、资金量、策略参数和费用写入；
- 现有 pred-backtest runtime：Qlib 回测；
- `metric_columns`、scheme/LOO 持久化和 Archive handler：指标与归档。

新 worker 只负责可靠地组织这些函数，不改变其公式、默认值、排序、成本、日期或结果解释。实施必须增加 old-sync-vs-durable parity test。

### 4.4 禁止私增门禁审批

- POST 创建成功后直接进入 `queued`，不存在 `awaiting_approval/approved` 状态。
- 节点繁忙只影响调度时间，不阻止研究方向，也不把 run 标记为失败。
- 数据或 artifact 缺失形成可见错误和恢复动作，不淘汰腿或方向。
- 配置摘要只是用户确认输入内容的普通 UI，不是审批流。
- 唯一硬边界是 QE-only 隔离。

## 5. 当前架构与复用清单

| 当前资产 | 现状 | 本设计处理 |
|---|---|---|
| `MultiAlphaCombineBacktestService` | 请求解析、run 创建、组合、child 执行、结果持久化 | 保留 facade；拆出 durable repository/orchestrator/execution adapter，旧 API 委托新路径 |
| `MultiAlphaCombineBacktestRepository` | run/scheme/LOO CRUD | 扩展 task/run/child/attempt/event 和 CAS；scheme/LOO 表继续使用 |
| `RemotePredBacktestExecutor` | 提交 QE Workspace loop 后同步 polling 到终态 | 拆成 prepare/submit/poll/fetch/cancel；远端 ID 提交后立即持久化 |
| `ShellPredBacktestExecutor` | 后端进程同步 subprocess | 保留测试/显式同步兼容；新异步 production run 不再由它拥有生命周期 |
| `QEWorkspaceClient` | create/status/metrics/kill/log/assets | 作为 WSL 与远端统一子任务契约直接复用 |
| `_NODE_RESERVATIONS` | 单进程内节点占用 | 新 durable worker 不使用；由 DB child 状态和节点事务锁计算 |
| `run_events.jsonl` | workspace 事件文件 | 保留为原始制品；新增 DB event 作为 UI/恢复权威事件 |
| `combine_ui_adapter.py` | 映射为 QE task/loop 读模型 | 扩展 first-class task、child、attempt、capabilities；不重写结果组件 |
| `LoopDetailPanel` | 单 Alpha / 多 Alpha Loop 结果 | 保留；增加 capability 过滤，不伪造单 Alpha 专属动作 |
| `EvolutionTrajectory` | 已支持 multi-alpha data source adapter | 保留并推广为共享 data-source contract |
| `LogsPanel` | 纯展示组件 | 直接复用 DB event/SSE 生成的日志行 |
| `CombineRunOperationsPanel` | retry/delete/archive/场景 replay | 扩展 pause/resume/cancel、child retry 和 reconciliation，视觉保持一致 |
| QE Archive / Prediction Store | 终态归档和 prediction CAS | 继续使用；运行状态表不复制大文件 |

## 6. Target Architecture / 目标架构

```text
QE 自动演进同风格 UI
  ├─ task list / create composer / status actions
  └─ task detail / run loops / child grid / logs / trajectory
                         │
                         ▼
backend/routers/multi_alpha.py
                         │
                         ▼
MultiAlphaCombineBacktestService  (兼容 facade)
  ├─ CombineTaskService           (task/run 创建与请求快照)
  ├─ CombineOrchestrator          (claim/reconcile/schedule/finalize)
  ├─ CombineExecutionAdapter      (prepare/submit/poll/fetch/cancel)
  ├─ MultiAlphaCombiner           (现有业务计算，不改)
  └─ CombineRepository            (PostgreSQL 权威状态)
            │                              │
            │                              ├─ multi_alpha_combine_task
            │                              ├─ ..._run（现有表增量）
            │                              ├─ ..._child
            │                              ├─ ..._child_attempt
            │                              └─ ..._event
            ▼
QEWorkspaceClient.for_node(node_id)
  ├─ wsl2-5080 QE Workspace API
  └─ rdagent-node1 QE Workspace API
            │
            └─ existing pred-backtest runtime / artifacts / metrics
```

FastAPI lifespan 在 multi-alpha schema 可用时启动一个轻量 `MultiAlphaOrchestratorLoop`；schema 不可用时记录 QE-scoped health failure 而不终止整个 FastAPI。worker 不是任务资产的唯一所有者：所有状态先写数据库，远端 QE loop 独立运行；后端停止期间在途远端 loop 继续，重启后由新 worker 核对并接管。

## 7. Domain Model / 任务层级与身份

### 7.1 层级

```text
CombineTask
  └─ CombineRun                    一个日期/资金/TopK/持仓/节点场景
       ├─ CombineChild baseline
       ├─ CombineChild scheme:equal
       ├─ CombineChild scheme:...
       └─ CombineChild loo:<scheme>:drop:<leg>
            └─ ChildAttempt #1/#2  每次实际执行或 results-only 回收
```

### 7.2 身份规则

- `task_id`：`mact_<uuid/sha>`；新任务为 first-class identity，不再只依靠 UI 临时 hash。
- `run_id`：继续使用现有 `macb_<roster>_<window>_<timestamp>`，保持历史兼容。
- `child_id`：`macbc_<sha256(run_id|child_key)>`，逻辑 child 在同一 run 内唯一。
- `attempt_id`：`macba_<child_id_suffix>_<attempt_no>_<uuid>`，每次执行 append-only。
- `child_key`：稳定字符串：
  - `baseline:<leg_id>`
  - `scheme:<weighting_scheme>`
  - `loo:<weighting_scheme>:drop:<leg_id>`
- `qe_task_id`：由 run/child/attempt 确定性生成，例如 `macb_remote_<run_hash>_<child_hash>_a<attempt>`。
- `qe_loop_id`：在提交前固定为 `Loop1` 或由稳定 `remote_loop_index` 生成；先持久化 submission intent，再调用远端。

同一 identity 不允许对应不同 request/artifact hash。冲突返回 `multi_alpha_identity_payload_conflict`，不得覆盖旧状态。

## 8. DB Contracts / 数据库详细设计

每个实施阶段使用独立、版本化、幂等、additive migration：P0-1A durable business schema 已部署，P0-1B 另增 reservation migration。实现脚本不得自动执行 migration，不得在执行前导出数据库；生产已有每日备份。DDL 应包含表、列、约束、索引、comment 和对应 rollback/preflight 说明。

### 8.1 `strategy_pkg.multi_alpha_combine_task`（新增）

用途：把当前 `task_key_for_run()` 的 synthetic grouping 提升为持久化任务身份，同时允许未来相同 roster 创建不同命名的研究任务。

| 字段 | 类型 | 语义 |
|---|---|---|
| `task_id` | TEXT PK | `mact_*` |
| `task_name` | TEXT NOT NULL | UI 显示名称 |
| `task_type` | TEXT NOT NULL | 固定 `multi_alpha_combine` |
| `description` | TEXT | 研究说明，不参与算法 |
| `roster_hash` | TEXT NOT NULL | 复用现有 hash |
| `roster_json` | JSONB NOT NULL | 腿、seed run、metadata 冻结快照 |
| `default_request_json` | JSONB NOT NULL | 创建器默认组合/回测/节点参数；是模板，不是完整 task identity |
| `legacy_group_key` | TEXT | 历史 UI group 映射；部分唯一索引 |
| `source_kind` | TEXT NOT NULL | `ui/api/mcp/legacy_backfill` |
| `created_by` | TEXT | 普通来源标识，不是审批者 |
| `created_at/updated_at` | TIMESTAMPTZ | 审计时间 |

不在 task 表存储可从 runs 推导的 metrics、best run 或状态，避免双写漂移。task 列表状态由其 runs 聚合。

task immutable identity 只由 canonical roster/roster hash、normalize method 和 walk-forward signature 构成，并对应 `legacy_group_key` 与 deterministic implicit task ID。OOS、TopK、initial cash、持仓参数、baseline、node 与 timeout 是 run 场景参数；同一 task 可以合法承载这些字段不同的多个 run。Repository 不得把完整 `default_request_json` 参与 identity 相等判断，也不得在提交新 run 时隐式覆盖 defaults。

### 8.2 `strategy_pkg.multi_alpha_combine_backtest_run`（现有表增量）

保留所有现有列和 result FK，新增：

| 字段族 | 字段 |
|---|---|
| parent | `task_id` FK、`request_hash`、`retry_of_run_id` |
| state | `status`、`phase`、`progress_json`、`row_version` |
| lease | `owner_id`、`fencing_token`、`lease_expires_at`、`heartbeat_at` |
| control | `pause_requested_at/by`、`cancel_requested_at/by` |
| execution | `node_parallelism_json`、`started_at`、`finished_at`、`updated_at` |
| error | `error_code`、`error_json` |

run status CHECK：

```text
queued
preparing
running
pause_requested
paused
cancel_requested
cancelling
succeeded
partial_failed
failed
cancelled
```

`reason` 保持兼容，只作为旧客户端聚合视图；新状态的权威字段是结构化列和 event 表。service 更新时同步生成兼容 `reason`，不得让两套状态独立演化。

### 8.3 `strategy_pkg.multi_alpha_combine_backtest_child`（新增）

| 字段 | 类型/语义 |
|---|---|
| `child_id` | TEXT PK |
| `run_id` | FK，ON DELETE CASCADE |
| `child_key` | TEXT，UNIQUE(run_id, child_key) |
| `child_kind` | `baseline/scheme/loo` |
| `weighting_scheme` | nullable |
| `dropped_leg_id` | nullable |
| `ordinal` | 稳定执行/显示顺序 |
| `status` | `pending/materializing/queued/running/reconciling/cancel_requested/cancelling/succeeded/not_computable/failed/cancelled` |
| `input_manifest_json` | 腿、日期、配置、artifact identity |
| `prediction_artifact_uri/hash` | 组合 prediction CAS 身份 |
| `selected_attempt_id` | 当前用于结果展示的 attempt |
| `created_at/updated_at` | 时间 |

child 表不复制 metrics；scheme 继续写 scheme result 表，LOO 继续写 LOO 表。baseline 指标写入 selected attempt 的版本化 `result_manifest_json.metrics`，并通过与 scheme/LOO 相同的 child-result read model/API 查询；parent reason 只保存结果引用和摘要，不复制完整 metrics，也不得把 baseline 埋在仅日志可见的位置。

### 8.4 `strategy_pkg.multi_alpha_combine_backtest_child_attempt`（新增）

| 字段族 | 字段 |
|---|---|
| identity | `attempt_id` PK、`child_id` FK、`attempt_no`、`retry_mode`、`retry_of_attempt_id` |
| remote | `node_id`、`qe_task_id`、`qe_loop_id`、`submission_intent_hash`、`remote_status` |
| state | `status`、`phase`、`row_version` |
| lease | `owner_id`、`fencing_token`、`lease_expires_at`、`heartbeat_at` |
| artifacts | `artifact_manifest_json`、`result_manifest_json` |
| errors | `error_code`、`error_json` |
| time | `queued_at/submitted_at/started_at/finished_at/created_at/updated_at` |

约束：

- UNIQUE(`child_id`, `attempt_no`)
- UNIQUE(`qe_task_id`, `qe_loop_id`) WHERE 两者非空
- `retry_mode IN ('initial','backtest_only','results_only','rematerialize_and_backtest')`
- `status IN ('queued','submitting','running','reconciling','succeeded','failed','cancelled')`
- `fencing_token >= 0`、`row_version >= 1`

### 8.5 `strategy_pkg.multi_alpha_combine_backtest_event`（新增）

| 字段 | 语义 |
|---|---|
| `event_id` BIGSERIAL PK | DB 顺序 |
| `run_id` FK | 必填 |
| `child_id/attempt_id` | nullable，细分事件 |
| `event_type` | created/claimed/submitted/status/log/reconciled/control/result/error/terminal |
| `phase` | 当前阶段 |
| `reason_code` | nullable |
| `payload_json` | 结构化上下文，禁止凭据 |
| `created_at` | 时间 |

索引：`(run_id,event_id)`、`(child_id,event_id)`、`(attempt_id,event_id)`、`(created_at)`。

DB event 是 UI、SSE 和重启恢复的权威事件；现有 `run_events.jsonl` 继续作为 workspace 可下载制品，不作为唯一状态源。

每次权威状态变化与对应 event 必须由 repository 在同一个 PostgreSQL transaction 中提交，例如 `transition_with_event()`；event 插入失败时状态变化一并回滚，API 不得返回成功。远端提交、取消等外部副作用采用“先持久化 intent 状态与 event，再调用远端，再以 CAS 持久化结果 event”的顺序，避免状态已改变但审计事件缺失，或远端已执行却没有可恢复 intent。

### 8.6 历史回填

历史回填只建立关联，不修改 metrics、status、reason、created_at 或 Archive：

1. 用现有 `task_key_for_run()` 对历史 run 分组。
2. 每组创建 `mact_legacy_<hash>` task，写 `legacy_group_key`。
3. 回填 run.task_id、updated_at；历史终态 run 不生成虚构 child/attempt。
4. 对能从 scheme/LOO 结果确定的历史 child，可写 `status=succeeded/failed` 和 `source=legacy_result_backfill`；无法确定远端 attempt 的字段保持 NULL，并在 UI 显示“历史执行映射不可用”。
5. 回填脚本提供 dry-run/execute/readback，但不输出数据库备份。

回填 task 的 `default_request_json` 可以继续来自组内第一条 run，作为 UI 默认模板；后续 run 复用 task 时只核对 immutable identity，不要求与第一条 run 的完整 backtest config/baseline 相等。

### 8.7 `infra.qe_execution_reservation`（P0-1B 新增）

用途：为所有生产 QE Workspace submit 提供 POST 前、跨进程、跨来源的唯一 slot reservation。它不是第二套实验状态机，也不复制任何 metrics、prediction 或组合结果。

| 字段族 | 字段/语义 |
|---|---|
| identity | `reservation_id` PK、`source_kind/source_execution_id` UNIQUE |
| node/remote | `node_id` FK、`qe_task_id/qe_loop_id/submission_intent_hash`，remote identity UNIQUE |
| state | `reserved/submitting/running/reconciling/released/failed/cancelled` |
| ownership | `owner_id/lease_expires_at/fencing_token/row_version`；lease 过期不自动释放 slot |
| evidence | `remote_status/release_reason_code/reserved_at/heartbeat_at/released_at/created_at/updated_at` |

容量检查、reservation INSERT 与 source row claim 必须在同一 node advisory transaction 中完成。只有权威 remote terminal 或 receipt 明确 `not_reserved` 才能 release；network unknown、deadline exceeded 和本地 lease 过期继续占用。Migration 必须 additive、幂等，包含 comments、partial active index、unique/check constraints、preflight 和 guarded rollback。

## 9. State Machines / 状态机

### 9.1 Run 状态机

```text
queued -> preparing -> running -> succeeded
                      │       ├-> partial_failed
                      │       └-> failed
                      ├-> pause_requested -> paused -> running
                      └-> cancel_requested -> cancelling -> cancelled/partial_failed
```

- `partial_failed`：至少一个 `succeeded` child 的 result identity/manifest 已验证通过，同时另有可执行 child 明确 `failed/cancelled`；不根据 IC、收益、主观“研究价值”或方向判断聚合状态。
- `failed`：没有任何 `succeeded` child，且输入、materialization 或可执行 child 出现明确技术失败；`not_computable` 不单独构成失败。
- `cancelled`：用户取消且没有成功 child；若已有成功 child，父状态为 `partial_failed`，成功结果继续可见。
- `not_computable` child 表示当前冻结输入和公式下该 scheme 数学上不可计算，不是基础架构失败、Alpha 失败或研究方向淘汰；只从可执行 child 分母中排除并在 UI/结果中保留原因。
- heartbeat/lease 过期不直接产生 terminal 状态；新 worker 进入 reconciliation。

### 9.2 Child / Attempt 状态机

```text
child pending -> materializing -> queued -> running -> reconciling -> succeeded/failed/cancelled
       └──────────────────────────────────────────────────────────> not_computable

attempt queued -> submitting -> running -> reconciling -> succeeded/failed/cancelled
```

`reconciling` 是“本地 worker 正在核对远端事实”，不是失败。远端 API 暂时不可达时保留该状态和最后已知 heartbeat，继续重试核对；不得依据本地 lease 过期推断远端进程已结束。

状态迁移不得跳过关键证据阶段：child 使用 `pending -> materializing -> queued -> running -> reconciling -> terminal`，attempt 使用 `queued -> submitting -> running -> reconciling -> terminal`。远端 receipt 尚为 `reserved_not_started`、network unknown 或 deadline exceeded 时都保持 reconciling，不直接伪造 terminal。

pause 仅作用于 parent run 的新 child 派发；当前 QE Workspace 不具备真实远端 pause，因此 running child/attempt 不进入 paused 状态，而是继续到明确终态。UI 不得把 parent pause 显示为 child 已被冻结。

### 9.3 聚合规则

- parent progress 来自 child count，不从日志字符串猜测。
- task status 来自 runs 聚合，不单独写第二套权威状态。
- child status 来自 selected/latest attempt；历史 attempt 保留。
- child 成功后其 scheme/LOO 结果只允许相同 identity 幂等重放；不同 hash 不能覆盖。

## 10. Durable Orchestrator / 持久化编排

### 10.1 Worker 生命周期

新增 `backend/services/multi_alpha/orchestrator.py`：

- `run_once()`：完成一次 claim/reconcile/materialize/schedule/finalize；便于单测和人工 smoke。
- `run_loop(shutdown_event)`：由 `backend/main.py` lifespan 启动，短轮询 DB；shutdown 只停止新 claim，不杀远端 child。
- 后端启动第一轮先 reconcile 已有 `running/submitting/reconciling/cancel_requested` attempt，再调度 queued child。
- worker 不创建每个 run 一个 daemon thread；CPU materialization 使用共享有界 executor 或 `asyncio.to_thread`，并发由统一 semaphore 控制。

### 10.2 Claim、lease、fencing、CAS

- repository 使用 `FOR UPDATE SKIP LOCKED` claim run/attempt。
- claim 时写 `owner_id`、递增 `fencing_token`、设置 lease 和 heartbeat。
- heartbeat、phase、result、error、terminal 更新必须匹配 owner + fencing token + row_version。
- 旧 worker 的写入返回 `multi_alpha_stale_fencing_token`，不得提交结果。
- lease 过期只允许新 worker claim/reconcile；不调用“mark stale failed”。
- `mark_stale_running_runs_failed` 仅保留为 legacy 诊断接口；对 durable run 返回 `durable_run_requires_reconciliation`，不得终态化。

### 10.3 Deterministic child plan

`MultiAlphaCombiner` 生成完可计算 scheme 后，orchestrator 以稳定顺序物化 child：

1. baseline；
2. request 中 scheme 顺序；
3. 每个可计算 scheme 的 LOO，按 roster 顺序。

不可计算 scheme 仍建立 child，确定性写 `not_computable` 和明确 reason，不创建 attempt、不调用远端；它不会阻止其他 child，也不改变其他结果的科研可用性。child plan hash 写入 run，重启后相同 request 必须得到相同 plan。

### 10.4 节点容量

- 新增共享 `QEActiveExecutionCapacityService`，实时容量只统计 `infra.qe_execution_reservation` 的 active rows，不把 `qe_evolution_loops`、`qe_multi_alpha_groups`、`qe_experiments` 或 durable attempts 事后拼成近似 reservation。
- 所有生产 `QEWorkspaceClient.create_and_run_loop()` 调用先进入同一 `QEWorkspaceSubmissionCoordinator`；按 node advisory lock 在同一事务内完成 active count、reservation INSERT 与 source-specific claim，然后才允许远端 POST。
- `qe_evolution_loops`、`qe_multi_alpha_groups`、`qe_experiments` 和 durable attempts 继续作为各自业务状态权威，只在 P0-1B 激活前导入当前 active execution、以及运行中做一致性核对。无法唯一定位 node 的 active execution 形成结构化诊断，并仅让相关节点 queue-only；不得静默漏计或淘汰研究方向。
- capacity API/UI 返回 reservation 按来源拆分的占用明细和 total，便于解释为什么排队；不得只返回一个无法核对的整数。
- 默认运营上限沿用用户既定规则：WSL 最多 2 个普通 Loop、远端最多 4 个 Loop；图模型训练仍由单 Alpha QE 的模型分类保持 WSL 1 串行。多 Alpha pred-backtest 不改变模型训练并发。
- 容量不足写 `waiting_capacity` event 并保持 queued，不报 `node_capacity_exhausted` 终态错误。

### 10.5 重启恢复算法

对每个非终态 attempt：

1. 若 `qe_task_id/qe_loop_id` 均存在，先查询 QE Workspace submission receipt，再调用 `get_loop_status()`。
2. remote=completed：读取 `qlib_results_enhanced.json`，验证 manifest/hash，幂等写结果。
3. remote=running：更新 heartbeat/phase，释放本轮 lease，后续继续轮询。
4. remote=failed/cancelled：读取可用日志并结构化终态。
5. loop status=404 但 receipt 存在：按 receipt 的 `reserved/started/running/terminal` 事实处理；不得重提。
6. receipt 权威返回 `not_reserved`：当前 owner 才允许以同一 identity/hash 提交；普通 404 不能证明未接收。
7. API 暂时不可达：保留 last-known state，写 `remote_status_unavailable` event，不终态化。

为覆盖“远端已接收、后端在保存 response 前崩溃”的窗口，提交前必须先保存确定性的 `qe_task_id/qe_loop_id/submission_intent_hash`，并要求 QE Workspace 服务端原子保存同 hash receipt。相同 identity/hash 的重复 POST 返回已有 receipt 而不再次注册 execution；不同 hash 返回 409。若 receipt 已 reserved 但没有 started evidence，保持 `reserved_not_started/reconciling`，P0-2 通过新 attempt 恢复，不覆盖或重放旧 execution。

### 10.6 Artifact 原子性

- combined prediction、combined factors、config 和 manifest 先写同目录临时文件，再 `os.replace`。
- manifest 包含 run/child/attempt、源 prediction URI/hash、dataset identity、日期、TopK/资金/策略参数、代码版本和文件 hash。
- Prediction Store/CAS 上传成功后再将 child 标记 queued。
- 已存在同 identity、同 hash 时幂等复用；同 identity、不同 hash fail-loud。
- 不把大文件写入 PostgreSQL；DB 只存 URI/hash/size/schema。

## 11. Execution Adapter / 执行适配器

当前 `execute_pred_backtest()` 将 prepare、submit、poll、fetch 合在一次同步调用中。实现时拆为以下接口，底层仍复用现有函数和 `QEWorkspaceClient`：

```text
prepare_child_attempt()  -> artifact manifest + workspace files
submit_child_attempt()   -> qe_task_id + qe_loop_id
poll_child_attempt()     -> remote status + progress
fetch_child_result()     -> enhanced metrics + result manifest
cancel_child_attempt()   -> remote kill result
read_child_logs()        -> safe log/event tail
```

### 11.1 WSL/远端统一

- `QEWorkspaceClient.for_node('wsl2-5080')` 和远端节点使用同一状态/取消/文件读取接口。
- 新 async production path 不允许本机 `subprocess.run` 成为生命周期所有者。
- `ShellPredBacktestExecutor` 可保留给定向单测、显式 `run_async=false` 兼容和故障定位，但不能作为 durable run 的隐藏 fallback。

### 11.2 `run_async=false` 兼容

- REST/UI 默认仍为 async queued。
- service test 或明确同步调用可以创建 durable run 后调用 `drain_until_terminal(run_id, wait_timeout_seconds)`；该函数只是等待 DB/worker 结果，不直接绕过 durable 状态机执行另一套算法。
- 等待到期返回 HTTP 202、`wait_timed_out=true` 和当前 durable 状态；连接断开不取消 run、不释放 reservation。`wait_timeout_seconds` 与 transport timeout、`scheme_timeout_seconds/run_timeout_seconds` execution deadline 分离。
- execution deadline 到期但远端仍 running/unknown 时写 evidence 并保持 reconciling；最终有效结果继续持久化，标记 `completed_after_deadline=true`，不得仅因 deadline 丢弃研究结果。

### 11.3 QE Workspace submission receipt contract

该合同由 QE Workspace owning repository `F:\Dev\RD-Agent-main` 实现，AIstock 只消费，不在客户端伪造：

- `LoopRunRequest` 必填 `submission_intent_hash`；服务端以 `(task_id, loop_id)` 原子持久化 receipt。Canonical request digest 覆盖 `loop_index/config/experiment_files` 内容 hash、`wsl_command/model_source`，排除仅用于通知的 `callback_url`。
- same identity/same hash replay 返回同一 loop/receipt，不能再次调用 background execution registration。
- same identity/different hash 返回结构化 HTTP 409。
- receipt 可独立于 loop status 查询，并在服务重启后恢复；至少覆盖 `reserved/started/running/completed/failed/cancelled`。
- receipt=`reserved_not_started` 时不自动重放，AIstock 保持 reconciling；后续通过新 attempt retry。
- WSL 与远端节点必须部署同一 contract；AIstock coordinated submit 不得回退到旧无 receipt POST。

## 12. Lifecycle Controls / 暂停、恢复、取消和重试

### 12.1 Pause

- UI “暂停”只停止派发新 child，并保持 run 可恢复。
- 已 running child 默认允许完成，完成后 run 进入 paused；UI 明确显示“等待 N 个在途子任务完成”。
- 不伪造远端 pause。未来若 QE Workspace 提供真实 pause，可作为新能力另行设计。

### 12.2 Resume

- paused run 恢复为 running，继续 queued/failed-retry child。
- 不重新 materialize 已有同 hash prediction，不重复提交已有 remote identity。

### 12.3 Cancel

- run 标记 `cancel_requested`，停止新派发。
- 对 running attempt 调用 `QEWorkspaceClient.kill_loop()`；逐个记录成功/失败。
- cancel API 只有在远端结果明确后返回当前状态；无法确认的 child 显示 `cancelling/remote_state_unknown`，后台继续 reconcile。
- 已完成 child 保留；父状态按聚合规则计算。
- 兼容 `stop` endpoint 委托同一 cancel service，保持现有单 Alpha “终止在途 Loop、取消未完成 Loop”的用户语义；UI 不新增第二套含义相同的 stop/cancel 按钮，也禁止把 stop 改成 cooperative pause。

### 12.4 Child retry modes

| mode | 行为 | 必需制品 | 禁止行为 |
|---|---|---|---|
| `backtest_only` | 复用已持久化 combined prediction，重新跑 Qlib | prediction URI/hash、runtime template、dataset identity | 不重新组合、不重新训练 |
| `results_only` | 读取已有远端/local workspace 结果并重新 ingest | qe task/loop、result artifact/hash | 缺结果时不得改成 backtest |
| `rematerialize_and_backtest` | 用冻结源 prediction identity 重新组合，再回测 | 所有源 prediction、request snapshot | 不允许换腿、换日期、换算法后仍称 exact retry |

retry 新增 attempt，不重写旧 attempt。UI 必须展示 retry_of、mode、原因和新 attempt ID。

### 12.5 Whole-run retry

保留现有 whole-run retry。它创建新 run，并冻结 `retry_of_run_id`；不会替代 child retry。用户可以选择整组新场景或仅修复缺失 child。

## 13. API Contracts

所有接口保持现有 `{"status":"success","data":...}` envelope 和结构化错误。旧 endpoint 继续可用并委托新 service。

### 13.1 Task / Run

- `POST /api/v1/multi-alpha/combine/tasks`
  - 创建 first-class task，并可携带一个或多个 `runs` 场景。
- `POST /api/v1/multi-alpha/combine/tasks/{task_id}/runs`
  - 在已有 task 下新增场景 run。
- `POST /api/v1/multi-alpha/combine-backtest/run`
  - 兼容入口；自动创建/解析 task 后进入相同 durable service。
- `GET /api/v1/multi-alpha/combine/tasks`
- `GET /api/v1/multi-alpha/combine/tasks/{task_id}`
- `GET /api/v1/multi-alpha/combine-backtest/runs/{run_id}`

### 13.2 Control

- `POST /api/v1/multi-alpha/combine-backtest/runs/{run_id}/pause`
- `POST /api/v1/multi-alpha/combine-backtest/runs/{run_id}/stop`（legacy cancel alias）
- `POST /api/v1/multi-alpha/combine-backtest/runs/{run_id}/resume`
- `POST /api/v1/multi-alpha/combine-backtest/runs/{run_id}/cancel`
- `POST /api/v1/multi-alpha/combine-backtest/runs/{run_id}/reconcile`

控制请求支持 `Idempotency-Key`；相同 key + 相同 payload 幂等返回，相同 key + 不同 payload 返回冲突。

### 13.3 Child / Attempt

- `GET /api/v1/multi-alpha/combine-backtest/runs/{run_id}/children`
- `GET /api/v1/multi-alpha/combine-backtest/children/{child_id}`
- `GET /api/v1/multi-alpha/combine-backtest/children/{child_id}/attempts`
- `POST /api/v1/multi-alpha/combine-backtest/children/{child_id}/retry`
- `POST /api/v1/multi-alpha/combine-backtest/attempts/{attempt_id}/cancel`
- `GET /api/v1/multi-alpha/combine-backtest/attempts/{attempt_id}/logs`

### 13.4 Events / SSE

- `GET /api/v1/multi-alpha/combine-backtest/runs/{run_id}/events?after_event_id=N`
- `GET /api/v1/multi-alpha/combine-backtest/runs/{run_id}/logs/stream`（SSE）
- 现有 `/logs` polling API 保留，内部合并 DB events 与安全 workspace/remote tail。

### 13.5 Capabilities

run/child/attempt 响应返回显式 capabilities：

```json
{
  "can_pause": true,
  "can_resume": false,
  "can_cancel": true,
  "can_retry_backtest": false,
  "can_retry_results_only": true,
  "can_rematerialize": true,
  "reason": null
}
```

UI 不自行猜测状态许可，也不把 capability=false 解释成研究门禁；它只表示当前对象和制品是否支持该操作。

### 13.6 QE-scoped health

- `GET /api/v1/multi-alpha/combine/health`
- 返回 schema、orchestrator、Archive capture、Prediction Store/CAS、reservation ledger/coordinator 和 QE Workspace receipt contract 的独立状态与结构化 reason；任一子能力不可用不得伪造成整体 healthy。
- schema 缺失时 multi-alpha 创建、控制和重试接口返回 HTTP 503 + `multi_alpha_schema_unavailable`；FastAPI、非 QE router 和不依赖新 schema 的兼容只读能力保持可用。
- health 只报告技术可用性，不包含 Alpha 方向审批、PASS/KILL 或研究准入判断。

## 14. UI Architecture / 前端详细设计

### 14.1 信息架构

多 Alpha 继续位于 QuantEvolver。页面必须沿用现有单 Alpha 自动演进工作台：

```text
QE 自动演进
  ├─ 单 Alpha 演进
  └─ 多 Alpha 组合演进
       ├─ 任务列表
       ├─ 创建组合任务
       └─ 任务详情
            ├─ 配置/轨迹/指标
            ├─ 运行与日志
            ├─ 子任务网格
            └─ 诊断/Archive/场景 replay
```

规范操作入口是现有 `/quantevolver/evolution` 页面，通过 `taskType=single_alpha|multi_alpha_combine` 使用同一 workspace shell、任务列表、创建器框架和详情布局。保留 `/quantevolver/multi-alpha/combine-backtest` 及详情 URL 仅用于兼容旧链接；兼容路由应重定向或薄委托到同一共享页面状态，不得继续维护独立 DOM/样式实现，也不得新增风格不同的“第二版控制台”。

### 14.2 共享组件抽取

从现有 `evolution/page.tsx` 增量抽取，不改变单 Alpha DOM 语义和视觉：

- `EvolutionWorkspaceShell`
- `EvolutionTaskToolbar`
- `EvolutionTaskList`
- `EvolutionTaskStatusBadge`
- `EvolutionCreateDialogFrame`
- `EvolutionTaskActions`
- `EvolutionRuntimePanel`

继续直接复用：

- `LoopDetailPanel`
- `LoopMetricsComparison`
- `EvolutionTrajectory`
- `LogsPanel`
- `TopologyPanel` 的状态/卡片语言；child grid 若因语义不同新增组件，也必须复用相同 style constants、Badge 和 action patterns。

抽取顺序必须先保证单 Alpha DOM、视觉截图和交互不变，再接入多 Alpha；不允许复制 4800 行页面形成第二份分叉。新增 child grid 等多 Alpha 专属组件不得定义新的颜色、字号、圆角、间距、阴影、Badge 或布局栅格，只能复用现有 QE 组件或抽取后的共享 token/style constants。

### 14.3 Data source adapter

把当前只覆盖 trajectory 的 `DataSourceAdapter` 扩展为共享 `EvolutionDataSourceAdapter`：

```ts
interface EvolutionDataSourceAdapter {
  taskType: "single_alpha" | "multi_alpha_combine";
  listTasks(...): Promise<TaskList>;
  getTask(...): Promise<TaskDetail>;
  getLoop(...): Promise<LoopDetail>;
  getTrajectory(...): Promise<Trajectory>;
  getLogs(...): Promise<LogEvent[]>;
  subscribeLogs?(...): EventSource;
  getCapabilities(...): Promise<Capabilities>;
  runAction(...): Promise<ActionResult>;
}
```

单 Alpha adapter 使用现有 `/api/v1/quantevolver/evolution/*`；多 Alpha adapter 使用 `/api/v1/multi-alpha/combine/*`。presentational components 不拼接业务 URL。

### 14.4 创建器完整字段

创建 Dialog 沿用单 Alpha现有大 Dialog/分区布局，分为：

1. **任务信息**：名称、说明。
2. **Alpha 腿**：从现有 source tasks/source experiments/QE Archive/Prediction Store 选择；每腿显示模型、因子集、horizon、seed runs、prediction identity、日期覆盖。
3. **组合配置**：schemes、normalize、walk-forward、rank fusion、baseline leg、min date coverage。
4. **回测场景**：OOS 日期、TopK、initial cash、strategy params、n_drop/min/max/dynamic、费用和 runtime template。
5. **执行配置**：node、node parallelism、scheme/run/read timeout；显示 WSL 2、远端 4 的当前运营上限。
6. **场景列表**：可添加多行资金量/TopK/持仓参数；同一 task 下生成多个 run，复用腿 prediction。
7. **配置摘要**：展示最终 request JSON 和 prediction/dataset identity；不是审批，不增加 review 状态。

创建器必须提交当前后端完整 request，不得用 UI 默认值覆盖用户未选择但已有 source config 的字段。无法解析的字段显示错误并阻止错误 payload 提交，但不影响用户返回修改，也不形成研究方向门禁。

### 14.5 任务列表

复用单 Alpha任务列表的布局和操作区，新增/映射：

- task name / roster summary；
- run 数量和 completed/partial/failed/running/paused；
- 当前 phase、progress、last heartbeat；
- latest node/child；
- actions：查看、暂停、恢复、取消、新增场景、clone config。

若一个 task 同时有多个 active run，task 行动作不能静默选择一个 run；UI 展开 active runs 让用户选择具体对象。

### 14.6 详情与子任务网格

详情页左侧仍是 run/场景列表，右侧仍是 `LoopDetailPanel`/trajectory。运行 Tab 增加 child grid：

| 列 | 内容 |
|---|---|
| Child | baseline/scheme/LOO + key |
| Attempt | 当前/历史 attempt、retry mode |
| Node | node_id |
| Remote | qe_task_id / qe_loop_id，可复制 |
| Status | pending/materializing/queued/running/reconciling/succeeded/not_computable/failed/cancelled |
| Phase | materialize/submit/backtest/result ingest/archive |
| Elapsed | queued/run/total |
| Heartbeat | 最后本地和远端时间 |
| Artifact | prediction/result manifest 状态 |
| Error | reason_code + 摘要 |
| Actions | logs、cancel、retry mode、reconcile |

点击 child 在右侧打开 attempt timeline、结构化错误、manifest 和日志；不离开现有页面 shell。

### 14.7 日志

- `LogsPanel` 展示由 DB event 转换的统一行，支持 SSE 增量。
- 用户可切换 parent / all children / specific child/attempt。
- workspace/remote log tail 读取失败必须显示错误；不能用空白假装“暂无日志”。
- 页面隐藏时暂停渲染/轮询，恢复可见时从 `last_event_id` 补齐，不丢事件。
- 不调用任何 GPU/显存监测接口。

### 14.8 单 Alpha 专属字段

多 Alpha 没有训练 loss、模型 IC、feature importance、Agent 决策时：

- 后端返回 `null/not_applicable`；
- UI 隐藏不适用 tab 或明确“该任务类型不适用”；
- 禁止填 0、空数组或借用某条腿指标冒充组合训练指标。

## 15. Failure Semantics / 失败语义

稳定 reason codes 至少包括：

| reason_code | 场景 |
|---|---|
| `multi_alpha_schema_unavailable` | durable schema 未应用、版本不兼容或 preflight 无法确认；仅 multi-alpha 写能力不可用 |
| `multi_alpha_identity_payload_conflict` | 同 identity 不同 payload/hash |
| `multi_alpha_stale_fencing_token` | 旧 worker 写入 |
| `multi_alpha_remote_status_unavailable` | 节点暂不可达，非终态 |
| `multi_alpha_remote_state_unknown` | 已提交但 remote identity 暂无法确认 |
| `multi_alpha_remote_submission_conflict` | deterministic remote identity 已存在但 payload 不同 |
| `multi_alpha_child_materialization_failed` | prediction/config/artifact 生成失败 |
| `multi_alpha_child_submit_failed` | 明确提交失败 |
| `multi_alpha_child_backtest_failed` | 远端明确失败 |
| `multi_alpha_child_cancel_unconfirmed` | cancel 请求后尚未确认 |
| `results_only_artifact_missing` | results-only 缺制品 |
| `results_only_identity_mismatch` | result manifest 不属于当前 attempt |
| `node_capacity_waiting` | 容量不足，保持 queued |
| `qe_capacity_identity_unresolved` | 激活前 active execution 无法唯一定位节点；相关节点 queue-only，已有研究不终止 |
| `qe_workspace_submission_identity_conflict` | 相同 remote identity 的 submission intent/request digest 不一致，HTTP 409 |
| `qe_workspace_submission_reserved_not_started` | receipt 已 reserved 但无 started evidence；保持 reconciling，不自动重提 |
| `multi_alpha_execution_deadline_exceeded` | child/run deadline 到期但远端仍 running/unknown；保留回收 |
| `multi_alpha_sync_wait_timeout` | 同步等待到期，返回 202，durable run 继续 |
| `multi_alpha_event_persist_failed` | 权威 event 写入失败；当前动作不得继续伪装成功 |
| `multi_alpha_archive_capture_unavailable` | Archive capture 初始化或调用不可用；metrics 保留，状态/event/UI 可见并支持补归档 |

通信错误与 Alpha 结果失败必须分开。task/run/child/attempt 每层都保留 raw status、reason code、context 和可用制品。

## 16. Concurrency / 并发与资源

- WSL 普通 QE Loop 全局最多 2；远端节点最多 4；图模型训练规则仍由单 Alpha模型分类控制。
- multi-alpha orchestrator 的 materialization 并发与远端 backtest 并发分开配置。
- 同一 run 的 child 可以并行，但节点 claim 必须跨进程一致。
- `results_only` 不占训练/回测 slot，只占短时 ingest worker。
- CPU/内存暂时紧张时排队；不通过无限并发、busy loop 或高频资源探测解决。
- worker poll 使用正常任务状态 API，默认秒级到十秒级；不每秒多次启动外部命令。

## 17. Archive 与文件资产

- terminal child result 先进入现有 scheme/LOO 结果表，再由 parent 聚合。
- parent terminal transaction 先独立提交；Archive handler 只读取 terminal run，不进入业务终态事务。
- post-terminal Archive pass 使用由 source/run/schema version 生成的确定性 event ID，继续调用现有 QE Archive event capture；duplicate enqueue 幂等返回，不重复归档。
- Archive enabled、duplicate、disabled、初始化失败和 enqueue/worker error 都写独立 durable event，并由 run read model 派生 `archive_status/archive_reason`。不得修改已 terminal 的 run status，不覆盖已经计算出的 Alpha 指标，也不得只写日志。
- `QEArchiveEventCapture` 初始化失败不得继续以 `_archive_event_capture=None` 静默运行。orchestrator health/API 必须暴露 `archive_capture.available=false`、稳定 reason code 和异常摘要；恢复后以同一 event ID 幂等补归档，不重算组合或回测。
- Prediction Store/CAS 保存源腿与组合 prediction identity；DB 不复制二进制。
- 历史 `artifact_count=0` 或文件缺失保留明确状态，可通过既有回填能力补关联，不伪造文件。

## 18. Backend File Plan / 后端逐文件方案

### 18.1 保留并改造

- `backend/services/multi_alpha/combine_backtest.py`
  - 保留请求解析、组合业务函数和 service facade；
  - 移除新 async run 的 per-run daemon thread；
  - `submit_run()` 改为创建 task/run 状态并唤醒 orchestrator；
  - 旧同步测试通过 durable drain helper；
  - 删除 Archive capture 初始化失败后仅写日志并保留 `None` 的静默路径，改为 health + durable archive event/read-model 状态和幂等补归档入口；不回写 terminal run status。
- `backend/services/multi_alpha/remote_dispatch.py`
  - 将同步 `execute_pred_backtest()` 拆为可持久化阶段；
  - 暴露 remote task/loop identity；
  - 复用 artifact sync 和 command builder。
- `backend/services/multi_alpha/combine_ui_adapter.py`
  - 从 synthetic groups 迁移到 first-class task；
  - 增加 child/attempt/capabilities；
  - 保留 legacy task key alias。
- `backend/routers/multi_alpha.py`
  - 增加 task/control/child/event/SSE endpoints；
  - 更新错误映射；
  - 旧 endpoints 委托同一 service。
- `backend/main.py`
  - lifespan 启动/停止 orchestrator loop；
  - 执行 QE scoped schema/worker health preflight：缺 schema 时 FastAPI 仍正常启动，禁止回退到 daemon thread；只将 multi-alpha orchestrator 和相关写接口标记为 `multi_alpha_schema_unavailable`，返回结构化 503，并保持非 QE 模块及可兼容的历史只读接口可用。

### 18.2 新增内部模块（不是新平台版本）

- `backend/services/multi_alpha/repository.py`
- `backend/services/multi_alpha/orchestrator.py`
- `backend/services/multi_alpha/execution_adapter.py`
- `backend/services/multi_alpha/state_models.py`
- `backend/services/multi_alpha/health.py`
- `backend/services/quantevolver/qe_execution_reservation.py`
- `backend/migrations/multi_alpha_durable_orchestration_20260718.sql`
- `backend/migrations/qe_execution_reservation_20260719{,.preflight,.rollback}.sql`
- 对应 rollback/preflight/schema contract tests

拆分目的只是降低当前 2000+ 行 service 的职责耦合；公共入口和业务计算继续由现有模块提供。

### 18.3 QE Workspace owning repository 配套变更

在 `F:\Dev\RD-Agent-main\rdagent\app\api_endpoints\qe_evolution_api.py` 与对应测试中实现 submission receipt。该变更单独提交、部署到 WSL/远端节点并验证 OpenAPI/same-hash replay/different-hash 409；AIstock 不复制 QE Workspace server，也不提供旧 schema fallback。

## 19. Frontend File Plan / 前端逐文件方案

- `frontend/src/app/quantevolver/evolution/page.tsx`
  - 作为 single-alpha 与 multi-alpha combine 的规范入口；抽取共享 shell/list/dialog/actions，通过 task-type adapter 切换数据源，不改变单 Alpha UI 输出。
- `frontend/src/app/quantevolver/evolution/components/*`
  - 增加共享 adapter、runtime panel、status/action components；
  - 复用现有 style constants。
- `frontend/src/app/quantevolver/components/EvolutionTrajectory.tsx`
  - 泛化 data source adapter，不改变默认 single-alpha URL。
- `frontend/src/app/quantevolver/evolution/components/LoopDetailPanel.tsx`
  - 基于 capabilities 隐藏不适用操作；多 Alpha 不调用 single-alpha candidate API。
- `frontend/src/app/quantevolver/multi-alpha/combine-backtest/page.tsx`
  - 改为到规范 evolution workspace 的兼容重定向/薄委托，不保留独立 DOM、状态机或风格实现。
- `frontend/src/app/quantevolver/multi-alpha/combine-backtest/[taskKey]/page.tsx`
  - 保留旧 URL/查询参数并映射到共享详情 shell，不复制详情实现。
- `CombineRunOperationsPanel.tsx`
  - 增加 run control、child retry、reconcile；保留 archive/delete/scenario replay。
- 新增 `MultiAlphaCreateComposer.tsx`、`MultiAlphaChildGrid.tsx`、`multiAlphaEvolutionAdapter.ts`。

## 20. Implementation Plan / 实施顺序

### P0-1A：schema 与 repository

1. 交付 additive migration、comments、rollback/preflight。
2. 实现 task/run/child/attempt/event repository。
3. 实现 request/artifact identity、CAS、lease、fencing、row_version。
4. 实现历史 task/run 关联 dry-run/backfill/readback。

### P0-1B：execution adapter 与 orchestrator

1. 在 QE Workspace owning repository 实现 submission receipt，并在 WSL/远端节点验证服务端幂等合同。
2. 新增 `infra.qe_execution_reservation` migration/repository；容量检查、reservation INSERT 与 source claim 同一事务。
3. 拆分 remote executor 阶段，WSL/远端统一走携带 `submission_intent_hash` 的 `QEWorkspaceClient`。
4. 实现 immutable task identity/run scene separation、deterministic child plan、atomic artifacts 和显式状态迁移。
5. 实现 startup reconcile、deadline/wait 语义、parent finalization 和 post-terminal Archive pass。
6. 移除新 run 的 daemon thread 所有权，并把所有生产 QE Workspace submit 接入同一 coordinator。

P0-1B 的实现级从属设计为 `docs/architecture/multi_alpha_p0_1b_durable_execution_orchestrator_f2_design_20260719.md`。父蓝图继续作为范围、隔离和总体验收权威；从属设计通过其独立 Design Acceptance Index 细化 F-204、F-205、F-206、F-209、F-210、F-215、F-216、F-218 的文件、状态机、事务、重启和验证合同，不建立平行平台或第二套业务语义。

### P0-2：control 与 recovery

1. pause/resume/cancel repository + service + remote kill；legacy stop 委托 cancel 并保持现有终止语义。
2. child attempts 和三种 retry mode。
3. whole-run retry 兼容、legacy stale endpoint 限定。
4. API/MCP 同一 service adapter，结构化 reason codes。

### P0-3：共享 QE UI 与创建器

1. 先抽取单 Alpha共享组件并做无视觉/行为回归验证。
2. 扩展 data source adapter。
3. 实现完整 multi-alpha create composer 和多场景 runs。
4. 旧多 Alpha URL 改为规范页面的兼容重定向/薄委托。

### P0-4：child grid、日志和恢复展示

1. DB event API/SSE。
2. child/attempt grid 与详情 timeline。
3. pause/resume/cancel/retry/reconcile 操作。
4. backend restart UI E2E、legacy run 展示和 Archive/diagnostic 回归。

P0-1～P0-4 可拆为多个可审查 PR，但任何阶段只能报告其真实范围；不能在 P0-4 未完成时宣称整个基础底座完成。

## 21. Verification Plan / 验证方案

### 21.1 Business oracle

同一冻结 fixture/request 在旧同步执行与 durable 执行中必须满足：

- child plan、combined prediction hash、scheme weights、LOO identity 相同；
- Qlib config 中日期、TopK、initial cash、strategy params、成本相同；
- 成功结果 metrics 在浮点容差内一致；
- 结果表、Archive payload 和 StrategyPackage source identity 不改变业务含义。

### 21.2 Schema / Repository

- migration 连续执行两次 pg_catalog 无漂移；
- historical backfill dry-run/execute/readback 幂等；
- task/run/child/attempt/event FK、unique/check/comment 完整；
- 8 worker 并发 claim 只有一个获得相同 attempt；
- stale fencing token/row_version 写入被拒绝；
- lease 过期后新 worker 可 reconcile，旧 worker 不能提交；
- 每个状态 transition 与 event 在同一 transaction 中提交；event 写入失败时状态回滚且 API 不返回成功。
- reservation migration 连续执行两次无漂移，comments、active partial index、source/remote unique constraints 和 guarded rollback 完整。
- capacity fixture 验证 reservation INSERT + source claim 同一 advisory transaction，所有生产 submitter 共用 ledger，并严格遵守 WSL 2/远端 4；旧 active source 导入后来源明细可由 API/UI 核对。
- network unknown、deadline exceeded 和 lease expiry 不释放 reservation；权威 terminal/not-reserved 后恰好释放一次。

### 21.3 Orchestrator restart matrix

至少覆盖：

1. 后端在 remote submit 前退出；
2. 远端已接收、后端保存 response 前退出；
3. remote running 时重启；
4. remote completed、结果 ingest 前重启；
5. scheme result 写入后、parent finalize 前重启；
6. cancel 请求中重启；
7. remote API 暂时不可达后恢复；
8. 两个 backend worker 同时启动。

每种场景验证：服务端 receipt 保证不重复注册远端 execution、不丢成功结果、不伪造失败、最终状态可收敛、reservation/event/attempt lineage 完整。另在 RD-Agent owning repository 验证并发 same-hash POST 只执行一次、different-hash 409、receipt 跨服务重启恢复。

### 21.4 Lifecycle / Retry

- pause 不再派发新 child，在途 child 完成后 paused；
- resume 只继续剩余 child；
- cancel 调用 kill，保留成功 child；
- legacy stop 与 cancel 使用同一 service/远端 kill/终态聚合，不得表现为 pause；
- `backtest_only` 不重新组合；
- `results_only` 不启动回测；
- `rematerialize_and_backtest` 使用冻结源 identity；
- 缺 artifact、identity mismatch、remote unknown 均有稳定 reason code；
- whole-run retry 与 child retry lineage 不混淆。

### 21.5 API

- 所有新旧 route 的 success/error envelope；
- Idempotency-Key same/same 与 same/different；
- task/run/child/attempt capabilities；
- events cursor/SSE 断线续传；
- legacy task key 兼容；
- durable run 不被 stale-fail endpoint 误终态化。
- `run_async=false` wait 到期返回 202，连接断开不取消；task identity 允许不同 OOS/TopK/资金/baseline/timeout 场景 run。
- Archive enabled/disabled/duplicate/error 都由 durable event/read model 可见，补归档使用同一 event ID。

### 21.6 UI

- `/quantevolver/evolution` 是规范入口；single-alpha 与 multi-alpha 使用同一共享 shell，旧 multi-alpha URL 只做兼容映射；
- single-alpha 页面抽取前后在相同 viewport、浏览器和 fixture 下执行 Playwright screenshot/golden 对照，关键区域逐像素无非预期变化；
- multi-alpha 列表、创建器、详情、child grid 与同区域 single-alpha 截图对照，证明未新增视觉语言、颜色、字号、间距、圆角、阴影或 Badge 样式；
- 多 Alpha创建器完整 payload snapshot；
- 多场景 task 创建；
- task list/detail/trajectory/logs/diagnostics；
- child grid status、attempt、remote ID、errors、actions；
- backend restart 后从 reconciling 回到 running/completed；
- hidden page 停止刷新、visible 后按 event cursor 补齐；
- 不适用训练字段不显示 0/伪数据；
- 不调用 GPU telemetry；
- 旧 URL 继续访问。

### 21.7 Regression

- 当前 multi-alpha combine/service/remote/UI adapter 全部定向 tests；
- single-alpha QE task create/stop/resume/retry/fork/custom/append/log/trajectory；
- QE Archive multi-alpha handler；
- Prediction Store；
- frontend TypeScript/build 和 Playwright；
- `git diff --check`、F2 workflow validator、changed-file scope。

## 22. Risks / 风险与处置

| 风险 | 处置 |
|---|---|
| 双 worker或响应丢失重复提交 | DB claim/fencing + bind-before-submit + QE Workspace 原子 submission receipt；same-hash 不再次注册 execution |
| remote 404 语义不明 | loop 404 不作为未提交证明；receipt not_reserved 才允许同 identity/hash POST |
| 后端重启丢任务 | remote loop 独立运行；DB scanner 重启接管 |
| 子任务成功但 parent 失败 | child/attempt/result 独立持久化；parent partial_failed 聚合 |
| 节点满载或跨 QE 路径容量竞态 | 单一 reservation ledger；INSERT + source claim 先于 POST且同事务；queued/waiting_capacity，不失败 |
| 历史 task 的首条 default request 阻止其他场景 | immutable task identity 与 run defaults 分离，场景差异不创建新 task/不报冲突 |
| pause 被误解为远端冻结 | UI 明确 cooperative pause；in-flight 完成后暂停 |
| stop 被误实现为 pause | legacy stop 委托 cancel/kill；与当前单 Alpha 停止语义保持一致并做 E2E |
| cancel 不能确认 | cancelling/remote_state_unknown，持续 reconcile |
| results-only 读取错 workspace | result manifest + run/child/attempt/hash 校验 |
| 共享 UI 重构影响单 Alpha | 先做零行为抽取、同 viewport screenshot/golden Playwright，再接 multi-alpha；旧 URL 只做兼容映射 |
| 旧历史 run 无 child 映射 | 显式 legacy unavailable，不伪造 attempt |
| Archive capture 初始化失败被静默禁用 | post-terminal archive event + outbox-derived status；metrics 保留并以相同 event ID 幂等补归档 |
| execution deadline 把仍运行或晚到结果判失败 | transport/wait/deadline 分离；running/unknown 保持 reconciling，有效 late result 入库并标记 evidence |
| schema 未部署便重启 | FastAPI 保持可用；仅 multi-alpha worker/写接口结构化 503，禁止 daemon fallback，不影响非 QE 模块 |

## 23. Design Acceptance Index

| ID | 设计要求 |
|---|---|
| F-201 | 只在现有 combine-backtest/QE Workspace/QE UI 上增量实现，不创建新版本或平行平台。 |
| F-202 | first-class task/run/child/attempt/event 层级和稳定 identity；task immutable identity 与可变 run 场景参数分离。 |
| F-203 | PostgreSQL lease/fencing/row-version CAS 防止重复所有者和旧 worker 写入。 |
| F-204 | 远端 qe_task_id/qe_loop_id/submission intent 在提交阶段持久化，并由 QE Workspace receipt 保证响应丢失后不重复执行。 |
| F-205 | WSL/远端统一复用 QEWorkspaceClient；new async run 不由 subprocess/daemon thread 持有。 |
| F-206 | `infra.qe_execution_reservation` 覆盖所有生产 QE submit 来源；reservation INSERT/source claim 原子，WSL 2、远端 4，满载排队不失败。 |
| F-207 | pause/resume/cancel 语义明确；legacy stop 保持 cancel/kill 语义且不伪造远端状态。 |
| F-208 | child retry 支持 backtest_only/results_only/rematerialize_and_backtest，模式不静默互换。 |
| F-209 | 成功 child 和历史 attempt append-only 保留；`not_computable` 与技术失败分离；父状态只按结构化结果聚合，不判断研究价值。 |
| F-210 | DB event 与状态 transition 同事务、workspace/remote logs 可追溯，错误必须 API/UI 可见。 |
| F-211 | `/quantevolver/evolution` 为规范入口，UI 沿用单 Alpha QE 自动演进页面和共享组件，并以 screenshot/golden 证明不改变设计风格。 |
| F-212 | 创建器覆盖完整现有 request 和多场景 runs，不重新训练模型。 |
| F-213 | child/attempt grid 展示节点、远端 ID、状态、阶段、耗时、heartbeat、错误、制品和动作。 |
| F-214 | legacy run/task key/read APIs 兼容，历史回填不改指标。 |
| F-215 | 组合、权重、LOO、回测和 Archive 业务结果与现有实现 parity；Archive delivery 状态独立、可见、可补偿。 |
| F-216 | QE-only；schema/worker 不可用仅影响 multi-alpha 写接口，非 QE 模块零读写/零调用/零运行影响。 |
| F-217 | 不新增研究门禁、淘汰规则、人工审批或 promotion 审批；文档不得以 `APPROVED_BY_USER` 代替真实用户确认。 |
| F-218 | 完整 receipt/restart/reservation concurrency/timeout/Archive/control/retry/API/UI/DB 验证，禁止简化、静默 fallback 和伪成功。 |

## 24. Design Acceptance Matrix

本矩阵只表达“设计条目是否已完整定义”，不声称对应代码、DDL、测试或运行证据已经存在。`DESIGN_READY` 表示可进入实现，真实实现状态统一以第 26 节和未来代码 PR 的更新为准；只有用户明确逐项确认时才能写 `APPROVED_BY_USER`，本设计不使用该标记。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-201 | `backend/services/multi_alpha/{combine_backtest,remote_dispatch,combine_ui_adapter}.py`; `frontend/src/app/quantevolver/{evolution,multi-alpha/combine-backtest}` | `backend/tests/test_multi_alpha_combine_backtest.py`; `backend/tests/test_multi_alpha_combine_ui_adapter.py`; `rtk git diff --check` | DESIGN_VERIFIED | 无 |
| F-202 | `backend/migrations/multi_alpha_durable_orchestration_20260718{,.preflight,.rollback}.sql`; `backend/services/multi_alpha/{durable_models,durable_repository}.py`；P0-1B task identity compare 修订 | `backend/tests/multi_alpha/test_durable_schema.py`; `backend/tests/multi_alpha/test_durable_repository.py`; `backend/tests/multi_alpha/test_durable_submission.py::test_task_identity_allows_distinct_run_scenarios`；2026-07-19 P0-1A 生产 SQL/application preflight | P0_1A_PRODUCTION_VERIFIED_P0_1B_DESIGN_READY | 无 |
| F-203 | `MultiAlphaDurableRepository` claim/heartbeat/transition CAS，lease/fencing/row-version；lease 使用数据库真实时钟判定，过期 owner 在新 owner claim 前也立即失权 | `backend/tests/multi_alpha/test_durable_repository.py`; `backend/tests/multi_alpha/test_durable_repository_postgres.py::test_eight_workers_claim_once_event_rollback_and_stale_fencing` | P0_1A_PRODUCTION_VERIFIED | 无 |
| F-204 | execution adapter + attempt remote identity + QE Workspace submission receipt；详见 P0-1B 从属设计的 remote identity/receipt acceptance items | `backend/tests/multi_alpha/test_durable_orchestrator_restart.py`; `F:/Dev/RD-Agent-main/test/app/test_qe_evolution_submission_receipt.py` | P0_1B_REVIEWED_DESIGN_READY | 无 |
| F-205 | `QEWorkspaceClient`、remote dispatch refactor；详见 P0-1B 从属设计的统一执行 adapter/receipt acceptance items | `backend/tests/test_multi_alpha_remote_dispatch.py`; `backend/tests/multi_alpha/test_durable_execution_adapter.py` | P0_1B_REVIEWED_DESIGN_READY | 无 |
| F-206 | canonical reservation ledger + atomic source claim；详见 P0-1B 从属设计的 capacity/reservation acceptance items | `backend/tests/multi_alpha/test_durable_capacity.py`; `backend/migrations/qe_execution_reservation_20260719.preflight.sql` | P0_1B_REVIEWED_DESIGN_READY | 无 |
| F-207 | pause/resume/cancel service/router/UI + legacy stop compatibility | `backend/tests/multi_alpha/test_durable_control.py`; `frontend/tests/quantevolver/multi-alpha-control.spec.ts` | DESIGN_READY | 无 |
| F-208 | child attempts/retry APIs | `backend/tests/multi_alpha/test_durable_retry.py` | DESIGN_READY | 无 |
| F-209 | child result persistence、`not_computable` 和 deterministic aggregate rules | `backend/tests/multi_alpha/test_durable_aggregation.py` | DESIGN_READY | 无 |
| F-210 | `durable_repository.py` atomic state/event transaction；SSE/log adapter 留在 P0-4 | `backend/tests/multi_alpha/test_durable_repository.py::test_event_failure_rolls_back_the_state_transition` | DESIGN_READY | 无 |
| F-211 | canonical shared QE page components + visual golden | `frontend/tests/quantevolver/evolution-shared-shell.spec.ts`; `frontend/tests/quantevolver/evolution-visual-parity.spec.ts` | DESIGN_READY | 无 |
| F-212 | multi-alpha create composer | `frontend/tests/quantevolver/multi-alpha-create.spec.ts` | DESIGN_READY | 无 |
| F-213 | child grid/runtime panel | `frontend/tests/quantevolver/multi-alpha-child-grid.spec.ts` | DESIGN_READY | 无 |
| F-214 | `backend/services/multi_alpha/durable_backfill.py`; `scripts/backfill_multi_alpha_durable_tasks.py`；只扫描 `legacy_backfill`/未绑定历史 run，技术失败与数学不可计算分别映射为 `failed`/`not_computable` | `backend/tests/multi_alpha/test_durable_backfill.py`; `backend/tests/multi_alpha/test_durable_repository_postgres.py::test_historical_backfill_is_idempotent_and_preserves_metrics_status_reason`；生产 12 task/41 run/138 child readback | P0_1A_PRODUCTION_VERIFIED | 无 |
| F-215 | existing combiner/pred-backtest result parity + post-terminal Archive visibility/retry | `backend/tests/multi_alpha/test_durable_parity.py`; `backend/tests/multi_alpha/test_archive_health.py`; `backend/tests/test_multi_alpha_combine_backtest.py` | P0_1B_REVIEWED_DESIGN_READY | 无 |
| F-216 | P0-1A `MultiAlphaDurableRepository.preflight_schema()` 核对基础结果/durable 表；P0-1B reservation preflight 只新增并核对 `infra.qe_execution_reservation`，两者均保持 QE multi-alpha scoped | `backend/tests/multi_alpha/test_durable_schema.py::test_schema_contract_is_qe_multi_alpha_scoped`; P0-1B reservation schema contract tests；P0-1A 生产 preflight `ready=true` | P0_1A_PRODUCTION_VERIFIED_P0_1B_DESIGN_READY | 无 |
| F-217 | state/API/UI audit without research gates or claimed approval | `backend/tests/multi_alpha/test_durable_contract.py`; `frontend/tests/quantevolver/multi-alpha-no-approval.spec.ts` | DESIGN_VERIFIED | 无 |
| F-218 | full receipt/reservation/restart/timeout/Archive/control/API/UI validation matrix | `pytest backend/tests/multi_alpha/test_durable_contract.py`; `playwright test frontend/tests/quantevolver/multi-alpha-control.spec.ts`; `artifact: F:/Dev/RD-Agent-main/test/app/test_qe_evolution_submission_receipt.py`; `rtk python scripts/aistock_feature_workflow.py validate --design docs/architecture/multi_alpha_qe_evolution_foundation_f2_design_20260718.md --tier F2` | DESIGN_READY | 无 |

## 25. Rollout / Rollback

### 25.1 Rollout 顺序

1. 合入设计文档。
2. 实现 AIstock reservation migration/repository/orchestrator/control/UI，并在 RD-Agent owning repository 实现 submission receipt；两仓库代码合入、DDL、节点部署和运行激活分开记录。
3. 用户授权后对生产目标执行 reservation schema preflight、幂等 DDL、comments/readback；不在 DDL 前额外导出数据库。
4. 先部署并验证 WSL/远端 QE Workspace receipt OpenAPI、same-hash replay 和 different-hash 409，再允许 AIstock coordinated submit 激活；禁止旧 contract fallback。
5. 后端重启前确认无旧 daemon run 仍需当前进程持有，并把可识别的 active QE executions 导入 reservation ledger；无法唯一定位的节点保持 queue-only、证据可见。
6. 重启后运行 isolated API、worker status、legacy list/detail、创建一个最小 QE pred-backtest canary、后端重启恢复、child retry/results-only smoke。
7. UI 验证单 Alpha无回归、多 Alpha创建/控制/日志/恢复完整。

### 25.2 Rollback

- 代码回滚停止新 durable task 创建；不删除 task/run/child/attempt/event 历史。
- migration rollback 仅在确认无新表/列被运行数据使用时执行；正常回滚优先保留 additive schema。
- 已启动远端 loop 不因 backend code rollback 被杀；通过持久化 remote identity继续查询或人工取消。
- 旧 read APIs 和现有 scheme/LOO 表保持可用。

## 26. Production Gates / 实施事实（工作流兼容标题，不定义科研门禁）

| 项目 | 当前状态 |
|---|---|
| design | `P0_1A_PRODUCTION_VERIFIED_P0_1B_REVIEWED_DESIGN_READY_CODE_PENDING`；P0-1B 从属设计：`docs/architecture/multi_alpha_p0_1b_durable_execution_orchestrator_f2_design_20260719.md`，已补齐 receipt/reservation/task identity/Archive/timeout 合同；父蓝图 18/18、从属设计 32/32 F2 校验通过 |
| source code | P0-1A 已通过 PR #2464 合入 durable models/repository/backfill，并完成 BUG-767 的 lease、claim、canonical identity、legacy backfill、schema preflight、cancel/reason/error 语义修复；现有 combine-backtest 运行路径仍未切换 |
| migration | P0-1A migration 已于 2026-07-19 对生产 `127.0.0.1:5432/aistock` 应用并验证，SHA256 `0da061f4d9964958976d704101257895af6dec59ac1e4e765057cc7dfe521595`；P0-1B reservation migration 尚未实现或应用 |
| P0-1A validation | 隔离 PostgreSQL 16 临时容器验证 migration 连续执行两次无 schema 漂移、历史回填幂等且不扫描 first-class run、技术失败/不可计算分类准确、8 worker 单一 claim、event 失败整事务回滚、lease 过期 owner 在重新 claim 前被拒绝且新 owner claim 后 stale fencing 被拒绝、schema 类型/约束/索引/注释缺失均 fail-loud |
| BUG-767 | PR #2464 / close-sync PR #2467 已合入，GitHub issue #2459 已关闭；BUG JSON 的 `production_ddl_gate` 仍需后续元数据 close-sync，不影响已验证的生产 schema 事实 |
| production DB | 已创建 durable schema；历史回填 12 task、41/41 run、138 child（59 scheme、79 LOO），attempt/event 均为 0，保护摘要 `733d48413364658972bbef1be625b205e1eb191c5df8e9e0f2465d3bea4bffa4` 不变，readback 无 mismatch/orphan |
| backend/frontend runtime | 本次 DDL/DML 未重启服务；正式 combine-backtest 仍使用 daemon thread、进程内容量预留和 child ThreadPoolExecutor，P0-1B 尚未激活 |
| QE experiments | 本设计不创建、停止、恢复或修改实验 |
| non-QE impact | `NONE_REQUIRED` |
| research gates/approvals | `NONE_ADDED` |
| production_ddl_gate | `applied_and_verified`（仅指已完成的 P0-1A 生产事实）；BUG-767 跟踪 JSON尚待元数据同步 |
| P0-1B reservation DDL | 设计已定义，代码未实现；未来实现合入后仍需用户单独授权才可应用 |
| production_historical_backfill | `applied_and_verified`；没有伪造 attempt/event，没有修改历史指标、状态、reason、created_at 或 Archive 业务结果 |
| production_frontend_dependency_gate | `noop` |
| production_backend_dependency_gate | `noop` |

## 27. 设计结论

AIstock 不需要重新建设一个多 Alpha 平台。正确路线是把现有 combine-backtest 从“进程内可运行的 Tier-1 研究服务”提升为“数据库持久业务状态 + 共享 execution reservation + QE Workspace 服务端 receipt/统一执行 + 单 Alpha QE 同风格 UI”的完整演进底座。

P0-1～P0-4 完成后，股票截面 prediction 多腿组合、资金/TopK/持仓场景和后续板块轮动组合研究将主要通过配置、模型、因子和策略插件推进；只有出现新的决策层级、执行频率或资产语义时，才需要继续扩展通用接口。任何实现都必须保留现有研究结果、失败证据和业务公式，不得用可靠性缺口淘汰 Alpha 方向。
