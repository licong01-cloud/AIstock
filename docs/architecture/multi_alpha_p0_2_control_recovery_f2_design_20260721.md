# 多 Alpha P0-2 任务控制与子任务恢复 F2 详细设计

- 文档类型：F2 阶段从属实现级详细设计
- 父级权威：`docs/architecture/multi_alpha_qe_evolution_foundation_f2_design_20260718.md`
- 模块：QuantEvolver / Multi-Alpha combine-backtest / QE Workspace / PostgreSQL durable orchestration / QE UI / QE MCP
- 日期：2026-07-21；实施状态复核：2026-07-29
- 状态：`SOURCE_MERGED_PRODUCTION_DDL_RUNTIME_VERIFIED`
- 当前事实：P0-2 AIstock PR #2580 与 RD-Agent companion PR #6 已合入；P0-2 additive DDL 已在 DEV/生产应用并通过 preflight/readback；此前已完成真实 QE-only canary 与运行态验收。2026-07-29 用户再次重启 backend 后，`8001` OpenAPI 仍加载 control/recovery/child/attempt/event/log 路由，最新 Multi-Alpha run 为 `succeeded/completed`
- 唯一运行边界：QE-only；不得读写或调用 Selection、Advisory、Paper、模拟盘、QMT、StrategyPackage 运行链或其他非 QE 模块
- 科研约束：本设计不增加研究门禁、人工审批、准入、晋级、PASS/KILL/GO/STOP 或指标淘汰逻辑；缺失数据和制品只形成可见证据与获取建议，不淘汰研究方向
- 运行约束：本设计和后续实现不得自行启动、停止或重启 AIstock 后端；生产 DDL、依赖安装和运行激活均是独立授权事项

## 0. Source And Runtime Implementation Record / 源码与运行态实施记录（2026-07-29）

- 已实现：P0-2 command/cancel-delivery ledger、lease/fencing/CAS、pause/resume/cancel/reconcile/stop alias、精确 attempt cancel、terminal child successor recovery、三种 retry mode 的严格分流、RD-Agent typed kill receipt、实际环境与数据集身份回传、Archive v2 recovery readback、HTTP/MCP/UI 控制闭环。
- 兼容性修复：P0-2 DDL 未部署时，既有 P0-1B durable submission 保持原始 SQL/child manifest 形态继续运行；返回明确 `multi_alpha_p0_2_schema_unavailable` 基础设施证据，不把该事实解释为研究方向淘汰或隐藏性 fallback。当前部署已越过该兼容阶段，control/recovery entrypoint 仍保留 fail-loud schema readiness 合同。
- 已验证：AIstock 核心与 MCP 定向矩阵 `236 passed, 9 skipped`，附加 Archive/Feature-workflow/Strategy-package/Results-only 回归矩阵 `56 passed, 1 deselected`，RD-Agent typed receipt/identity tests `34 passed`；AIstock changed-file Ruff、RD-Agent 新增模块与 submission receipt 全量 Ruff、`qe_evolution_api.py` 新增行 Ruff 审计、两个仓库 Python compile、MCP manifest validation、F2 workflow validation、验收矩阵引用检查和变更前端文件 TypeScript compiler API 检查均通过。RD-Agent 既有 `qe_evolution_api.py` 全文件仍含与本功能无关的历史 Ruff 债务，本交付不将新增行审计表述成旧文件全量清零。`9 skipped` 为未配置显式测试 DSN 的可选 PostgreSQL 集成测试；新增的 zero-child PostgreSQL 生命周期用例已实现但未执行，没有创建测试库、没有执行 DDL、没有导出数据库。新增 Playwright 控制闭环用例已被现有 Playwright 配置成功发现（2 条场景），但本轮未启动前后端服务、未执行浏览器 E2E，因此不将源码发现表述为 Playwright 运行通过。
- 后续合入修复：BUG-850/851/856/857/858/859/860/864/865/867/872/878/881/882/883/887/889 已继续收紧 attempt/run identity、external dataset/workspace binding、reference/derived result ancestry、exact child plan、command/cancel transaction、heartbeat token 与 reservation terminal release；这些修复是 P0-2 合同闭环，不创建第二套恢复平台。
- 2026-07-29 运行态只读复核：用户重启后的 backend 于 12:44 +08:00 监听 `8001`，OpenAPI 有 47 条 Multi-Alpha 路径；最新 run `macb_453ca2d0c5b21b40_20240701_20260629_20260728T021052319863Z_00cf02ce` 为 `succeeded/completed`。该 run 的 execution identity evidence 仍明确缺少 dataset manifest/root 与 runtime lock/executor commit，故运行成功不等于 provenance 完整。
- 本次 BUG-904 仅更新文档；未执行 DDL、服务启停、实验、Archive 回填或数据库写入。

---

## 1. Background / 背景、结论与必须锁定的架构决策

P0-2 在现有 P0-1B durable run/child/attempt、QE Workspace receipt、共享 reservation、lease/fencing/CAS 和 Archive 之上增量实现，不创建“多 Alpha v2”，不复制组合算法，也不改变单 Alpha QE 的生命周期语义。

编码前锁定以下决策：

1. **暂停是 cooperative drain**：立即停止新的 plan/dispatch，不伪造远端 pause，不 kill 已提交 attempt；在途 attempt 继续 reconcile 和收结果，全部收口后 run 才进入 `paused`。
2. **取消是 durable intent + asynchronous reconcile**：API 先原子持久化取消意图，再由 orchestrator 精确终止远端 attempt；HTTP kill 成功、404 或网络异常都不是权威终态，不能据此提前释放 reservation 或写 `cancelled`。
3. **终态不可变**：P0-2 control/recovery 自动流程不得重新打开、覆盖或删除已经终态的 run、child、attempt、业务结果和 Archive；用户显式调用既有 terminal DELETE 是唯一例外，仍按原删除语义执行并删除该 run 范围内的 command/event，successor 必须在此前冻结自洽 lineage。
4. **终态 child 恢复使用 successor recovery run**：从终态 run 选择失败/取消 child 时，创建显式 `retry_of_run_id` 继承 run；只执行目标及由业务依赖图、retry mode 和代码身份共同确定的闭包，其余兼容的成功结果按来源 child 和制品 hash 引用复用。API 必须返回原/新 run、child、attempt 身份，不能把它伪装成原 child 内追加 attempt。
5. **原 run 只追加 results-only reference attempt**：仅当 parent 非终态、Archive 未捕获、child 稳定处于 `reconciling`、selected attempt 已 `succeeded`、业务结果尚未固化且不存在 active attempt 时，command worker 才可重新 collect/verify，并在一个事务中追加 terminal `reference_result` attempt、切换 selected attempt 和继续业务组装。failed/cancelled/not_computable/succeeded child 不重开；`backtest_only/rematerialize` 永不走 in-place。若 child 或 parent 已终态，等待来源 run 收口后使用 successor recovery。preview 与 execute 之间事实变化时返回显式 scope 冲突，不静默换拓扑。
6. **三种恢复模式严格分流**：`results_only`、`backtest_only`、`rematerialize_and_backtest` 不互相回退，不自动改成 full train，也不使用当前默认数据/节点替换冻结身份。
7. **恢复范围按依赖闭包计算**：用户选择一个 child，系统展示并持久化 `execute/reuse_result/recompute_derived/preserve_unavailable` 闭包；未被本轮选择且不属于依赖闭包的失败/取消 sibling 显式保留为 `not_recovered`，不静默扩大重跑范围或伪装成功。这只是结果一致性计算，不是研究审批。
8. **durable run stop alias 等价 cancel**：新增的 `/combine-backtest/runs/{run_id}/stop` 只作为同一 durable run cancel 的兼容别名；不得映射为 pause 或 DELETE。现有 `/experiments/{experiment_id}/stop` 与 single-Alpha stop 保持原实现；没有经过验证的 exact `experiment_id -> run_id` 映射时不得猜测、广播或跨系统转发 cancel。
9. **capability 是状态事实，不是门禁**：后端返回动作状态、制品证据和 reason code；UI 不隐藏研究方向。缺失制品时保留恢复模式和获取建议，执行请求显式返回当前缺口，不把候选方向标记为无价值。
10. **P0-2 UI 交付完整控制闭环但不复制 P0-4 页面**：现有详情页必须可执行 run/attempt control、child recovery、查看依赖闭包、重新发现 active/latest command 并在刷新后恢复进度；完整可排序 ChildGrid、共享页面壳和长时间线仍归 P0-4/P0-3。

## 2. 权威关系、范围与非目标

### 2.1 权威关系

- 父蓝图继续负责 QE-only 隔离、总体验收和 P0-1～P0-4 顺序。
- 本文细化父蓝图 `F-207`（pause/resume/cancel）与 `F-208`（child recovery），并补足控制幂等、终态恢复拓扑、重启竞态、API/MCP/UI 和验证合同。
- P0-1B 详细设计及其已合入实现继续拥有 submission receipt、reservation、lease/fencing、执行适配和 Archive delivery 底座。
- 现有 whole-run retry 继续是“按完整请求创建新 run”；P0-2 的 child-targeted recovery 与其并存，不互相冒充。

### 2.2 In Scope

1. run pause、resume、cancel、durable run stop alias、manual reconcile；
2. 单 attempt cancel 及其 child/parent 聚合语义；
3. 非终态 child append-only retry；
4. 终态 run 的 child-targeted successor recovery；
5. `results_only/backtest_only/rematerialize_and_backtest` 严格执行路径；
6. durable control command 幂等账本、claim/lease/fencing、逐 attempt cancel delivery、状态事实与事件；
7. orchestrator control/cancel/reconcile pass 和重启恢复；
8. 后端 capabilities、结构化 reason、children/attempt read API；
9. 现有多 Alpha 详情页的控制按钮、command 重新发现和轻量恢复对话框；
10. QE MCP 同源 read/control/recovery 工具；
11. additive migration、preflight、rollback 和针对性验证矩阵。

### 2.3 Non-goals

- 不修改模型、因子、标签、数据集、训练、组合权重、回测公式或研究结论。
- 不新增任何研究方向准入、晋级、淘汰、人工审批或 promotion 流程。
- 不因数据或制品暂缺而放弃方向；只记录缺口、来源和补取方法。
- 不改变现有 multi-Alpha experiment stop 或 single-Alpha `stop/resume/retry` 的既有实现或语义。
- 不把 running DELETE、legacy stale-fail 或 whole-run retry 当成 cancel/child retry。
- 不提前实现 P0-3 CreateComposer、共享 EvolutionTrajectory 重构或 P0-4 完整 ChildGrid/SSE 时间线。
- 不增加 GPU/显存/资源遥测，不调用 `nvidia-smi`，不改变模型训练资源策略。
- 不触及 Selection、Advisory、Paper、模拟盘、QMT、StrategyPackage、LocalSIM 或生产交易。
- 不在本设计 PR 执行 DDL、安装依赖、重启服务、创建或恢复 QE 实验。

## 3. 当前源码事实与缺口

| 领域 | 当前事实 | P0-2 缺口 |
|---|---|---|
| durable model | run/child/attempt 状态及三种 retry mode 已定义 | 没有 operator control/recovery service |
| repository | worker transition、lease/fencing/CAS、cancel claim policy、append-only attempt 校验已存在 | 没有 control command 幂等事务、pause/cancel 写入、recovery transaction |
| schema | run 已有 pause/cancel 字段，event 可存 control | 没有 command claim/lease/fencing、逐 attempt cancel delivery、idempotency/scope hash；没有跨 run recovery lineage |
| orchestrator | cycle 已有 planner、dispatch、reconcile、finalize、archive | 没有 control/cancel pass；finalizer 不能收口 pause/cancel |
| execution adapter | 可 materialize、submit、inspect、collect | 没有 kill 封装，三种 retry mode 尚未执行分流 |
| QE Workspace | `kill_loop()`、submission receipt、status/read result 已存在 | kill API 没有幂等 kill receipt；当前 owning service 会直接固化 `cancelled`，不能满足 completed-race 保留成功结果的合同 |
| API | submit/read/whole-run retry/delete/log/archive 已存在 | 没有 pause/resume/cancel/reconcile/child retry/attempt cancel |
| UI | `CombineRunOperationsPanel` 有 whole-run retry、scenario、archive、delete | 没有 durable controls、children/attempt evidence、恢复模式选择 |
| MCP | multi-alpha preview/run/result/list 已存在 | 没有控制、恢复和 child/attempt 查询工具 |

现有 terminal run/child 无出边，finalizer 只处理运行态；scheme/LOO 结果又以 run 内业务键唯一。直接“重开终态 child 并覆盖结果”会同时破坏 terminal event、唯一结果行和 Archive 证据。因此本文明确采用 successor recovery run，而不是修改结果表为多代版本或静默覆盖旧行。

## 4. 目标架构与组件所有权

```text
UI / MCP / HTTP API
        |
        v
DurableControlService                     # 唯一控制与恢复业务语义
        |
        +--> DurableRepository             # command/delivery claim/CAS/event/recovery transaction
        +--> RecoveryPlanner               # target + dependency closure
        +--> DurableOrchestrator            # control/cancel/reconcile/finalize passes
                     |
                     v
            DurableExecutionAdapter
                     |
                     v
            QEWorkspaceClient + submission/kill receipt/status/result
                     |
                     v
       infra.qe_execution_reservation       # P0-1B 权威容量事实
```

### 4.1 新增组件

- `backend/services/multi_alpha/durable_control.py`
  - control command canonicalization；
  - pause/resume/cancel/stop/reconcile；
  - attempt cancel；
  - child retry 与 successor recovery；
  - capability/evidence/reason 计算；
  - HTTP/MCP 共用错误模型。
- `backend/services/multi_alpha/durable_recovery.py`
  - 计算目标 child 的确定性依赖闭包；
  - 构建 successor request/recovery scope；
  - 校验来源 run/child/attempt/制品身份；
  - 生成 `execute/reuse_result/recompute_derived/preserve_unavailable` 计划。
- `backend/services/multi_alpha/durable_identity.py`
  - 从真实 dataset manifest、calendar/instruments/ST PIT、prediction、runtime、code/config/materializer/formula 内容生成 `multi_alpha_execution_identity_v1`；
  - canonicalize、验证 SHA-256，并返回 legacy 缺失证据清单；不得以路径相同冒充内容相同。

- RD-Agent QE Workspace owning service 增量：
  - 新增 typed kill-intent endpoint，不改写现有 `/kill` endpoint；
  - submission/status receipt 持久化并回显 `{pid,pgid,start_time_ticks}` 与 actual environment manifest identity；
  - 接受稳定 `kill_intent_hash/command_id/generation`；
  - 在 Loop workspace 内持久化 typed kill receipt；
  - same-intent 重放不重复发送信号，不同 intent 返回结构化冲突；
  - create/setup/pre-spawn/background/status/legacy kill/typed kill 等所有 writer 共用 per-loop 跨进程锁和 terminal CAS；pre-start cancel 阻止 Popen，只有确认进程因取消退出且不存在有效完成结果时才把 submission receipt 收口为 `cancelled`。

这些 AIstock 模块是现有 multi-alpha service 的内部实现，RD-Agent 变更只扩展现有 QE Workspace owning service；它们都不是新平台或第二套业务入口。

### 4.2 必须修改的现有组件

- `durable_repository.py`：control command/delivery claim、operator CAS、recovery transaction、reference/derived attempt、queued cancellation。
- `durable_orchestrator.py`：control/cancel pass、pause drain、cancel reconcile、recovery dispatch/finalize。
- `durable_plan.py`：补齐 dataset/runtime/materializer/business-formula identity，并为恢复闭包提供冻结身份。
- `durable_execution_adapter.py`：三种 retry 分流、source artifact verification、reference/derived result manifest。
- `qe_workspace_client.py`：新增 `kill_loop_typed(...)` 调用 typed kill-intent endpoint；现有两参数 `kill_loop(task_id, loop_id)` 方法、payload、endpoint 和返回行为原样保留给 experiment/single-Alpha stop。P0-2 禁止调用裸 kill 作为终态证据。
- `combine_backtest.py`：兼容 facade 委托；保留 whole-run retry 和终态 DELETE。
- `combine_ui_adapter.py`：完整 durable status、capabilities、child/attempt 摘要。
- `backend/routers/multi_alpha.py`：新增控制和恢复 API。
- `backend/mcp/modules/qe_archive.py`、`backend/mcp/tool_manifest.py`：同源 MCP 工具。
- `CombineRunOperationsPanel.tsx`：控制按钮和轻量恢复对话框。
- `[taskKey]/page.tsx`：仅补类型和刷新 wiring。

### 4.3 明确不修改

- `backend/routers/quantevolver_evolution.py`；
- `backend/services/quantevolver/qe_evolution_service.py`；
- `backend/routers/quantevolver.py` 的既有 `/experiments/{experiment_id}/stop`；
- 单 Alpha evolution 页面；
- `combiner.py`、`panels.py` 及现有研究公式；
- Selection、Advisory、Paper、QMT、StrategyPackage、LocalSIM；
- 已部署的 2026-07-18 / 2026-07-19 migration 文件。

## 5. Control Command 与幂等合同

所有写操作要求调用方提供 `Idempotency-Key`。该 key 是防重复写/重复 recovery 的请求身份，不是审批 token；它不能替代 command claim/lease/fencing 或逐 attempt kill delivery。HTTP UI 为一次用户动作生成一个 UUID，在请求结果未知时复用同一 key，并通过 run command 列表重新发现；MCP 调用方显式传入稳定 key。

### 5.1 Canonical payload

`payload_hash = SHA256(canonical_json({action, run_id, child_id, attempt_id, retry_mode, requested_scope}))`。durable run `stop` alias 在 canonicalization 前规范为 `action=cancel`，原始入口只作为 `requested_alias=stop` 审计字段，因此 stop/cancel 不会形成两套领域动作。

child recovery 另冻结：

```text
scope_hash = SHA256(canonical_json({
  source_run_id, target_child_key, retry_mode,
  dependency_plan,
  request_hash, roster_hash, dataset_identity,
  source_prediction_hashes, backtest_runtime_identity,
  source_materializer_code_identity,
  recovery_materializer_code_identity,
  business_formula_version
}))
successor_run_id = "macb_recovery_" + SHA256(
  canonical_json({source_run_id, command_id, scope_hash})
)
successor_child_id = make_child_id(successor_run_id, child_key)
successor_attempt_id = make_attempt_id(successor_child_id, attempt_no)
```

`command_id` 在首次接受 idempotency key 时确定并永久复用，因此 HTTP/MCP 重放得到相同 successor identities。preview 返回的 scope hash 与 execute 在来源锁内重算值不同即返回 `recovery_scope_stale`，不得用当前默认值补齐。

- 同 `run_id + Idempotency-Key + payload_hash`：返回第一次命令的同一 `command_id` 和当前状态；
- 同 `run_id + Idempotency-Key`、不同 hash：409 `control_idempotency_conflict`；
- 双击、HTTP retry、MCP retry、后端重启都不能产生第二条状态迁移、第二个 attempt、第二个 successor run 或并发 remote kill。

### 5.2 Command 生命周期

```text
accepted -> applying -> reconciling -> succeeded
    |          |             |
    +----------+-------------+-----> failed|superseded
```

- `accepted`：命令与用户意图已持久化；
- `applying`：本地状态事务已生效；
- `reconciling`：仍需权威远端事实；
- `succeeded`：命令目标已收口，不等价于研究结果 succeeded；对 child retry 表示 recovery plan 与必要本地制品已完整发布并可由 orchestrator 调度，不等待新 backtest 结束；
- `failed`：控制操作本身失败，保留结构化错误和恢复建议。
- `superseded`：仅用于同一 run 锁事务中被更高优先级的显式 control 替代，例如 cancel 替代尚未收口的 pause，或 resume 撤回 pause_requested；必须保存 successor command id 和 reason，不能返回伪成功。

各 action 的 command 收口点固定如下，避免 HTTP、MCP、UI 和重启 worker 各自解释：

| action | `succeeded` 条件 | 非终态/异常结果 |
|---|---|---|
| pause | run 已 `paused`；若 drain 时全部计划 child 已终态，则 run 正常完成/归档，command 以 `pause_raced_with_completion` 成功收口 | 仍有在途 attempt 时 `reconciling` |
| resume | run 已按 durable 事实 CAS 到 `preparing/running`，且原 artifact/attempt/remote identity 未被重建 | 状态冲突 `failed/control_state_conflict` |
| cancel | run 范围内每个 cancel delivery 均取得权威 terminal，结果已收集，parent 已聚合为 `cancelled` 或完成竞态下的 `succeeded` | remote unknown 保持 `reconciling` |
| attempt_cancel | 目标 attempt 权威 terminal，child 与 parent 的控制上下文已持久化；成功完成竞态的结果已收集 | remote unknown 保持 `reconciling` |
| reconcile | 一次有界 inspect/collect/reservation/aggregate pass 的观察结果和游标已持久化；目标仍 unknown 也以 `reconcile_observation_persisted` 完成本次命令，但对象保持 reconciling | repository/identity 错误才 `failed` |
| child_retry | 所需来源文件已验证并原子发布，in-place 或 successor DB 事务已完整提交，新 execution/reference/derived attempt 已可由 read model 读取；不等待远端 backtest 结束 | 缺失或冲突显式 `failed`，不改 mode |

### 5.3 原子顺序

1. 插入或读取 command；
2. `SELECT ... FOR UPDATE` 锁目标 run/child/attempt；
3. 校验对象身份和 row version；
4. command 通过 DB-time lease/fencing claim，更新 durable 对象状态并递增各自 `row_version/fencing_token`；
5. 同事务写 `event_type=control`；
6. run cancel 为每个 exact active attempt 原子创建或读取 cancel delivery；commit 后由持有 delivery lease 的 orchestrator 调用远端；
7. 通过 kill receipt、submission receipt、status/result 再执行 delivery/attempt/child/run/command CAS 收口。

API 请求线程不得直接 kill 后猜终态，也不得因 worker 暂时不可用拒绝持久化 pause/cancel 意图。

## 6. Additive Database Design

P0-2 新增独立 migration、preflight 和 rollback；不得改写已部署 migration。

### 6.1 `strategy_pkg.multi_alpha_combine_backtest_command`

| 列 | 合同 |
|---|---|
| `command_id` | `macmd_...` 主键 |
| `command_seq` | `BIGSERIAL UNIQUE`，仅作稳定分页游标，不参与业务身份 |
| `run_id` | 必填 FK，`ON DELETE CASCADE`，与现有终态 run 删除语义一致 |
| `child_id` / `attempt_id` | 可空 FK，必须属于 run，`ON DELETE SET NULL`；run 删除仍由 `run_id` cascade command |
| `action` | `pause/resume/cancel/reconcile/attempt_cancel/child_retry`；durable run stop alias 规范为 cancel |
| `idempotency_key` | 调用方稳定 key |
| `payload_hash` | canonical request SHA-256 |
| `request_json` | 原始规范化请求，不含 secret |
| `response_json` | 第一次接受响应及后续状态摘要 |
| `status` | `accepted/applying/reconciling/succeeded/failed/superseded` |
| `requested_by` | 审计身份；不承担审批 |
| `error_code/error_json` | 控制错误，不污染研究结果 error |
| `scope_hash` | preview/execute 与 target dependency closure 的 SHA-256；非 recovery action 可空 |
| `owner_id/row_version/fencing_token` | command 多实例 claim 与 stale-owner 拒绝 |
| `lease_expires_at/heartbeat_at` | DB 时钟控制的可恢复 lease |
| `delivery_attempt_count/next_delivery_at/last_delivery_at` | command 级消费节流摘要；逐 attempt 细节在 cancel delivery 表 |
| `staging_manifest_json/staging_manifest_hash` | successor 文件发布 crash window 的恢复身份；非 recovery action 为空 |
| `created_at/updated_at/completed_at` | 时间证据 |

约束与索引：

- `UNIQUE(run_id, idempotency_key)`；
- `CHECK(payload_hash ~ '^[0-9a-f]{64}$')`；
- `scope_hash/staging_manifest_hash` 为 NULL 或合法 SHA-256，staging JSON/hash 同空同非空；
- active command claim 索引 `(status, next_delivery_at, lease_expires_at, updated_at)`；
- 同一 run/action/target/scope 同时最多一个 active command；不同 idempotency key 并发请求由 partial unique 或同一 target advisory lock 返回现有 active command/409，不能创建第二个 retry；
- run control 在同一 run row lock 下串行：active cancel 时 pause/resume 返回 `control_cancel_in_progress`；cancel 可把 active pause/resume 标记为 `superseded`，resume 可撤回 pause_requested 并 supersede 原 pause command。任何替代都写双方 command/event identity；
- child 表增加 `UNIQUE(run_id, child_id)`；command 的 `(run_id, child_id)` 使用组合 FK。attempt target 同时保存 `child_id`，并以 `(child_id, attempt_id)` 组合 FK 保证三者同 scope；不能只依赖应用层猜测；
- command claim/heartbeat/yield/transition 全部使用 DB clock、row-version 和 fencing；lease 过期 owner 即使尚未被新 owner claim 也不能继续写。

### 6.2 `strategy_pkg.multi_alpha_combine_backtest_cancel_delivery`

run cancel 会为每个 exact active attempt 创建一条 delivery，而不是用一个 command 摘要冒充逐远端状态：

| 列 | 合同 |
|---|---|
| `delivery_id` | 确定性主键，来自 attempt + submission intent + node/task/loop，不包含 command 或尚未产生的 process identity |
| `originating_command_id/run_id/child_id/attempt_id` | 首次创建者与组合 FK，固定目标范围 |
| `node_id/qe_task_id/qe_loop_id/submission_intent_hash` | 从 attempt 冻结的 exact remote identity |
| `kill_target_key` | canonical attempt/submission/node/task/loop SHA-256；不包含 command/process identity，因此 run-cancel 与 attempt-cancel 对同一执行收敛 |
| `expected_process_identity_json/hash` | 可空；process 已创建时必须保存 submission/status receipt 返回的 `{pid,pgid,start_time_ticks}` 及 canonical hash，不能只保存 PID |
| `kill_intent_generation/kill_intent_hash` | generation 从 1 单调递增；hash 绑定 stable target、generation 及 expected process identity 或明确 `pre_process_start` 标记 |
| `status` | `pending/sending/reconciling/succeeded/failed`；remote unknown 不得写 succeeded/failed |
| `owner_id/row_version/fencing_token/lease_expires_at/heartbeat_at` | delivery claim 与 stale owner 拒绝 |
| `delivery_attempt_count/next_delivery_at/last_delivery_at` | 有界退避和重启节流 |
| `kill_receipt_json/remote_status/error_json` | typed owning-service 证据，不把 HTTP 200/404 当终态 |

新增 `strategy_pkg.multi_alpha_combine_backtest_command_delivery(command_id, delivery_id, created_at)` 关联表，`PRIMARY KEY(command_id, delivery_id)`。delivery 使用 `UNIQUE(kill_target_key)`，并增加 `UNIQUE(attempt_id) WHERE status IN ('pending','sending','reconciling')`。run-cancel 与 attempt-cancel 若命中同一 active attempt，只给后来的 command 增加 link 并复用同一 delivery/kill receipt；所有关联 command 随 delivery 终态分别收口，不能依赖 RD-Agent different-hash 409 才发现重复。

AIstock 在每次 delivery 前读取同一 submission/status receipt：若 owning service 仍为 `reserved/started` 且没有 process identity，则以 `pre_process_start` generation 请求原子 pre-start cancel；若已有 process，则把完整 process identity 固化到 delivery 后生成该 generation 的 intent。若 pre-start request 与 spawn 竞态而返回 `process_started`/incarnation mismatch，当前 generation 以“未发 signal”终止，repository 在同一 delivery 上保存 receipt、刷新 process identity、递增 generation 后重试；不得创建第二条 active delivery。只有 submission/kill receipt 或受同一 per-loop 锁保护的权威 status/result 证明 terminal 后才释放 reservation。

### 6.3 Run 增量

在 `multi_alpha_combine_backtest_run` 增加：

- `recovery_kind TEXT NULL CHECK IN ('child_targeted')`；
- `recovery_scope_json JSONB NOT NULL DEFAULT '{}'`；
- `recovery_scope_hash TEXT NULL`；
- `retry_of_run_id` 继续作为来源 run 权威 FK，不新增第二个 source-run 字段。

run status CHECK 与所有 terminal/read-model 集合增加 `partial_recovered`，仅允许 `recovery_kind='child_targeted'` 的 run 使用；普通 run 不得产生该状态。

普通 run 的 `recovery_kind IS NULL`、scope 为空且 hash 为空；successor recovery 必须同时具有 `retry_of_run_id`、`recovery_kind`、非空规范 scope 和合法 hash。DDL 使用交叉 CHECK 固化该四元组，不能只靠 service。

### 6.4 Child 增量

在 child 增加：

- `source_child_id TEXT NULL REFERENCES ...child(child_id) ON DELETE SET NULL`；
- `execution_disposition TEXT NOT NULL DEFAULT 'execute' CHECK IN ('execute','reuse_result','recompute_derived','preserve_unavailable')`；
- `source_lineage_json JSONB NULL` 与 `source_lineage_hash TEXT NULL`：冻结 source run/child/attempt、业务 key、raw result、artifact/result manifest、dataset/runtime/materializer/business-formula identity 和 SHA-256 身份；DDL 要求 JSON object、两列同空同非空及合法 hash；
- 扩展 `source_kind` 为 `runtime/legacy_result_backfill/recovery_reference`。

`reuse_result/recompute_derived` child 不创建伪远端执行，但必须创建显式 `reference_result/derived_result` attempt，以保持现有 `selected_attempt_id -> result_manifest_json` read model。该 attempt 的 `execution_kind` 明确表明没有远端 submit/reservation，冻结 source attempt 与已验证 result manifest；UI/API/Archive 不得把它显示为训练或回测执行。其成功状态由专用 repository 事务验证：来源 child/attempt 已成功、业务键一致、来源制品 URI/hash 一致，并写 lineage event。来源摘要冻结进 successor scope 和 lineage；即使来源 run 被显式删除，successor 仍自洽。

`preserve_unavailable` child 不创建 attempt，状态固定为新增 terminal `not_recovered`，允许来源状态为 `failed/cancelled/not_computable/not_recovered`。若来源已经是上一轮 successor 的 `not_recovered`，必须沿 frozen lineage 继续携带最初失败/取消/不可计算状态、reason、source child/attempt 和缺失制品，同时追加本轮 predecessor identity；不得用新的 `not_recovered` 覆盖或丢失原始失败证据。它只表示“不在本次 target/closure 内恢复”，不是研究淘汰或数学不可计算。child status CHECK、terminal 集合、API/UI/Archive 必须显式加入 `not_recovered`。

### 6.5 Attempt 增量

在 attempt 增加：

- `source_attempt_id TEXT NULL REFERENCES ...child_attempt(attempt_id) ON DELETE SET NULL`。
- `execution_kind TEXT NOT NULL DEFAULT 'remote_execution' CHECK IN ('remote_execution','reference_result','derived_result')`。
- `result_manifest_hash TEXT NULL`：reference/derived attempt 必填且为 SHA-256；remote attempt 在结果收口时写入，与 `result_manifest_json` canonical hash 一致。

约束调整为：

1. 普通 initial：`attempt_no=1, retry_mode=initial, retry_of_attempt_id/source_attempt_id IS NULL`；
2. 同 child retry：`attempt_no>1, retry_mode<>initial, retry_of_attempt_id=同 child 紧邻前 attempt, source_attempt_id IS NULL`；
3. successor remote 首 attempt：`attempt_no=1, execution_kind=remote_execution, retry_mode<>initial, retry_of_attempt_id IS NULL, source_attempt_id=来源 run attempt`；
4. successor 引用/派生 attempt：`attempt_no=1, execution_kind IN (reference_result,derived_result), retry_mode=results_only, source_attempt_id=来源 run attempt, status=succeeded`，remote identity/reservation 全空，result manifest 非空且 hash 已验证。

不得让 `retry_of_attempt_id` 跨 child；跨 run lineage 只用 `source_attempt_id`，避免破坏现有 immediate-lineage 含义。来源 remote identity、receipt、artifact/result manifest hash 还必须冻结到 successor child lineage 和 attempt manifest；可空 FK 只提供在线导航，不是唯一证据。

同一 child 只允许一个 active `remote_execution` attempt；migration 在加 partial unique 前必须 preflight 现有重复。reference/derived attempt 在创建事务内直接 terminal，不进入 dispatch/cancel claim。

### 6.6 Result 与 Archive

- 不给 scheme/LOO 结果表增加 generation，不覆盖来源 run 的唯一结果行。
- successor run 为 `reuse_result` child 在现有结果表中物化 exact 小型结构化结果行，并通过 reference attempt、frozen lineage、`source_child_id`、event 和 artifact hash 保留来源；baseline metrics 存入 reference attempt 的 verified `result_manifest_json`，继续走现有 selected-attempt read contract，不创建平行 metrics 表。
- `recompute_derived` 只以 lineage 中冻结且 hash 已验证的来源 raw metrics 和 successor 内已收口依赖重新计算派生差值；derived attempt 保存公式版本、输入 hash 和结果 manifest。实现必须调用或抽取现有 `DurableBusinessResultAssembler`/`delta` 权威公式，禁止复制一份近似公式。
- 目标 retry 真正需要的文件必须在 successor 进入可 dispatch 状态前使用现有 staging/atomic-publish 原语复制到 successor attempt workspace 并重新校验 SHA-256；不得让 successor 运行依赖一个以后可能被来源 run DELETE 清理的路径，也不得用未验证 hardlink/路径别名冒充独立制品。未被执行路径读取的成功 child 大文件不复制。
- successor Archive 是新 run 的独立不可变快照，handler/readback 必须包含 source run、recovery scope/hash、每个 child 的 disposition、source identity、reference/derived attempt 类型和 verified manifest。`qe_archive.run.logical_experiment_id` 继续等于 durable `run_id`，run 级 `attempt_no=1` 是“一个 durable run 对应一个 Archive run”的明确兼容语义，不表示 child 只有一次 attempt；真实 child attempt 历史必须按实际 `attempt_no/execution_kind/source_attempt_id` 写入下述 recovery attempt 快照，禁止由 run 级 `attempt_no` 猜测。
- successor 中存在 `not_recovered` child 且 target/closure 已成功时，新增 terminal run status `partial_recovered`，reason 为 `recovery_scope_completed_with_preserved_unavailable`；它表示本轮 scope 成功但完整来源仍有显式缺口，不冒充 `succeeded` 或 Alpha `partial_failed`。当前 `partial_recovered` run 已终态且永不重开；后续只能以它为来源创建下一 successor。只有新 successor 的全部 child 均已收口且不存在 `not_recovered` 时，新 successor 才可 `succeeded`。
- `cancelled` run 也必须进入 Archive evidence 路径并保留已成功 child；Archive 可标记整体研究结果未完整完成，但不能丢失 preserved results。
- 来源 Archive 永不改写；Archive delivery 也不因 successor 创建而回退或重放。
- 现有终态 run DELETE 保持可用：command 随 run 删除，recovery FK `SET NULL`，successor 的 frozen lineage 和已物化结果/必要文件继续自洽。仅当来源 run 存在 `applying` 中且尚未完成必要文件复制的 child-retry command 时，DELETE 在 workspace quarantine 前显式返回 409 `recovery_source_copy_in_progress`；复制发布完成或命令失败收口后立即恢复原删除能力。这是防止并发损坏的瞬时互斥，不是研究门禁。

“终态不可变/证据不删除”约束的是 P0-2 自动控制与恢复流程；用户显式调用现有 terminal DELETE 仍按原语义删除该 run 及 command/event。successor 必须在来源可删除前冻结 originating command id、source identities、manifest hashes 和必要文件，因此不能依赖 cascade 后仍存在的 command FK。rollback 检测到任何 command、delivery、recovery run 或新增 lineage 数据时必须拒绝 destructive rollback，只允许保留 additive schema 的代码回退。

Archive 不能只扩展 handler 常量而遗漏数据库和 readback。P0-2 新增独立 `qe_archive_multi_alpha_p0_2_recovery_20260721` migration/preflight/guarded rollback，并将现有 Archive contract 升级为 `multi_alpha_combine_completed_v2`：

1. `qe_archive.run.status`、`qe_archive.multi_alpha_run.status/logical_status` 的 CHECK 与 repository terminal filter 显式增加 `cancelled/partial_recovered`；旧 `v1` event 仍可读，新的 P0-2 run 一律发出 `v2` payload。
2. `qe_archive.multi_alpha_run` 增加 `archive_schema_version/retry_of_run_id/recovery_kind/recovery_scope_json/recovery_scope_hash/execution_identity_json/execution_identity_hash`；JSON/hash 同空同非空，hash 必须验证 canonical JSON。`retry_of_run_id` 是冻结字符串身份，不建立会因来源 DELETE 级联的 Archive FK。
3. 新增 `qe_archive.multi_alpha_recovery_child`，主键 `(run_id, child_id)`，保存 `child_key/kind/status/execution_disposition/selected_attempt_id/source_child_id/source_lineage_json/hash/input_manifest_json/prediction_artifact_uri/hash`；新增 `qe_archive.multi_alpha_recovery_attempt`，显式保存 `run_id/child_id/attempt_id`，主键 `(run_id, attempt_id)`，并以 `(run_id, child_id)` 组合 FK 指向 recovery child，保存实际 `attempt_no/retry_mode/execution_kind/status/source_attempt_id/artifact_manifest_json/result_manifest_json/hash`。attempt 另加 `UNIQUE(run_id, child_id, attempt_no)` 与 `UNIQUE(run_id, child_id, attempt_id)`；recovery child 的 `(run_id, child_id, selected_attempt_id)` 组合 FK 引用后一唯一键，确保 selected attempt 属于同一 child，`not_recovered` 的 selected attempt 为 NULL。这里只保存可重评、可追溯的小型结构化快照和 URI/hash，不复制 prediction、position、trade 或其他大文件。
4. handler `fetch_multi_alpha_combine_run()` 必须一次读取 run、children、attempts、scheme、LOO；`archive_multi_alpha_bundle()` 在一个 Archive transaction 中写入 header/source/child/attempt/result，任何一类写入失败整体回滚。Repository list/detail/readback 与 backfill status filter 同时认识 `cancelled/partial_recovered`，不得因整体非成功而漏掉 preserved child result。
5. `cancelled` 归档保留已成功 child、selected/reference/derived attempt 和现有 scheme/LOO 行；`partial_recovered` 归档同时保留 `not_recovered` child 及其缺失证据。Archive research-valid 字段只陈述完整性状态，不承担研究淘汰、审批或是否继续实验的判定。
6. preflight 必须验证旧 Archive 状态分布、同 run child/attempt 唯一性、JSON/hash、孤儿和 v1 readback；guarded rollback 在出现 v2 event、新状态或 recovery child/attempt 行时拒绝破坏性回退。历史 v1 Archive 不伪造 child attempt；只有来源 durable 表存在可验证证据时，显式 backfill v2 关联。

### 6.7 RD-Agent `qe_kill_receipt_v1`

owning service 新增 `POST /tasks/{task_id}/loops/{loop_id}/kill-intents`，请求包含 `command_id/kill_intent_generation/kill_intent_hash/expected_submission_intent_hash/expected_process_identity`；`expected_process_identity` 只能是完整 `{pid,pgid,start_time_ticks}` 或显式 `null + expected_phase=pre_process_start`。receipt 在 Loop workspace 的独立 `.kill_receipts/` 下原子保存：

- `schema_version/command_id/kill_intent_hash`；
- `task_id/loop_id/expected_submission_intent_hash/kill_intent_generation`；
- `process_identity={pid,pgid,start_time_ticks}`，其中 start time 来自 owning OS 的不可复用进程出生身份，不能只相信可复用 PID；
- `status=requested|signal_sent|reconciling|completed|cancelled|failed`；
- `signal_attempt_count/signal_sent_at`；
- `process_observation/result_observation/submission_receipt_status`；
- `terminal_reason/signal_sent`，其中 pre-start、incarnation mismatch、process-started race 均必须明确记录；
- `created_at/updated_at/completed_at/error`。

submission receipt/status API 必须把现有仅有的 `pid` 扩展为完整 `process_identity={pid,pgid,start_time_ticks}`，并在持有同一 per-loop 跨进程锁时完成 `Popen -> 读取/校验 process identity -> receipt running`；对外 receipt 回显该身份。后台 runner 在实际 Popen 前也必须在同一锁内复核 receipt：若 typed/legacy cancel 已把 `reserved/started` 原子收口为 `status=cancelled, terminal_reason=cancelled_before_process_start`，则不创建进程并正常退出；若 Popen 已在锁内完成，cancel 只能拿到并核对完整 process identity 后再发 signal。这样 `reserved/started` 且无进程的取消不需要虚构 PID，也没有“已取消但随后仍 spawn”的窗口。

typed endpoint 在发送任何信号前必须核对当前 submission receipt 的 intent hash、当前 PID/PGID/start-time 与请求 expected identity 完全一致；不一致返回 `kill_execution_incarnation_mismatch` 并明确 `signal_sent=false/current_process_identity`，绝不对复用 PID 发信号。同一 loop 的 create/workspace-setup failure、pre-spawn/start transition、background completed、background failed、health/status reconciler、legacy `/kill` 与 typed kill 等所有 terminal/status writer 必须共用一个 per-loop state transaction/跨进程 lock 和同一 terminal CAS helper；现有 legacy `/kill` 只保持签名/响应兼容，不能继续绕过 helper 直接写 `status.txt` 或 receipt。typed endpoint 先在锁内写 `requested` 并观察是否已 completed，释放锁后发送必要信号，再重新加锁核对进程退出、有效 result artifact 和 submission receipt：有效 completed 永远优先；只有确认取消导致退出且没有有效完成结果才写 `cancelled`。SIGKILL 发出后必须重新观察，不得仅以“信号已发送”写 terminal。receipt 必须回显 expected/current submission intent 与 process identity。same `kill_intent_hash` 返回现有 receipt，不重复 signal；different hash 对同一 active cancellation 返回 409；只有上一 generation 已以 `signal_sent=false + incarnation_mismatch/process_started` 终止时，才允许同一 delivery 的下一 generation 绑定新观察到的 process identity。transport 断开后 AIstock 用该 receipt 恢复，不从 HTTP 状态码猜终态。

现有 `POST .../kill` 与 `QEWorkspaceClient.kill_loop(task_id, loop_id)` 完全不改签名、请求或响应，继续服务已有 experiment/single-Alpha 调用；P0-2 只使用新增 `kill_loop_typed(...)`。直接 contract test 必须证明三个现有两参数调用无需改动且行为一致，typed endpoint 的 receipt 语义不会渗入旧路径。

## 7. Run / Child / Attempt 状态机

### 7.1 Pause / Resume

```text
queued/preparing/running
        -> pause_requested
        -> paused
        -> preparing|running
```

- pause 接受 `queued/preparing/running`；当前原子 materialization 可以完成发布，但不得开始下一 child 或 remote submit。
- 提交线性化点是同一数据库事务中的 `attempt queued -> submitting + reservation/source claim`。事务前到达的 pause/cancel 必须使该事务失败且不得 POST；事务提交后该 attempt 已属于“在途”，即使 HTTP 尚未发出，dispatcher 仍可完成 exact-intent POST，随后 pause 允许其自然完成、cancel 通过 delivery 协调终止。设计不宣称数据库事务能与外部 HTTP 原子提交。
- planner 在每个 child materialization 开始前和 staging publish/child queued 前复核 parent 状态与 fencing。控制在 materialization 中到达时允许本次 staging 写完，但 publish 前必须丢弃/隔离 staging 并正常 yield；不得记录为 Alpha 技术失败或发布未挂载 artifact。
- `pause_requested` 中允许 resume 撤回；恢复目标由 durable 事实确定：尚无 child plan 回 `preparing`，已有 plan 回 `running`。
- `paused` 不释放已终态结果，也不重建 artifact/remote identity。
- 在途 attempt 仍 active 时状态保持 `pause_requested`，UI 显示 `remaining_active`。
- drain 后若仍存在未执行 child，run 进入 `paused`；若全部计划 child 已权威终态，则直接执行正常 parent finalization 和 Archive，command 以 `pause_raced_with_completion` 收口，不能留下“已完成但 paused”的僵尸 run。

#### 7.1.1 Control acceptance linearization clarification (BUG-882)

Run-level `pause` / `cancel` acceptance and the corresponding parent status
transition are one database transaction.  A successful API response therefore
means the parent is already `pause_requested` / `cancel_requested`; it does not
wait for `apply_one_local_command()` to expose that intent.  The transition
clears the current planner lease and increments its fencing token so an
in-flight materializer may finish private staging work but cannot publish a
child, create/claim a dispatchable attempt, or issue a new remote POST.

The control worker remains responsible for pause drain, queued-attempt
cancellation, typed remote-cancel delivery, reconciliation, and terminal
finalization.  This clarification changes only the acceptance-to-worker race;
it does not infer a remote terminal state and does not alter any research
result or direction.

Resume preserves the existing durable-plan decision: both `pause_requested`
and `paused` may return to `preparing` when planning is incomplete or to
`running` when the child plan already exists.  The PostgreSQL lifecycle test
covers `pause -> paused -> preparing -> cancel_requested -> cancelled` without
requiring a fabricated child or remote result.

### 7.2 Cancel

```text
run: queued/preparing/running/pause_requested/paused
       -> cancel_requested -> cancelling -> cancelled|succeeded

child: pending/materializing/queued -> cancelled
       running/reconciling -> cancel_requested -> cancelling -> cancelled|succeeded|failed

attempt: queued 且无 receipt -> cancelled
         submitted/running/reconciling
              -> reconciling(phase=kill_requested)
              -> cancelled|succeeded|failed
```

收口规则：

- 从未被 QE Workspace 接受的 queued attempt 可本地取消并释放 reservation；
- 已绑定 receipt 或 remote identity 的 attempt 必须 inspect；
- kill 404/超时/网络失败只写 `remote_cancel_state_unknown`，保持 reservation；
- remote `cancelled` 才写 cancelled；
- remote `completed` 竞态正常 collect，成功结果必须保留；
- 只要 accepted operator cancel 导致任一计划 child 未完成，无论是否已有成功结果，run 均为 `cancelled`；progress/reason 分别保存 `successful_child_count`、`preserved_results`、`cancelled_scope` 和取消前已存在的技术失败；不得把控制取消聚合为 Alpha `partial_failed`；
- cancel 到达前全部结果已经成功：run `succeeded`，control outcome `cancel_raced_with_completion`。

durable run stop route 在进入 service 前规范为 cancel，返回同一 command schema、状态迁移和 reason；不得保存第二套 stop 状态或调用 DELETE。现有 experiment/single-Alpha stop 不进入该映射。

attempt 暂不增加 `cancelling` 枚举，使用现有 `reconciling + phase=kill_requested`；逐 attempt delivery 承担 kill 的 sending/reconcile 状态。attempt-level cancel 只终止目标 active remote execution：若 remote completed 竞态成功则保留结果；若取消生效，child 记录 operator-cancel context。若该取消导致 run roster 不完整，parent 最终为 `cancelled`；其他无关 child 继续收口，不把 attempt cancel 偷换为 run 广播 cancel。

### 7.3 Manual reconcile

manual reconcile 只执行：receipt/status inspect、result collect、reservation reconcile、parent aggregate 和 Archive delivery check。它不得 materialize、新建 attempt、申请新 reservation 或提交远端 execution。

### 7.4 Legacy stale-fail

现有 stale-fail 只能处理没有 durable identity 的 legacy row。若 run 已有 `task_id/request_hash`，接口返回 `durable_run_requires_reconciliation`，不得按 heartbeat 年龄把 durable run 改成 failed。

## 8. Child Retry 与 Successor Recovery

### 8.1 拓扑选择

后端先计算并返回显式 topology：

- `append_results_reference_in_place`：parent 非终态、Archive 未捕获、child 稳定为 `reconciling`、selected attempt=`succeeded`、无 active attempt且业务结果未固化；command worker 先完成 exact result collect/verify，再在一个事务中追加 terminal reference attempt、切换 selected attempt 并继续 business assemble；不创建 queued execution attempt；
- `successor_recovery_run`：source parent 与 target child 已终态；Archive 是否已捕获不改变来源不可变性。
- source parent 尚未终态但 target child 已终态时，不重开 child，也不从尚在变化的来源创建 successor；API 返回 `recovery_source_run_nonterminal`、当前 active children 和继续 reconcile 的建议。来源终态后同一研究方向仍可恢复，不写淘汰或准入状态。

调用方不得指定一个不符合事实的 topology；返回值必须包含选择依据。服务也不得在请求后静默换 topology：preview 和 execute 使用同一 scope hash，事实发生变化时返回 409 `recovery_scope_stale`，调用方刷新后重试。

### 8.2 Successor recovery 创建与原子发布

1. 先持久化并 claim `child_retry/applying` command，写入预分配 successor identities、scope hash 和 staging manifest；现有 DELETE 在 workspace quarantine 前检查该 active command；
2. 锁来源 run，验证已终态且属于同一 task，读取冻结 request、roster、child plan、source attempt、结果和 artifact identity；
3. 按 target、retry mode、数据/运行/代码身份和业务依赖图计算 closure；同 target/scope 的不同 key 并发请求由 active-command uniqueness 收敛为一个；
4. 在 successor staging workspace 复制执行真正需要的来源文件，使用 resolved-path containment 限定在 QE workspace/CAS root，拒绝 symlink escape、path traversal、hardlink identity 冒充，并逐文件校验 SHA-256；
5. 使用 P0-1B atomic publish 把 staging 切换为预分配的正式 successor workspace。此时数据库尚无 successor run/attempt，因此不存在 dispatch；
6. 单一数据库事务验证最终 workspace manifest/hash，创建 `retry_of_run_id=source_run_id` 的新 run、全部 successor children、remote/reference/derived attempts、exact/recomputed 结果行、lineage events，并把 command 置为 `succeeded`；remote attempt 只在该事务内首次成为 queued；
7. commit 后由现有 orchestrator 调度 `execute` attempt。崩溃若发生在第 5～6 步之间，command recoverer 根据同一 final manifest/hash 幂等完成第 6 步；不同内容显式 `recovery_artifact_publish_conflict`，不覆盖。

任一步冲突都不能暴露可 dispatch 的半成品。文件发布前失败只清理本 command 的 staging；文件发布后、DB 创建前失败保留 command 与 final manifest 供重启续做，不创建 failed successor 假对象。DB 事务提交后 successor 已完整可读/可调度，不再存在“DB 已提交但文件未发布”的窗口。command 收口后来源 run DELETE 不再受该命令阻挡。

### 8.3 依赖闭包

下表只是同一 source/recovery code identity 下的最小示例，不是硬编码闭包：

| 用户目标 | `execute` | `recompute_derived` | `reuse_result` | `preserve_unavailable` |
|---|---|---|---|---|
| baseline | baseline | 所有 scheme 的 `vs_baseline_*` 派生值 | compatible scheme raw metrics、全部 compatible LOO raw/derived | 与闭包无关的其他失败/取消 child |
| scheme | 目标 scheme | 同 weighting scheme 的 LOO marginal/delta | compatible 其他 scheme/LOO、baseline | 与闭包无关的其他失败/取消 child |
| LOO | 目标 LOO | 目标 LOO 对 full scheme 的 marginal/delta | compatible baseline、所有 scheme、其他 LOO | 与闭包无关的其他失败/取消 child |

RecoveryPlanner 必须从现有业务公式权威生成有向依赖图，并把以下 identity 纳入 scope hash：target child、retry mode、request/roster、dataset、source prediction、backtest runtime/template、materializer code、business formula version。只要 source/recovery code identity 不同，所有受影响的 scheme/LOO/raw/derived 节点必须扩展为 execute/recompute；不能复用旧代码 raw result 后计算新代码 marginal。若用户要研究跨版本差异，应创建普通新 research run/scene 并明确比较身份，不能冒充 recovery。

### 8.4 `multi_alpha_execution_identity_v1`

冻结身份不能只是路径或字段名。P0-2 对新 initial execution 在 materialization 前生成 canonical identity，并写入 child input manifest、attempt artifact manifest 和 submission intent；远端 POST 前再次校验：

```text
dataset = {
  deployment_snapshot_id,
  dataset_manifest_sha256,
  cutoff_trade_date,
  qlib_calendar_sha256,
  qlib_instruments_sha256,
  st_pit_snapshot_id,
  st_pit_manifest_sha256,
  resolved_node_id,
  resolved_data_root_uri
}
prediction_sources = [{leg_id, seed_run_id, artifact_uri, artifact_sha256}]
runtime = {
  qlib_runtime_template_sha256,
  conda_environment_lock_sha256,
  execution_environment_snapshot_id,
  execution_environment_manifest_sha256,
  executor_code_commit,
  executor_file_set_sha256,
  backtest_config_sha256
}
materializer = {
  aistock_commit,
  planner_version,
  combiner_file_sha256,
  panel_builder_file_sha256,
  materializer_file_set_sha256
}
business_formula = {
  formula_version,
  assembler_file_sha256,
  delta_formula_sha256
}
execution_identity_hash = SHA256(canonical_json(all_above))
```

`resolved_data_root_uri` 只用于定位，不能证明内容；`deployment_snapshot_id + dataset_manifest_sha256` 才是权威数据内容身份。数据发布流程必须提供覆盖 Qlib bin、calendar、instruments、ST PIT 和本次 backtest 实际读取资产的不可变 manifest；同一路径内容更新后 hash 不同，不能称 exact retry。若某资产已有受控 immutable snapshot/CAS id，可用该 id 与其 manifest hash，避免重复存储大文件 hash 清单。

`conda_environment_lock_sha256` 只表示期望环境，不能单独证明执行节点的实际安装内容。WSL/远端 owning service 在 worker deployment/startup 生成并缓存实际环境 manifest（至少包括 Python implementation/version、installed package name/version/build/source、Qlib package/commit、OS/container image identity 和执行器依赖文件 hash），返回不可变 `execution_environment_snapshot_id + execution_environment_manifest_sha256`；submission receipt 必须绑定并回显这两个值，AIstock 在 POST 前后的 identity 校验中验证一致。环境 manifest 只在 deployment/startup 或环境变更时生成，不做 per-loop 高频命令探测，也不采集 GPU/显存资源遥测。

P0-2 不能为既有 P0-1B run 伪造从未捕获的身份：

- 可从已固化 manifest/artifact 内容确定性回算的字段，记录 `identity_source=historical_manifest_reconstruction` 和证据 hash；
- 无法证明 dataset/runtime/code 内容的，返回 `legacy_execution_identity_incomplete` 及缺失清单；`results_only` 若 exact remote result identity 完整仍可执行，`backtest_only/rematerialize` 不得仅凭相同路径继续；
- 缺失身份不淘汰研究方向，保留补取 manifest、使用普通新 run 重现实验或多证据对照的建议。

### 8.5 成功 child 与新研究场景

child recovery 面向失败、取消或结果落库失败的执行恢复。成功 child 不在原 run 上重做；修复后的 materializer 仅在冻结输入可证明且 dependency closure 扩展完整时用于 `rematerialize_and_backtest`。用户主动更换数据、模型、腿、参数或研究公式时创建普通新 run/scene，并显式保留 lineage。这是身份语义，不是研究方向门禁。

## 9. 三种恢复模式

### 9.1 `results_only`

- 来源：`source_attempt_id` 或同 child `retry_of_attempt_id` 的 exact QE task/loop、receipt 和 result artifact；
- 行为：由已 claim 的 command worker 先 collect/verify；成功后在同一 DB 事务创建 terminal `reference_result` attempt、更新 selected attempt 并进入现有 business assemble。它不创建 queued/reconciling execution attempt，也不进入 remote dispatch claim；
- 必须：零 remote run POST、零新 execution reservation、零 materialization；
- 缺失：返回 `results_only_artifact_missing` 或 `results_only_remote_identity_missing`，同时保留来源、缺失清单和重新获取建议；不自动改成其他模式。

### 9.2 `backtest_only`

- 来源：exact combined prediction URI/hash、冻结 backtest config、dataset identity、Qlib/runtime template 与 code digest；
- 行为：创建 attempt-specific workspace/config，申请新 reservation，提交新 Qlib backtest；
- 必须：不重新组合、不训练、不替换 prediction；
- 缺失或 hash 不符：`backtest_prediction_missing` / `backtest_prediction_hash_mismatch` / `backtest_identity_missing`；不自动 rematerialize。

### 9.3 `rematerialize_and_backtest`

- 来源：冻结 request、roster、source prediction identity、因子/数据集身份、runtime/materializer/business-formula identity 和 child input manifest；
- 行为：用当前明确记录的 materializer code identity 重建 prediction，发布 attempt-specific CAS artifact，再提交 Qlib backtest；
- 必须：记录 source 与 recovery code identity；允许修复后的确定性代码产生新 hash，但不得把它宣称为原 hash；
- 禁止：读取更新后的默认数据、当前默认节点、其他 run 制品或自动 full train；
- 缺失：`rematerialize_source_identity_missing`，保留可补取来源。

### 9.4 模式证据展示

capability response 对每个模式同时返回：

- `state_allowed`：当前对象状态是否符合该操作语义；
- `evidence_status`：`complete/partial/missing/unknown`；
- `missing_evidence[]`；
- `acquisition_suggestions[]`；
- `will_submit_remote`、`will_reserve_slot`、`will_materialize`。

UI 不因 `partial/missing` 隐藏研究方向。若用户仍执行无法完成的模式，API 明确返回 409 和同一证据包；该结果不写研究淘汰状态。

## 10. Orchestrator Passes 与并发

P0-2 后每轮顺序：

```text
control pass
  -> planner pass
  -> dispatch pass
  -> cancel pass
  -> reconcile pass
  -> pause/finalize pass
  -> archive pass
```

关键不变量：

1. control pass 只消费 durable command；后端重启后可继续。
2. dispatch 线性化事务必须复核 parent 状态；pause/cancel 在线性化事务前提交才禁止 remote POST。线性化事务已提交的 attempt 属于在途，允许完成同一 exact-intent POST，再按 pause/cancel 语义收口，不能遗留无 receipt 的 `submitting + reservation`。
3. cancel pass 按 exact `node_id/qe_task_id/qe_loop_id` kill，不广播 stop。
4. 每个 remote kill intent 有持久化 cancel delivery 与 owning-service kill receipt；delivery claim/lease/fencing 和 `next_delivery_at` 控制重试，不能每个轮询周期重复 kill。
5. reconcile 对 active/unknown reservation 保持占位；只有权威终态释放。
6. stale owner 的 fencing token 失效后不能写回 attempt 或 command。
7. 多实例 orchestrator 通过 command/delivery/attempt 的 SKIP LOCKED + lease/fencing/CAS 消费；HTTP 响应丢失后用 typed kill receipt 恢复，不重复发送信号。
8. recovery run 使用现有共享 capacity；不创建第二套并发计数器。
9. BUG-793 已排除的 terminal-parent stale group 不得因 recovery 引用重新占用容量；只有 successor 的 active attempt 占位。
10. manual reconcile 不进入 dispatch 分支。

## 11. API Contracts

路由沿用现有 multi-alpha router 前缀：

```text
POST /multi-alpha/combine-backtest/runs/{run_id}/pause
POST /multi-alpha/combine-backtest/runs/{run_id}/resume
POST /multi-alpha/combine-backtest/runs/{run_id}/cancel
POST /multi-alpha/combine-backtest/runs/{run_id}/stop
POST /multi-alpha/combine-backtest/runs/{run_id}/reconcile
GET  /multi-alpha/combine-backtest/runs/{run_id}/children
GET  /multi-alpha/combine-backtest/runs/{run_id}/commands?after_command_seq=...
GET  /multi-alpha/combine-backtest/children/{child_id}
GET  /multi-alpha/combine-backtest/children/{child_id}/attempts
POST /multi-alpha/combine-backtest/children/{child_id}/retry/preview
POST /multi-alpha/combine-backtest/children/{child_id}/retry
POST /multi-alpha/combine-backtest/attempts/{attempt_id}/cancel
GET  /multi-alpha/combine-backtest/commands/{command_id}
```

### 11.1 Mutation request

- Header：`Idempotency-Key` 必填；
- Body：retry 包含 `retry_mode` 和 preview 返回的 `scope_hash`；不接受客户端提供 `requested_by`。当前 router 没有 user auth/RBAC，因此 HTTP 记录固定可信 transport principal `multi_alpha_http_api`，MCP 记录 `qe_multi_alpha_mcp`；未来若平台已有认证上下文可附加 display actor，但不得在 P0-2 新建 auth middleware、角色、RBAC 或审批，也不得让 actor 决定 capability；
- Response：`command_id/action/status/run/child/attempt/source_identity/successor_identity/capabilities/evidence/reason`。

### 11.2 HTTP 状态

- 200：已完成或同 key 幂等重放；
- 202：durable intent 已接受，等待 orchestrator reconcile；
- 404：对象不存在；
- 409：状态冲突、scope 过期、idempotency 冲突、制品当前不可执行；
- 422：非法 mode/payload；
- 503：只有 repository/DB 本身无法持久化时使用。worker 或 QE Workspace 暂时异常不妨碍先接受 pause/cancel intent。

### 11.3 Reason codes

至少稳定支持：

- `control_idempotency_conflict`
- `control_state_conflict`
- `control_cancel_in_progress`
- `control_superseded`
- `recovery_scope_stale`
- `durable_run_requires_reconciliation`
- `remote_cancel_state_unknown`
- `cancel_raced_with_completion`
- `cancelled_with_preserved_success`
- `results_only_artifact_missing`
- `results_only_remote_identity_missing`
- `backtest_prediction_missing`
- `legacy_execution_identity_incomplete`
- `kill_execution_incarnation_mismatch`
- `cancelled_before_process_start`
- `kill_process_started_race`
- `recovery_scope_completed_with_preserved_unavailable`
- `archive_recovery_snapshot_incomplete`
- `backtest_prediction_hash_mismatch`
- `backtest_identity_missing`
- `rematerialize_source_identity_missing`
- `source_lineage_mismatch`
- `recovery_source_run_nonterminal`
- `recovery_active_attempt_conflict`
- `recovery_active_command_conflict`
- `recovery_artifact_publish_conflict`
- `kill_receipt_conflict`

错误必须包含可机器读取 context 和恢复建议，不把 control error 写成 Alpha 研究失败。

## 12. MCP Contracts

MCP 与 HTTP 调用同一 `DurableControlService`，不复制状态判断。MCP server 直接使用 service adapter 并把 tool input 的稳定 `idempotency_key` 传入 canonical command，不通过全局 `AIstockApiClient` 注入 header，因此不修改 `backend/mcp/common.py`，也不影响其他 MCP 模块：

- read-only：run controls/capabilities、children、attempts、command status；
- mutation：pause、resume、cancel/stop、reconcile、child retry、attempt cancel。

MCP mutation 必须传稳定 `idempotency_key` 并返回同一 command payload。不得新增 feature-specific “确认 token”、人工审批表或研究准入状态；若 MCP 网关已有全局危险操作提示，只保留通用传输层行为，不进入 P0-2 领域模型。

## 13. UI Design

### 13.1 Existing panel 增量

在 `CombineRunOperationsPanel` 增加：

- `暂停`：显示 cooperative drain 说明和 remaining active；
- `恢复`：显示恢复目标状态；
- `终止`：明确“停止新提交，并异步终止在途 QE attempt”；
- `立即对账`：只 inspect/collect，不启动执行；
- `恢复子任务`：打开轻量 child/attempt 对话框。
- `取消 attempt`：只对后端返回 `can_cancel_attempt=true` 的 active remote execution 显示，展示 exact node/task/loop 和不会广播到其他 child 的说明。

按钮由后端 `state_allowed` 驱动；evidence 缺失不隐藏模式，展示缺失与补取建议。UI 为每次动作生成并缓存一个 idempotency key，网络结果未知或刷新后先从 run 的 `active_commands/latest_commands` 重新发现，不生成第二个命令。现有 whole-run retry、scenario、archive、delete 保留原语义。

### 13.2 轻量恢复对话框

展示：

- child kind/key/status/selected attempt；
- 来源 run/child/attempt；
- 三模式的执行行为与证据；
- preview dependency closure；
- topology：in-place 或 successor recovery；
- 预计 remote submit/reservation/materialization；
- command 和 successor identity。

P0-2 不实现 P0-4 的完整可排序 ChildGrid、长时间线或 SSE 组件。操作完成后复用现有 `loadDetail(newRunId)` 刷新。

### 13.3 Refresh / restart

run detail 固定返回有界 `active_commands` 和 `latest_commands` 摘要；完整列表通过 cursor API 分页。UI 刷新后从 API 重建 command/run/child/attempt 状态，不依赖浏览器内存；`pause_requested/cancelling/reconciling` 必须显示真实进度、每个 cancel delivery 和 successor identity，不能只显示旧页面的 `running`。

## 14. File Plan

### 14.1 新增

- `docs/architecture/multi_alpha_p0_2_control_recovery_f2_design_20260721.md`
- `backend/services/multi_alpha/durable_control.py`
- `backend/services/multi_alpha/durable_recovery.py`
- `backend/services/multi_alpha/durable_identity.py`
- `backend/migrations/multi_alpha_p0_2_control_recovery_20260721.sql`
- `backend/migrations/multi_alpha_p0_2_control_recovery_20260721.preflight.sql`
- `backend/migrations/multi_alpha_p0_2_control_recovery_20260721.rollback.sql`
- `backend/migrations/qe_archive_multi_alpha_p0_2_recovery_20260721.sql`
- `backend/migrations/qe_archive_multi_alpha_p0_2_recovery_20260721.preflight.sql`
- `backend/migrations/qe_archive_multi_alpha_p0_2_recovery_20260721.rollback.sql`
- `backend/tests/multi_alpha/test_durable_control.py`
- `backend/tests/multi_alpha/test_durable_retry.py`
- `backend/tests/qe_archive/test_multi_alpha_recovery_archive.py`
- `frontend/tests/quantevolver/multi-alpha-control.spec.ts`
- `frontend/tests/quantevolver/multi-alpha-no-approval.spec.ts`
- RD-Agent `rdagent/app/api_endpoints/qe_environment_identity.py`、`test/app/test_qe_runtime_environment_identity.py` 与 `test/app/test_qe_evolution_kill_receipt.py`。

### 14.2 修改

- `backend/services/multi_alpha/durable_models.py`
- `backend/services/multi_alpha/durable_plan.py`
- `backend/services/multi_alpha/durable_repository.py`
- `backend/services/multi_alpha/durable_orchestrator.py`
- `backend/services/multi_alpha/durable_execution_adapter.py`
- `backend/services/multi_alpha/combine_backtest.py`
- `backend/services/multi_alpha/combine_ui_adapter.py`
- `backend/services/multi_alpha/__init__.py`
- `backend/services/quantevolver/qe_workspace_client.py`
- `backend/services/qe_archive/handlers/multi_alpha_combine_archive_handler.py`
- `backend/services/qe_archive/multi_alpha_provenance.py`
- `backend/services/qe_archive/repository.py`
- `backend/services/qe_archive/models.py`
- `backend/routers/multi_alpha.py`
- `backend/mcp/modules/qe_archive.py`
- `backend/mcp/tool_manifest.py`
- `frontend/src/app/quantevolver/multi-alpha/combine-backtest/components/CombineRunOperationsPanel.tsx`
- `frontend/src/app/quantevolver/multi-alpha/combine-backtest/[taskKey]/page.tsx`
- 相关 manifest/inventory/gateway/isolated tests。
- RD-Agent owning repository：`rdagent/app/api_endpoints/qe_evolution_api.py`、`rdagent/app/api_endpoints/qe_submission_receipt.py`、worker deployment/startup environment manifest 与 typed kill-receipt 模块；AIstock 与 RD-Agent PR、部署和运行证据分别记录。

## 15. Implementation Plan / 实施方案与顺序

### P0-2A：Schema 与 repository primitives

1. additive command/recovery 与 Archive v2 migration、preflight、guarded rollback；
2. control command/cancel-delivery claim、lease/fencing、idempotency transaction；
3. operator CAS、queued cancel、active attempt/command uniqueness、recovery plan persistence；
4. `multi_alpha_execution_identity_v1` canonical builder 与内容指纹验证；
5. successor reference child/result copy 与 Archive child/attempt snapshot invariants；
6. PostgreSQL concurrency/schema/readback tests。

### P0-2B：Control service 与 orchestrator

1. pause/resume/cancel/durable-run-stop/reconcile service；
2. planner publish 与 dispatch submission linearization 修复；
3. cancel pass、pause drain 和 finalization；
4. AIstock client + RD-Agent typed kill receipt/locked completed-race 实现；
5. restart/fencing/kill race tests。

### P0-2C：Retry execution paths

1. in-place attempt append 与 successor transaction；
2. results-only zero-submit path；
3. backtest-only exact artifact path；
4. rematerialize frozen-input path；
5. reference/derived attempt read model、dependency-derived recompute 和 Archive v2 manifest/readback；
6. `cancelled/partial_recovered/not_recovered` Archive、v1 compatibility 与可验证历史关联回填。

### P0-2D：API / MCP / UI

1. router + capability/evidence/read APIs；
2. MCP same-service tools；
3. existing panel controls + lightweight recovery dialog；
4. command rediscovery、attempt cancel、refresh/restart/API/MCP/UI E2E。

### P0-2E：Compliance 与交付

1. F2 validator；
2. DESIGN-COMPLIANCE-001 item-by-item review；
3. targeted + expanded regression；
4. PR、语义审计和合入；
5. 合入后才分别申请 DDL、依赖和运行激活授权。

## 16. Verification Plan

### 16.1 Static / pure

- migration/rollback/preflight schema contract，包括 command/delivery claim 与 process-identity/generation 字段、recovery 四元组、lineage/hash、active command/attempt uniqueness、Archive v2 状态/child/attempt composite FK/snapshot 与 guarded rollback；
- state transition、canonical payload hash、reason code、capability/evidence；
- dependency closure determinism；
- no auto mode fallback；
- changed-file Ruff/compile/type/lint。

### 16.2 PostgreSQL 16

- 同 key/同 payload并发只生成一个 command/attempt/successor run；不同 key 对同 target/scope 也只能形成一个 active command/attempt；
- 同 key/异 payload 409；
- pause/cancel 与 dispatch 并发只允许一边通过；
- terminal source rows、result rows和既有 Archive business rows 不被 recovery 自动流程覆盖；
- recovery reference/hash/composite FK/unique/preflight/guarded rollback；
- Archive `cancelled/partial_recovered` CHECK、v2 JSON/hash、child/attempt uniqueness、v1 compatibility、transactional readback 与 guarded rollback；
- stale owner fencing 后不能写回；
- 测试数据零残留。

### 16.3 Fake QE Workspace

- pause 不 kill、停止新 submit；
- cancel queued、kill success、kill 404、transport unknown、remote still active；
- reserved/started 无 process identity 时 pre-start cancel 阻止后续 Popen；Popen race 返回 process-started evidence 后同一 delivery 递增 generation，不生成第二 delivery；
- kill/completed race 保留成功结果；
- `results_only` POST=0、reservation=0；
- `backtest_only` combine/train=0；
- `rematerialize` 只读冻结 source identity；
- source/recovery code identity 不同会扩展 dependency closure，禁止混用旧 raw result；
- 相同期望 lock 但实际 installed environment manifest 不同，execution identity 必须不同且 receipt 校验失败；同一 deployment snapshot 重放返回相同 manifest hash，不执行 per-loop 高频探测；
- reference/derived attempt 不 POST、不 reservation，baseline/read model/Archive 仍可读取 exact metrics；
- 两个以上失败 sibling 的 targeted recovery 只执行 closure，其余形成 `not_recovered/preserve_unavailable`，run 收口为 `partial_recovered` 并可继续创建下一 successor；
- 每种缺失证据返回稳定 reason，零静默 fallback。

RD-Agent owning-service contract 另行验证：现有三组两参数 `kill_loop(task_id, loop_id)` caller 不变；same kill-intent 重放不重复 signal、different intent 冲突、同一 attempt 的 run-cancel/attempt-cancel 共用 delivery、create/setup failure/pre-spawn/background/status/legacy kill/typed kill 全部 terminal writer 共锁/CAS、kill/completed 竞态以有效完成结果优先、process incarnation 不匹配绝不发信号、pre-start cancel 不 spawn、SIGTERM/SIGKILL 后重新观察进程/result/status、submission receipt 与 kill receipt 不分叉。

### 16.4 Restart / multi-instance

- backend 在 command accept、远端 kill、receipt 保存、result collect、successor 创建各 crash window 重启；
- paused/cancelling/reconciling 状态不丢；
- remote submit/kill 不重复；
- command/cancel delivery lease takeover 后 stale owner 不能写回；
- 文件已 publish、successor DB 尚未创建时重启可按 manifest/hash 幂等续做，且从未暴露 queued 半成品；
- results-only 在 collect/verify crash window 不留下 queued/reference 半成品，恢复后仍保持零 remote submit/reservation；
- 多 orchestrator 实例只消费一次；
- reservation 直到权威终态才释放。

### 16.5 API / MCP / UI

- 所有 endpoint 的 200/202/404/409/422/503；
- Idempotency-Key header 传递；
- stop 与 cancel 返回同一 command 语义；
- UI 显示 remaining active、unknown、dependency closure、source/successor identity；
- 刷新后通过 run active/latest command 重新发现进度，不依赖旧浏览器内存；
- 每个 action 的 command terminal 条件与 attempt-cancel 非广播语义；
- MCP 与 HTTP 返回一致 reason/capabilities。
- Archive list/detail/backfill 可发现 `cancelled/partial_recovered`，并完整读回 preserved result、`not_recovered` child、真实 child attempt history 和 execution identity；旧 v1 Archive 仍可读。

### 16.6 Zero-regression / isolation

- single-Alpha stop/resume/retry tests 原样通过；
- whole-run retry、scenario、archive、delete 原样通过；
- P0-1B receipt/reservation/capacity/restart tests 原样通过；
- BUG-786 `trust_env=False`、BUG-793 terminal-parent capacity 语义不回退；
- Selection/Advisory/Paper/QMT/StrategyPackage/LocalSIM route、schema、import 零变化；
- 现有 multi-Alpha experiment stop 与 single-Alpha stop 原样通过；`test_multi_alpha_promotion.py` 证明 recovery run 不自动触发 promotion，普通 promotion 行为不变；
- MCP 不修改全局 `AIstockApiClient`，无 auth/RBAC/role/approval 新依赖；
- successor source path 仅限 QE workspace/CAS root，拒绝 traversal、symlink escape 和未验证 hardlink；
- 无 GPU/显存遥测调用。

### 16.7 非生产真实 canary

源码测试完成后，在现有 DEV PostgreSQL 的任务专属测试数据/事务范围内，并在非生产 WSL/远端 QE Workspace 各运行最小 canary：cancel → typed kill receipt → status/result reconcile → reservation release，并在 kill 后、AIstock receipt 保存前重启开发端口 backend 验证接管。不得为此新建测试数据库，也不得操作生产 `8001`。canary 只验证基础设施，不对 Alpha、模型或研究方向作准入/淘汰判断；生产 DDL、部署和服务重启仍需独立明确授权。

## 17. Design Acceptance Index

| ID | 验收合同 |
|---|---|
| F-401 | P0-2 只在父蓝图、现有 combine-backtest、P0-1B durable orchestration 和 QE Workspace 上增量实现。 |
| F-402 | 唯一边界为 QE-only；不新增研究门禁、审批、晋级、淘汰或 metric-driven capability。 |
| F-403 | 所有控制命令具有持久化 Idempotency-Key、payload/scope hash、claim/lease/fencing/CAS、event 和可重启状态。 |
| F-404 | pause 停止新 dispatch、不中断在途 attempt，drain 后才 paused。 |
| F-405 | resume 从 durable 事实恢复，不重建既有 artifact、attempt 或 remote identity。 |
| F-406 | cancel 先持久化意图，远端权威终态前不释放 reservation、不伪造 cancelled。 |
| F-407 | durable run stop 精确委托 cancel；现有 multi-Alpha experiment stop、single-Alpha stop 与 DELETE 语义不变。 |
| F-408 | 终态 run/child/result/Archive 不可变；终态 child 恢复创建显式 successor recovery run。 |
| F-409 | 非终态 child retry 只追加 attempt，旧 attempt 与业务证据不可覆盖。 |
| F-410 | RecoveryPlanner 按业务依赖图及 mode/code identity 持久化 execute/reuse_result/recompute_derived/preserve_unavailable 闭包。 |
| F-411 | `results_only` 为零 remote run POST、零 reservation、零 materialization。 |
| F-412 | `backtest_only` 只使用 exact prediction URI/hash 和冻结 backtest identity。 |
| F-413 | `rematerialize_and_backtest` 只使用冻结输入并记录 source/recovery code identity。 |
| F-414 | 三种 retry mode 不互换、不 fallback；缺失证据保留并给出补取建议，不淘汰研究方向。 |
| F-415 | BUG-785 receipt/reservation/fencing 和 P0-1B restart contract 不回退。 |
| F-416 | BUG-786 transport 和 BUG-793 capacity 语义不回退。 |
| F-417 | API/MCP/UI 使用同一 service、同一 capability/evidence/reason contract。 |
| F-418 | UI 增量复用现有 panel，不提前复制 P0-3/P0-4 页面架构。 |
| F-419 | PostgreSQL、Fake Workspace、restart/race、API/MCP/UI 和 zero-regression 矩阵覆盖关键语义。 |
| F-420 | DDL、依赖、服务重启和运行激活均保持独立授权；设计/源码合入不等于生产激活。 |
| F-421 | command 与逐 attempt cancel delivery 均具备 DB-time claim/lease/fencing/CAS、process-identity generation、退避、重启接管和 active-target 唯一性。 |
| F-422 | RD-Agent owning service 提供 process-incarnation-bound typed kill receipt；所有 terminal writer 共用 per-loop lock/CAS，pre-start cancel 不 spawn，completed/cancel 竞态不吞成功结果。 |
| F-423 | successor 文件先原子发布、DB 后一次性可见；任何 crash window 都不暴露可 dispatch 半成品。 |
| F-424 | reference/derived attempt 保持 selected-attempt/read-model/baseline/Archive 合同，同时明确不代表远端执行。 |
| F-425 | recovery scope 包含 dataset/runtime/materializer/business-formula identity，依赖闭包随 mode/code identity 扩展，不混用不兼容结果。 |
| F-426 | planner publish 与 dispatcher 的数据库线性化点明确；pause/cancel 竞态不伪造跨 DB/HTTP 原子性。 |
| F-427 | 每种 command 的收口点、run command 重新发现、稳定 UI/MCP idempotency 和 attempt-cancel 非广播语义完整。 |
| F-428 | 不新增 auth/RBAC/审批；durable stop 与现有 experiment/single-Alpha stop 边界精确；MCP 不修改全局 client。 |
| F-429 | Archive v2 的 schema、handler、repository、list/detail/backfill 同时支持 cancelled/partial_recovered、recovery child/attempt 快照和 v1 兼容读取。 |
| F-430 | targeted recovery 对未选失败 sibling 显式保存 preserve_unavailable/not_recovered；本轮闭包成功以 partial_recovered 收口并可继续恢复。 |

## 18. Design Acceptance Matrix / 设计验收矩阵

本矩阵的 `VERIFIED_SOURCE` 表示实现、源码级测试和静态验证已完成；它不表示 DDL、部署、服务重启、真实 QE canary 或生产激活已经执行，也不形成任何研究准入、审批或淘汰状态。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-207 | §5、§7、§10～§13；父蓝图 run control | `backend/tests/multi_alpha/test_durable_control.py`；`frontend/tests/multi-alpha-combine-backtest.spec.ts` | VERIFIED_SOURCE | none |
| F-208 | §6、§8、§9；父蓝图 child recovery | `backend/tests/multi_alpha/test_durable_retry.py` | VERIFIED_SOURCE | none |
| F-401 | §1～§4 | `backend/tests/multi_alpha/test_durable_contract.py` | VERIFIED_SOURCE | none |
| F-402 | §1、§2.3、§20.4 | `backend/tests/multi_alpha/test_durable_contract.py`；`frontend/tests/multi-alpha-combine-backtest.spec.ts` | VERIFIED_SOURCE | none |
| F-403 | §5、§6.1 | `backend/tests/multi_alpha/test_durable_control.py`；`backend/tests/multi_alpha/test_durable_repository_postgres.py` | VERIFIED_SOURCE | none |
| F-404 | §7.1、§10 | `backend/tests/multi_alpha/test_durable_control.py` | VERIFIED_SOURCE | none |
| F-405 | §7.1、§10 | `backend/tests/multi_alpha/test_durable_control.py`；`backend/tests/multi_alpha/test_durable_orchestrator_restart.py` | VERIFIED_SOURCE | none |
| F-406 | §7.2、§10 | `backend/tests/multi_alpha/test_durable_control.py`；`backend/tests/multi_alpha/test_durable_orchestrator_restart.py` | VERIFIED_SOURCE | none |
| F-407 | §7.2、§11 | `backend/tests/multi_alpha/test_durable_control.py`；`backend/tests/unified_engine/test_qe_stop_task.py` | VERIFIED_SOURCE | none |
| F-408 | §6、§8 | `backend/tests/multi_alpha/test_durable_retry.py`；`backend/tests/multi_alpha/test_archive_health.py` | VERIFIED_SOURCE | none |
| F-409 | §6.4、§8.1 | `backend/tests/multi_alpha/test_durable_retry.py`；`backend/tests/multi_alpha/test_durable_repository.py` | VERIFIED_SOURCE | none |
| F-410 | §8.2、§8.3 | `backend/tests/multi_alpha/test_durable_retry.py` | VERIFIED_SOURCE | none |
| F-411 | §9.1 | `backend/tests/multi_alpha/test_durable_retry.py`；`backend/tests/unified_engine/test_qe_results_only_retry.py` | VERIFIED_SOURCE | none |
| F-412 | §9.2 | `backend/tests/multi_alpha/test_durable_retry.py`；`backend/tests/multi_alpha/test_durable_execution_adapter.py` | VERIFIED_SOURCE | none |
| F-413 | §9.3 | `backend/tests/multi_alpha/test_durable_retry.py`；`backend/tests/multi_alpha/test_durable_execution_adapter.py` | VERIFIED_SOURCE | none |
| F-414 | §9.4、§20.2、§20.4 | `backend/tests/multi_alpha/test_durable_retry.py`；`backend/tests/multi_alpha/test_durable_contract.py` | VERIFIED_SOURCE | none |
| F-415 | §10、§16.6 | `backend/tests/multi_alpha/test_qe_submission_coordinator.py`；`backend/tests/multi_alpha/test_durable_capacity.py` | VERIFIED_SOURCE | none |
| F-416 | §10、§16.6 | `backend/tests/multi_alpha/test_active_execution_import.py`；`backend/tests/multi_alpha/test_durable_contract.py` | VERIFIED_SOURCE | none |
| F-417 | §11～§13 | `backend/tests/multi_alpha/test_durable_control.py`；`backend/tests/mcp/test_qe_archive_module.py`；`frontend/tests/multi-alpha-combine-backtest.spec.ts` | VERIFIED_SOURCE | none |
| F-418 | §13、§14 | `frontend/tests/multi-alpha-combine-backtest.spec.ts` | VERIFIED_SOURCE | none |
| F-419 | §16 | `backend/tests/multi_alpha/test_durable_repository_postgres.py::test_p0_2_zero_child_pause_resume_cancel_lifecycle_in_postgres`；`backend/tests/multi_alpha/test_durable_orchestrator_restart.py`；`frontend/tests/multi-alpha-combine-backtest.spec.ts`；PostgreSQL 用例已实现，本 worktree 未配置测试 DSN、未执行 DDL | VERIFIED_SOURCE | none |
| F-420 | §2.3、§15、§21 | `backend/tests/scripts/test_aistock_feature_workflow.py` | VERIFIED_SOURCE | none |
| F-421 | §5、§6.1～§6.2、§10 | `backend/tests/multi_alpha/test_durable_control.py`；`backend/tests/multi_alpha/test_durable_repository_postgres.py` | VERIFIED_SOURCE | none |
| F-422 | §4、§6.7、§7.2、§10、§16.3、§16.7 | `F:/Dev/RD-Agent_worktrees/qe-typed-kill-receipt-p0-2-20260721/test/app/test_qe_evolution_submission_receipt.py::test_typed_kill_completed_result_wins_after_signal_reservation`；`::test_typed_kill_uses_exact_process_incarnation_and_is_idempotent`；`backend/tests/multi_alpha/test_durable_cancellation.py` | VERIFIED_SOURCE | none |
| F-423 | §6.1、§6.6、§8.2、§16.4 | `backend/tests/multi_alpha/test_durable_retry.py::test_successor_files_publish_before_database_visibility`；`backend/tests/multi_alpha/test_durable_execution_adapter.py::test_published_manifest_rejects_path_escape_before_reading_external_file` | VERIFIED_SOURCE | none |
| F-424 | §6.4～§6.6、§8、§16.3 | `backend/tests/multi_alpha/test_durable_retry.py::test_results_only_successor_references_verified_results_and_never_creates_remote_attempt`；`backend/tests/qe_archive/test_multi_alpha_recovery_archive.py::test_archive_v2_preserves_partial_recovery_children_attempts_and_identity` | VERIFIED_SOURCE | none |
| F-425 | §6.4、§8.3、§9、§16.1～§16.3 | `backend/tests/multi_alpha/test_durable_control.py::test_code_identity_change_expands_dependency_closure_without_mixed_results` | VERIFIED_SOURCE | none |
| F-426 | §7.1、§10、§16.2～§16.4 | `backend/tests/multi_alpha/test_durable_repository.py::test_early_pause_cancel_and_resume_sql_cover_zero_child_runs`；`::test_zero_child_pause_and_cancel_are_claimable_for_terminalization`；`backend/tests/multi_alpha/test_durable_repository_postgres.py::test_p0_2_zero_child_pause_resume_cancel_lifecycle_in_postgres`；`backend/tests/multi_alpha/test_durable_orchestrator_restart.py::test_stale_worker_cannot_terminalize_successor_attempt_or_child`；PostgreSQL 用例需显式测试 DSN 才会执行，本轮未执行 | VERIFIED_SOURCE | none |
| F-427 | §5.2、§11、§13、§16.5 | `frontend/tests/multi-alpha-combine-backtest.spec.ts`；`backend/tests/multi_alpha/test_durable_retry.py::test_recovery_preview_replay_keeps_command_and_successor_identity_stable`；`backend/tests/multi_alpha/test_durable_router.py::test_recovery_execute_rejects_command_identity_different_from_preview` | VERIFIED_SOURCE | none |
| F-428 | §1、§2.3、§11～§14、§16.6 | `backend/tests/multi_alpha/test_durable_contract.py`；`backend/tests/mcp/test_qe_archive_module.py::test_durable_recovery_mcp_reuses_preview_idempotency_without_global_client_change`；`backend/tests/unified_engine/test_qe_stop_task.py` | VERIFIED_SOURCE | none |
| F-429 | §6.6、§14～§16 | `backend/tests/qe_archive/test_multi_alpha_recovery_archive.py` | VERIFIED_SOURCE | none |
| F-430 | §6.3～§6.6、§8、§16.2～§16.4 | `backend/tests/multi_alpha/test_durable_retry.py::test_terminal_targeted_recovery_freezes_dependency_closure_and_preserves_siblings`；`backend/tests/multi_alpha/test_durable_parent_finalization.py::test_partial_recovered_requires_every_child_in_recovery_scope_to_succeed`；`::test_partial_recovered_preserves_unavailable_siblings_after_successful_recovery` | VERIFIED_SOURCE | none |

`VERIFIED_SOURCE` 仅表示本 worktree 的代码与测试证据；DDL、依赖安装、服务启停和真实外部副作用仍保持独立授权。文档、API、MCP 和 UI 不新增 `AWAITING_USER_CONFIRMATION/APPROVED_BY_USER` 等研究审批状态。

## 19. Risks / 风险与对策

| 风险 | 对策 |
|---|---|
| pause/cancel 与 remote POST 竞态 | DB 线性化事务前到达则禁止 POST；事务后到达则 attempt 视为在途，完成 exact-intent POST 后自然完成或走 typed cancel，不伪造跨 DB/HTTP 原子性 |
| kill 返回成功但远端仍执行 | kill response 不作为终态；receipt/status/result 权威 reconcile，reservation 保持 |
| 终态 child 重开覆盖结果或 Archive | 使用 successor recovery run，来源终态对象永不改写 |
| recovery 外键阻断现有 DELETE 或来源删除后 successor 失效 | source FK `SET NULL`；lineage/hash 冻结，必要文件原子复制；只在 active source-copy 窗口用 command 形成瞬时互斥 |
| successor 只重跑目标导致派生指标不一致 | 预览并持久化确定性 dependency closure；派生值只从权威 raw metrics 重算 |
| retry mode 缺制品后静默换路径 | 三模式严格分派；返回 evidence 和补取建议，不 fallback、不淘汰方向 |
| 双击或重试产生重复 kill/attempt/run | command ledger + Idempotency-Key + payload hash + CAS |
| 多实例或 crash window 重复发送 kill | command/delivery lease-fencing + typed owning-service kill receipt；same intent 不重复 signal |
| reserved/started 取消与 Popen 竞态 | submission receipt、pre-start cancel 与 Popen/process identity 持久化共用 per-loop lock；竞态在同一 delivery 上递增 intent generation，不伪造 PID、不二次 delivery |
| PID 复用误杀新进程 | typed request/receipt 绑定 pid+pgid+start-time；incarnation mismatch 明确无 signal，所有旧/新 writer 共用 terminal CAS helper |
| 文件未发布便 dispatch successor | final workspace 先原子发布，successor DB/queued attempt 后一次性可见；command manifest 支持重启续做 |
| reference result 破坏 baseline/read model | 显式 reference/derived attempt 保存 verified result manifest，不冒充远端执行 |
| rematerialize 混用不同代码结果 | scope 冻结 code identity，依赖图自动扩展受影响 child，不兼容结果不得 reuse |
| Archive 只改 handler、数据库约束仍拒绝新状态 | 独立 Archive v2 migration 同步扩展 generic/multi-alpha status CHECK、handler、repository、list/detail/backfill 和事务 readback |
| targeted recovery 隐藏未选失败 sibling | 显式 `preserve_unavailable/not_recovered`，本轮成功记为 `partial_recovered`，证据保留且可继续恢复 |
| backend 重启重复执行 | command/attempt/receipt/reservation 全持久化，lease/fencing 拒绝 stale owner |
| P0-2 UI 扩张成另一套 QE 页面 | 只增量修改现有 operations panel；完整 grid/shell 留在 P0-3/P0-4 |
| 控制错误污染 Alpha 研究失败 | command error 与 run/child research result 分离，reason namespace 独立 |
| P0-2 影响非 QE 路径 | 路由、service、schema、import 和回归测试固定 QE multi-alpha scope |

## 20. DESIGN-COMPLIANCE-001 Review

### 20.1 禁止简化版

- 没有把 whole-run retry 冒充 child retry；
- 没有把 DELETE/stale-fail 冒充 cancel；
- 没有只做 API 而遗漏 repository/orchestrator/adapter/UI/MCP；
- 没有用重开终态 row 的简化方法覆盖结果和 Archive；
- 没有用一个 command row 冒充多 attempt kill delivery，也没有只用 Fake Workspace 代替 owning-service contract；
- P0-4 明确后置，但 P0-2 所需的 command rediscovery、attempt cancel、恢复证据和 UI 操作闭环完整保留。

### 20.2 禁止静默错误

- kill/inspect unknown 保持可见；
- artifact 缺失、hash 不符、scope 过期、identity 冲突都有稳定 reason；
- retry mode 不自动切换；
- topology 不静默切换；
- kill HTTP 结果不冒充 submission terminal，kill/completed 竞态由 typed receipt 与权威结果收口；
- reference/derived attempt 明确标记非远端执行；
- command/control error 与研究结果 error 分离。

### 20.3 禁止业务逻辑偏移

- 组合、LOO、baseline、回测、训练和研究指标公式不变；
- whole-run retry、single-Alpha lifecycle、Archive source contract 保持；
- 派生值重算只使用原 raw metrics 和新依赖，不改写 raw 结果；
- 成功结果和完成竞态始终保留。
- operator cancel 不聚合为 Alpha `partial_failed`；不同 code identity 不混算 raw/derived 指标。

### 20.4 禁止私增门禁/审批

- command 状态是技术幂等/执行事实，不是审批；
- capability/evidence 是对象和制品事实，不评价 Alpha；
- 缺失数据/制品不淘汰研究方向；
- 不新增 user approval、promotion、PASS/KILL/GO/STOP 状态；
- 不新增设计合入或进入编码的人工确认状态；生产 DDL、依赖、服务启停等外部副作用仍遵守既有独立授权边界。

### 20.5 2026-07-29 实施复核

- **完整性**：已实施控制命令账本、执行身份与证据、仅子级重试、typed kill receipt、Archive v2 读回、API/MCP/UI 闭环与 RD-Agent owning-service 合约；没有以单一 API、内存状态或假回执替代这些边界。
- **显式错误语义**：P0-2 schema、远端身份、数据集清单、制品或恢复输入不可用时均返回结构化 evidence 和可操作建议；不会静默回退、伪造成功、改变 retry mode 或把技术证据缺口写成研究结果。
- **兼容性**：P0-1B 提交在 P0-2 additive DDL 尚未应用时保持原有 SQL 形状和可用性，并附带 `multi_alpha_p0_2_schema_unavailable` 基础设施证据；P0-2 控制/恢复入口则明确说明其自身 schema 前置条件。该区分不是研究审批或方向淘汰。
- **边界**：未修改 Alpha 公式、标签、回测、LOO、baseline、训练、指标计算或研究方向；未新增 PASS/KILL/GO/STOP、人工审批、数据缺失淘汰规则或自动降级。
- **验证收据**：源码级目标测试、静态检查、TypeScript、MCP manifest 和 F2 validator 的结果记录在第 0 节；DEV/生产 DDL、运行态 canary 与生产激活均已在后续受控步骤完成。2026-07-29 仅做当前 runtime 的 GET/OpenAPI 复核，没有重复执行任何生产副作用。

## 21. Rollout / Rollback / Production Gates / 发布回滚与生产门禁

### 21.1 当前文档同步与既有生产事实分离

- 原始设计、源码 PR、DDL、服务重启和 canary 已按独立阶段完成；本次 BUG-904 仅同步其状态，不重放任何既有生产步骤。
- `production_ddl_gate=noop`；
- `production_backend_dependency_gate=noop`；
- `production_frontend_dependency_gate=noop`；
- 本次不改数据库、不改 runtime、不重启服务、不启动实验。

### 21.2 后续维护顺序

1. 继续以现有 durable repository/orchestrator/recovery 为唯一实现入口；
2. 对 provenance 缺口补齐 dataset manifest/root 与 runtime lock/executor commit，不以 run success 隐藏缺失证据；
3. 新源码合入仍不等于新的 DDL、依赖或 runtime 激活；
4. rollback 优先停用 P0-2 API/UI/MCP/control consumption，保留 command/recovery 证据；不得删除已产生的 run/child/attempt/result/Archive。

### 21.3 当前状态

- P0-1B source：merged；
- P0-1B reservation DDL：production applied and verified；
- P0-1B runtime：此前已完成启动 smoke；实时进程状态不是本设计的持久权威，操作前必须现场核查。本次修订于 2026-07-21 14:27 只读观察到 `0.0.0.0:8001` 正在监听，但未启停服务；
- P0-2 design/source：`SOURCE_MERGED_PRODUCTION_DDL_RUNTIME_VERIFIED`；AIstock PR #2580 / RD-Agent PR #6 已合入；
- P0-2 DEV/production DDL：已应用并验证；
- P0-2 runtime/restart/actual QE canary：此前已完成；2026-07-29 用户重启后再次只读确认路由加载与成功 run readback；
- P0-2 provenance gap：最新成功 run 的 execution identity evidence 仍不完整，缺项保持显式，待独立后续任务补齐。

## 22. 退出条件与下一步

BUG-904 的完成条件是：本文与父蓝图、项目摘要同步当前已合入/已激活事实，同时保留 provenance 缺口；通过 F2 文档 validator、语义 diff 审核并形成可审查 PR。后续新的生产 DDL、依赖安装和服务启停仍分别授权。

研发过程中出现缺失数据、缺失制品、远端 unknown 或部分失败时，继续记录、补取、交叉验证和恢复，不得据此删除研究方向或私增审批门禁。
