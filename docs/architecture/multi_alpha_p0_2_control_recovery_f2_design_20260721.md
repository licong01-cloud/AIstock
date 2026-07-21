# 多 Alpha P0-2 任务控制与子任务恢复 F2 详细设计

- 文档类型：F2 阶段从属实现级详细设计
- 父级权威：`docs/architecture/multi_alpha_qe_evolution_foundation_f2_design_20260718.md`
- 模块：QuantEvolver / Multi-Alpha combine-backtest / QE Workspace / PostgreSQL durable orchestration / QE UI / QE MCP
- 日期：2026-07-21
- 状态：`DESIGN_READY_AWAITING_USER_CONFIRMATION_CODE_PENDING`
- 当前事实：P0-1B 源码与 QE Workspace receipt 配套已合入，reservation DDL 已在生产应用并验证；P0-2 尚未编码
- 唯一运行边界：QE-only；不得读写或调用 Selection、Advisory、Paper、模拟盘、QMT、StrategyPackage 运行链或其他非 QE 模块
- 科研约束：本设计不增加研究门禁、人工审批、准入、晋级、PASS/KILL/GO/STOP 或指标淘汰逻辑；缺失数据和制品只形成可见证据与获取建议，不淘汰研究方向
- 运行约束：本设计和后续实现不得自行启动、停止或重启 AIstock 后端；生产 DDL、依赖安装和运行激活均是独立授权事项

---

## 1. Background / 背景、结论与必须锁定的架构决策

P0-2 在现有 P0-1B durable run/child/attempt、QE Workspace receipt、共享 reservation、lease/fencing/CAS 和 Archive 之上增量实现，不创建“多 Alpha v2”，不复制组合算法，也不改变单 Alpha QE 的生命周期语义。

编码前锁定以下决策：

1. **暂停是 cooperative drain**：立即停止新的 plan/dispatch，不伪造远端 pause，不 kill 已提交 attempt；在途 attempt 继续 reconcile 和收结果，全部收口后 run 才进入 `paused`。
2. **取消是 durable intent + asynchronous reconcile**：API 先原子持久化取消意图，再由 orchestrator 精确终止远端 attempt；HTTP kill 成功、404 或网络异常都不是权威终态，不能据此提前释放 reservation 或写 `cancelled`。
3. **终态不可变**：已经终态的 run、child、attempt、业务结果和 Archive 不重新打开、不覆盖、不删除历史证据。
4. **终态 child 恢复使用 successor recovery run**：从终态 run 选择失败/取消 child 时，创建显式 `retry_of_run_id` 继承 run；只执行目标及确定性依赖闭包，其余成功结果按来源 child 和制品 hash 引用复用。API 必须返回原/新 run、child、attempt 身份，不能把它伪装成原 child 内追加 attempt。
5. **非终态 child 可原 run 追加 attempt**：仅当 parent 尚未终态、未完成 Archive capture、目标 child 无已固化冲突业务结果时，才允许在同一 child 上创建 `attempt_no=N+1`。不满足条件时明确使用 successor recovery，不做静默拓扑切换。
6. **三种恢复模式严格分流**：`results_only`、`backtest_only`、`rematerialize_and_backtest` 不互相回退，不自动改成 full train，也不使用当前默认数据/节点替换冻结身份。
7. **恢复范围按依赖闭包计算**：用户选择一个 child，系统展示并持久化 `execute/reuse_result/recompute_derived` 闭包；这只是结果一致性计算，不是研究审批。
8. **legacy stop 等价 cancel**：多 Alpha legacy stop 委托同一 durable cancel service；不得映射为 pause、DELETE 或 single-Alpha 的数据库重置实现。
9. **capability 是状态事实，不是门禁**：后端返回动作状态、制品证据和 reason code；UI 不隐藏研究方向。缺失制品时保留恢复模式和获取建议，执行请求显式返回当前缺口，不把候选方向标记为无价值。
10. **P0-2 UI 只补控制与轻量恢复面板**：完整 child/attempt grid、共享页面壳和长时间线仍归 P0-4/P0-3，避免在本阶段偷做平行 UI。

## 2. 权威关系、范围与非目标

### 2.1 权威关系

- 父蓝图继续负责 QE-only 隔离、总体验收和 P0-1～P0-4 顺序。
- 本文细化父蓝图 `F-207`（pause/resume/cancel）与 `F-208`（child recovery），并补足控制幂等、终态恢复拓扑、重启竞态、API/MCP/UI 和验证合同。
- P0-1B 详细设计及其已合入实现继续拥有 submission receipt、reservation、lease/fencing、执行适配和 Archive delivery 底座。
- 现有 whole-run retry 继续是“按完整请求创建新 run”；P0-2 的 child-targeted recovery 与其并存，不互相冒充。

### 2.2 In Scope

1. run pause、resume、cancel、legacy stop alias、manual reconcile；
2. 单 attempt cancel；
3. 非终态 child append-only retry；
4. 终态 run 的 child-targeted successor recovery；
5. `results_only/backtest_only/rematerialize_and_backtest` 严格执行路径；
6. durable control command 幂等账本、状态事实与事件；
7. orchestrator control/cancel/reconcile pass 和重启恢复；
8. 后端 capabilities、结构化 reason、children/attempt read API；
9. 现有多 Alpha 详情页的控制按钮和轻量恢复对话框；
10. QE MCP 同源 read/control/recovery 工具；
11. additive migration、preflight、rollback 和针对性验证矩阵。

### 2.3 Non-goals

- 不修改模型、因子、标签、数据集、训练、组合权重、回测公式或研究结论。
- 不新增任何研究方向准入、晋级、淘汰、人工审批或 promotion 流程。
- 不因数据或制品暂缺而放弃方向；只记录缺口、来源和补取方法。
- 不改变 single-Alpha `stop/resume/retry` 的既有实现或语义。
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
| schema | run 已有 pause/cancel 字段，event 可存 control | 没有 idempotency key/payload hash；没有跨 run recovery lineage |
| orchestrator | cycle 已有 planner、dispatch、reconcile、finalize、archive | 没有 control/cancel pass；finalizer 不能收口 pause/cancel |
| execution adapter | 可 materialize、submit、inspect、collect | 没有 kill 封装，三种 retry mode 尚未执行分流 |
| QE Workspace | `kill_loop()`、receipt、status/read result 已存在 | kill 后没有 P0-2 的持久化 reconcile 语义 |
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
        +--> DurableRepository             # command/CAS/event/recovery transaction
        +--> RecoveryPlanner               # target + dependency closure
        +--> DurableOrchestrator            # control/cancel/reconcile/finalize passes
                     |
                     v
            DurableExecutionAdapter
                     |
                     v
            QEWorkspaceClient + receipt/status/result/kill
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
  - 生成 `execute/reuse_result/recompute_derived` 计划。

两者是现有 multi-alpha service 的内部模块，不是新平台或第二套业务入口。

### 4.2 必须修改的现有组件

- `durable_repository.py`：control command、operator CAS、recovery transaction、reference child/result copy、queued cancellation。
- `durable_orchestrator.py`：control/cancel pass、pause drain、cancel reconcile、recovery dispatch/finalize。
- `durable_execution_adapter.py`：exact kill、三种 retry 分流、source artifact verification。
- `combine_backtest.py`：兼容 facade 委托；保留 whole-run retry 和终态 DELETE。
- `combine_ui_adapter.py`：完整 durable status、capabilities、child/attempt 摘要。
- `backend/routers/multi_alpha.py`：新增控制和恢复 API。
- `backend/mcp/modules/qe_archive.py`、`backend/mcp/tool_manifest.py`：同源 MCP 工具。
- `CombineRunOperationsPanel.tsx`：控制按钮和轻量恢复对话框。
- `[taskKey]/page.tsx`：仅补类型和刷新 wiring。

### 4.3 明确不修改

- `backend/routers/quantevolver_evolution.py`；
- `backend/services/quantevolver/qe_evolution_service.py`；
- 单 Alpha evolution 页面；
- `combiner.py`、`panels.py` 及现有研究公式；
- Selection、Advisory、Paper、QMT、StrategyPackage、LocalSIM；
- 已部署的 2026-07-18 / 2026-07-19 migration 文件。

## 5. Control Command 与幂等合同

所有写操作要求调用方提供 `Idempotency-Key`。该 key 是防重复写/重复 kill/重复 recovery 的技术身份，不是审批 token。

### 5.1 Canonical payload

`payload_hash = SHA256(canonical_json({action, run_id, child_id, attempt_id, retry_mode, requested_scope}))`。legacy `stop` 在 canonicalization 前规范为 `action=cancel`，原始入口只作为 `requested_alias=stop` 审计字段，因此 stop/cancel 不会形成两套领域动作。

- 同 `run_id + Idempotency-Key + payload_hash`：返回第一次命令的同一 `command_id` 和当前状态；
- 同 `run_id + Idempotency-Key`、不同 hash：409 `control_idempotency_conflict`；
- 双击、HTTP retry、MCP retry、后端重启都不能产生第二条状态迁移、第二个 attempt、第二个 successor run 或并发 remote kill。

### 5.2 Command 生命周期

```text
accepted -> applying -> reconciling -> succeeded
                 |             |
                 +-----------> failed
```

- `accepted`：命令与用户意图已持久化；
- `applying`：本地状态事务已生效；
- `reconciling`：仍需权威远端事实；
- `succeeded`：命令目标已收口，不等价于研究结果 succeeded；对 child retry 表示 recovery plan 与必要本地制品已完整发布并可由 orchestrator 调度，不等待新 backtest 结束；
- `failed`：控制操作本身失败，保留结构化错误和恢复建议。

### 5.3 原子顺序

1. 插入或读取 command；
2. `SELECT ... FOR UPDATE` 锁目标 run/child/attempt；
3. 校验对象身份和 row version；
4. 更新 durable 状态并递增 `row_version/fencing_token`；
5. 同事务写 `event_type=control`；
6. commit 后由 orchestrator 调用远端；
7. 通过 receipt/status/result 再执行 CAS 收口。

API 请求线程不得直接 kill 后猜终态，也不得因 worker 暂时不可用拒绝持久化 pause/cancel 意图。

## 6. Additive Database Design

P0-2 新增独立 migration、preflight 和 rollback；不得改写已部署 migration。

### 6.1 `strategy_pkg.multi_alpha_combine_backtest_command`

| 列 | 合同 |
|---|---|
| `command_id` | `macmd_...` 主键 |
| `run_id` | 必填 FK，`ON DELETE CASCADE`，与现有终态 run 删除语义一致 |
| `child_id` / `attempt_id` | 可空 FK，必须属于 run，`ON DELETE SET NULL`；run 删除仍由 `run_id` cascade command |
| `action` | `pause/resume/cancel/reconcile/attempt_cancel/child_retry`；legacy stop 规范为 cancel |
| `idempotency_key` | 调用方稳定 key |
| `payload_hash` | canonical request SHA-256 |
| `request_json` | 原始规范化请求，不含 secret |
| `response_json` | 第一次接受响应及后续状态摘要 |
| `status` | `accepted/applying/reconciling/succeeded/failed` |
| `requested_by` | 审计身份；不承担审批 |
| `error_code/error_json` | 控制错误，不污染研究结果 error |
| `created_at/updated_at/completed_at` | 时间证据 |

约束与索引：

- `UNIQUE(run_id, idempotency_key)`；
- `CHECK(payload_hash ~ '^[0-9a-f]{64}$')`；
- active command 索引 `(status, updated_at)`；
- child/attempt FK 一致性由 repository 在同一锁事务校验。

### 6.2 Run 增量

在 `multi_alpha_combine_backtest_run` 增加：

- `recovery_kind TEXT NULL CHECK IN ('child_targeted')`；
- `recovery_scope_json JSONB NOT NULL DEFAULT '{}'`；
- `retry_of_run_id` 继续作为来源 run 权威 FK，不新增第二个 source-run 字段。

普通 run 的 `recovery_kind IS NULL` 且 scope 为空；successor recovery 必须同时具有 `retry_of_run_id`、`recovery_kind` 和非空规范 scope。

### 6.3 Child 增量

在 child 增加：

- `source_child_id TEXT NULL REFERENCES ...child(child_id) ON DELETE SET NULL`；
- `execution_disposition TEXT NOT NULL DEFAULT 'execute' CHECK IN ('execute','reuse_result','recompute_derived')`；
- `source_lineage_json JSONB NULL` 与 `source_lineage_hash TEXT NULL`：冻结 source run/child/attempt、业务 key、raw result、artifact/result manifest 和 SHA-256 身份；两列必须同时为空或同时有效；
- 扩展 `source_kind` 为 `runtime/legacy_result_backfill/recovery_reference`。

`reuse_result/recompute_derived` child 不创建伪 attempt。其成功状态必须由专用 repository 事务验证：来源 child 已成功、业务键一致、来源制品 URI/hash 一致，并写 lineage event。来源 run/child/key/hash/raw result 摘要冻结进 successor `recovery_scope_json` 和 `source_lineage_json`；即使以后按现有功能删除来源 run，successor 仍保留自证据，不能只依赖可空 FK，也不能污染原有 `input_manifest_json` 的输入语义。

### 6.4 Attempt 增量

在 attempt 增加：

- `source_attempt_id TEXT NULL REFERENCES ...child_attempt(attempt_id) ON DELETE SET NULL`。

约束调整为：

1. 普通 initial：`attempt_no=1, retry_mode=initial, retry_of_attempt_id/source_attempt_id IS NULL`；
2. 同 child retry：`attempt_no>1, retry_mode<>initial, retry_of_attempt_id=同 child 紧邻前 attempt, source_attempt_id IS NULL`；
3. successor 首 attempt：`attempt_no=1, retry_mode<>initial, retry_of_attempt_id IS NULL, source_attempt_id=来源 run attempt`。

不得让 `retry_of_attempt_id` 跨 child；跨 run lineage 只用 `source_attempt_id`，避免破坏现有 immediate-lineage 含义。来源 remote identity、receipt、artifact/result manifest hash 还必须冻结到 successor child lineage 和 attempt manifest；可空 FK 只提供在线导航，不是唯一证据。

### 6.5 Result 与 Archive

- 不给 scheme/LOO 结果表增加 generation，不覆盖来源 run 的唯一结果行。
- successor run 为 `reuse_result` child 在现有结果表中物化 exact 小型结构化结果行，并通过 frozen lineage、`source_child_id`、event 和 artifact hash 保留来源；不创建平行 metrics 表。
- `recompute_derived` 只以 `source_lineage_json` 中冻结且 hash 已验证的来源 raw metrics 和 successor 内已收口依赖重新计算派生差值；不得重跑模型或更改 raw metrics。
- 目标 retry 真正需要的文件必须在 successor 进入可 dispatch 状态前使用现有 staging/atomic-publish 原语复制到 successor attempt workspace 并重新校验 SHA-256；不得让 successor 运行依赖一个以后可能被来源 run DELETE 清理的路径，也不得用未验证 hardlink/路径别名冒充独立制品。未被执行路径读取的成功 child 大文件不复制。
- successor Archive 是新 run 的独立不可变快照，manifest 包含 source run、recovery scope、每个 child 的 disposition 和 source identity。
- 来源 Archive 永不改写；Archive delivery 也不因 successor 创建而回退或重放。
- 现有终态 run DELETE 保持可用：command 随 run 删除，recovery FK `SET NULL`，successor 的 frozen lineage 和已物化结果/必要文件继续自洽。仅当来源 run 存在 `applying` 中且尚未完成必要文件复制的 child-retry command 时，DELETE 在 workspace quarantine 前显式返回 409 `recovery_source_copy_in_progress`；复制发布完成或命令失败收口后立即恢复原删除能力。这是防止并发损坏的瞬时互斥，不是研究门禁。

## 7. Run / Child / Attempt 状态机

### 7.1 Pause / Resume

```text
queued/preparing/running
        -> pause_requested
        -> paused
        -> preparing|running
```

- pause 接受 `queued/preparing/running`；当前原子 materialization 可以完成发布，但不得开始下一 child 或 remote submit。
- dispatcher 在“reservation/source claim + remote POST”事务边界再次锁 parent，只有 `preparing/running` 可提交，关闭 pause/cancel 竞态窗口。
- `pause_requested` 中允许 resume 撤回；恢复目标由 durable 事实确定：尚无 child plan 回 `preparing`，已有 plan 回 `running`。
- `paused` 不释放已终态结果，也不重建 artifact/remote identity。
- 在途 attempt 仍 active 时状态保持 `pause_requested`，UI 显示 `remaining_active`。

### 7.2 Cancel

```text
run: queued/preparing/running/pause_requested/paused
       -> cancel_requested -> cancelling -> cancelled|partial_failed|succeeded

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
- 全部可执行 child 取消且无成功结果：run `cancelled`；
- 部分成功、其余因取消未完成：run `partial_failed`，reason `cancelled_with_preserved_success`；
- cancel 到达前全部结果已经成功：run `succeeded`，control outcome `cancel_raced_with_completion`。

legacy stop route 在进入 service 前规范为 cancel，返回同一 command schema、状态迁移和 reason；不得保存第二套 stop 状态或调用 DELETE。

attempt 暂不增加 `cancelling` 枚举，使用现有 `reconciling + phase=kill_requested`，避免无必要 schema 扩张；run/child 已有 cancellation 状态。

### 7.3 Manual reconcile

manual reconcile 只执行：receipt/status inspect、result collect、reservation reconcile、parent aggregate 和 Archive delivery check。它不得 materialize、新建 attempt、申请新 reservation 或提交远端 execution。

### 7.4 Legacy stale-fail

现有 stale-fail 只能处理没有 durable identity 的 legacy row。若 run 已有 `task_id/request_hash`，接口返回 `durable_run_requires_reconciliation`，不得按 heartbeat 年龄把 durable run 改成 failed。

## 8. Child Retry 与 Successor Recovery

### 8.1 拓扑选择

后端先计算并返回显式 topology：

- `append_attempt_in_place`：parent 非终态、Archive 未捕获、child 业务结果未固化冲突；
- `successor_recovery_run`：parent 已终态、Archive 已捕获或业务结果已固化。

调用方不得指定一个不符合事实的 topology；返回值必须包含选择依据。服务也不得在请求后静默换 topology：preview 和 execute 使用同一 scope hash，事实发生变化时返回 409 `recovery_scope_stale`，调用方刷新后重试。

### 8.2 Successor recovery 创建与原子发布

1. 先持久化 `child_retry/applying` command；现有 DELETE 在 workspace quarantine 前必须检查该 active command；
2. 锁来源 run，验证已终态且属于同一 task，读取冻结 request、roster、child plan、source attempt、结果和 artifact identity；
3. 计算 target 与依赖闭包，预分配确定性的 successor run/child/attempt identity；
4. 在 successor staging workspace 原子复制并验证本次执行真正需要的来源文件；不需要的成功 child 大文件不复制；
5. 单一数据库事务创建 `retry_of_run_id=source_run_id` 的新 run、全部 successor children、目标 first attempt、exact/recomputed 结果行和 lineage events；新 run 在必要文件发布完成前保持不可 dispatch 的 `preparing`；
6. 使用 P0-1B atomic publish 把 staging 切换为正式 successor workspace；
7. 最终 CAS 事务把目标 attempt 置为 queued、写 command response 的完整 source/successor identity，并把 command 置为 `succeeded`；
8. commit 后由现有 orchestrator 调度 `execute` attempt。

任一步冲突都不能暴露可 dispatch 的半成品。数据库提交前失败只清理本 command 的 staging；数据库提交后文件发布失败则保留 successor run 为显式 `failed/recovery_artifact_publish_failed` 和 command error，禁止 fallback 或静默删除证据。command 收口后来源 run DELETE 不再受该命令阻挡。

### 8.3 依赖闭包

| 用户目标 | `execute` | `recompute_derived` | `reuse_result` |
|---|---|---|---|
| baseline | baseline | 所有 scheme 的 `vs_baseline_*` 派生值 | scheme raw metrics、全部 LOO raw/derived（若不依赖 baseline） |
| scheme | 目标 scheme | 同 weighting scheme 的 LOO marginal/delta | 其他 scheme/LOO、baseline |
| LOO | 目标 LOO | 目标 LOO 对 full scheme 的 marginal/delta | baseline、所有 scheme、其他 LOO |

如果当前业务字段证明某派生值还依赖其他 child，RecoveryPlanner 必须扩展闭包并在 preview 显示；不得为了少跑任务而保存内部不一致结果。

### 8.4 成功 child 与新研究场景

child recovery 面向失败、取消或结果落库失败的执行恢复。成功 child 不在原 run 上重做；用户需要用新代码、不同数据或不同参数研究时，继续创建正常新 run/scene，并显式保留 lineage。这是身份语义，不是研究方向门禁。

## 9. 三种恢复模式

### 9.1 `results_only`

- 来源：`source_attempt_id` 或同 child `retry_of_attempt_id` 的 exact QE task/loop、receipt 和 result artifact；
- 行为：`queued -> reconciling -> collect/ingest`；
- 必须：零 remote run POST、零新 execution reservation、零 materialization；
- 缺失：返回 `results_only_artifact_missing` 或 `results_only_remote_identity_missing`，同时保留来源、缺失清单和重新获取建议；不自动改成其他模式。

### 9.2 `backtest_only`

- 来源：exact combined prediction URI/hash、冻结 backtest config、dataset identity、runtime identity；
- 行为：创建 attempt-specific workspace/config，申请新 reservation，提交新 Qlib backtest；
- 必须：不重新组合、不训练、不替换 prediction；
- 缺失或 hash 不符：`backtest_prediction_missing` / `backtest_prediction_hash_mismatch` / `backtest_identity_missing`；不自动 rematerialize。

### 9.3 `rematerialize_and_backtest`

- 来源：冻结 request、roster、source prediction identity、因子/数据集身份和 child input manifest；
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
2. dispatch transaction 必须复核 parent 状态，pause/cancel 后不能发生新 remote POST。
3. cancel pass 按 exact `node_id/qe_task_id/qe_loop_id` kill，不广播 stop。
4. 每个 remote kill intent 有持久化 command/event；重试节流，不能每个轮询周期重复 kill。
5. reconcile 对 active/unknown reservation 保持占位；只有权威终态释放。
6. stale owner 的 fencing token 失效后不能写回 attempt 或 command。
7. 多实例 orchestrator 通过 SKIP LOCKED + CAS 只消费一次 command/attempt。
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
GET  /multi-alpha/combine-backtest/children/{child_id}
GET  /multi-alpha/combine-backtest/children/{child_id}/attempts
POST /multi-alpha/combine-backtest/children/{child_id}/retry/preview
POST /multi-alpha/combine-backtest/children/{child_id}/retry
POST /multi-alpha/combine-backtest/attempts/{attempt_id}/cancel
GET  /multi-alpha/combine-backtest/commands/{command_id}
```

### 11.1 Mutation request

- Header：`Idempotency-Key` 必填；
- Body：`requested_by` 由认证上下文取得，不能由客户端伪造；retry 包含 `retry_mode` 和 preview 返回的 `scope_hash`；
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
- `recovery_scope_stale`
- `durable_run_requires_reconciliation`
- `remote_cancel_state_unknown`
- `cancel_raced_with_completion`
- `cancelled_with_preserved_success`
- `results_only_artifact_missing`
- `results_only_remote_identity_missing`
- `backtest_prediction_missing`
- `backtest_prediction_hash_mismatch`
- `backtest_identity_missing`
- `rematerialize_source_identity_missing`
- `source_lineage_mismatch`

错误必须包含可机器读取 context 和恢复建议，不把 control error 写成 Alpha 研究失败。

## 12. MCP Contracts

MCP 与 HTTP 调用同一 `DurableControlService`，不复制状态判断：

- read-only：run controls/capabilities、children、attempts、command status；
- mutation：pause、resume、cancel/stop、reconcile、child retry、attempt cancel。

MCP mutation 必须传稳定 `idempotency_key` 并返回同一 command payload。不得新增 feature-specific “确认 token”、人工审批表或研究准入状态；若 MCP 网关已有全局危险操作提示，只保留通用传输层行为，不进入 P0-2 领域模型。

若 `AIstockApiClient` 当前不能传 header，应以向后兼容可选 headers 参数补齐并测试，不能把幂等 key 偷放日志或 request body 后宣称满足 HTTP 合同。

## 13. UI Design

### 13.1 Existing panel 增量

在 `CombineRunOperationsPanel` 增加：

- `暂停`：显示 cooperative drain 说明和 remaining active；
- `恢复`：显示恢复目标状态；
- `终止`：明确“停止新提交，并异步终止在途 QE attempt”；
- `立即对账`：只 inspect/collect，不启动执行；
- `恢复子任务`：打开轻量 child/attempt 对话框。

按钮由后端 `state_allowed` 驱动；evidence 缺失不隐藏模式，展示缺失与补取建议。现有 whole-run retry、scenario、archive、delete 保留原语义。

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

UI 刷新后从 API 重建 command/run/child/attempt 状态，不依赖浏览器内存；`pause_requested/cancelling/reconciling` 必须显示真实进度，不能只显示旧页面的 `running`。

## 14. File Plan

### 14.1 新增

- `docs/architecture/multi_alpha_p0_2_control_recovery_f2_design_20260721.md`
- `backend/services/multi_alpha/durable_control.py`
- `backend/services/multi_alpha/durable_recovery.py`
- `backend/migrations/multi_alpha_p0_2_control_recovery_20260721.sql`
- `backend/migrations/multi_alpha_p0_2_control_recovery_20260721.preflight.sql`
- `backend/migrations/multi_alpha_p0_2_control_recovery_20260721.rollback.sql`
- `backend/tests/multi_alpha/test_durable_control.py`
- `backend/tests/multi_alpha/test_durable_retry.py`
- `frontend/tests/quantevolver/multi-alpha-control.spec.ts`

### 14.2 修改

- `backend/services/multi_alpha/durable_models.py`
- `backend/services/multi_alpha/durable_repository.py`
- `backend/services/multi_alpha/durable_orchestrator.py`
- `backend/services/multi_alpha/durable_execution_adapter.py`
- `backend/services/multi_alpha/combine_backtest.py`
- `backend/services/multi_alpha/combine_ui_adapter.py`
- `backend/services/multi_alpha/__init__.py`
- `backend/routers/multi_alpha.py`
- `backend/mcp/common.py`（仅当 headers contract 缺失）
- `backend/mcp/modules/qe_archive.py`
- `backend/mcp/tool_manifest.py`
- `frontend/src/app/quantevolver/multi-alpha/combine-backtest/components/CombineRunOperationsPanel.tsx`
- `frontend/src/app/quantevolver/multi-alpha/combine-backtest/[taskKey]/page.tsx`
- 相关 manifest/inventory/gateway/isolated tests。

## 15. Implementation Plan / 实施方案与顺序

### P0-2A：Schema 与 repository primitives

1. additive command/recovery migration、preflight、rollback；
2. control command idempotency transaction；
3. operator CAS、queued cancel、recovery plan persistence；
4. successor reference child/result copy invariants；
5. PostgreSQL concurrency tests。

### P0-2B：Control service 与 orchestrator

1. pause/resume/cancel/stop/reconcile service；
2. dispatch parent-lock race fix；
3. cancel pass、pause drain 和 finalization；
4. restart/fencing/kill race tests。

### P0-2C：Retry execution paths

1. in-place attempt append 与 successor transaction；
2. results-only zero-submit path；
3. backtest-only exact artifact path；
4. rematerialize frozen-input path；
5. dependency-derived recompute 和 Archive manifest。

### P0-2D：API / MCP / UI

1. router + capability/evidence/read APIs；
2. MCP same-service tools；
3. existing panel controls + lightweight recovery dialog；
4. refresh/restart/API/MCP/UI E2E。

### P0-2E：Compliance 与交付

1. F2 validator；
2. DESIGN-COMPLIANCE-001 item-by-item review；
3. targeted + expanded regression；
4. PR 和用户确认；
5. 合入后才分别申请 DDL、依赖和运行激活授权。

## 16. Verification Plan

### 16.1 Static / pure

- migration/rollback/preflight schema contract；
- state transition、canonical payload hash、reason code、capability/evidence；
- dependency closure determinism；
- no auto mode fallback；
- changed-file Ruff/compile/type/lint。

### 16.2 PostgreSQL 16

- 同 key/同 payload 并发只生成一个 command/attempt/successor run；
- 同 key/异 payload 409；
- pause/cancel 与 dispatch 并发只允许一边通过；
- terminal source rows、result rows和 Archive delivery 不变；
- recovery reference/hash/FK/unique/preflight/rollback；
- stale owner fencing 后不能写回；
- 测试数据零残留。

### 16.3 Fake QE Workspace

- pause 不 kill、停止新 submit；
- cancel queued、kill success、kill 404、transport unknown、remote still active；
- kill/completed race 保留成功结果；
- `results_only` POST=0、reservation=0；
- `backtest_only` combine/train=0；
- `rematerialize` 只读冻结 source identity；
- 每种缺失证据返回稳定 reason，零静默 fallback。

### 16.4 Restart / multi-instance

- backend 在 command accept、远端 kill、receipt 保存、result collect、successor 创建各 crash window 重启；
- paused/cancelling/reconciling 状态不丢；
- remote submit/kill 不重复；
- 多 orchestrator 实例只消费一次；
- reservation 直到权威终态才释放。

### 16.5 API / MCP / UI

- 所有 endpoint 的 200/202/404/409/422/503；
- Idempotency-Key header 传递；
- stop 与 cancel 返回同一 command 语义；
- UI 显示 remaining active、unknown、dependency closure、source/successor identity；
- 刷新后恢复进度；
- MCP 与 HTTP 返回一致 reason/capabilities。

### 16.6 Zero-regression / isolation

- single-Alpha stop/resume/retry tests 原样通过；
- whole-run retry、scenario、archive、delete 原样通过；
- P0-1B receipt/reservation/capacity/restart tests 原样通过；
- BUG-786 `trust_env=False`、BUG-793 terminal-parent capacity 语义不回退；
- Selection/Advisory/Paper/QMT/StrategyPackage/LocalSIM route、schema、import 零变化；
- 无 GPU/显存遥测调用。

## 17. Design Acceptance Index

| ID | 验收合同 |
|---|---|
| F-401 | P0-2 只在父蓝图、现有 combine-backtest、P0-1B durable orchestration 和 QE Workspace 上增量实现。 |
| F-402 | 唯一边界为 QE-only；不新增研究门禁、审批、晋级、淘汰或 metric-driven capability。 |
| F-403 | 所有控制命令具有持久化 Idempotency-Key、payload hash、CAS、event 和可重启状态。 |
| F-404 | pause 停止新 dispatch、不中断在途 attempt，drain 后才 paused。 |
| F-405 | resume 从 durable 事实恢复，不重建既有 artifact、attempt 或 remote identity。 |
| F-406 | cancel 先持久化意图，远端权威终态前不释放 reservation、不伪造 cancelled。 |
| F-407 | legacy multi-alpha stop 精确委托 cancel；single-Alpha 与 DELETE 语义不变。 |
| F-408 | 终态 run/child/result/Archive 不可变；终态 child 恢复创建显式 successor recovery run。 |
| F-409 | 非终态 child retry 只追加 attempt，旧 attempt 与业务证据不可覆盖。 |
| F-410 | RecoveryPlanner 持久化 execute/reuse_result/recompute_derived 依赖闭包。 |
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

## 18. Design Acceptance Matrix / 设计验收矩阵

当前只完成详细设计，未获得用户对实现细节和合入的确认；不得把 `DESIGN_READY` 报告成已实施。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-207 | §5、§7、§10～§13；父蓝图 run control | `backend/tests/multi_alpha/test_durable_control.py`；`frontend/tests/quantevolver/multi-alpha-control.spec.ts` | DESIGN_READY | none |
| F-208 | §6、§8、§9；父蓝图 child recovery | `backend/tests/multi_alpha/test_durable_retry.py` | DESIGN_READY | none |
| F-401 | §1～§4 | `backend/tests/multi_alpha/test_durable_contract.py` | DESIGN_READY | none |
| F-402 | §1、§2.3、§20.4 | `backend/tests/multi_alpha/test_durable_contract.py`；`frontend/tests/quantevolver/multi-alpha-no-approval.spec.ts` | DESIGN_READY | none |
| F-403 | §5、§6.1 | `backend/tests/multi_alpha/test_durable_control.py`；`backend/tests/multi_alpha/test_durable_repository_postgres.py` | DESIGN_READY | none |
| F-404 | §7.1、§10 | `backend/tests/multi_alpha/test_durable_control.py` | DESIGN_READY | none |
| F-405 | §7.1、§10 | `backend/tests/multi_alpha/test_durable_control.py`；`backend/tests/multi_alpha/test_durable_orchestrator_restart.py` | DESIGN_READY | none |
| F-406 | §7.2、§10 | `backend/tests/multi_alpha/test_durable_control.py`；`backend/tests/multi_alpha/test_durable_orchestrator_restart.py` | DESIGN_READY | none |
| F-407 | §7.2、§11 | `backend/tests/multi_alpha/test_durable_control.py`；`backend/tests/unified_engine/test_qe_stop_task.py` | DESIGN_READY | none |
| F-408 | §6、§8 | `backend/tests/multi_alpha/test_durable_retry.py`；`backend/tests/multi_alpha/test_archive_health.py` | DESIGN_READY | none |
| F-409 | §6.4、§8.1 | `backend/tests/multi_alpha/test_durable_retry.py`；`backend/tests/multi_alpha/test_durable_repository.py` | DESIGN_READY | none |
| F-410 | §8.2、§8.3 | `backend/tests/multi_alpha/test_durable_retry.py` | DESIGN_READY | none |
| F-411 | §9.1 | `backend/tests/multi_alpha/test_durable_retry.py`；`backend/tests/unified_engine/test_qe_results_only_retry.py` | DESIGN_READY | none |
| F-412 | §9.2 | `backend/tests/multi_alpha/test_durable_retry.py`；`backend/tests/multi_alpha/test_durable_execution_adapter.py` | DESIGN_READY | none |
| F-413 | §9.3 | `backend/tests/multi_alpha/test_durable_retry.py`；`backend/tests/multi_alpha/test_durable_execution_adapter.py` | DESIGN_READY | none |
| F-414 | §9.4、§20.2、§20.4 | `backend/tests/multi_alpha/test_durable_retry.py`；`backend/tests/multi_alpha/test_durable_contract.py` | DESIGN_READY | none |
| F-415 | §10、§16.6 | `backend/tests/multi_alpha/test_qe_submission_coordinator.py`；`backend/tests/multi_alpha/test_durable_capacity.py` | DESIGN_READY | none |
| F-416 | §10、§16.6 | `backend/tests/multi_alpha/test_active_execution_import.py`；`backend/tests/multi_alpha/test_durable_contract.py` | DESIGN_READY | none |
| F-417 | §11～§13 | `backend/tests/multi_alpha/test_durable_control.py`；`backend/tests/test_aistock_qe_mcp_servers.py`；`frontend/tests/quantevolver/multi-alpha-control.spec.ts` | DESIGN_READY | none |
| F-418 | §13、§14 | `frontend/tests/quantevolver/multi-alpha-control.spec.ts`；`frontend/tests/multi-alpha-combine-backtest.spec.ts` | DESIGN_READY | none |
| F-419 | §16 | `backend/tests/multi_alpha/test_durable_repository_postgres.py`；`backend/tests/multi_alpha/test_durable_orchestrator_restart.py`；`frontend/tests/quantevolver/multi-alpha-control.spec.ts` | DESIGN_READY | none |
| F-420 | §2.3、§15、§21 | `backend/tests/scripts/test_aistock_feature_workflow.py` | DESIGN_READY | none |

`DESIGN_READY` 仅表示设计合同完整，不表示代码、DDL、测试或运行已经实现。用户确认状态保留在文档头部和 PR，不进入产品状态机，也不使用 `APPROVED_BY_USER` 冒充真实确认。

## 19. Risks / 风险与对策

| 风险 | 对策 |
|---|---|
| pause/cancel 与 remote POST 竞态 | reservation/source claim 事务再次锁 parent；状态不再允许时禁止 POST |
| kill 返回成功但远端仍执行 | kill response 不作为终态；receipt/status/result 权威 reconcile，reservation 保持 |
| 终态 child 重开覆盖结果或 Archive | 使用 successor recovery run，来源终态对象永不改写 |
| recovery 外键阻断现有 DELETE 或来源删除后 successor 失效 | source FK `SET NULL`；lineage/hash 冻结，必要文件原子复制；只在 active source-copy 窗口用 command 形成瞬时互斥 |
| successor 只重跑目标导致派生指标不一致 | 预览并持久化确定性 dependency closure；派生值只从权威 raw metrics 重算 |
| retry mode 缺制品后静默换路径 | 三模式严格分派；返回 evidence 和补取建议，不 fallback、不淘汰方向 |
| 双击或重试产生重复 kill/attempt/run | command ledger + Idempotency-Key + payload hash + CAS |
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
- P0-4 明确后置，但 P0-2 所需的轻量恢复证据完整保留。

### 20.2 禁止静默错误

- kill/inspect unknown 保持可见；
- artifact 缺失、hash 不符、scope 过期、identity 冲突都有稳定 reason；
- retry mode 不自动切换；
- topology 不静默切换；
- command/control error 与研究结果 error 分离。

### 20.3 禁止业务逻辑偏移

- 组合、LOO、baseline、回测、训练和研究指标公式不变；
- whole-run retry、single-Alpha lifecycle、Archive source contract 保持；
- 派生值重算只使用原 raw metrics 和新依赖，不改写 raw 结果；
- 成功结果和完成竞态始终保留。

### 20.4 禁止未经确认的门禁/审批

- command 状态是技术幂等/执行事实，不是审批；
- capability/evidence 是对象和制品事实，不评价 Alpha；
- 缺失数据/制品不淘汰研究方向；
- 不新增 user approval、promotion、PASS/KILL/GO/STOP 状态；
- 当前 `pending` 仅表示用户尚未确认这份设计是否合入，不会进入产品运行状态机。

## 21. Rollout / Rollback / Production Gates / 发布回滚与生产门禁

### 21.1 本设计 PR

- 仅文档；`production_ddl_gate=noop`；
- `production_backend_dependency_gate=noop`；
- `production_frontend_dependency_gate=noop`；
- 不改数据库、不改 runtime、不重启服务、不启动实验。

### 21.2 后续代码 PR

1. 先在 DEV PostgreSQL 验证 additive migration/preflight/rollback；不额外导出数据库；
2. 完成 source + targeted/expanded validation 后提交 PR；
3. 合入不等于生产 DDL/依赖/运行激活；
4. 只有用户分别授权后，才应用生产 DDL、安装依赖或重启运行；
5. rollback 优先停用 P0-2 API/UI/MCP/control consumption，保留 command/recovery 证据；不得删除已产生的 run/child/attempt/result/Archive。

### 21.3 当前状态

- P0-1B source：merged；
- P0-1B reservation DDL：production applied and verified；
- P0-1B runtime：此前已完成启动 smoke；当前后端进程按用户明确要求停止，本设计不得重启；
- P0-2 design：ready，等待用户确认；
- P0-2 code/DDL/runtime：not started。

## 22. 退出条件与下一步

本阶段的退出条件仅是：本文通过 F2 文档 validator、父蓝图同步、diff 审核并形成待确认 PR。用户确认前不合入；用户确认设计后才进入 P0-2A～P0-2E 编码。

研发过程中出现缺失数据、缺失制品、远端 unknown 或部分失败时，继续记录、补取、交叉验证和恢复，不得据此删除研究方向或私增审批门禁。
