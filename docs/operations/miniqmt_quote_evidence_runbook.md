# MiniQMT quote evidence operator runbook

本文覆盖 P1-D 的 durable quote evidence、markout、auction observation、metrics、只读诊断，以及 P1-E 的 `B0_QUOTE_V2` assignment/action/child/markout readback。它不创建或修改 binding/config，不启用 ingress，不调用或重放 broker，不修改 `LEGACY_B0`，也不构成 LIVE 或 `ADAPTIVE_IS_L1` 操作手册。

## 当前自动边界

- `MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED=false` 是默认状态；P1-D/P1-E 代码合入不改变它。switch=false 时只允许已有 durable active `B0_QUOTE_V2` runtime 自动恢复为 `DRAINING`，不得接纳新 assignment。
- `production_ddl_gate=pending` 前，不能启用 quote ingress；代码合入、单元测试通过或本地 SQL 文件存在都不是 production DDL 已应用的证据。
- P1-D 没有审批、RBAC、人工 acknowledge、confirm-run 或“手工恢复成功”状态。数据、连接或生命周期条件恢复后，下一个合法 lifecycle tick 自动再次尝试；历史 evidence 永不原地修改。
- durable ack 只在 repository 事务提交并 post-commit readback 核对 `event_id/type/source/hash/created_at` 后成立。日志、outbox 入队、metrics 或 API `ok=true` 都不能替代它。

`GET /simulation-runtime/scheduler/status` 的
`miniqmt_quote_ingress_activation` 是 production composition 的只读状态：

- `BLOCKED`：process switch=true，但 production schema readback 非 exact target 或读取失败；
  `factory_available=false`，不得创建 B0 runtime，也不得把它解释成 LEGACY fallback。
- `READY`：switch=true、`production_ddl_gate=applied_and_verified`、唯一 scheduler factory 已构造；
  这只证明代码入口可用，不证明 binding 已创建、feed 已订阅或 broker 已调用。
- `DRAINING`：switch=false、lazy factory 仅允许 durable active algo/child runtime 恢复；普通
  LEGACY startup 不读 schema、不构造 QMT/subscriber。真正恢复时才要求 schema exact 并构造唯一
  supervisor；`accept_new_assignments=false`。无 durable active fact 的新/空/terminal runtime 必须在
  runtime/gateway 构造前拒绝。
- `DISABLED`：switch=false 且 schema 未就绪/不可读，未构造 factory；LEGACY 继续。此状态不能恢复
  active B0，因此已有 B0 parent 时不得回滚 production CHECK schema。

同时核对 `process_config_sha256` 与 `runtime_config_sha256`：DRAINING 时 runtime config 仅把 admission
switch 保持为上一启用值以恢复原 evidence config identity，因此两个 hash 有意不同并被显式报告；
其他 process capacity 值不得在 drain recovery 中被推断或静默替换。status/readback 不启动 subscriber，
只有合法 controller consumer acquisition 才建立 physical feed。

## 只读诊断

以下端点不会构造 subscriber、scheduler、gateway 或 broker client，不会写 event、自动 repair 或重连：

```text
GET /simulation-runtime/miniqmt/quote-diagnostics?runtime_id=<runtime_id>&symbol=<optional>&cursor=<optional>&limit=1..500
GET /simulation-runtime/miniqmt/quote-evidence?runtime_id=<runtime_id>&market_data_id=<id>|evidence_id=<id>&include_archived=false&cursor=<optional>&limit=1..500
```

`quote-diagnostics` 的 `ok=true` 仅说明查询完成。必须单独查看 `health.status`、`production_ddl_gate`、outbox backlog、persist failure、最近 reason/stage、markout coverage 和分页 cursor。无 durable health row 时返回 `UNKNOWN`，不得将其解释为 healthy。

`quote-evidence` 的每条 `durable_receipt.durable_ack`、`readback_verified` 与 `link_complete` 独立判断；`link_complete=false` 时必须读取 `missing_links`，字段缺失和 identity 无法 readback 分别显式报告。空 records 只是没有匹配的 evidence，不是 action/child/trade/markout 闭环成功。两个 endpoint 都把 `(sequence,event_id)` cursor 下推到 repository，禁止先加载完整 runtime journal 再内存分页。诊断响应不输出 account id、secret 或完整 raw callback；symbol 和受限 hash/link 仅在该 runtime 的分页查询中出现。

## 自动故障处理语义

| 条件 | 可观察 reason/stage | 自动语义 | 禁止操作 |
|---|---|---|---|
| high-priority evidence outbox 满 | `ADAPTIVE_IS_MARKET_DATA_EVIDENCE_OUTBOX_FULL` / `PERSIST` | 对应未来 B0_QUOTE_V2 symbol 保持 fail-closed；health/cadence 使用独立低优先级 slot；已发生 child receipt 使用显式 reserve；无关 symbol 可继续 | 丢弃 action/child/markout、让 health/cadence 挤占、提交 broker |
| transient PostgreSQL SQLSTATE | `...EVIDENCE_PERSIST_FAILED` / `PERSIST` | 仅 `08000/08001/08003/08006/40001/40P01/55P03/57P01` 使用显式有界指数退避 | retry schema/CHECK/FK/hash 冲突；回退 JSON 或内存成功 |
| runtime-event CHECK 在运行期漂移 | `MINIQMT_KERNEL_EVENT_SCHEMA_CONSTRAINT_FAILED` / `CALLBACK|WATCHDOG` | 首次真实 `23514` fail loud；按 runtime、lifecycle generation、operation 单写，60/120/240/480/960/1920/3600 秒自动退避；未到期 callback 零 repository 访问且返回显式 suppression；每个 runtime 仅有一个持久 callback actor，quote 只进入持有该 symbol 的 runtime，同 symbol 多 runtime 才 fan-out；阻塞 runtime/supervisor/release worker 不阻塞 peer | 每个 tick 重试 SQL、向非 owner runtime 广播、每 tick 创建线程、停订阅、切 legacy、人工 acknowledge/RBAC/审批 |
| logical consumer / physical lease / callback actor 身份漂移 | `MINIQMT_K6_PRODUCT_CALLBACK_WORKER_OWNER_DRIFT`、`LEASE_OWNER_DRIFT` 或 `ACQUIRE_ROLLBACK_UNKNOWN` / `WATCHDOG|RELEASE` | 使用 lock-free immutable lease snapshot 精确核对 consumer、symbols、sink、generation、physical subscription；合法 union/rebuild/release-survivor rollover同步刷新全部 survivor；missing/forged/stale owner聚合失败后继续peer。acquire rollback不能证明完成时保留exact retained owner并自动二次release | 调用持锁health阻塞cadence、信任本地consumer冒充physical lease、丢弃ghost lease、人工清理owner |
| callback 在 10ms 内尚未完成 | `ASYNC_IN_FLIGHT` / `CALLBACK` | 返回 frame-bound pending carrier，`executed=true/outcome_pending=true/business_success=null`；actor 最终成功才清除 active failure，最终失败保留 runtime/worker evidence | 固定 `True` ACK、把 pending 记为业务成功、等待 callback 阻塞全部 peer |
| callback 晚到的非 schema 失败 | `MINIQMT_K6_PRODUCT_OPERATION_FAILED` / `CALLBACK` | health 保持 `RETRY_READY` 和 active failure，下一条合法 live quote 自动重试；成功后才恢复 `HEALTHY` | 日志后把 health 记为 healthy、无限 backoff、人工 acknowledge 或新业务 gate |
| B0 controller create/release 部分完成或 readback unknown | `RELEASE_UNKNOWN` / `B0_QUOTE_V2_RELEASE` | 稳定 bound sink identity；create/rollback/release均携exact symbols+sink；先unregister sink再release physical lease。仅两步都闭合后标记CLOSED；unknown保持fenced owner并由后续cadence精确重试 | 先释放physical lease、release失败仍标记CLOSED、盲目重注册、删除retained owner |
| scheduler pre-plan broker side effect 为 `UNKNOWN` | `MINIQMT_K6_PRODUCT_SCHEDULER_TICK_*` / `MINIQMT_K6_PRODUCT_SCHEDULER_TICK` | 只接受current plan deterministic runtime/generation/token；global/unmatched failure在全部peer尝试后形成bounded receipt。UNKNOWN按durable outbox、account/date/binding/run/runtime/order authority自动reconcile并受durable backoff约束；零side-effect证明后才建replacement，已存在side-effect则自动terminal/pending | 同binding同日即视为owner、每tick扫描broker、直接重建计划、人工acknowledge/审批 |
| writer batch / generation replay 中途失败 | `WRITER_FRAME_SINK_FAILED`、`WRITER_BATCH_ABORTED_AFTER_FRAME_SINK_FAILURE`、`PENDING_PUBLISH_REPLAY_REJECTED` 或 `PENDING_PUBLISH_REPLAY_ABORTED` | 对失败帧和所有已 pop 未处理帧逐一记录 bounded reason/count/generation/sequence；不产生成功 ACK | 静默丢弃 batch 尾部、清空 backlog 后假报成功 |
| idempotency conflict | `...EVIDENCE_IDEMPOTENCY_CONFLICT` / `PERSIST` | coordinator 进入 FAILED，保留两侧 hash context | 覆盖 event、换 event type/source、宣称 durable ack |
| target 后无可证明首个 mark quote | `...MARK_WINDOW_EXPIRED` 或 `...MARKOUT_QUOTE_UNAVAILABLE` / `MARKOUT` | 追加 stable `UNAVAILABLE` mark | 等待午休后、下一交易日或选择更晚 quote |
| history/generation/restart gap | `...MARKOUT_HISTORY_UNAVAILABLE` / `MARKOUT` | 追加 stable `UNAVAILABLE`，不猜测首个 quote | 用重启后的 latest quote 替代 |
| continuous segment 结束 | `...MARKET_SESSION_ENDED` / `MARKOUT` | 追加 stable `UNAVAILABLE` | 跨午休、收盘或交易日取 mark |
| auction 原始字段不可用 | `...CLOSING_AUCTION_CAPABILITY_UNAVAILABLE` / `ELIGIBILITY` | `OBSERVE_ONLY` unavailable evidence；continuous 不受阻塞 | 从 last/pre-close/L1-L5/limit/15:00 合成 auction 字段 |

持久化 retry 达到配置上限后 health=`FAILED`；这不是需要人工确认的状态。后续合法 lifecycle epoch 可以自动建立新的 writer，但不得把旧 outbox 内容替换为新 evidence 或改变既有 market/action/trade identity。

## DDL gate 与 production readback

BUG-1019 的唯一 operator artifact 是下列三件套；20260712 的两约束 migration 已退休，不能再用于 KERNEL_V2。应用启动、诊断 API 和 agent 均不得自动执行生产 DDL：

```text
backend/migrations/miniqmt_execution_kernel_event_contract_repair_20260811.preflight.sql
backend/migrations/miniqmt_execution_kernel_event_contract_repair_20260811.sql
backend/migrations/miniqmt_execution_kernel_event_contract_repair_20260811.rollback.sql
```

canonical-LF SHA-256 固定为：preflight=`013ca9838ff0f88bdd3c30682895114adc5a2c7d9d07832516cb63bf6f5f1217`，forward=`b1cf49270234af5034461fc6c6c30e6ee56c2278defb922fb3b4d879cd9c3e9a`，rollback=`741d6cd667600d2ae09be15da28a5b928f86a4248706ff2c3a65e235ff170c96`。执行前必须从实际 checkout 以 canonical LF 独立重算并相等；不得从日志或 PR 描述抄写后冒充 readback。

forward 在一个锁定事务中把 `ck_miniqmt_event_id`、`ck_miniqmt_event_sequence`、`ck_miniqmt_event_type`、`ck_miniqmt_event_source`、`ck_miniqmt_k2_event_composite`、`ck_miniqmt_k2_event_contract` 六项完整 authority 从 exact immediate predecessor 原子替换为 exact target，并验证全部六项。target 的 identity/type/source/composite/contract CHECK 均使用二值 `IS TRUE`，因此即使未来列的 NOT NULL 属性发生漂移，NULL 也不能借 PostgreSQL 三值 CHECK 漏过。artifact 的 transaction-local `search_path` 固定为 `pg_catalog,qmt_strategy,pg_temp`，显式把临时 schema 放到最后；K2-D helper 继续保持冻结合同 `proconfig IS NULL` 并继承该调用上下文。catalog canonical order 固定 `COLLATE "C"`；target no-op 不重建 helper，也不改变 OID/xmin/body/config。

执行顺序固定如下：preflight 自己拥有一个 RR/RO transaction，在同一 transaction 内完成 assertion 和 receipt 后提交；forward 与 rollback 各自先提交锁定的 DDL transaction，再启动独立 RR/RO post-COMMIT assertion/receipt transaction。禁止额外加 `--single-transaction`：

```powershell
psql -X -v ON_ERROR_STOP=1 -f backend/migrations/miniqmt_execution_kernel_event_contract_repair_20260811.preflight.sql
# 仅在 production_ddl_gate 已由用户明确授权后：
psql -X -v ON_ERROR_STOP=1 -f backend/migrations/miniqmt_execution_kernel_event_contract_repair_20260811.sql
```

preflight 只读且必须先成功；forward 只允许在用户明确生产 DDL 授权后执行。项目已有每日数据库备份，本流程不要求也禁止 agent 自行导出、备份或创建快照。执行 DDL 时 backend-main 必须保持用户已停止状态；启动/停止/重启始终由用户负责，不因 DDL、merge 或 aftercare 自动授权。

forward 的独立 production readback receipt 必须同时保存：database/user/table OID、server version、database collation；六个 CHECK 的 name/OID/validated/definition SHA；K2 与 K2-D helper 的 OID/body SHA/config；helper、独立重算与 code-owned 的 K2/K2-D catalog SHA 及各自 verified 布尔；durable event/KERNEL_V2 counts和查询时间。三份 artifact 都必须先 exact 校验 K2-D helper 的无参签名、SQL/STABLE、`proconfig IS NULL` 与冻结 body，再独立执行冻结 body，并与 helper readback、code-owned K2-D catalog SHA 三方相等；最终 receipt 显式输出这三份 SHA 与 equality 结果，仅输出 helper 返回值不构成 readback。application 的 `applied_and_verified` 还必须由 `PostgresMiniQMTKernelRepository.preflight_schema()` 在同一 RR/RO snapshot 与完整 relation locks 下闭合；单独六-CHECK receipt 只能报告 `pending_full_kernel_readback`，不得产生假绿色。

rollback 命令仅用于已明确授权的生产 rollback：

```powershell
psql -X -v ON_ERROR_STOP=1 -f backend/migrations/miniqmt_execution_kernel_event_contract_repair_20260811.rollback.sql
```

rollback 只接受 exact target，且必须证明 KERNEL_V2 event 及全部 K2、K2-D、K6 durable fact tables 均为零；任一 fact 存在即 `destructive rollback refused`。它不删除、改写、归档或重排任何 durable row。提交后第二个 RR/RO transaction 必须重新锁定完整 graph、重数每个 successor fact table、验证 predecessor 六项 CHECK、helper body/config、独立 K2/K2-D catalog，并在 receipt 中输出逐表 counts。第二次 rollback 是真实 no-op，不得重建 helper。

source merge、生产 DDL、用户重启和 runtime 生效是四个独立状态。DDL 独立 readback 成功后由用户启动 backend-main，再核对 `GET /api/v1/runtime-identity` 的 commit 与 main/deployed source 一致，随后只读检查 `GET /api/v1/simulation-runtime/scheduler/status`：schema gate 必须为 `applied_and_verified`，无同 constraint 高频重试，健康 peer cadence 不受失败 runtime 阻塞。未完成这些 readback 前，close-sync 保持 pending。

## 留存、指标与告警

- action/reject/child/protection/markout evidence 与 trade/child anchors 不按普通 sequence prune；至少 active 90 天且关联 trade 的 60/300/900 秒 mark 全部 terminal 前保持 active。满足条件后只能 soft archive，`include_archived=true` 仍须重建同一 identity 闭包；任何 pending mark anchor 都不受 count prune。
- `QUOTE_OBSERVED` cadence 和 `QUOTE_INGRESS_HEALTH` aggregate 在 active 14 天后可 soft archive；archived row 只能通过 `include_archived=true` 的只读 readback 查询，绝不 delete。
- metrics 的 labels 仅限 market、capture type、state、reason code、stage、horizon、source method、quantile 等有界值；严禁 runtime/binding/parent/child/trade/market-data/symbol/account id labels。
- 观察 metric/alert 永远 `observation_only=true`、`execution_gate=false`、`requires_acknowledge=false`。告警不是 broker gate、审批或人工任务。

## P1-D / P1-E 边界

P1-D 提供 evidence contract、repository/readback、migration、markout selector、auction manifest、metrics、只读 diagnostics 和本 runbook。它不接通 action submit/cancel 或 broker/reconcile，也不激活真实 SIM ingress。

P1-E 才负责冻结并显式绑定 `B0_QUOTE_V2`、真实 SIM parity、action durable-ack-before-submit 的运行接线与 Phase 0B production evidence。BUG-599/600/604/614 与 `LEGACY_B0` 的既有业务路径不属于 P1-D 改动范围。

## P1-E 只读核验

按 binding/date 生成 Phase 0B v2 export 时，只允许显式版本 `miniqmt_execution_tca_evidence_v2`。查询必须在一个 TCA-owned read snapshot 内先解析有限 parent/runtime IDs，再读取对应 runtime journal；不得全库 JSONB 扫描。v1 默认输出保持不变。

核验顺序固定如下：

1. `TCA_PARENT.lineage` 的 `binding_id/binding_hash/runtime_id` 与 `PARENT_ASSIGNMENT` 一一对应，且同 parent 只有一个 `control_revision/revision_id/assignment_id`。
2. `CONTROL_REVISION` 的 policy/adapter/code/schema hashes 与 assignment、`ACTION_INPUT`、`CHILD_RECEIPT`、markout 完全一致；任一 hash set 不唯一即 `quote_control_complete=false`。
3. `ACTION_EVENT.action_evidence_id/action_market_data_id` 必须命中 durable `ACTION_INPUT`；它之后只能出现同 deterministic action 对应的一个 `CHILD_EVENT`。
4. `CHILD_RECEIPT.source_child_event_id` 反向命中 child event，并保留 action evidence、anchor market data 和 receipt 自身 market data；append-only child event 不回填 receipt ID。
5. 每个 authoritative trade anchor 必须有 60/300/900 秒 terminal markout；captured 与 unavailable 都是明确结果，缺 horizon 不是零收益或成功。
6. 查看 manifest 的 `missing_link_count/duplicate_child_count/revision_conflict_count/hash_conflict_count/identity_conflict_count`、五档/age/cadence/markout coverage；`quote_control_complete=true` 才表示该 export 的链路完整，不表示 ingress 已启用或真实 SIM 已运行。

pending action 自动恢复只读取原 `action_evidence_candidate`、durable receipt、deterministic child 与 broker/reconcile fact。诊断或 runbook 不调用 submit/cancel、不重放 action、不修改 runtime event。parity violation 使对应 revision 停止接纳新 parent且不切回 LEGACY；assignment/persistence fault 只影响对应 runtime/symbol，均不需要人工 acknowledge、approval 或 RBAC。
