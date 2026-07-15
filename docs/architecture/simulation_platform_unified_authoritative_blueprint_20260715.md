# AIstock LocalSIM / MiniQMT 模拟盘统一权威蓝图

> 文档状态：`design_ready`；本文完成只代表整体设计可直接实施，不代表所有运行时代码、迁移、生产绑定或交易日验证已经完成。
>
> 权威级别：模拟盘平台唯一上位蓝图（Single Authoritative Blueprint）
>
> Feature Workflow：`F2 / T3 design-driven`
>
> 适用模块：StrategyPackage admission、Selection/Target/Rebalance、LocalSIM、MiniQMT SIM、`simulation_runtime`、`miniqmt_execution_runtime`、Paper v2 durable facts、QMT ledger、TCA、diagnostics、metrics、runbook
>
> 基线日期：2026-07-15
>
> 首次基线代码：`main@954e7ac691975b0d88cfb658835b380cca912c13`

## 0. 权威声明与执行红线

### 0.1 唯一权威关系

本文是 AIstock 模拟盘平台的唯一上位设计蓝图。任何 LocalSIM 或 MiniQMT SIM 的新功能、BUG 修复、重构、迁移、运行配置、测试方案、专项详细设计和 operator runbook，都必须：

1. 引用本文稳定的 `F-*` Design Acceptance Index；
2. 不得与本文的业务语义、数据来源、身份、状态机、事务、执行路径和失败语义冲突；
3. 在同一个 PR 中更新 §15 进度账本、实现引用、测试证据和运行/生产状态；
4. 若确需改变本文条款，必须先前后一致地修改本文、专项设计、迁移/兼容方案和验收矩阵，并取得用户对业务变化的明确确认；
5. 不得在代码完成后通过修改本文来掩盖设计偏移。

既有文档按以下关系保留，不再拥有与本文竞争的上位权威：

| 文档 | 关系 | 保留内容 |
| --- | --- | --- |
| `simulation_remediation_project_design_20260521.md` | 历史基线，已被本文取代 | 早期问题背景和历史决策 |
| `miniqmt_unified_vnpy_execution_runtime_design_20260608.md` | 下位专项契约 | MiniQMT 唯一 runtime 与 vn.py 语义映射 |
| `miniqmt_durable_execution_runtime_design_20260623.md` | 下位专项契约 | durable event loop、OMS、callback 和恢复 |
| `miniqmt_phase7_b_fallback_retirement_evaluation_20260623.md` | 历史阶段记录 | 2026-06-23 当时的 A/B fallback 决策，不约束当前目标态 |
| `localsim_strategy_package_single_admission_f2_design_20260714.md` | 下位专项契约 | 策略包唯一一次准入和 package type 兼容 |
| `miniqmt_adaptive_is_phase0_tca_design.md` | 下位专项契约 | Phase 0A TCA 事实面 |
| `miniqmt_adaptive_is_phase1_quote_contract_design.md` | 下位专项契约 | `B0_QUOTE_V2` quote/control/evidence 契约 |
| `miniqmt_intraday_execution_strategy_analysis_20260710.md` | 下位算法域蓝图 | `ADAPTIVE_IS_L1` 目标算法，不拥有平台执行路径 |
| `miniqmt_adaptive_is_phase0b_baseline_observation_design.md` | 下位阶段设计 | B0 基线观察；必须在本文 P0 修复满足后实施 |

当下位文档与本文冲突时，以本文为准；同时必须在修复该冲突的同一 PR 中更新下位文档，不能长期保留双重解释。

### 0.2 四项不可裁剪约束

所有设计、代码审核、BUG 修复、PR、CI 和完成汇报必须分别证明：

- `no_simplified_delivery`：不得用简化版、子集版、POC、placeholder、mock-only、静态成功或只覆盖 happy path 冒充完整实现；
- `no_silent_error`：不得用 `except: pass`、非法值归零、默认业务值、旧缓存、内存结果或成功外壳掩盖失败；
- `no_business_semantic_drift`：不得擅自改变信号、选股、数量、价格、方向、执行算法、身份、时序、数据来源、事务或 broker side effect；
- `no_unrequested_gate_or_approval`：不得新增用户未要求的 RBAC、approval、人工 acknowledge、confirm-run、双人复核、permit 或人工恢复门禁。

### 0.3 允许的技术条件不是人工审批

以下条件属于模拟盘正确运行必须具备的局部、确定、可自动恢复事实，不属于审批功能：

- 交易日历、交易时段和市场阶段；
- 对应场景所需行情/分钟数据的来源、时钟、完整性和 freshness；
- 已冻结 release/binding/profile/policy identity；
- SIM 账户、资金、持仓、T+1、板块手数、涨跌停、停牌、价格和数量合法性；
- durable repository、schema、幂等、去重、单 writer、事务和 readback；
- broker/QMT 连接、订阅、callback、订单/成交 reconcile；
- 明确设计的 LIVE 安全锁。本文不授权 LIVE。

合法数据、连接或配置恢复后，SIM 生命周期必须自动继续或在下一调度 tick 重试；不能等待人工点击解除。

## 1. Background / 背景

AIstock 已形成共享的策略包、选股、目标仓位和执行计划链，并已实现 MiniQMT durable event loop、`B0_QUOTE_V2` 五档行情契约、quote evidence、TCA 以及 LocalSIM durable Paper v2 投影。2026-07-15 的定向代码审计和直接测试同时证明，平台仍有跨文档、跨执行后端的结构性缺陷：

1. LocalSIM 的 live V25 broker 仍在提交时一次性消费“当前已经看到的分钟 bars”，默认允许部分成交，却把订单立即终结；尚未消费的盘中 schedule 没有 durable continuation owner。
2. LocalSIM Paper v2 投影按 order/fill/event/cash/position/snapshot/run 顺序独立写入，缺少覆盖全链的数据库事务或 durable outbox；中途失败可能留下部分事实。
3. LocalSIM position mark 会从计划 `reference_price` 或 `limit_price` 补值，可能把执行意图价格当成账户估值行情。
4. MiniQMT 产品主路已是 tick callback 驱动的 `MiniQMTExecutionRuntime`，但 `LEGACY_B0` binding、Paper v2 `MiniQMTSimBackend`、raw QMT order API 和兼容入口仍存在，尚未达到“只有 B0_QUOTE_V2 tick 路径”的目标态。
5. MiniQMT 恢复判断仍有非法计数 `except ...: pass`/归零；raw batch API 即使部分或全部失败仍返回顶层 `success=true`，会形成假绿色。
6. `test_unattended_roll_forward_preserves_b0_quote_control_without_revision_drift` 使用 in-memory runtime repository 时仍泄漏到生产 `StrategyPackageRepository`，没有 DB 凭据即失败，说明测试隔离不完整。
7. 2026-05-21 总体设计、2026-06 MiniQMT runtime 设计、2026-07 LocalSIM admission 和 Adaptive IS 设计各自声明权威，已不适合继续分散指导平台开发。

本蓝图统一这些边界，并把“代码已合入”“生产 DDL/配置已应用”“服务已重启”“binding 已迁移”“交易日真实 SIM 已验证”保持为互不替代的事实。

## 2. Scope / 范围

### 2.1 范围内

- 新增 `SINGLE_ALPHA`、`MULTI_ALPHA` 及合法模型类型的策略包进入模拟盘后的统一身份和运行链；
- Selection/Target/Rebalance 的共享上游边界；
- LocalSIM 历史日和当日盘中的分钟事件执行；
- MiniQMT SIM 的实时 tick callback 执行；
- `ExecutionPlan`、parent/algo/child/order/trade/cash/position/TCA/quote evidence 的 durable 事实链；
- scheduler、重启恢复、跨日终结、迟到事实、幂等和 reconcile；
- `LEGACY_B0` 与旧产品旁路的迁移和退役；
- read-only diagnostics、metrics、alerts 和 operator runbook；
- 设计、代码、测试和生产状态的持续进度同步。

### 2.2 支持的策略包

新进入平台的策略包必须在唯一 admission 边界完整校验一次，并生成 immutable admission receipt。支持：

- 单 Alpha 与多 Alpha；
- 需要模型代码文件的模型类型；
- 设计上不需要模型代码文件的模型类型；
- manifest 明确声明的模型资产、数据资产和 runtime contract 组合。

Selection、Advisory、LocalSIM、MiniQMT SIM 和 QMT ledger 不得再次执行策略包内容完整性校验。它们只核对冻结 identity/receipt，并检查本次运行动态需要的组件。

### 2.3 当前目标环境

- `environment=SIM`；
- LocalSIM 为模拟成交后端；
- MiniQMT 连接 SIM 账户；
- 本文不授权 LIVE broker side effect；
- 本设计 PR 不执行 DDL/DML、不写生产配置、不调用 broker、不重启服务。

## 3. Non-Goals / 非目标

- 不改变策略信号、选股或荐股业务逻辑；
- 不把 LocalSIM 和 MiniQMT 合并为相同市场数据源或相同成交模型；
- 不用分钟线、TDX quote、timer 或合成数据替代 MiniQMT tick callback；
- 不要求 LocalSIM 改成 tick 撮合；LocalSIM 的权威粒度仍是 causal minute bar；
- 不在本文实现 `ADAPTIVE_IS_L1` 或改变 B0 下单决策；
- 不修复历史缺少 admission receipt 的遗留单 Alpha 数据；旧包可按生命周期淘汰；
- 不新增审批、RBAC、人工确认或 operator acknowledge；
- 不把设计完成描述为代码完成、运行激活或生产可用。

## 4. Architecture / 目标架构

### 4.1 统一主链

```text
StrategyPackage one-time admission
  -> immutable PackageAdmissionReceipt
  -> StrategyRuntimeRelease + SimulationReleaseBinding (frozen identity)
  -> shared SelectionEvidence
  -> TargetPortfolio / RebalanceIntent
  -> immutable ExecutionPlan
       |
       +-> LocalSIM MinuteExecutionRuntime
       |     -> causal minute-bar event
       |     -> durable algo/order/fill/cash/position facts
       |
       +-> MiniQMTExecutionRuntime
             -> XtQuant subscribe_whole_quote callback
             -> get_full_tick bootstrap only
             -> B0_QUOTE_V2 normalized tick/context/evidence
             -> vn.py-derived AlgoInstance
             -> OMS/Gateway -> MiniQMT SIM broker
             -> order/trade callback + reconcile facts

Both backends
  -> shared run lifecycle / idempotency / recovery
  -> TCA and performance projections
  -> read-only diagnostics / metrics / alerts / runbook
```

Selection/Target 只决定“买卖什么和目标数量”；LocalSIM/MiniQMT 执行层决定“在各自真实市场数据语义下如何执行”。两层不得反向耦合：执行故障不能修改 alpha 信号或重新选股，选股也不能绕过 execution policy 直接下单。

### 4.2 所有权

| 能力 | 唯一 owner | 禁止的第二 owner |
| --- | --- | --- |
| 策略包完整性校验 | StrategyPackage admission | Selection、Paper runner、LocalSIM、MiniQMT、QMT binding 再校验资产 |
| release/binding identity | `StrategyRuntimeReleaseService` + durable repository | runtime 根据“当前 manifest”重写冻结身份 |
| 日级生命周期 | `SimulationLifecycleScheduler` | router、页面请求或旧 Paper day runner 自建调度 |
| LocalSIM 日内执行状态 | `LocalSimMinuteExecutionRuntime`（本蓝图要求新增/收敛） | submit-time 一次性 broker object 终结全天计划 |
| MiniQMT 产品执行 | `MiniQMTExecutionRuntime` | `MiniQMTSimBackend`、raw QMT router、day runner 直接 `place_order` |
| MiniQMT 行情订阅 | scheduler-owned quote ingress supervisor | 每个策略/请求各自订阅 |
| OMS/ledger 事实 | durable repository single writer | JSON/in-memory snapshot 冒充权威事实 |
| 运行诊断 | read-only ops/diagnostics | diagnostics 启动 feed、修复数据或触发 broker |

### 4.3 LocalSIM 事件模型

LocalSIM 必须从“一次 submit 立即终结”升级为 durable minute event loop：

```text
PLAN_ACCEPTED
  -> WAITING_FOR_CAUSAL_BAR
  -> ACTIVE
       -> BAR_APPLIED (0..N)
       -> PARTIALLY_FILLED (0..N)
       -> ACTIVE
  -> FILLED | CANCELLED | REJECTED | EXPIRED_WITH_RESIDUAL
  -> PERSISTED_TERMINAL
```

每个 intent/algo 至少持久化：

- `plan_id`、`intent_id`、`algo_instance_id`、symbol、side；
- target/total/filled/remaining quantity；
- schedule version/hash、next slice index、last applied bar identity/time；
- causality cursor、market session、trade date；
- latest order/fill/cash/position sequence；
- terminal status/reason 和 residual classification；
- idempotency key 和 state hash。

当次 tick 只消费新到达且晚于 causality cursor 的 bars。部分成交后 remaining quantity 继续由后续分钟事件驱动；不能因为当前 bars 用尽就把全天计划报成功。收盘后才可根据完整 schedule、可交易性和 residual policy 终结。

### 4.4 MiniQMT tick 模型

MiniQMT SIM 的产品执行行情只有以下来源：

1. `subscribe_whole_quote` 的真实 XtQuant callback；
2. 订阅建立时 `get_full_tick` 只用于 bootstrap 当前 snapshot；
3. callback 进入 Phase 1 normalizer/ordering/freshness/clock/tradability；
4. `B0_QUOTE_V2` context/evidence durable ack 后驱动 `MiniQMTExecutionRuntime`；
5. runtime 通过唯一 OMS/Gateway 提交 child，broker order/trade callback 及 reconcile 反向更新状态。

禁止：分钟线代理 tick、定时器合成 tick、普通 quote 合成 auction 字段、提交后只查一次、旧 compiler/day runner 直接下单、B0_V2 失败时回退 LEGACY_B0。

### 4.5 Durable fact plane

事实链必须可从任一 terminal run 重建：

```text
admission_receipt
  -> release_id -> binding_id -> daily_run_id
  -> selection_evidence_id -> target/rebalance ids -> execution_plan_id
  -> algo_instance_id -> child_intent_id
  -> order_event_id <-> broker_order_id
  -> trade_event_id <-> broker_trade_id
  -> cash_entry_id / position_lot_id / account_snapshot_id
  -> market_data_id / quote evidence / markout / TCA receipt
```

写路径必须有单 writer、单调递增 sequence、canonical economic hash、幂等 key、重复冲突检测和 readback。DB 成功前不得返回 durable success；内存 snapshot 和日志不能替代事实落库。

## 5. Contracts / 契约

### 5.1 `FrozenSimulationIdentityV1`

| 字段 | 约束 |
| --- | --- |
| `package_id/package_manifest_sha256` | 来自唯一 admission receipt，运行时不可重算替换 |
| `admission_receipt_id/admission_hash` | required；证明 package type 和所需资产已在入口校验 |
| `release_id/release_hash` | immutable |
| `binding_id/binding_hash` | immutable；绑定 backend/account/capital/profile/policy |
| `runtime_profile_version_id` | immutable |
| `execution_policy_id/policy_hash` | immutable |
| `quote_control_revision` | MiniQMT required；目标态仅 `B0_QUOTE_V2` 或未来经本文批准的 revision |
| `created_at/effective_from/retired_at` | append-only lifecycle |

相同 release/binding 未发生 manifest 变化时允许无人值守 roll-forward；不能伪造 successor。真实新 manifest 必须在 admission 产生新 receipt/release，而不是 runtime 热替换。

### 5.2 `LocalSimExecutionStateV1`

`LocalSimExecutionStateV1` 是每个 plan/intent 的 durable 权威状态。canonical identity：

```text
state_id = sha256(
  "localsim_execution_state_v1",
  binding_id,
  trade_date,
  plan_id,
  intent_id,
  algo_instance_id
)
```

每次状态转移使用 compare-and-swap：`expected_sequence + expected_state_hash -> next_sequence + next_state_hash`。同一 bar identity 重放必须得到相同结果且不重复成交；不同 payload 复用同一 bar identity 必须 typed conflict。

### 5.3 LocalSIM market-data contract

- 历史日：权威持久分钟数据，不要求当天实时 quote；
- 当日盘中：TDX realtime minute source，只在交易时段检查正常 freshness；非交易时段不得用盘中 freshness 阈值报 stale；
- 每个 bar 必须有 symbol、exchange/trading date、bar end time、OHLCV、source identity/hash；
- 禁止 future bar、跨日 bar、重复不同 payload 或无单位 volume；
- suspend/limit/pre-close/lot 等 V25 所需字段缺失时 typed failure，不得默认。

### 5.4 LocalSIM terminal contract

`SUCCEEDED` 必须同时满足：

- 所有 plan intents 均有 terminal state；
- 每个 filled quantity 与 fill/cash/position facts 闭合；
- 未成交 residual 具有明确、设计允许的 terminal reason；
- durable transaction/outbox 完成并独立 readback；
- account snapshot 使用权威市场 mark；
- run、Paper v2 projection 与 performance/TCA 引用同一 generation。

当前时点 bars 用尽、只有部分成交、只有 order 没有 fill/cash、或写入了部分表，都不得返回成功。

### 5.5 LocalSIM transaction/outbox contract

单个 minute event 的业务提交必须在一个数据库事务内完成：

1. lock/CAS execution state；
2. append order/fill/order-event/cash/position deltas；
3. 更新 account snapshot generation；
4. append run event；
5. 更新 runtime state/sequence；
6. 写 projection outbox；
7. commit 后独立 readback。

Paper v2 日快照、TCA/performance 等可派生投影由 transactional outbox 重放。投影失败不能回滚已经提交的经济事实，但必须保持 `PROJECTION_RETRYABLE` 并自动重试，不能把 run 报为完全成功。

只允许注册的连接中断、serialization/deadlock/lock timeout 做有界重试。schema/hash/CAS/idempotency/business conflict 不重试，直接 typed failure。

### 5.6 Market mark contract

账户估值只能使用与 snapshot time 对齐的权威行情：

- 当日：已接受的 realtime minute close/quote mark；
- 历史日：权威 EOD/minute close；
- 停牌：上一合法交易日 close，必须标注 `SUSPENDED_PREV_CLOSE` provenance；
- 缺失：`LOCALSIM_MARK_PRICE_MISSING`，不得生成成功快照。

`reference_price`、`limit_price`、order price、fill-independent plan price 不能作为 position mark fallback。

### 5.7 MiniQMT control assignment contract

每个 `binding_id + trade_date + parent_id` 只允许一个冻结 control revision。目标态为 `B0_QUOTE_V2`；active parent 不能在运行中切换 revision。`LEGACY_B0` 仅允许读取历史事实和完成迁移前已存在的 active parent，不接纳新 parent。

### 5.8 MiniQMT route uniqueness contract

所有产品下单、撤单、replace、reconcile 操作必须进入：

```text
SimulationLifecycleScheduler
  -> MiniQMTExecutionRuntimeClient
  -> MiniQMTExecutionRuntime
  -> OMS/Gateway
```

`backend/routers/qmt.py` 的 raw order/batch/cancel 产品入口必须退役，或改为调用同一 runtime operator command；不得直接 `XtQuantQMTClient.place_order`。`MiniQMTSimBackend` 和旧 Paper v2 day runner 不得拥有 broker side effect。

### 5.9 Error and health contract

任何外部 payload/DB fact 的数量、状态、价格、时间解析失败必须产生：

- stable `reason_code`；
- 原始字段名和值的安全结构化上下文；
- run/binding/runtime identity；
- 明确 `retryable` 或 `terminal`；
- metrics/diagnostics 可见；
- 调用方可判定的非成功结果。

禁止 `_safe_int -> 0` 用于业务状态、`except ...: pass` 后返回无 side effect、raw batch 顶层恒真、scheduler alive 代替 daily run healthy。

### 5.10 Scheduler health contract

状态必须分层：

| 层 | required truth |
| --- | --- |
| process | scheduler thread/task 是否存活 |
| lifecycle | 当前交易日 tick、阶段、last success/error |
| binding | 每个 binding 的 plan/run/submit/reconcile 状态 |
| backend | LocalSIM source 或 QMT connection/subscription/callback 状态 |
| durability | latest committed sequence、outbox lag、projection status |
| business | orders/fills/residual/cash/position/TCA closure |

只要任一 active binding 失败，整体状态不能只返回绿色；同时单个 binding 失败不得阻塞其它独立 binding。

### 5.11 Blueprint progress synchronization contract

每个涉及模拟盘的功能或 BUG PR 必须在同一 PR 更新 §15 对应行；没有既有行时新增稳定 `SIM-P-*` 行。更新至少包含：

- `last_verified_main` 或待合入 commit；
- PR/BUG/feature identifier；
- implementation refs；
- 直接 nodeid/CI/运行 readback；
- `source_merge`、`production_ddl`、`production_config`、`restart`、`binding_migration`、`runtime_observation` 六类独立状态；
- 新发现风险及其 `F-*` 映射。

同一 PR 未更新蓝图属于开发交付不完整，不能用 PR body 或聊天记录代替。Git 历史承担变化审计，§15 只维护当前摘要，不追加无限流水账。

## 6. MiniQMT 旧路径迁移与退役

### 6.1 术语消歧

- 旧 durable runtime 文档中的 `A`：event-loop runtime；它已经是唯一 runtime 目标。
- 旧文档中的 `B/compiler`：一次性 compiler/fallback，必须保持退役。
- `LEGACY_B0`：旧 quote/control revision，不等于旧 `B/compiler`。
- `B0_QUOTE_V2`：当前唯一允许新增 parent 的基准 quote/control revision。
- `ADAPTIVE_IS_L1`：未来算法 revision，不等于 runtime A，也不等于 B0。

任何实现和指标必须使用完整枚举，禁止只写模糊的“A/B/B0”。

### 6.2 迁移顺序

1. read-only inventory：binding、active parent、open broker orders、runtime events、quote revision；
2. 证明 B0_V2 scheduler context、subscription、callback、durable evidence 和 restart recovery；
3. 停止创建新的 LEGACY_B0 binding/parent；
4. 对无 active parent/open order 的 binding 创建保持 package/release/account/capital/policy 等价的新 B0_V2 binding；
5. active LEGACY parent 继续原 revision 到 terminal，不在运行中切换；
6. independent readback 证明新 binding identity 和旧 binding retired state；
7. 删除旧产品调用和 direct broker side effect；
8. 保留历史 decoder/read model，删除 runtime fallback；
9. static path uniqueness 和真实 SIM 证据通过后结束退役。

生产 binding DML 必须另行获得用户授权。不得用手工改 DB 代替代码和迁移契约。

### 6.3 Rollback 边界

迁移前应用回滚是部署上一个已验证 build；已创建的 B0_V2 parent 不回写为 LEGACY_B0。若 B0_V2 发生故障：

- 停止接纳新 parent；
- active parent 按冻结 revision 自动 drain/reconcile；
- 保留 broker/evidence facts；
- 修复后从 durable state 恢复；
- 不重新启用旧产品旁路。

## 7. Diagnostics、Metrics、Alerts 与 Runbook

### 7.1 Read-only diagnostics

必须可按 `trade_date/binding_id/run_id/runtime_id/plan_id` 定向读取：

- frozen identity/hash chain；
- scheduler phase 和 last tick；
- LocalSIM per-intent schedule/filled/remaining/last bar/sequence；
- MiniQMT quote lease/subscription/generation/context/control revision；
- parent/algo/child/order/trade/reconcile 链；
- transaction/outbox/projection lag；
- terminal/residual/error reason；
- TCA/markout/evidence completeness。

diagnostics 绝不启动 feed、修改状态、重放订单、修复 DB 或触发 broker。

### 7.2 Bounded metrics

允许 label：backend、control_revision、status、reason_code、market_phase、source。禁止 run/order/symbol/package 等高 cardinality label。

至少包含：

- scheduler tick success/failure/lag；
- binding success/retryable/terminal；
- LocalSIM active algo、bar lag、partial/residual、transaction failure、outbox lag；
- MiniQMT callback age、normalized/rejected、pending algo、submitted child、reconcile mismatch；
- invalid payload count；
- false-green prevention count；
- durable readback mismatch。

### 7.3 Alerts

- 交易时段 scheduler 停止或 tick lag；
- active binding 连续失败；
- LocalSIM causal bar 不推进；
- MiniQMT subscription active 但 callback/normalized tick 不推进；
- durable sequence/hash/readback conflict；
- active algo 在收盘后仍无 terminal classification；
- outbox backlog 超阈值；
- raw/direct broker path 被调用。

告警是可观测通知，不是运行审批。恢复后自动解除；不要求人工 acknowledge 才恢复业务。

### 7.4 Operator runbook

runbook 固定顺序：process → lifecycle → binding → data/backend → durable facts → broker/reconcile → TCA。每步给出只读命令、正常/异常示例、reason code、自动恢复条件和需要代码修复的条件。禁止“先重启看看”、手工改状态或删除事实。

## 8. Risks / Failure Modes / 风险与失败模式

| Failure mode | Required behavior | Forbidden behavior |
| --- | --- | --- |
| LocalSIM 只取得早盘 bars | 保持 ACTIVE，后续 bar 继续 schedule | 部分成交后把全天 run 报成功 |
| LocalSIM commit 中途失败 | 事务回滚或 outbox retry，typed failure | 留下半套事实后报 PERSISTED |
| mark 缺失 | loud missing，不能生成成功快照 | 用 reference/limit/0/成本价代替 |
| MiniQMT tick stale/invalid | 当前 symbol/revision fail closed 并可观测 | 回退 LEGACY/minute/旧缓存 |
| broker callback 迟到/重复 | economic hash 去重并 reconcile | 重复成交或静默丢弃冲突 |
| scheduler 单 binding 异常 | 记录该 binding 失败，继续其它 binding | 整个调度 tick 被历史异常饿死 |
| 非法 count/time/price | typed invalid payload | pass、归零、默认成功 |
| 重启时 active state | 从 DB state/outbox/broker facts 重建 | 依赖进程内对象或重复 submit |
| 跨交易日 active residual | 明确 expire/carry policy 并终结当日 generation | 静默带入次日或改写昨日 |
| 旧 route 被调用 | loud route-retired error/静态测试失败 | compatibility no-op 或 direct broker |
| 设计与代码不同 | 同 PR 更新设计并取得必要确认 | 事后把偏移写成“符合现实” |

## 9. Implementation Plan / 实施方案

### P0-A：LocalSIM durable minute event loop

承接 `F-007` 至 `F-012`：建立 durable per-intent/algo state、逐分钟 causal 消费、partial continuation、收盘 residual、重启恢复。不得继续扩展 submit-time 一次性终结模型。

### P0-B：LocalSIM 原子事实与权威估值

承接 `F-013` 至 `F-015`：repository transaction/CAS/outbox/readback；移除 reference/limit mark fallback；保证 run/Paper projection/account snapshot/TCA generation 闭合。

### P0-C：MiniQMT B0_V2 单一路径和旧路径退役

承接 `F-016` 至 `F-019`：冻结 B0_V2 assignment、迁移 LEGACY binding、移除 Paper v2/direct raw broker side effect、保留历史 read model。必须先证明 active/open-order 安全边界，再执行生产 DML。

### P0-D：Fail-loud health、diagnostics 和 test isolation

承接 `F-020` 至 `F-023`：移除 silent count/price/time parse、raw batch 假成功、scheduler false green；修复 repository fixture 泄漏；补齐 read-only diagnostics、metrics、alerts、runbook。

### P1-A：Phase 0B B0 baseline observation

在 P0-A 至 P0-D 的适用前置事实通过后，按下位 Phase 0B 设计实施观察 spec、freeze artifact 和 baseline receipt。Phase 0B 不改变 broker action。

### P1-B：Adaptive IS 分阶段实现

Phase 0B 可重建基线完成后，`ADAPTIVE_IS_L1` 才按下位算法蓝图和独立阶段设计实施。不得提前创建可达 broker submit 路径。

## 10. Verification Plan / 验证方案

### 10.1 Direct contract tests

- one-time admission：单/多 Alpha、需要/不需要模型代码、receipt identity；
- frozen release/binding/hash 和 unchanged manifest roll-forward；
- LocalSIM minute state、duplicate/out-of-order/cross-day bars；
- 240-minute schedule、partial fill、停牌/涨跌停、odd lot/T+1；
- MiniQMT real tick projection、B0_V2 revision、no minute synthesis；
- strict invalid count/time/price/error contract。

### 10.2 Repository and transaction tests

- 每个写点 fault injection，证明 LocalSIM economic facts 原子提交；
- CAS conflict、idempotent retry、duplicate economic hash；
- commit success/readback failure 和 outbox replay；
- process crash at before-commit/after-commit/before-projection；
- migration preflight、apply、重复 apply、readback、rollback；
- late order/trade/fill 和 archive read path。

### 10.3 Restart and trading-day tests

- 上午部分执行 → 进程退出 → 下午恢复；
- 午休期间不误判行情 stale；
- 收盘 residual terminalization；
- 次日 roll-forward 不改昨日 identity；
- MiniQMT pending algo 在 restart 后由 tick 驱动且不重复 child；
- 单 binding 失败不影响其它 LocalSIM/MiniQMT binding。

### 10.4 Route uniqueness tests

- 产品源代码中 direct `place_order` owner 仅允许 Gateway；
- router/Paper v2/day runner 无 broker side effect；
- MiniQMT quote 来源没有 minute-bar adapter；
- `LEGACY_B0` 不接纳新 parent；
- raw batch 任一失败时顶层状态准确；
- retired route 调用 loud failure，无 compatibility no-op。

### 10.5 Real-path evidence

每个生产相关 slice 分别记录：source merged、CI、DDL、config、restart、binding DML、readback、正常交易日 LocalSIM、正常交易日 MiniQMT、broker/reconcile/TCA。任何前项不能替代后一项。

### 10.6 Coverage

新增/修改 Python line coverage >= 80%，branch coverage >= 70%；交易执行、cash/position/ledger、失败恢复和 no-silent-fallback 必须有 direct business oracle。广泛回归交由 CI/Validation Center/nightly，但设计条款没有证据时仍不能标记完成。

## 11. Rollout / Rollback / 发布与回滚

### 11.1 代码发布

每个 P0 slice 独立 worktree/PR，按 Design Acceptance Index 验收。代码 merge 不自动启用生产配置，不自动执行迁移，不自动改变 binding。

### 11.2 DB 迁移

如 LocalSIM state/outbox 需要新 schema，必须提供 forward migration、幂等 preflight、exact CHECK/enum、rollback、DEV migration tests 和生产 readback。只有用户明确授权才可执行生产 DDL；不得在 SQL 前增加项目未要求的数据库导出。

### 11.3 Runtime rollout

生产步骤固定为：merge → dependency/DDL gate → 用户重启 → readback → binding migration DML（单独授权）→ 正常交易日观察。Codex 不自行重启服务。

### 11.4 Application rollback

回滚到上一个已验证 build；不删除已提交 facts，不反向改写 parent revision，不用 LEGACY 路径承接新的 B0_V2 work。必要 projection 可从 durable facts 重建。

## 12. Production Gates / 生产门禁

本文设计 PR：

- `production_ddl_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
- runtime restart：`noop`
- production DML/config：`noop`
- broker side effect：`noop`

这些是交付状态标签，不是产品审批功能。各代码 slice 必须按实际变化独立填写，不能沿用本文的 noop。

## 13. Design Acceptance Index / 设计验收索引

| ID | Stable design item |
| --- | --- |
| `F-001` | 本文是模拟盘平台唯一上位蓝图，下位设计不得竞争权威 |
| `F-002` | 每个模拟盘功能/BUG PR 同步更新本文进度和证据 |
| `F-003` | 四项 DESIGN-COMPLIANCE 红线不可裁剪 |
| `F-004` | 策略包只在 admission 完整校验一次并兼容合法 package/model 类型 |
| `F-005` | release/binding/profile/policy identity 冻结，runtime 不二次校验或热替换 manifest |
| `F-006` | Selection/Target 与执行层隔离且 LocalSIM/MiniQMT 共享 broker-neutral 上游 |
| `F-007` | LocalSIM 使用 causal minute event loop，不以一次 submit 终结全天计划 |
| `F-008` | LocalSIM partial fill、remaining schedule 和收盘 residual 完整 |
| `F-009` | LocalSIM restart 从 durable state 恢复且不重复成交 |
| `F-010` | LocalSIM 历史/当日/午休/非交易时段数据 freshness 语义正确 |
| `F-011` | LocalSIM V25/market-state/lot/T+1/limit/suspend 语义不漂移 |
| `F-012` | LocalSIM terminal success 要求所有 intents 和 residual 闭合 |
| `F-013` | LocalSIM economic facts 以 transaction/CAS/single-writer/outbox 持久化 |
| `F-014` | LocalSIM retry/dedupe/readback/projection failure 语义完整 |
| `F-015` | LocalSIM account mark 只使用权威市场价格，无计划价 fallback |
| `F-016` | MiniQMT 唯一产品 runtime 是 callback-driven `MiniQMTExecutionRuntime` |
| `F-017` | MiniQMT 新 parent 仅使用 `B0_QUOTE_V2`，不回退 LEGACY_B0 |
| `F-018` | LEGACY binding/active parent 安全迁移并保留历史事实 |
| `F-019` | Paper v2/day runner/raw QMT 等 direct broker 产品旁路退役 |
| `F-020` | count/time/price/status 非法值 fail loud，无静默归零/跳过 |
| `F-021` | scheduler/process/binding/backend/durability/business health 分层且不假绿色 |
| `F-022` | diagnostics 只读，metrics/alerts bounded 且恢复不要求人工 ack |
| `F-023` | test fixtures 完全隔离生产 repository/DB/broker |
| `F-024` | code/DDL/config/restart/binding/runtime evidence 分别追踪 |
| `F-025` | Phase 0B 和 Adaptive IS 只能在本文对应前置事实完成后实施 |

## 14. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-001` | §0.1 authority table and subordinate-doc banners | authority keyword/link scan | design_ready | none |
| `F-002` | §5.11 same-PR progress contract and §15 ledger | changed-simulation-PR blueprint diff review | design_ready | none |
| `F-003` | §0.2、§16 | DESIGN-COMPLIANCE-001 four-control review | design_ready | none |
| `F-004` | §2.2 one-time admission contract | single/multi Alpha and model-code-required/optional admission tests | design_ready | none |
| `F-005` | §5.1 `FrozenSimulationIdentityV1` | unchanged/changed manifest and immutable binding tests | design_ready | none |
| `F-006` | §4.1 shared main chain | selection-target-execution isolation tests | design_ready | none |
| `F-007` | §4.3 LocalSIM event model | 240 causal minute-bar schedule test | design_ready | none |
| `F-008` | §4.3、§5.4 | partial fill and remaining schedule tests | design_ready | none |
| `F-009` | §5.2、§10.3 | crash/restart/no-duplicate fill tests | design_ready | none |
| `F-010` | §5.3 | historical/current-day/lunch/non-session freshness tests | design_ready | none |
| `F-011` | §5.3、§10.1 | V25 market-state/lot/T+1/limit/suspend regression tests | design_ready | none |
| `F-012` | §5.4 terminal contract | unclosed intent/residual negative tests | design_ready | none |
| `F-013` | §5.5 transaction/outbox contract | per-write fault injection and atomicity tests | design_ready | none |
| `F-014` | §5.2、§5.5 | CAS/retry/dedupe/readback/outbox replay tests | design_ready | none |
| `F-015` | §5.6 market mark contract | reference/limit/default-price rejection tests | design_ready | none |
| `F-016` | §4.4、§5.8 | XtQuant callback source and route owner tests | design_ready | none |
| `F-017` | §5.7 | B0_V2 frozen assignment and no LEGACY fallback tests | design_ready | none |
| `F-018` | §6.2 migration sequence | active/open-order migration and historical readback tests | design_ready | none |
| `F-019` | §5.8、§6 | Paper/day-runner/raw-router direct broker static guards | design_ready | none |
| `F-020` | §5.9 | malformed count/time/price/status fail-loud tests | design_ready | none |
| `F-021` | §5.10 | process/binding/backend/durability/business false-green tests | design_ready | none |
| `F-022` | §7 | diagnostics no-side-effect and bounded metrics/auto-clear alert tests | design_ready | none |
| `F-023` | §10.1、§10.4 | in-memory fixture no-DB/no-broker isolation tests | design_ready | none |
| `F-024` | §5.11、§10.5、§15 | independent merge/DDL/config/restart/binding/runtime state receipts | design_ready | none |
| `F-025` | §9 P1 sequencing | Phase 0B/Adaptive acceptance mapping and boundary validator | design_ready | none |

## 15. Current Implementation Progress Ledger / 当前实现进度账本

状态枚举：`IMPLEMENTED_VERIFIED`、`REPAIR_REQUIRED`、`EVIDENCE_REFRESH_REQUIRED`、`DESIGN_ONLY`、`HISTORICAL_RETIRED`。本表记录当前摘要；详细历史以 Git/PR/BUG/CI 为准。

| Progress ID | Acceptance IDs | Current state at `main@954e7ac6` | Evidence | Status | Next implementation slice |
| --- | --- | --- | --- | --- | --- |
| `SIM-P-001` | `F-004..006` | 单/多 Alpha 策略包一次准入、冻结 identity 和 broker-neutral selection/target 已建立 | `localsim_strategy_package_single_admission_f2_design_20260714.md`、PR #2103 | IMPLEMENTED_VERIFIED | 持续防止 runtime 二次 package 校验 |
| `SIM-P-002` | `F-005,016,017` | BUG-654/657 已修复 B0 context 发布、lot/tradability authority、失败持久化和安全恢复 | commits `02e73de6`、`f4392711`；本设计核对 2026-07-15 相关 direct tests 7 passed | IMPLEMENTED_VERIFIED | 纳入唯一路径退役验证 |
| `SIM-P-003` | `F-005` | BUG-658 已允许 unchanged authoritative manifest roll-forward 且拒绝虚假变更 | commit `43ce19de`；本设计核对 2 direct tests passed | IMPLEMENTED_VERIFIED | 保持 frozen identity contract |
| `SIM-P-004` | `F-007..012` | LocalSIM 仍由 submit-time `execute_order` 消费当前 bars，默认 partial，订单立即 terminal | `paper_trading_v2/broker/localsim.py:94,357-368` | REPAIR_REQUIRED | P0-A |
| `SIM-P-005` | `F-013,014` | LocalSIM Paper facts 仍为多个 repository 方法顺序写入，无覆盖全链事务/outbox | `simulation_runtime/scheduler.py:7273-7465` | REPAIR_REQUIRED | P0-B |
| `SIM-P-006` | `F-015` | position marks 仍可从 plan reference/limit price 补值 | `simulation_runtime/scheduler.py:7606-7627` | REPAIR_REQUIRED | P0-B |
| `SIM-P-007` | `F-016` | MiniQMT canonical runtime 已有真实 tick callback、bootstrap、durable event loop | Phase 1 design/PR #2019、BUG-604/614 tests | IMPLEMENTED_VERIFIED | 唯一路径静态与真实 SIM 持续验证 |
| `SIM-P-008` | `F-017..019` | LEGACY_B0、Paper `MiniQMTSimBackend`、raw QMT direct order/batch 仍存在 | `paper_trading_v2/broker/minqmtsim.py:247`、`routers/qmt.py:220-466` | REPAIR_REQUIRED | P0-C |
| `SIM-P-009` | `F-020,021` | MiniQMT recovery/pending/submitted 解析仍有 pass/归零；raw batch 顶层恒真 | `scheduler.py:7776-7794,7897-7914`、`routers/qmt.py:458-464` | REPAIR_REQUIRED | P0-D |
| `SIM-P-010` | `F-023` | roll-forward fixture 泄漏到生产 StrategyPackage repository；无 DB 密码时失败 | 2026-07-15 targeted matrix：9 passed, 1 failed；失败 nodeid `test_unattended_roll_forward_preserves_b0_quote_control_without_revision_drift` | REPAIR_REQUIRED | P0-D |
| `SIM-P-011` | `F-022,024` | Phase 1 quote diagnostics/evidence 已存在，但平台级 LocalSIM/MiniQMT 聚合 health/runbook 尚未按本文统一 | Phase 1 diagnostics/runbook、当前 scheduler ops | REPAIR_REQUIRED | P0-D |
| `SIM-P-012` | `F-024,025` | Phase 0A 专项文档只明确记录 0A-0..0A-3；当前源码已有 TCA read API/EOD/projector 组件，专项进度与代码证据尚未重新闭合 | Phase 0A 专项文档顶部状态、`simulation_runtime/tca_*` 当前源码 | EVIDENCE_REFRESH_REQUIRED | 在相关 TCA PR 前先按当前 main 刷新专项证据和本行 |
| `SIM-P-013` | `F-016,017,022,024` | Phase 1 A-E、B0_V2 activation wiring 和 quote evidence/diagnostics 已合入；生产 DDL/config/restart/binding/真实 SIM 必须继续单独记录 | Phase 1 专项设计 §13、PR #1988/#1994/#2005/#2011/#2019/#2033 | IMPLEMENTED_VERIFIED | 按本蓝图 P0-C/D 收敛旧路径和平台 health |
| `SIM-P-014` | `F-025` | Phase 0B 详细设计在 PR #2141，已在该 PR 声明本文为唯一上位蓝图并映射适用 P0 前置；设计尚未合入 | PR #2141 authority-sync commit、F2 validator 与 CI；state OPEN（2026-07-15 readback） | DESIGN_ONLY | 合入状态、实现、DDL/spec DML、窗口启动和真实观察继续分别更新 |
| `SIM-P-015` | `F-025` | `ADAPTIVE_IS_L1` 仅有算法域蓝图和 Phase 0A/1 基础，不存在经本文批准的可达新算法 broker submit | algorithm domain blueprint、Phase 0A/1 designs | DESIGN_ONLY | Phase 0B 可重建基线完成后再做阶段设计 |

每次更新本表必须使用当时最新 `origin/main` 和可重复证据；不得把旧运行快照写成当前事实。若只完成代码而没有生产授权，状态说明必须明确 `source merged`，不能写成 runtime activated。

## 16. DESIGN-COMPLIANCE-001 设计复核

| Control | Review result | Design evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | LocalSIM event loop、durability、迁移、诊断、测试和生产状态均定义完整；未把现有子集写成目标完成 |
| `no_silent_error` | pass | §5.9、§8 和 `F-020..023` 明确 typed failure、readback 和 false-green 反例 |
| `no_business_semantic_drift` | pass | §3、§4、§5 固定 Selection/Execution 隔离、数据源、V25 与 B0 语义；变化必须先更新蓝图 |
| `no_unrequested_gate_or_approval` | pass | §0.3 区分技术条件与审批；禁止 RBAC/ack/confirm-run，自动恢复 |
| design-to-implementation traceability | pass | §13 稳定索引、§14 验收矩阵、§15 当前进度和 same-PR 更新契约 |
| production state separation | pass | §10.5、§11、§12、`F-024` 分离 merge/DDL/config/restart/binding/runtime evidence |

## 17. Definition of Done

本蓝图本身完成的条件是：权威关系、目标架构、契约、迁移、失败模式、测试、发布/回滚、生产边界、稳定索引和当前进度均完整，并通过 F2 validator 与 DESIGN-COMPLIANCE-001。

模拟盘平台整体完成必须是 §15 所有 `REPAIR_REQUIRED` 项均由真实实现和直接证据更新为 `IMPLEMENTED_VERIFIED`，Phase 0B/Adaptive 阶段按各自批准范围完成；在此之前，只能汇报具体 slice 的完成状态，不能宣称整个平台已经完成。
