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

每次 callback/lifecycle evaluation 必须使用同一次采样得到的当前 wall clock 与 monotonic clock；scheduler 启动时刻或前一轮 tick 的时间不能复用为后续 quote eligibility 时钟。single writer 在生成 observation 时必须把同一个不可变 projection context 一并交付 controller；controller 先校验 observation/context identity，再保存这份原始 authority，不得在 callback sink 中重新读取可能已经推进的 current context 来猜测原始 identity。已接受 observation 只有 calendar/policy/continuity generation/clock domain/trade date/symbol authority 与当前 evaluation context 全部一致时才可在当前时钟重新评价；不得重写 observation identity，也不得用 timer 合成新 quote。

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

Selection inference 的 in-flight coalescing identity 至少包含 `package_id + manifest_sha256 + release_id + release_hash + trade_date + data_source + selection_runtime_config_hash`。不同 runtime release 即使共享 package/manifest/runtime config，也必须各自启动、记录和完成 inference；不得共享 future、结果或错误上下文。

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

只允许注册的连接中断、serialization/deadlock/lock timeout 做有界重试，单个 outbox 最多执行 3 次 projection attempt（首次 + 2 次自动重试）；第 3 次失败写 `local_sim_projection_terminal_failure_v1` 并进入 `FAILED_TERMINAL`。schema/hash/CAS/idempotency/business conflict 第一次即写同一 terminal failure receipt，不重试。projection 已提交后的独立 readback 使用单独的 `attempt_count`，最多自动复核 3 次；耗尽后保留 `PROJECTED` 事实、写 `local_sim_projection_readback_terminal_failure` 并 fail loud，不得重放 projection side effects。

P0-B 的现行持久化 schema 固定为：

- `local_sim_economic_receipts_v1`：按 generation 保存 `LocalSimEconomicReceiptV1`，其 `economic_hash` 覆盖 order/fill/order-event/cash/state/position/mark/account snapshot 的 canonical hash；
- `local_sim_projection_outbox_v1`：当前 generation 的 `LocalSimProjectionOutboxV1`，状态仅允许 `PENDING`、`PROJECTION_RETRYABLE`、`PROJECTED`，payload/hash/identity 不因重试改写；
- `local_sim_projection_receipts_v1`：成功重放后的 `LocalSimProjectionReceiptV1`，与 outbox、economic hash 和 projection hash 一一关联；
- `local_sim_projection_terminal_failure`：非重试错误或第 3 次 projection attempt 失败的 durable typed terminal receipt；保留 outbox/economic facts，不允许 scheduler 再自动执行 projection；
- `local_sim_economic_generation` 与 `local_sim_projection_generation`：run、Paper snapshot、performance/TCA 的共同单调 generation。

生命周期 scheduler 通过 `PaperTradingV2Repository.local_sim_economic_transaction` 取得 single-writer advisory lock 和唯一写连接，Paper facts 与 `SimulationRuntimeRepository.stage_local_sim_economic_commit` 共享该连接。事务提交后必须分别 readback economic receipt/state/outbox 和 Paper facts。projection 在第二个 single-writer 事务中重放；若 projection 已提交而独立 readback 失败，保持 `PROJECTED` 事实并写 `local_sim_projection_readback_failure`，下一次自动复核 readback，禁止重写经济事实或重复 run event。

### 5.6 Market mark contract

账户估值只能使用与 snapshot time 对齐的权威行情：

- 当日：已接受的 realtime minute close/quote mark；
- 历史日：权威 EOD/minute close；
- 停牌：上一合法交易日 close，必须标注 `SUSPENDED_PREV_CLOSE` provenance；
- 缺失：`LOCALSIM_MARK_PRICE_MISSING`，不得生成成功快照。

`reference_price`、`limit_price`、order price、fill-independent plan price 不能作为 position mark fallback。

持久化 mark 使用 `LocalSimMarketMarkV1`，必须带 `source`、实际行情 `as_of_time`、`provenance` 和 `mark_hash`。支持的 provenance 为 `REALTIME_MINUTE_CLOSE`、`HISTORICAL_MINUTE_CLOSE`、`SUSPENDED_PREV_CLOSE`；非有限值、非正值、未知 source、晚于 account snapshot 的 mark 或无法由上一交易日 close 证明的停牌价格均 typed failure。scheduler 只能调用 LocalSIM broker 的 `load_authoritative_position_marks`，由 broker 从本次执行使用的同一 `PaperV2MinuteMarketDataProvider` 读取截至 snapshot time 的最后一个真实分钟 close；停牌时必须调用 authoritative `PreviousCloseProvider`。通用 `current_prices`、`price_by_symbol` 仅用于 target/order 构建，不得转译成 position mark 或伪造 provenance。

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

event-loop retry 若发现同一 runtime 已持久化 parent intent，只能在 incoming request、durable batch、remark index 和 parent intent 构成完整精确所有权链，且 `broker_called=false/broker_call_pending=true` 时恢复原 batch。quote 导致的动态 preflight price 变化不得把原 batch 自身 remark 或 pending sell reservation 当成外部重复；恢复必须复用 durable request/preflight，不得再次扣减同一 strategy-slot lot。任一 parent 集合、业务字段、runtime identity、remark owner、batch link 或 submit 状态不一致必须 typed fail loud，不能放宽外部 duplicate-order 防线。

### 5.9 Error and health contract

任何外部 payload/DB fact 的数量、状态、价格、时间解析失败必须产生：

- stable `reason_code`；
- 原始字段名和值的安全结构化上下文；
- run/binding/runtime identity；
- 明确 `retryable` 或 `terminal`；
- metrics/diagnostics 可见；
- 调用方可判定的非成功结果。

禁止 `_safe_int -> 0` 用于业务状态、`except ...: pass` 后返回无 side effect、raw batch 顶层恒真、scheduler alive 代替 daily run healthy。

MiniQMT TCA observation 对“字段缺失”和“字段已提供但非法”必须严格分离：quote price/time 全部已提供别名都必须分别可解析且相互一致，price 拒绝 bool、非有限、非正数，time 拒绝空串/布尔/未知格式；缺少全部可选 quote 字段才允许显式 `MISSING/MISSING_TIME`。preflight `allowed` 必须是 exact bool，before/after quantity 必须是非负整数且 after 不得大于 before，classification flags 不得冲突，deadline `RESOLVED/UNRESOLVED` 必须与 deadline/reason 闭合。任一非法值必须抛带稳定 reason、字段、原始类型和有界安全值的 `TcaCaptureDataError`，由既有 sidecar single-writer 写入 durable `capture_errors`；error message 最长 2048、context 最多 20 项且值最长 512，单次 first-write capture attempt 明确 `retryable=false/terminal=true/observation_only=true/execution_gate=false`。TCA 是 observation-only，capture error 不得回滚、重试或改写已经确定的 B0 broker execution，也不得被解释为执行成功证据。

`ACTION_REJECT` 是 market-data evidence：有 normalized observation 时必须携带其 `market_data_id/tradability/raw ingress` 关联；没有可归属的 raw/normalized observation 时不得伪造 quote-less evidence。首次 quote 尚未到达或 context 尚无兼容 observation 时，runtime 必须持久化有界去重的 `b0_quote_v2_quote_waiting_v1` lifecycle event，明确 reason/stage/parent/algo/clock、`market_data_id=null`、`broker_called=false`，并保持 algo 等待真实 tick；wait event 必须携带由完整 semantic payload 计算的 deterministic fingerprint，controller 恢复时从包含 archived 的 durable journal 校验并重建已见 fingerprint/current wait，不得因进程重建重复写相同 runtime/algo/semantic wait；hash 或 event carrier 冲突必须 typed fail loud。持久化失败必须向调用方抛出，不能静默跳过。

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

background scheduler 的每次 loop tick 只有两种明确结果：`run_once()` 正常返回，或 top-level exception。后者即使 thread 仍存活，也必须原子更新当前 `last_result/last_blocking_result`，并暴露 exact `simulation_background_scheduler_loop_health_v1`：稳定 reason、exception type、最长 2048 字符 message、allowlist 且单值最长 512 字符的 context、首次/最近失败时间、连续/累计失败数和最近成功 tick。ops 聚合必须把活动 loop failure 计入当前技术 blocker 并返回 `BLOCKED`；background scheduler 声明 control API 可用却缺失或提供非法 loop health 时必须 typed fail loud，不能降级成绿色。非 background scheduler 仅显式投影 `NOT_APPLICABLE`。

loop health 是 process-local 当前故障面，`execution_gate=false`。下一次 `run_once()` 正常返回时自动清除 active failure、连续失败数归零，但保留 last failure/累计计数/最近成功时间；无需人工 acknowledge、审批或重启。跨重启的业务失败仍由 durable `simulation_daily_run` 当日 readback 提供，process-local loop health 不冒充数据库事实，也不改变任何 binding、执行计划或 broker side effect。

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

### 6.4 新 binding 与新 parent 的唯一准入契约

`StrategyRuntimeReleaseService.create_binding` 创建 `minqmt_sim` binding 时必须收到 exact
`miniqmt_quote_control_binding_v1/B0_QUOTE_V2`；省略、显式 `LEGACY_B0`、未知字段或未知 revision
均以 typed `MINIQMT_B0_QUOTE_V2_BINDING_REQUIRED` 拒绝。这里不允许把省略值默认升级成 B0，
也不允许把非法值解释为 LEGACY。`QuoteControlBindingV1.from_binding_config({})` 只为历史事实
解码保留 omitted -> LEGACY 规则，不是新 binding 的默认值。

`ExecutionPlanCompiler` 对 `minqmt_sim + LEGACY_B0` 禁止编译新 plan/parent，返回
`MINIQMT_LEGACY_B0_NEW_PARENT_FORBIDDEN` 且 `broker_called=false`。已经持久化的 LEGACY plan、
runtime、parent、child 和 broker fact 仍按冻结 revision 读回、恢复、reconcile 至 terminal；恢复路径
不得重新调用 compiler 创建 parent，也不得转换其 binding/revision/hash。

### 6.5 `MiniQMTRouteMigrationInventoryV1`

迁移只接受定向 source binding，不进行全表业务扫描。inventory 的 exact canonical fields 为：

| field | contract |
| --- | --- |
| `schema_version` | 固定 `miniqmt_route_migration_inventory_v1` |
| `source_binding_id/source_binding_hash` | 历史 LEGACY binding immutable identity |
| `target_release_id/target_release_hash` | 已具备 exact B0 policy/revision 的 immutable release |
| `effective_trade_date` | 新 B0 binding 生效日；source `effective_to = effective_trade_date - 1` |
| `runtime_ids_examined` | 同 account group 的 bounded durable runtime ids |
| `active_parent_ids` | source binding 的 active algo/parent ids |
| `active_child_order_ids` | source binding 的 active child ids |
| `broker_open_order_ids` | 通过 child broker id 或 strategy/order-remark attribution 归属 source 的可撤 broker orders |
| `broker_attribution_conflicts` | 同 strategy/prefix 但无法唯一关联 durable child 的 broker facts |
| `observed_at_utc` | timezone-aware inventory anchor；不得使用 DB write time 代替 |
| `inventory_sha256` | 对以上除自身外 canonical payload 的 lowercase sha256 |

只要 active parent、active child、open broker order 或 attribution conflict 任一非空，迁移必须
typed terminal/block，写入次数为零；不得取消旧订单、切换 active parent、静默忽略未知 broker fact，
也不得阻塞可明确归属其它 binding/slot 的订单。runtime repository 查询必须按 account group 和
非 terminal runtime 定向且有显式上限；超过上限返回 `MINIQMT_ROUTE_MIGRATION_INVENTORY_LIMIT`，
不截断后继续。

### 6.6 target marker、identity 与可重建 receipt

成功迁移在新 target binding 的 `binding_config_json.metadata.miniqmt_route_migration` 中持久化 exact
`MiniQMTRouteMigrationMarkerV1`：schema、source binding id/hash、target release id/hash、effective date、
source effective-to、inventory hash/observed-at、operator、applied-at 和 `marker_sha256`。marker 不包含
target binding hash，避免自引用；target binding hash 覆盖完整 marker。

独立 readback 从 source row、target row 和 target marker 重建 `MiniQMTRouteMigrationReceiptV1`，包含
`migration_id = mqrm_<canonical sha256 前 24 位>`、source/target binding id/hash、target release id/hash、
effective date、inventory/marker hash、`runtime_owner=MiniQMTExecutionRuntime`、
`source_control_revision=LEGACY_B0`、`target_control_revision=B0_QUOTE_V2`、applied-at 及
`receipt_sha256`。marker、binding hash、effective window、identity/equivalence 或 receipt 任一不一致均
fail loud；不得用内存对象或 apply 返回值冒充 readback。

source/target 必须保持 strategy、package、manifest、broker backend/account、account group、slot、capital、
strategy name、order remark prefix 和 approval state 等价；release id/hash 可因 B0 immutable policy 不同。
任何其它业务字段变化均为 `MINIQMT_ROUTE_MIGRATION_IDENTITY_CONFLICT`。

### 6.7 repository transaction、幂等、重试与失败语义

PostgreSQL apply 使用一个 connection/transaction：`SELECT source FOR UPDATE`、CAS 校验 source hash/window、
插入 immutable target binding、把 source `effective_to` 收口为生效日前一日、事务内 exact readback；
不修改 source `binding_config_json`、`binding_hash`、历史 plan/run/event/order/trade。InMemory repository
必须提供等价 snapshot rollback。target hash 已存在且 source window/marker 完全一致时返回同一 receipt；
同 source/effective date 指向不同 target 或不同 marker 时 terminal conflict。

只有连接、serialization、deadlock、lock-timeout 类错误允许最多 3 次完整事务重试；schema、identity、
inventory、CAS、hash、broker attribution 和 readback mismatch 不重试。commit 后必须使用新 connection
独立 readback；若 commit outcome 未知，只按 deterministic target hash/source window 查询，不重放写入，
直到读回同一 receipt 或产生 typed unknown-outcome failure。

### 6.8 产品旁路退役契约

- `/api/v1/qmt/order`、`/order/batch`、`/cancel` 永久返回 HTTP 410 和
  `MINIQMT_RAW_BROKER_ROUTE_RETIRED`，即使旧 diagnostics 环境变量或 trade password 存在也不得调用 QMT；
- Paper v2 `run-day` 遇到 `minqmt_sim` 返回
  `MINIQMT_PAPER_DAY_RUNNER_ROUTE_RETIRED`，指向 simulation lifecycle/runtime；
- `MiniQMTSimBackend` 仅保留 account/position/order/trade 历史 read model；其 submit/cancel 方法固定 typed
  `MINIQMT_PAPER_BROKER_SIDE_EFFECT_RETIRED`，不得调用 `place_order/cancel_order`；
- 真正 broker side effect 只允许 `MiniQMTExecutionRuntime -> QmtClientMiniQMTEventLoopGateway -> OMS/Gateway`。

这些是自动路由边界，不是人工审批、RBAC、acknowledge 或 confirm-run 门禁。read-only QMT status、account、
position、order、trade 与历史 diagnostics 保持可用。

### 6.9 生产工具与状态分离

迁移工具默认只读 dry-run，`--apply` 只执行上述 repository transaction；它不得创建/修改 StrategyPackage、
调用下单/撤单、重启服务或写配置。apply 前后的 broker 访问仅允许读取 cancelable orders 用于 inventory；
生产 DML 必须另获用户授权，且不以数据库导出/快照为前置条件。source merge、production DDL、production
config、restart、binding migration 和真实 SIM observation 六类状态继续独立记录。

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

MiniQMT quote diagnostics 的唯一权威投影为 `/api/v1/simulation-runtime/miniqmt/quote-diagnostics`。同一响应必须按 `runtime_id` 同时呈现 durable health ack/readback/event time/age、callback subscription progress、single-writer health、B0 controller health、gateway state 和 OMS state，并保留各层原始事实；canonical status 必须消费 durable payload 自身的 `HEALTHY/DEGRADED/FAILED`，并以当前 process config 的 evidence cadence 两倍作为只读 freshness 上限，超期必须给出稳定 `STALE` reason，durable `FAILED`、invalid readback/status 或未来事件时间不得被 live component 绿色覆盖。该 freshness 只影响 diagnostics，不成为执行、审批或人工确认门禁。缺少 durable health 时整体为明确 `DEGRADED` 与稳定 reason，而不是与 scheduler live telemetry 形成两个互相竞争的布尔状态。scheduler status 仅标注为 in-process live component telemetry；`/monitor/miniqmt/status` 仅是 legacy 手工接口连接状态，必须显式 `authoritative_for_simulation_runtime=false`，不得参与模拟盘健康判定。

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

`BUG-687` 将本节实现为唯一只读聚合端点 `/api/v1/simulation-runtime/platform-diagnostics` 和
`simulation_platform_diagnostics_v1`。查询可按 `trade_date/binding_id/run_id/runtime_id/plan_id`
组合定向读取；无 identity 时只读当前有效交易日，只带 `runtime_id` 时即使 daily run 尚未创建也联合
canonical MiniQMT quote health。响应固定包含 process、lifecycle、binding、backend、durability、
business 六层事实、最多 256 个低基数 metric series、最多 100 个当前 active alert、runbook ref 和
`side_effect_contract`。metrics label 仅允许 `backend/control_revision/status/reason_code/market_phase/source`；
run/order/symbol/package/strategy/binding/runtime/plan 只允许出现在 diagnostics/alert identity，不得进入
metric labels。scheduler tick lag 阈值为两倍 scheduler interval，LocalSIM causal bar/outbox backlog 的
只读通知阈值为 120 秒；阈值不改变执行资格。非法 bool/count/time/schema/identity/cardinality、超过 500
条的非定向扫描和 metric cardinality 超限均 typed fail loud，不裁剪成成功。alerts 每次从当前 facts
重算，恢复后自动消失，明确 `execution_gate=false/acknowledge_required=false`。完整命令、正常/异常示例、
reason 和恢复条件见 `docs/operations/simulation_platform_operator_runbook_20260717.md`。

## 8. Risks / Failure Modes / 风险与失败模式

| Failure mode | Required behavior | Forbidden behavior |
| --- | --- | --- |
| LocalSIM 只取得早盘 bars | 保持 ACTIVE，后续 bar 继续 schedule | 部分成交后把全天 run 报成功 |
| LocalSIM commit 中途失败 | 事务回滚或 outbox retry，typed failure | 留下半套事实后报 PERSISTED |
| mark 缺失 | loud missing，不能生成成功快照 | 用 reference/limit/0/成本价代替 |
| MiniQMT tick stale/invalid | 当前 symbol/revision fail closed 并可观测 | 回退 LEGACY/minute/旧缓存 |
| broker callback 迟到/重复 | economic hash 去重并 reconcile | 重复成交或静默丢弃冲突 |
| scheduler 单 binding 异常 | 记录该 binding 失败，继续其它 binding | 整个调度 tick 被历史异常饿死 |
| scheduler top-level loop 异常但 thread 仍存活 | 当前 loop health 与 ops 聚合立即 `BLOCKED`，成功 tick 自动清除 active failure并保留历史 | 只写日志、沿用旧绿色结果、要求人工 ack 或重启解除 |
| 非法 count/time/price | typed invalid payload | pass、归零、默认成功 |
| 重启时 active state | 从 DB state/outbox/broker facts 重建 | 依赖进程内对象或重复 submit |
| 跨交易日 active residual | 明确 expire/carry policy 并终结当日 generation | 静默带入次日或改写昨日 |
| 旧 route 被调用 | loud route-retired error/静态测试失败 | compatibility no-op 或 direct broker |
| 设计与代码不同 | 同 PR 更新设计并取得必要确认 | 事后把偏移写成“符合现实” |

## 9. Implementation Plan / 实施方案

### P0-A：LocalSIM durable minute event loop

承接 `F-007` 至 `F-012`：建立 durable per-intent/algo state、逐分钟 causal 消费、partial continuation、收盘 residual、重启恢复。不得继续扩展 submit-time 一次性终结模型。

`BUG-660` 实现回执（本变更）：

- `LocalSimExecutionStateV1` 按 `binding + trade_date + plan + intent + algo instance` 生成 canonical identity，包含冻结 schedule/plan hash、next slice、causality cursor、last bar identity、数量闭合、sequence、idempotency key 和 state hash；
- `simulation_daily_run.run_payload_json.local_sim_execution_states_v1` 是 P0-A durable state plane，repository 使用行锁和 batch CAS，独立 readback 校验 schema/hash；P0-B/BUG-661 已进一步把 economic facts、state、outbox 收敛为同一事务，并完成 `F-013..015` 的 source implementation 与直接验证；
- 当日 TDX LocalSIM 只调用 `execute_order_incremental`，当前无 bar 时持久化 `WAITING_FOR_CAUSAL_BAR`，partial 保持 `ACTIVE/INTRADAY_RUNNING`，restart 从 Paper order + durable state 恢复且只消费严格晚于 cursor 的 bar；历史闭市日仍使用完整权威分钟集同步执行；
- 同一 cursor bar readback payload 改写、duplicate/out-of-order bar、CAS/hash/identity drift、收盘 bar 缺失均 typed failure；完整收盘 bar 后剩余数量进入 `EXPIRED_WITH_RESIDUAL`，不得冒充成功；
- direct tests 覆盖 waiting、partial continuation、相同 tick replay、restart、不同 payload 冲突、CAS conflict、close residual、close bar missing，以及 scheduler 从计划到多 tick 终态的真实 `MinuteExecutionEngine` 路径；
- 本项不执行生产 DDL/DML/config，不调用 broker，不重启服务。source merge、运行重启和正常交易日证据继续分开记录。

### P0-B：LocalSIM 原子事实与权威估值

承接 `F-013` 至 `F-015`：repository transaction/CAS/outbox/readback；移除 reference/limit mark fallback；保证 run/Paper projection/account snapshot/TCA generation 闭合。

`BUG-661` 实现回执（本变更）：

- Paper order/fill/order-event/cash 与 LocalSIM state、economic receipt、run intermediate status、projection outbox 通过同一个 PostgreSQL transaction/connection 提交；InMemory repository 具有等价 rollback snapshot；
- canonical economic receipt/outbox/projection receipt 均执行 schema、identity、hash、generation、CAS 和独立 readback；同 state、同 causal bar 且无新经济 delta 的恢复重放复用原 generation，不重复 fill、run event 或 projection；仅 PostgreSQL connection/serialization/deadlock/lock 错误进入最多 3 次 projection attempt，business/schema/CAS 冲突立即 terminal，readback 具有独立 3 次复核预算；
- position/daily snapshot/performance/TCA generation 由 durable outbox 自动重放；projection 失败进入 retryable，projection 已提交但 readback 失败时只重试 readback，不反向改写经济事实；
- `reference_price`、`limit_price`、`current_prices`、`price_by_symbol` 的估值 fallback 已删除；账户只接受 LocalSIM broker 从同一执行行情 provider 读取、带真实 as-of/provenance/hash 的 realtime/historical/suspended-prev-close market mark；缺失或非法 mark 使用 `LOCALSIM_MARK_*` typed failure；
- direct tests 覆盖 PostgreSQL 单连接 commit/rollback、跨 repository 回滚、通用价格拒绝、realtime/historical/suspended mark provenance、projection readback 恢复、same-bar restart dedupe 和多分钟 partial-to-terminal generation；
- 本项不新增 DB object，不执行生产 DDL/DML/config，不调用生产 broker，不重启服务；source merge 与正常交易日 runtime evidence 继续分开记录。

### P0-C：MiniQMT B0_V2 单一路径和旧路径退役

承接 `F-016` 至 `F-019`：冻结 B0_V2 assignment、迁移 LEGACY binding、移除 Paper v2/direct raw broker side effect、保留历史 read model。必须先证明 active/open-order 安全边界，再执行生产 DML。

实施必须同时交付：new-binding/new-parent fail-closed、`MiniQMTRouteMigrationInventoryV1`、target marker 与
可重建 receipt、PostgreSQL/InMemory 原子迁移和独立 readback、dry-run/apply operator、raw/Paper broker
旁路 410/typed retirement，以及静态 route uniqueness。任一部分未完成时只能报告对应 slice，不能把
P0-C 标为完成；本项代码 PR 不执行 production DML、不调用 broker、不重启服务。

`B0_QUOTE_V2` parent 的初始参考价必须在 broker-neutral plan 构建时冻结：目标仍在当日 authoritative
selection 中时使用 target `reference_price`；仅对 `DROPPED_FROM_SELECTION` 的既有持仓使用同一
`SimulationRunContext.current_prices` 中的权威 current mark，并把 `reference_price`、
`reference_price_source` 纳入 intent/plan identity。运行时不得从普通 quote、broker cache、分钟线或
默认值合成该字段；冻结值缺失/非法必须在 broker call 前 fail loud，且不得改变方向、数量、T+1、lot、
limit/suspend 或选股语义。

同一 binding/trade-date 发生 side-effect-free plan rebuild 时，assignment transition 必须先于新 quote
context 对 callback consumer 可见。factory 仅可自动释放“无 active algo、无任何 child order”的旧
controller/lease；必须同时覆盖 runtime_id 相同和 plan identity 导致 runtime_id 变化两种情况。若旧
runtime 存在 active algo、pending action 或任一 child fact，则 typed assignment conflict、broker call=0、
不释放 lease、不切换 assignment，也不得回退 LEGACY 或要求人工 acknowledge。client 在 context 发布后
仍须做第二道 exact assignment/readback 防线，防止并发漂移。

### P0-D：Fail-loud health、diagnostics 和 test isolation

承接 `F-020` 至 `F-023`：移除 silent count/price/time parse、raw batch 假成功、scheduler false green；修复 repository fixture 泄漏；补齐 read-only diagnostics、metrics、alerts、runbook。

`BUG-668..672` source implementation（2026-07-16 批次）分别完成：跨 release selection inference 隔离；B0 callback 当前成对时钟与 observation authority pairing；自有 durable parent retry/remark/strategy-slot lot 恢复；无 quote 时 runtime wait event 取代非法 `ACTION_REJECT`；durable/live/controller/gateway/OMS canonical diagnostics 与 legacy authority 标注。五项保持独立 BUG/Issue 和直接验收证据，但在同一模拟盘 runtime 批次 PR 交付；本批次不执行 DDL/DML/config、不调用 broker、不重启服务，source merge、CI、restart 与正常交易日 runtime observation 继续分别记录。

`BUG-674..676` 针对 2026-07-16 运行证据补齐三个恢复/诊断断点：selection inference `IN_PROGRESS` 是持久化 `SIGNAL_GENERATING` 等待态而不是 `FAILED_RETRYABLE`，真实 timeout/worker error 仍 fail loud，完成后清除等待/失败诊断并继续同一 frozen identity；MiniQMT 仅在 immutable plan、`B0_QUOTE_V2` assignments、batch/runtime id、durable pending algo 数量、零 child/零 broker side effect 和 exact runtime-owned duplicate 结果全部闭合时，允许 `FAILED_RETRYABLE` 自动回到既有 tick driver，绝不重提 parent；background scheduler 保留最后一次 blocking result，operator status 还必须从 `simulation_daily_run` 有界 readback 当日失败，并原样呈现 selection/watchdog/runtime/quote context。所有状态均为技术恢复与只读诊断，不新增审批、RBAC、人工 acknowledge 或执行门禁。

`BUG-678` 修复验证环境本身的三类漂移：所有 custom selection fixture 显式提供 in-memory StrategyPackage lifecycle authority；真实 B0 callback harness 成对交付 observation 与原 projection context；ops/roll-forward fixture 复用显式注入的 in-memory scheduler/repository，不再连接生产 StrategyPackage DB。该修复只恢复测试隔离和当前接口契约，不放宽产品 runtime 的 package authority、B0 context pairing 或 fail-fast 行为。

`BUG-677` 修复 LocalSIM 上游 ingestion 的重启僵尸状态：`refresh_schedules()` 在注册/驱动 schedule 和 due target 前，使用既有 120 分钟 active-job lease 仅将超时 `running` job 原子更新为显式 `timeout`，保留原 summary 并写入版本化 reconciliation 证据；合法 `schedule_id` 同步投影为失败，持久 `data_sync_target_id` 写回 `retry` 和 attempt 事实，随后由既有 30 秒 refresh cadence 的 due-target reconciliation 自动续跑。fresh running job、Selection/Target、LocalSIM execution 和 MiniQMT 均不受此修复影响；任何 reconciliation/target projection 异常均记录 exception，不返回假成功。2026-07-16 生产只读证据确认原 `sector_data` job 由 `data_sync_target_due` 创建且带 durable target id；本 PR 只交付 source/test/蓝图，不执行数据库写入、服务重启或人工补单。

`BUG-680` 收敛 `F-020` 的 MiniQMT count/price/callback 子切片：tick-driver 持久化只接受 exact `miniqmt_event_loop_tick_driver_v1`、固定 source、相同 runtime id，以及 top-level/runtime-evidence 双份均存在且逐项一致的 submitted/rejected/pending 非负整数；缺失、布尔、负数、非整数、非有限值或冲突均以 typed reason code 失败，不再归零或跳过。trade/tick callback 的数量和价格别名必须全部可解析、正数、有限且彼此一致；order/trade callback 在 append durable event 前完成确定性 numeric、cumulative quantity 和 OMS canonical fact 预检。有效 trade event、child metadata 与 qmt_strategy trade ledger 共享同一 trade id 和 `canonical_trade_fact_sha256`；后续 repository I/O/projection 失败继续向上抛出，durable event 保留可重放的 canonical fact。本 slice 不改变 Selection、方向、数量、T+1、lot、limit/suspend、B0 tick source 或 broker route，不新增审批/门禁，也不执行 DDL/DML/config、broker call 或服务重启。

`BUG-681` 收敛 `F-021` 的 scheduler top-level loop false-green 子切片：uncaught tick exception 在同一锁域内生成有界、版本化的活动失败快照并覆盖当前 blocking result；thread 存活不能再抵消该失败。ops 对 background loop health 进行 schema/status/count/`execution_gate=false` 校验，将活动失败加入当日技术 blockers 并把 effective health 投影为 `BLOCKED`；缺失或非法 health typed fail loud。后续成功 tick 自动清除 active failure，同时保留 last failure、累计失败和最近成功时间。该诊断不触发 feed、DB、broker 或业务状态修改，不改变调度窗口、binding 选择、submit、Selection、LocalSIM 或 MiniQMT 语义，也不新增审批、人工 ack 或执行门禁。

`BUG-682` 收敛 `F-020` 的 MiniQMT TCA quote/preflight 证据子切片：quote payload 和 preflight payload 必须是 mapping；price/time 多别名逐项解析并检测冲突；非法/布尔/非有限/非正 price、非法 time、非布尔 allowed、负数/布尔/浮点/字符串 quantity、after 大于 before、classification/deadline status 冲突均产生专用 typed reason。client 将字段、原始类型和有界值写入原 batch 的 durable TCA `capture_errors`，保留已成功写入的 arrival 事实；不存在把字符串 `false` 解释成允许或把负数量归零后参与 hash 的路径。该修复仅保证 observation evidence 真实，不改变 B0 callback、preflight 决策对象、order/child、broker_called、方向数量或任何 broker side effect，也不新增审批、人工 ack 或执行门禁。

`BUG-683` 收敛 `F-020` 剩余的 MiniQMT raw durable batch/replay 子切片：event-loop client 对 `request_json.orders`、`result_json.results`、batch identity、parent/intent identity、boolean、amount、quantity 和 request/result cardinality 做一次共享 exact validation，任何非 mapping 行、缺失/重复/冲突 identity、字符串/布尔 count、非有限值或 result 数量不闭合均以稳定 `MINIQMT_EVENT_LOOP_DURABLE_BATCH_*` reason fail loud；不再过滤坏行后 `zip(strict=False)` 错配，也不再 padding/fabricate pending result。scheduler 对 tick-driver `batch_results` 的每个 current/foreign row、embedded batch id、status、result_json、metadata、result row、durable `qmt_batch_result`、total 与 `broker_called` 做 exact validation，合法无新 child tick 仍允许 `batch_results={}`；非法证据在 `simulation_daily_run` 覆盖前失败并保持原 readback。该修复不改变 Selection、B0 tick authority、preflight 决策、方向数量、child/order 或 broker route，不新增 RBAC、审批、人工 acknowledge 或 execution gate。

`BUG-687` 收敛 `F-021/F-022/F-024` 的平台聚合 health 子切片：`SimulationPlatformObservability`
联合 scheduler loop/current-day blocker、daily run、LocalSIM state/persistence/outbox/readback、MiniQMT durable
batch/runtime/quote health 和 business/reconcile facts，生成六层 `simulation_platform_diagnostics_v1`；任何
非 exact bool/count/mapping/status/runtime identity 或 batch cardinality/count 闭合失败均 typed fail loud。
metrics 仅使用六个低基数 label 并显式拒绝高基数 label；scheduler tick lag、LocalSIM causal bar/outbox、
MiniQMT quote progress、binding/durability/business failure alerts 全部是当前只读通知，恢复后自动解除，
不启动 feed、不写 DB、不重放 order、不调用 broker，不新增审批、RBAC、人工 ack 或 execution gate。
operator runbook 与同一 schema/阈值/reason 对齐；source/CI/merge、dependency/DDL/config、restart、binding DML
和正常交易日 LocalSIM/MiniQMT runtime readback 继续分别记录。

严格代码审核后的补充修复证据：五 BUG 最终小矩阵 `10 passed`，另有 durable wait fingerprint tamper 反例 `1 passed`；覆盖 projection-context 并发交付、current-clock eligibility、controller reconstruction 去重、hash 冲突 fail-loud、durable `FAILED+STALE` 非假绿，以及既有 release isolation/owned retry。该补充不新增执行 gate、审批或人工确认。

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
- 新 `minqmt_sim` binding 省略/显式 LEGACY/非法 control 均 fail loud，历史 omitted binding 仍可只读解码；
- legacy plan compiler 新建 parent 在任何 gateway/QMT 调用前拒绝；durable active legacy parent 仍可原 revision 恢复；
- migration inventory 对 active parent/child、归属 open order、attribution conflict、bounded overflow 分别拒绝且 write=0；
- PostgreSQL 单连接 commit/rollback/CAS、InMemory rollback、同 receipt 幂等、冲突 marker、commit-unknown 独立 readback；
- source/target package/account/capital/slot/policy identity 等价与篡改反例；
- target/dropped-position 冻结参考价、source/hash identity、缺价 broker-call=0 反例；
- assignment transition 在新 context 前释放 empty old runtime，并对 active algo/child runtime fail loud；
- raw order/batch/cancel 即使旧 env/password 有效也返回 410 且 QMT call=0；MiniQMTSim submit/cancel call=0；
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
| `F-013` | §5.5；`PaperTradingV2Repository.local_sim_economic_transaction`；`SimulationRuntimeRepository.stage_local_sim_economic_commit` | PostgreSQL single-connection commit/rollback、InMemory cross-repository rollback、economic receipt/state/outbox readback tests | implemented_verified | none |
| `F-014` | §5.2、§5.5；`LocalSimEconomicReceiptV1`、`LocalSimProjectionOutboxV1`、`LocalSimProjectionReceiptV1` | same-bar restart dedupe、CAS、connection retry max-3、business-conflict terminal、projection readback recovery、tamper/schema fail-loud tests | implemented_verified | none |
| `F-015` | §5.6；`LocalSimMarketMarkV1`；`LocalSimBackend.load_authoritative_position_marks`；`_local_sim_position_marks` | generic price/plan fallback rejection、realtime/historical/suspended as-of/source/provenance/hash validation tests | implemented_verified | none |
| `F-016` | §4.4、§5.8；`B0QuoteV2ControllerFactory.prepare_assignment_transition` | real `WHOLE_QUOTE_CALLBACK` scheduler integration、empty/non-empty assignment transition、route owner tests | implemented_verified | explicitly approved production-state separation：正常交易日 production runtime evidence 仍按 `F-024` 单独记录；不表示 source 未完成或 runtime 已激活 |
| `F-017` | §5.7、§9 P0-C；`RebalanceIntentService`、`ExecutionPlanCompiler`、`PaperTradingV2Service.create_live_approval_candidate` | frozen target/dropped-position reference identity、new/live-candidate binding、new parent、no LEGACY fallback tests | implemented_verified | explicitly approved production-state separation：production binding migration DML 尚未执行；不表示 source 未完成或 migration 已执行 |
| `F-018` | §6.2、§6.4..6.9 migration sequence；`MiniQMTRouteMigrationService` | active parent/child/open-order/conflict/overflow zero-write、transaction/rollback/retry/readback/idempotency tests | implemented_verified | explicitly approved production-state separation：operator 仅完成 source；production dry-run/apply/readback 尚未执行 |
| `F-019` | §5.8、§6；retired router/Paper/day-runner/client paths | direct broker static guards、410/typed retirement、historical read-model tests | implemented_verified | explicitly approved production-state separation：source merge 后仍需正常交易日唯一路径观察；不表示观察已完成 |
| `F-020` | §5.9；`SimulationLifecycleScheduler._validated_miniqmt_tick_driver_result/_validated_miniqmt_tick_driver_batch_results`；`MiniQMTExecutionRuntimeClient._event_loop_existing_batch_result`；`_event_loop_requests_from_batch/_event_loop_results_from_batch`；`QmtClientMiniQMTEventLoopGateway`；`MiniQMTExecutionRuntime.record_order_event/record_trade_event`；`MiniQMTOmsLedger.prepare_trade_fill`；`TcaCaptureDataError`、TCA exact parsers、lifecycle/client durable projection | BUG-680 execution count/numeric/canonical fact direct 31 passed；BUG-682 quote time/price aliases、preflight boolean/count/status、bounded durable error/no-broker-drift matrix 37 passed + lifecycle exact-reason 2 passed；BUG-683 raw batch/replay direct 34 passed、scheduler related 18 passed、client event_loop 24 passed + 1 exact-main baseline failure | implemented_verified | none |
| `F-021` | §5.10；`SimulationLifecycleBackgroundScheduler._record_loop_exception/_record_loop_success`；`SimulationRuntimeOpsService._scheduler_loop_health/_current_trade_date_blockers/platform_diagnostics`；`SimulationPlatformObservability` | BUG-681 live-thread exception、bounded snapshot、repeat/recovery、missing/malformed health、ops blocker tests；BUG-687 六层 process/lifecycle/binding/backend/durability/business health、scheduler bool/tick-lag、binding isolation、LocalSIM/MiniQMT exact fact tests | implemented_verified | none |
| `F-022` | §7；`/api/v1/simulation-runtime/platform-diagnostics`；`simulation_platform_diagnostics_v1`；`simulation_platform_operator_runbook_20260717.md` | BUG-687 read-only/no-side-effect、五类 identity、bounded metric label/cardinality、scheduler/LocalSIM/MiniQMT alerts、recovery auto-clear、runbook platform direct 17 passed；run-detail boolean exact 1 passed；live-admission exact bool/count related 3 passed；新增 observability line 85.80%、branch 71.85% | implemented_verified | none |
| `F-023` | §10.1、§10.4 | in-memory fixture no-DB/no-broker isolation tests | design_ready | none |
| `F-024` | §5.11、§10.5、§15；BUG-687 runbook §6 与 progress ledger | code/CI/dependency/DDL/config/restart/binding/runtime observation 独立状态；source direct tests 和 coverage 不冒充生产 restart/readback | implemented_verified | explicitly approved production-state separation：production runtime 仍需用户重启后按 runbook 独立只读核对，不表示 source contract 未完成或 runtime 已激活 |
| `F-025` | §9 P1 sequencing | Phase 0B/Adaptive acceptance mapping and boundary validator | design_ready | none |

## 15. Current Implementation Progress Ledger / 当前实现进度账本

状态枚举：`IMPLEMENTED_VERIFIED`、`REPAIR_REQUIRED`、`EVIDENCE_REFRESH_REQUIRED`、`DESIGN_ONLY`、`HISTORICAL_RETIRED`。本表记录当前摘要；详细历史以 Git/PR/BUG/CI 为准。

| Progress ID | Acceptance IDs | Current state after this PR（base `origin/main@34def06c`） | Evidence | Status | Next implementation slice |
| --- | --- | --- | --- | --- | --- |
| `SIM-P-001` | `F-004..006` | 单/多 Alpha 策略包一次准入、冻结 identity 和 broker-neutral selection/target 已建立 | `localsim_strategy_package_single_admission_f2_design_20260714.md`、PR #2103 | IMPLEMENTED_VERIFIED | 持续防止 runtime 二次 package 校验 |
| `SIM-P-002` | `F-005,016,017` | BUG-654/657 已修复 B0 context 发布、lot/tradability authority、失败持久化和安全恢复 | commits `02e73de6`、`f4392711`；本设计核对 2026-07-15 相关 direct tests 7 passed | IMPLEMENTED_VERIFIED | 纳入唯一路径退役验证 |
| `SIM-P-003` | `F-005` | BUG-658 已允许 unchanged authoritative manifest roll-forward 且拒绝虚假变更 | commit `43ce19de`；本设计核对 2 direct tests passed | IMPLEMENTED_VERIFIED | 保持 frozen identity contract |
| `SIM-P-004` | `F-007..012` | BUG-660 已实现 durable per-intent state、batch CAS/readback、realtime incremental minute loop、partial continuation、restart cursor/bar-hash 去重和 close residual；历史闭市日同步路径保持不变 | PR #2174、merge `02cc8bde`；direct LocalSIM/scheduler tests | IMPLEMENTED_VERIFIED | P0-B source merge 后由用户重启并补正常交易日 runtime evidence |
| `SIM-P-005` | `F-013,014` | BUG-661 已实现 Paper facts/state/run/outbox 同连接事务、single-writer、canonical receipt/hash、projection replay、same-bar dedupe、commit/readback 分离恢复；仅连接类错误执行最多 3 次 projection attempt，business/schema/CAS 冲突立即 terminal，readback 独立有界复核 | PR #2187、merge `f74fdf3b`；close-sync PR #2189、merge `7d7d1434`；direct transaction/restart/retry-exhaustion/nonretryable/readback tests | IMPLEMENTED_VERIFIED | source 已合入；production DDL/config 为 noop，restart/runtime observation 尚未核验；补正常交易日 runtime evidence |
| `SIM-P-006` | `F-015` | BUG-661 已删除计划价和通用 price map 的 mark fallback；broker 从执行使用的同一 minute provider 读取最后一个 causal close，`LocalSimMarketMarkV1` 强制真实 source/as-of/provenance/hash 与 authoritative previous-close 停牌证明 | PR #2187、merge `f74fdf3b`；close-sync PR #2189、merge `7d7d1434`；generic-price negative、realtime/historical/suspended provenance、LocalSIM partial/restart tests | IMPLEMENTED_VERIFIED | source 已合入；restart/runtime observation 尚未核验；核对真实历史日/当日 mark provenance |
| `SIM-P-007` | `F-016` | MiniQMT canonical runtime 已有真实 tick callback、bootstrap、durable event loop | Phase 1 design/PR #2019、BUG-604/614 tests | IMPLEMENTED_VERIFIED | 唯一路径静态与真实 SIM 持续验证 |
| `SIM-P-008` | `F-016..019` | BUG-662 将 MiniQMT 新 binding/new parent 收敛到 exact `B0_QUOTE_V2`，补齐 LEGACY binding 原子迁移/readback operator，永久退役 Paper/day-runner/raw-router/client broker side-effect，并以真实 callback 驱动 scheduler 回归；production DML/config/restart/broker 均未执行 | BUG-662 / issue #2190；migration 13 tests、route/Paper core 50 tests、scheduler 158 passed + 6 exact-main baseline failures、assignment transition 5 tests | IMPLEMENTED_VERIFIED | source merge/close-sync 后，单独执行 production migration dry-run/readback；用户授权 DML 与重启后再做正常交易日证据 |
| `SIM-P-009` | `F-020,021` | BUG-680 已移除 MiniQMT tick-driver/recovery/pending/submitted count 的 pass/归零并收紧 callback/OMS canonical fact；BUG-681 已消除 scheduler thread-alive false green；BUG-682 已消除 TCA quote/preflight 静默归一；BUG-683 已移除 durable request/result 过滤、错配、padding 以及 raw batch/status/total/default carrier；BUG-687 在 `SIM-P-011/030` 独立闭合全平台聚合 health。 | BUG-680 / issue #2232 direct 31 passed；BUG-681 / issue #2236 related 20+1 passed；BUG-682 / issue #2239 related 37 + lifecycle 2 passed；BUG-683 / issue #2243 direct 34 passed、related 18 + 24 passed，另有 1 个 exact-main no-child 旧语义测试失败 | IMPLEMENTED_VERIFIED | 用户重启后分别只读核对 tick-driver/batch 与平台 diagnostics，不把 source tests 写成 runtime activated |
| `SIM-P-010` | `F-023` | BUG-678 已为 roll-forward/ops/custom selection fixture 注入显式 in-memory StrategyPackage lifecycle authority 和 scheduler，B0 harness 使用当前 paired observation-context callback；测试不再连接生产 DB | BUG-678 / issue #2222；原失败 nodeid direct 1 passed；main 基线 33 failures 修复后 `pytest --lf` 28 passed | IMPLEMENTED_VERIFIED | 保持 fixture 与 package/B0/ops 当前接口同步，禁止用产品 fallback 修测试 |
| `SIM-P-011` | `F-021,022,024` | BUG-687 已实现平台级 LocalSIM/MiniQMT 六层聚合 health、五类 identity 定向只读查询、exact payload/identity/cardinality validation、bounded metrics、当前 facts 自动解除 alerts 与固定 operator runbook；不存在 feed/DB/broker/order replay side effect | BUG-687 / issue #2254 / PR #2269；platform direct 17 passed、run-detail exact 1 passed、ops related matrix 33 passed、live-admission related 3 passed；new observability line 85.80%、branch 71.85%；Ruff/format/pycompile/diff-check、CodeGraph/UA 4/4、F2 25/25、l0/registry pass；PR CI green | IMPLEMENTED_VERIFIED | source merge 后由用户重启；按 runbook 分别补生产 LocalSIM/MiniQMT readback，不新增人工门禁 |
| `SIM-P-012` | `F-024,025` | Phase 0A 专项文档只明确记录 0A-0..0A-3；当前源码已有 TCA read API/EOD/projector 组件，专项进度与代码证据尚未重新闭合 | Phase 0A 专项文档顶部状态、`simulation_runtime/tca_*` 当前源码 | EVIDENCE_REFRESH_REQUIRED | 在相关 TCA PR 前先按当前 main 刷新专项证据和本行 |
| `SIM-P-013` | `F-016,017,022,024` | Phase 1 A-E、B0_V2 activation wiring 和 quote evidence/diagnostics 已合入；生产 DDL/config/restart/binding/真实 SIM 必须继续单独记录 | Phase 1 专项设计 §13、PR #1988/#1994/#2005/#2011/#2019/#2033 | IMPLEMENTED_VERIFIED | 按本蓝图 P0-C/D 收敛旧路径和平台 health |
| `SIM-P-014` | `F-025` | Phase 0B 详细设计在 PR #2141，未合入；当前应受本文约束 | PR #2141 CI green，state OPEN（2026-07-15 readback） | DESIGN_ONLY | 本蓝图合入后更新其上位权威和前置映射 |
| `SIM-P-015` | `F-025` | `ADAPTIVE_IS_L1` 仅有算法域蓝图和 Phase 0A/1 基础，不存在经本文批准的可达新算法 broker submit | algorithm domain blueprint、Phase 0A/1 designs | DESIGN_ONLY | Phase 0B 可重建基线完成后再做阶段设计 |
| `SIM-P-016` | `F-005,006,021` | BUG-668 将 selection inference in-flight identity 补齐 `release_id/release_hash`，不同 runtime release 不再共享 future、结果或错误上下文 | BUG-668 / issue #2207；`test_scheduler_auto_generated_selection_inference_isolated_by_runtime_release`；批次 direct matrix 8 passed | IMPLEMENTED_VERIFIED | PR/CI/merge 后，正常调度日核对各 release inference evidence 独立推进 |
| `SIM-P-017` | `F-016,020,021` | BUG-669 由 B0 controller 每次 lifecycle tick 使用成对当前 wall/monotonic sample 推进 clock；single writer 同步交付 observation 的原 projection context，controller 不再以可能已推进的 current context 拒绝合法 callback；仅在静态 authority/continuity/clock domain/trade date 兼容时以当前时钟评价 | BUG-669 / issue #2208；paired-clock no-provider-IO、projection-context handoff、interleaved current-clock child submit direct tests | IMPLEMENTED_VERIFIED | PR/CI/merge 与用户重启后，正常交易时段观察 callback time 持续推进且无 stale scheduler snapshot/context race rejection |
| `SIM-P-018` | `F-016,020` | BUG-670 仅对 exact runtime-owned durable parent chain 恢复原 batch/preflight；动态 quote repricing 不再把自身 remark/pending sell lot 当外部重复，foreign/mismatch 仍 typed fail loud 且 broker call=0 | BUG-670 / issue #2209；self-duplicate sell reservation、repriced retry、foreign owner negative direct test；批次 direct matrix 8 passed | IMPLEMENTED_VERIFIED | PR/CI/merge 与用户重启后，readback 原失败 run 自动恢复且不重复 child/order |
| `SIM-P-019` | `F-016,020,022` | BUG-671 在无兼容 observation 时写带 deterministic semantic fingerprint 的 runtime wait event，不再构造缺少 raw ingress identity 的 quote-less `ACTION_REJECT`；恢复从 durable journal 校验并重建去重状态，algo 保持等待真实 tick | BUG-671 / issue #2210；no-observation durable wait/no-QUOTE_REJECTED/broker-call-zero、controller reconstruction no-duplicate direct test | IMPLEMENTED_VERIFIED | PR/CI/merge 与用户重启后，观察首次 callback 到达后 wait 自动清除并进入真实 tick 路径 |
| `SIM-P-020` | `F-021,022,024` | BUG-672 建立 runtime-id canonical quote health，联合 durable ack/readback/payload status/cadence freshness、callback、writer、controller、gateway、OMS；durable failed/invalid/future/stale 不得假绿，scheduler live 与 legacy monitor 状态均明确非权威范围；BUG-687 将该 canonical health 纳入平台 backend 层和 bounded metrics/alerts | BUG-672 / issue #2211；canonical healthy、failed+stale、missing durable degraded、paginated read-only direct tests；BUG-687 runtime-only query direct test | IMPLEMENTED_VERIFIED | source merge 后补真实 runtime diagnostics/platform readback |
| `SIM-P-021` | `F-005,006,020,021` | BUG-674 将 selection inference `IN_PROGRESS` 持久化为 `SIGNAL_GENERATING` 非失败等待，旧等待失败可自动修正；timeout/worker error 仍为明确失败，成功规划后清除等待与失败诊断 | BUG-674 / issue #2218；pending non-failure、timeout fail-loud、completion cleanup、release isolation direct tests | IMPLEMENTED_VERIFIED | source PR/CI/merge 后由用户重启；正常调度日核对等待、完成与跨窗口 readback，不把 source test 写成 runtime activated |
| `SIM-P-022` | `F-016,020,021` | BUG-675 为 `FAILED_RETRYABLE` 增加 exact durable pending runtime 恢复入口；仅接受 B0 plan/runtime/batch/ownership/count/side-effect 全闭合证据，恢复既有 controller/tick driver，不重提 parent | BUG-675 / issue #2219；restart controller reconstruction、owned duplicate、runtime-id/count/zero-side-effect、no-parent-resubmit direct test | IMPLEMENTED_VERIFIED | source PR/CI/merge 与用户重启后，在正常交易时段观察 callback subscription/controller/child evidence 自动推进 |
| `SIM-P-023` | `F-021,022,024` | BUG-676 保留最后 blocking tick，并按当前有效交易日分别有界读取 durable `FAILED_RETRYABLE`/`FAILED_TERMINAL`；即使首个 scheduler tick 尚未发生也不得假绿。BUG-687 将 blocker/loop status 纳入 process/lifecycle/binding 六层聚合并生成自动解除 alerts | BUG-676 / issue #2220；no-op rollover、before-first-tick/current-day blocker readback、inactive scheduler、status-specific bounded projection、component projection direct tests；BUG-687 binding isolation/tick-lag auto-clear tests | IMPLEMENTED_VERIFIED | source merge 后补生产 scheduler/platform read-only status |
| `SIM-P-024` | `F-020,021,023` | BUG-678 修复 package lifecycle reader、B0 paired observation context、ops scheduler 与 roll-forward authority 的 test fixture 漂移；不引入生产 DB/broker/mock fallback | BUG-678 / issue #2222；base 代表失败复现；修复后 5 representative、28 last-failed、roll-forward direct tests passed | IMPLEMENTED_VERIFIED | source PR/CI/merge 后由 CI 运行完整矩阵；fixture 漂移不得作为放宽产品 fail-fast 的理由 |
| `SIM-P-025` | `F-010,021,022,024` | BUG-677 在 startup/每次 schedule refresh 复用同一 stale-running lease reconciliation：超时 job 持久化为 `timeout`，linked schedule/target 明确失败与 retry，target retry 继续由既有 durable due-target cadence 驱动；fresh running job 不变 | BUG-677 / issue #2221；生产只读 job/target linkage；expired/fresh predicate、schedule projection、target retry+attempt、refresh ordering、shared 23:00 helper direct tests 36 passed | IMPLEMENTED_VERIFIED | source PR/CI/merge 后由用户重启；只读核对旧 job timeout、target retry/attempt 及新 sector_data job 完成，不把 source merge 写成 runtime recovered |
| `SIM-P-026` | `F-020,024` | BUG-680 为 MiniQMT tick-driver result 建立 exact schema/source/runtime/count contract；callback numeric alias invalid/conflict 在 durable append 前失败；order/trade cumulative quantity 与 OMS canonical trade fact 预检完成，event/child/ledger hash 可重建；BUG-687 只读消费该 durable fact，不改变执行 | BUG-680 / issue #2232；gateway、OMS、runtime、scheduler direct matrix 31 passed；Ruff changed-files pass | IMPLEMENTED_VERIFIED | source merge 后只读观察正常交易时段 tick-driver/platform evidence；DDL/DML/config/restart/broker activation 均为 noop |
| `SIM-P-027` | `F-021,022,024` | BUG-681 为 background scheduler uncaught loop exception建立 current failure/last blocking/health 快照；BUG-687 进一步 exact 校验 scheduler bool/interval/time，将 loop/tick lag 投影到 process 层、bounded metric 和自动解除 alert | BUG-681 / issue #2236 related matrix 20 + 1；BUG-687 boolean coercion、tick-lag/recovery direct tests | IMPLEMENTED_VERIFIED | source merge 后由用户重启，再做生产 scheduler/platform status readback；DDL/DML/config/broker call 均为 noop |
| `SIM-P-028` | `F-020,022,024` | BUG-682 为 TCA quote/preflight observation 建立 exact payload/alias/time/price/boolean/count/deadline status 校验和 durable sidecar；BUG-687 runbook/平台 diagnostics 将 TCA 保持为最后只读层，不把 observation failure 改成执行门禁 | BUG-682 / issue #2239；source PR #2241 merge `bef8512d`；close-sync PR #2242 merge `03d7bdd1`；related matrix 37 + lifecycle 2；BUG-687 side-effect/runbook tests | IMPLEMENTED_VERIFIED | 用户重启后只读核对 TCA sidecar 与平台层；DDL/DML/config/broker call 均为 noop |
| `SIM-P-029` | `F-020,024` | BUG-683 为 event-loop durable batch replay 和 scheduler tick-driver batch persistence 建立 exact request/result/carrier/status/cardinality contract；非法 current/foreign batch 在覆盖 run 前 typed fail loud，合法 no-child tick 保持空 batch_results；不再过滤、错配或伪造 pending result | BUG-683 / issue #2243；new direct 34 passed；scheduler `-k miniqmt_tick_driver` 18 passed；client `-k event_loop` 24 passed + `test_event_loop_submit_no_child_order_fails_loudly_without_silent_batch` 在 origin/main 与本分支均失败；Ruff/py_compile/diff-check | IMPLEMENTED_VERIFIED | source PR/CI/merge 后由用户重启并只读核对 batch/status/total/readback；DDL/DML/config/broker call 均为 noop |
| `SIM-P-030` | `F-021,022,024` | BUG-687 建立 `/platform-diagnostics` 和 `simulation_platform_diagnostics_v1`：五类 identity、六层 facts、LocalSIM/MiniQMT exact durable/business projection、低基数 metrics、自动解除 alerts、read-only side-effect contract 与固定 operator runbook；异常扫描/cardinality/payload/identity 及 live-admission bool/count 均 typed fail loud | BUG-687 / issue #2254 / PR #2269；platform direct 17 passed、run-detail exact 1 passed、ops related matrix 33 passed、live-admission related 3 passed；new observability statements 85.80%、branches 71.85%；Ruff/format/pycompile/diff-check、CodeGraph/UA 4/4、F2 25/25、l0/registry pass；PR CI green | IMPLEMENTED_VERIFIED | source merge 后由用户重启；分别补正常交易日 LocalSIM/MiniQMT platform readback，production DDL/dependency/config/binding/broker 均为 noop |

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

`BUG-661` source implementation 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | 经济事实与 projection 使用两个完整 single-writer transaction；包含 canonical receipt/outbox、CAS/readback、连接类 max-3 retry、non-retryable terminal receipt 和真实 market provider mark，不以顺序写或通用 price map 代替 |
| `no_silent_error` | pass | schema/hash/identity/CAS/readback/mark/retry exhaustion 均抛 typed reason code；失败状态写回，未增加 `pass`、空集合成功或异常吞噬 |
| `no_business_semantic_drift` | pass | 仅改变 LocalSIM persistence/projection/mark authority；Selection、target、V25/T+1/limit/suspend 决策和 MiniQMT broker route 未改写 |
| `no_unrequested_gate_or_approval` | pass | 未新增 RBAC、人工 ack、审批、confirm-run 或业务开关；恢复由 outbox 自动执行并受技术性 retry budget 约束 |

`BUG-662` source implementation 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | new binding/parent、LEGACY migration inventory/marker/receipt、PostgreSQL/InMemory transaction/readback、callback-driven B0 scheduler、Paper/raw/client route retirement和 operator 均为完整产品路径；未以 mock-only、no-op 或尾部占位替代 |
| `no_silent_error` | pass | 非法 control/reference/assignment/hash/CAS/readback/route 均 typed fail loud；side-effect 后无 broker 成交/订单证明时保持 `FAILED_RETRYABLE`/reconciliation warning，不把 broker call 伪装成业务成功 |
| `no_business_semantic_drift` | pass | Selection/target、方向、数量、T+1、lot、limit/suspend 不变；仅冻结 dropped holding 的权威 current mark 作为 B0 parent reference，并纳入 identity；真实 broker side effect 仍仅由 tick callback → runtime → gateway 产生 |
| `no_unrequested_gate_or_approval` | pass | 未新增 RBAC、审批、人工 acknowledge、confirm-run 或产品业务开关；assignment transition 对 empty runtime 自动执行，对存在 durable side effect 的 runtime 仅以技术一致性冲突 fail loud |
| `production state separation` | pass | 本 PR 不执行 DDL/DML/config、不调用生产 broker、不重启服务；source merge、production route migration、用户重启和正常交易日 runtime evidence 分开记录 |

`BUG-668..672` source implementation 批次的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | release identity、paired clock、exact projection-context handoff、owned retry identity、remark/lot attribution、durable wait fingerprint recovery、canonical health status/freshness 各自实现完整正反路径；未以全局放宽 duplicate、旧 clock、合成 quote、进程内-only 去重、mock-only diagnostics 或 legacy fallback 代替 |
| `no_silent_error` | pass | release/clock/context/request/batch/parent/remark/runtime/wait hash identity 不一致均 typed failure；无 quote 明确写 durable wait event，写失败向上抛出；canonical health 对 durable 缺失、FAILED、invalid、future、stale 均返回明确 reason，不伪装绿色 |
| `no_business_semantic_drift` | pass | Selection score/target、方向、数量、T+1/lot/limit/suspend、B0 tick source 和唯一 broker route 均未改变；只修复并发 identity、当前时钟、exact retry 和 diagnostics projection |
| `no_unrequested_gate_or_approval` | pass | 未新增 RBAC、审批、人工 acknowledge、confirm-run、业务开关或重启前置；恢复由 exact durable ownership 自动判定，foreign/mismatch 仅作为已有技术一致性约束 fail loud |
| `production state separation` | pass | 本批次只改 source/test/蓝图；DDL、DML、production config、broker call、服务重启和正常交易日 observation 均未执行，PR/CI/merge 继续独立记录 |

`BUG-674..676/678` source implementation 批次的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | selection 等待、真实 timeout/worker error、旧失败修正、MiniQMT frozen plan/runtime/batch/assignment/count/side-effect 全链校验、restart controller 重建、durable blocker readback 均有正反路径；fixture 使用显式 in-memory authority，不以放宽全部 `FAILED_RETRYABLE`、产品 fallback 或内存布尔值代替 |
| `no_silent_error` | pass | inference timeout/worker error、recovery identity/count/route/assignment/side-effect 冲突继续 typed fail loud；operator readback 失败抛 `SIMULATION_SCHEDULER_BLOCKER_READBACK_FAILED`，空窗口只保留历史 tick 事实而不伪装当前业务成功 |
| `no_business_semantic_drift` | pass | Selection score/target、V25/T+1/lot/limit/suspend、MiniQMT B0 callback、parent/child 方向数量均未改变；补丁仅修复等待状态、精确恢复可达性、只读投影与测试 authority/context 注入 |
| `no_unrequested_gate_or_approval` | pass | 未新增 RBAC、人工确认、审批、业务开关或执行 gate；恢复由既有 durable ownership 自动判定，diagnostics 明确 `execution_gate=false` |
| `production state separation` | pass | 本批次未执行 DDL/DML/config、未调用 broker、未重启服务；source/CI/merge、用户重启与正常交易日 runtime evidence 分开记录 |

`BUG-677` source implementation 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | startup、周期 refresh 与 23:00 cleanup 复用同一 reconciliation；job、schedule、durable target 和 attempt 链均保留，fresh/expired 正反谓词有直接测试 |
| `no_silent_error` | pass | reconciliation、schedule projection、target retry/attempt 异常均 exception 级记录；job timeout 写版本化 summary，不删除原事实、不返回 success |
| `no_business_semantic_drift` | pass | 未改 dataset readiness、Selection/Target、LocalSIM/MiniQMT 执行语义；只修复超过既有 120 分钟 lease 且不可能跨 restart 存活的 ingestion 状态 |
| `no_unrequested_gate_or_approval` | pass | 未新增审批、RBAC、人工 acknowledge、confirm-run 或 execution gate；恢复沿用既有 durable target/cadence 自动执行 |
| `production state separation` | pass | 仅执行生产 HTTP 只读核对；本 PR 未执行 DML/DDL/config、未重启服务、未人工提交 ingestion job |

`BUG-680` source implementation 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | tick-driver schema/source/runtime/三项 count 双份一致性、callback numeric alias、order/trade cumulative quantity、OMS preflight、durable event/child/ledger canonical hash 均覆盖正反路径；未以单层 `int(... or 0)`、mock-only 或仅日志告警替代产品契约 |
| `no_silent_error` | pass | missing/invalid/negative/non-integer/non-finite/conflicting count/price/quantity 均 typed fail loud；确定性 numeric/OMS 错误发生在 durable event append 前，后续 repository/projection 异常仍向上抛出 |
| `no_business_semantic_drift` | pass | Selection、target、方向、数量、T+1、lot、limit/suspend、B0 callback tick source 和唯一 broker route 不变；只规范执行证据解析、canonical identity 与投影顺序 |
| `no_unrequested_gate_or_approval` | pass | 未新增 RBAC、审批、人工 acknowledge、confirm-run、业务开关或执行 gate；数据恢复仍依赖既有自动 scheduler/runtime 路径 |
| `production state separation` | pass | 本 slice 只改 source/test/蓝图；未执行 DDL/DML/config、未调用 broker、未重启服务，PR/CI/merge 和正常交易日 readback 继续独立记录 |

`BUG-681` source implementation 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | loop exception 的 current/last/result/health 全链、重复失败计数、有界 allowlist context、thread-alive projection、ops blocker、成功自动恢复和历史保留均有产品代码与正反直接测试；不是仅增加日志或 mock-only 状态 |
| `no_silent_error` | pass | background control scheduler 缺失 health、schema/status/reason/count/active failure/message/context/自动清除/执行门禁字段非法均 typed fail loud；活动异常强制覆盖当前 blocking result 并进入 effective `BLOCKED`，不沿用旧绿色 |
| `no_business_semantic_drift` | pass | 未改变调度窗口、binding 选择、run_once、submit、Selection、target、LocalSIM V25、MiniQMT B0 tick、方向数量或 broker route；仅修复调度 loop 技术健康与只读投影 |
| `no_unrequested_gate_or_approval` | pass | health/blocker 均显式 `execution_gate=false`；成功 tick 自动解除 active failure，不新增 RBAC、审批、人工 acknowledge、confirm-run、业务开关或重启门禁 |
| `production state separation` | pass | 本 slice 只改 source/test/蓝图/BUG 元数据；未执行 DDL/DML/config、未调用 broker、未重启服务，source/CI/merge 与重启后生产只读 readback 分开记录 |

`BUG-682` source implementation 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | quote payload、price/time direct/list aliases、冲突、缺失语义、preflight payload/allowed/quantity/classification/deadline、bounded carrier、lifecycle decision/client batch 两条 durable sidecar 和 valid/missing/policy-error 回归均有产品代码与直接测试，不是只改 helper 或仅写日志 |
| `no_silent_error` | pass | 非 mapping、invalid/bool/non-finite/non-positive price、非法/冲突 time、字符串 false、负数/布尔/浮点/字符串 quantity、after>before、classification/deadline 冲突均稳定 typed failure；client 写 exact durable capture error，不将非法值归零、转真或伪装成合法 hash |
| `no_business_semantic_drift` | pass | TCA 仍是 observation-only；未改变 B0 tick authority、preflight 决策对象、parent/child/order、方向数量、broker_called 或 broker side effect；capture error 不回滚或重试既有执行 |
| `no_unrequested_gate_or_approval` | pass | 未新增 RBAC、审批、人工 acknowledge、confirm-run、execution gate 或业务开关；错误由既有自动 sidecar projection 记录 |
| `production state separation` | pass | 本 slice 只改 source/test/蓝图/BUG 元数据；未执行 DDL/DML/config、未调用 broker、未重启服务，source/CI/merge 与用户重启后只读 TCA readback 分开记录 |

`BUG-683` source implementation 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | durable request、result、preflight、batch carrier、current/foreign batch、run payload/status/total/readback 全链均由产品代码 exact 校验；client 四个 replay 消费点共享同一 parser，scheduler 在覆盖前验证，不是仅在测试或日志中拦截 |
| `no_silent_error` | pass | 非 mapping 行、request/result 数量不闭合、batch/parent/intent identity 缺失或冲突、boolean/count/amount/status 非法、total 与 results 冲突均 typed fail loud；删除过滤坏行、`zip(strict=False)`、padding pending、`int(... or ...)` 和 status truthy fallback |
| `no_business_semantic_drift` | pass | 合法无新 child tick 仍允许 `batch_results={}`；Selection、B0 quote/tick authority、preflight 决策、方向数量、child/order、broker_called 真实事实和 broker route 均未改写；baseline no-child 测试漂移不在本 BUG 中强行改变产品语义 |
| `no_unrequested_gate_or_approval` | pass | 仅增加 durable evidence 技术一致性 fail-loud；未新增 RBAC、审批、人工 acknowledge、confirm-run、业务开关或 execution gate，恢复仍沿用既有自动 runtime/scheduler 路径 |
| `production state separation` | pass | 本 slice 只改 source/test/蓝图/BUG 元数据；未执行 DDL/DML/config、未调用生产 broker、未重启服务；source/CI/merge 与用户重启后的生产只读 readback 分开记录 |

`BUG-687` source implementation 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | 不是只给一个绿色 status：产品 endpoint 同时交付五类 identity、六层 process/lifecycle/binding/backend/durability/business facts、LocalSIM state/persistence/outbox/readback、MiniQMT batch/runtime/quote/reconcile、bounded metrics、auto-clear alerts、side-effect contract 和完整 runbook；runtime-only daily-run-before-create 也有直接路径 |
| `no_silent_error` | pass | scheduler boolean/string coercion、LocalSIM state/schema/count、MiniQMT runtime identity、batch result cardinality/count/top-level conflict、非 mapping、非法时间、bounded scan 和 metric cardinality 均稳定 typed fail loud；不使用 `_safe_int`、truthy bool、过滤坏行、默认成功或截断假成功 |
| `no_business_semantic_drift` | pass | projection 只读取 scheduler/repository/runtime journal；不启动 feed、不写 run/DB、不重放 parent/child/order、不调用 broker，不改变 Selection、V25、B0 tick authority、方向数量、T+1/lot/limit/suspend、reconcile 或 TCA 语义；单 binding failure 保持隔离 |
| `no_unrequested_gate_or_approval` | pass | metrics/alerts/freshness/lag 均是只读通知，所有层显式 `execution_gate=false`，alerts 显式 `acknowledge_required=false/auto_clears_on_recovery=true`；未新增 RBAC、审批、confirm-run、人工 ack 或业务开关 |
| `production state separation` | pass | 本 slice 只改 source/test/runbook/蓝图/BUG 元数据；未执行 dependency/DDL/DML/config、未调用 broker、未重启服务。platform direct 17 passed、run-detail exact 1 passed、live-admission related 3 passed 和 coverage 不冒充生产已激活；merge/CI 与用户重启后的 LocalSIM/MiniQMT readback 分开记录 |

## 17. Definition of Done

本蓝图本身完成的条件是：权威关系、目标架构、契约、迁移、失败模式、测试、发布/回滚、生产边界、稳定索引和当前进度均完整，并通过 F2 validator 与 DESIGN-COMPLIANCE-001。

模拟盘平台整体完成必须是 §15 所有 `REPAIR_REQUIRED` 项均由真实实现和直接证据更新为 `IMPLEMENTED_VERIFIED`，Phase 0B/Adaptive 阶段按各自批准范围完成；在此之前，只能汇报具体 slice 的完成状态，不能宣称整个平台已经完成。
