# AIstock LocalSIM / MiniQMT 模拟盘统一权威蓝图

> 文档状态：`design_ready`；本文完成只代表整体设计可直接实施，不代表所有运行时代码、迁移、生产绑定或交易日验证已经完成。
>
> MiniQMT execution-kernel进度（2026-08-05）：K1/K2/K3/K4/K5 overall=`implemented_verified + merged`。K6 base design、K6-A与implementation-readiness revision已分别通过PR #2993/#3004/#3024合入。K6-C0 strict contracts、successor migration与versioned repository preflight已通过PR #3032 / merge `2a3622a3ba63585e3dfe12ef7ccb3f33b00dcb63`完成`implemented_verified + merged`；BUG-953 deterministic lineage、lifecycle authority与exact physical mapping closure已通过PR #3048 / merge `f4da00f6838f6da6344223f6bba55dfe606def3e`完成`implemented_verified + merged + runtime_verified`。K6-C1 generic product authority/materializer已通过PR #3080合入，K6-B final dependent-BUY coordinator已通过PR #3120合入且successor production DDL=`9/9 true`。K6-D下位设计PR #3129已合入；K6-D code已通过PR #3146 / merge `33c09049e82c11cdbae7cd9b596b3666cb481349`完成`implemented_verified + merged`与`source_merge=merged_pr_3146`，唯一KERNEL_V2 composition、真实QMT snapshot callback、scheduler no-quote clock/EOD、per-binding failure isolation、legacy产品caller物理退役、fresh-process source capability及additive只读diagnostics均已进入main。K6 overall仍=`implementation_in_progress`，用户restart、runtime activation和正常交易日验收均未完成，产品runtime未切换。
>
> 2026-08-03 final-architecture-only revision=`design_revision_ready + merged`、`source_merge=merged_pr_3098`，merge `b986e8f655137e9aea88f17ea91d3eb153f1c017`：后续所有MiniQMT execution-kernel研发预算只投入最终KERNEL_V2隔离架构。legacy产品代码冻结，禁止C2、旧路线修复/backport、legacy parity、compatibility bridge、translator、双写和额外shadow product route；旧实例只使用当前版本自然drain，K6-D必须删除旧产品caller并完成唯一route与正常交易日验收。
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
| `miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md` | 下位 F2 实施蓝图 | MiniQMT durable execution kernel、算法插件 SPI、真实 timer、command outbox 与 vn.py compatibility façade；受本文 `F-043..F-052` 约束 |
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
             -> durable runtime event + per-algo delivery
             -> ExecutionAlgoPluginV2 / VnpyAlgoEngineFacadeV1
             -> durable command outbox
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
| MiniQMT 算法行为 | side-effect-free `ExecutionAlgoPluginV2`，由唯一 runtime kernel 调度 | scheduler/client/runtime 为具体算法写分支，或插件直接访问 broker/repository |
| MiniQMT 行情订阅 | scheduler-owned quote ingress supervisor | 每个策略/请求各自订阅 |
| OMS/ledger 事实 | durable repository single writer | JSON/in-memory snapshot 冒充权威事实 |
| 运行诊断 | read-only ops/diagnostics | diagnostics 启动 feed、修复数据或触发 broker |

### 4.3 LocalSIM 事件模型

LocalSIM 必须从“一次 submit 立即终结”升级为 durable minute event loop：

```text
PLAN_ACCEPTED
  -> WAITING_FOR_CAUSAL_BAR
  -> WAITING_FOR_MARKET_DATA
  -> WAITING_FOR_CAPITAL
  -> ACTIVE
       -> BAR_APPLIED (0..N)
       -> PARTIALLY_FILLED (0..N)
       -> ACTIVE
  -> FILLED | CANCELLED | REJECTED | FAILED_TERMINAL | EXPIRED_WITH_RESIDUAL
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

broker-neutral plan 必须保留全部合法 intent。临时 quote 不可用、当前资金尚未由卖出成交释放、当前分钟无新 bar，均由执行状态分别表达为 `WAITING_FOR_MARKET_DATA`、`WAITING_FOR_CAPITAL`、`WAITING_FOR_CAUSAL_BAR`；不得在计划阶段删除 intent、伪造 `NO_REBALANCE` 或提前写成功。LocalSIM 每轮调度先处理 SELL，再处理 BUY；BUY 只能消费 ledger 已提交的真实现金，卖出回款到达后自动继续，直到 filled 或收盘 residual terminalization。历史闭日回放也必须保留原始 intent；任何未完成 order 都形成 `localsim_historical_residual_v1`，并区分 `CAPITAL_RESIDUAL` 与 `SCHEDULE_RESIDUAL_AT_HISTORICAL_CLOSE`，不得恢复计划期估价跳单。

单 symbol 行情暂时不可用只影响该 symbol 的 state，其它 symbol 继续执行并持久化；只有共享 provider/transaction 失败才可使整批失败。duplicate/out-of-order/payload-conflict 等确定性行情完整性错误把对应 state 置为 `FAILED_TERMINAL` 并保留 reason/context，不得通过回滚健康 symbol 的经济事实来隐藏冲突。

### 4.4 MiniQMT tick 模型

MiniQMT SIM 的产品执行行情只有以下来源：

1. `subscribe_whole_quote` 的真实 XtQuant callback；
2. 订阅建立时 `get_full_tick` 只用于 bootstrap 当前 snapshot；
3. callback 进入 Phase 1 normalizer/ordering/freshness/clock/tradability；
4. `B0_QUOTE_V2` context/evidence durable ack 后驱动 `MiniQMTExecutionRuntime`；
5. runtime 通过唯一 OMS/Gateway 提交 child，broker order/trade callback 及 reconcile 反向更新状态。

normalized tick 进入 kernel 时必须先形成有 durable identity/sequence 的 `RuntimeEventEnvelopeV2`，再按 active algo 形成 `AlgoEventDeliveryV1`。真实 `TIMER`、session boundary 和 EOD 由同一 `ExchangeSessionClock` 生成，不来自 quote，也不得由 tick 计数、快速 `for` 循环或 scheduler polling 伪造；B0 quote authority 与 timer authority 必须保持独立。

每次 callback/lifecycle evaluation 必须使用同一次采样得到的当前 wall clock 与 monotonic clock；scheduler 启动时刻或前一轮 tick 的时间不能复用为后续 quote eligibility 时钟。single writer 在生成 observation 时必须把同一个不可变 projection context 一并交付 controller；controller 先校验 observation/context identity，再保存这份原始 authority，不得在 callback sink 中重新读取可能已经推进的 current context 来猜测原始 identity。已接受 observation 只有 calendar/policy/continuity generation/clock domain/trade date/symbol authority 与当前 evaluation context 全部一致时才可在当前时钟重新评价；不得重写 observation identity，也不得用 timer 合成新 quote。

xtquant quote 的 `openInt/open_interest` 是可选交叉证据：字段缺失时不构成 capability/tradability gate；字段存在时才执行 registered-phase 解析，未知或冲突值必须 fail loud。普通股票真实 quote 不得因为没有期货式 open-interest 字段被拒绝。

五档盘口中的 exact `price=0 && quantity in {missing, 0}` 只表示该档为空，normalizer 将该 pair 规范为 `None/None`；`price=0 && quantity>0`、负数、非有限或 price/quantity 单边缺失仍是 typed invalid。BUY 仅要求可用 ask side，SELL 仅要求可用 bid side；不得用另一侧或 last price 合成缺失深度，也不得把合法单边市场判成全盘口无效。

authority refresh 只有 calendar/policy/symbol static authority、canonical tradability payload 或 source version 实际变化时才推进 context generation。仅 observation timestamp、加载时刻或对象重建变化时，必须复用上一份 compatible calendar/symbol context 和 generation，避免把同一权威事实误判为 assignment/context 漂移。

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

LocalSIM execution policy 是 release identity 的必需冻结组件，不是 runtime 可选择的业务配置。release/admission writer 负责把 policy snapshot 冻结进 release；既有 binding admission 继续只检查 package lifecycle，不新增 package 内容二次门禁。LocalSIM scheduler/runtime 在实际构造 run context 时必须验证 release 内精确 snapshot schema（`policy_version_id|validated_execution_policy_id|policy_id` 三种已登记 schema 中恰有一种、`policy_sha256`、`policy_json`）、release model ID/SHA 一致性，以及 normalized policy hash；该动作只校验运行必需的 frozen component，不读取或复核 StrategyPackage alpha/model/asset 内容，`strategy_package_revalidation_performed=false`。scheduler/runtime 与 `LocalSimBackend` 只消费这一显式 snapshot，禁止从 Paper portfolio、`manifest.minute_execution_policy`、flat policy JSON 或单边 ID/SHA 推断另一份 policy。历史缺失/冲突 release/binding 必须 typed fail loud 并退休/忽略，不补造 DML，不要求人工 acknowledge/审批；roll-forward 只复制已经完整且自洽的 source snapshot。

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

当日盘中每个 scheduler cadence 必须构造一个 immutable `LocalSimMarketSnapshotV1`：

```text
snapshot_id/hash = sha256(
  schema_version,
  trade_date,
  as_of_time,
  source,
  sorted(symbol -> canonical minute stream/context hash),
  sorted(symbol -> typed error payload)
)
```

同一 cadence 内每个 unique symbol 只允许调用 provider 和完整 stream validation 一次；所有 intent、execution state 和 position mark 必须复用这份 snapshot。cadence 开始时必须冻结完整 symbol 集合：全部 active execution symbols 与 passive held-position symbols 的并集。下一 cadence 使用新的 `as_of_time` 生成新 snapshot 并一次性加载该并集，不能永久复用旧 bars，也不能因为逐 intent/逐 mark 懒加载而重复读取前序 symbol。相同 `trade_date + as_of_time` 的 snapshot 不允许动态扩容；请求未覆盖 symbol 必须以 `LOCALSIM_MARKET_SNAPSHOT_SYMBOL_MISSING` fail loud，不能重新抓取并悄悄改写 snapshot。snapshot 的 `market_inputs` 与 `errors` 必须互斥且覆盖冻结集合；identity/hash/readback 不一致 typed failure。

`LocalSimMarketSnapshotV1` 的 hash 输入只允许 canonical JSON-like 类型：string-key mapping、list/tuple、string、boolean、integer、finite float、finite `Decimal`、date/datetime、enum 和 null。禁止 `default=str`、set、任意 object、NaN/Infinity、非字符串 key 或依赖对象 `repr` 的 hash；相同语义但 key 插入顺序不同必须得到相同 `snapshot_id/hash`。`suspend_status.is_suspended` 必须是 canonical boolean；字符串、数字、truthy alias 或 malformed object 都是 typed schema failure，不得转真/转假。

校验所有权与频率固定如下：

| 对象 | 唯一 owner | 频率 | 后续禁止 |
| --- | --- | --- | --- |
| StrategyPackage 内容/资产完整性 | admission | 每个 package version 一次 | Selection/LocalSIM/MiniQMT 二次校验 |
| frozen plan/release/binding identity | scheduler/repository | plan 建立与恢复 readback | 因行情暂时不可用重写 plan |
| LocalSIM minute stream/static market context | `LocalSimMarketSnapshotV1` builder | 每 cadence、每 unique symbol 一次 | 每 intent 全日重拉/重复全量校验 |
| side/cash/remaining delta | LocalSIM execution runtime/ledger | 每 state transition | 计划期预估现金后删除 BUY |
| economic facts/schema/hash/readback | repository single writer | 每 generation | 以日志、内存 snapshot 或假成功代替 |

Selection/Target 构建 LocalSIM broker-neutral plan 时不得消费 same-day quote 作为 intent admission gate。停牌、涨跌停、当前无 quote/无新 bar等动态事实只在执行 cadence 由 market state/runtime state 处理；数据完整性错误仍 fail loud，但只按共享故障或对应 symbol 隔离，不得回写 alpha、改选股或删除原 intent。

### 5.4 LocalSIM terminal contract

`SUCCEEDED` 必须同时满足：

- 所有 plan intents 均有 terminal state；
- 每个 filled quantity 与 fill/cash/position facts 闭合；
- 未成交 residual 具有明确、设计允许的 terminal reason；
- durable transaction/outbox 完成并独立 readback；
- account snapshot 使用权威市场 mark；
- run、Paper v2 projection 与 performance/TCA 引用同一 generation。

当前时点 bars 用尽、只有部分成交、只有 order 没有 fill/cash、或写入了部分表，都不得返回成功。

`WAITING_FOR_MARKET_DATA`、`WAITING_FOR_MARKET_STATE`、`WAITING_FOR_CAPITAL` 和 `WAITING_FOR_CAUSAL_BAR` 都是非终态；只要任一 state 仍处于这些状态，run 必须保持 `INTRADAY_RUNNING`。权威 `suspend_status.is_suspended=true` 且当日不存在分钟 bar 时，盘中使用 `WAITING_FOR_MARKET_STATE / LOCALSIM_SUSPENDED_NO_BAR`，不得伪造 0-volume bar、拒单或报 `LOCALSIM_CLOSE_BAR_MISSING`；15:00 后以 `EXPIRED_WITH_RESIDUAL / MARKET_SESSION_CLOSED_SUSPENDED / SUSPENDED_AT_CLOSE` 显式终结剩余数量。非停牌且 closing bar 缺失仍必须 `LOCALSIM_CLOSE_BAR_MISSING` fail loud。`FAILED_TERMINAL` 仅用于确定性、不可重试的 symbol-level 数据完整性冲突，并必须形成 `local_sim_terminal_failure_v1`。资金不足不得转换成 `BrokerRejectedError` 后整批回滚：可负担数量按权威 ledger/fee model 成交，未负担数量保留在 state/order 的 `local_sim_capital_dependency_order_v1`；后续卖出 cash entry 到达自动继续。SELL 仍先于 BUY；同一 side 内必须严格保持 frozen `ExecutionPlan.intents` 的相对顺序，重启恢复也按 plan 顺序重建 broker records，不能按 symbol/intent_id 字典序改写资金竞争结果。只有完整收盘 policy 后仍未完成，或历史闭日 broker execution 已穷尽全部权威分钟/现金事实，才形成 `localsim_historical_residual_v1`；纯资金残差使用 `PERSISTED_WITH_CAPACITY_RESIDUAL`，其它历史/收盘 schedule 残差使用 `PERSISTED_WITH_RESIDUAL`，且不得沿用 `CAPACITY_RESIDUAL_SKIPPED` 或“计划期跳单”语义。

首根 causal minute 尚未闭合时，order acceptance 与 `WAITING_FOR_CAUSAL_BAR` state 本身已经是必须持久化的业务事实。该 generation 必须在 economic transaction 中提交 order/state、receipt 和 outbox，使用显式 `FIRST_CAUSAL_BAR_WAIT` projection kind，保持 run/Paper run 为 running；mark、account snapshot、NAV 与 performance 必须缺席而不是使用计划价、盘前价或上一条无来源 performance。projection/readback 失败沿同一 outbox 自动恢复，不得重建 parent order。失败 run 只有在最新 `PROJECTED` outbox、对应 economic/projection receipt、完整 state hash 集及 Paper economic facts 均独立 readback 闭合时才可恢复 minute loop；缺任一载体或 identity/hash 冲突必须 fail loud，历史不可重建 run 不得补造事实。

Trading Core 已验证的 order quantity 是板块手数权威。symbol-aware 分钟算法和 participation sizing 必须调用统一 board-lot rule；不得把科创板合法 `>=200` 且按 1 股递增的数量、创业板/主板整手或合法 SELL residual 再按硬编码 100 股改写。算法若改变已验证 order total，必须 fail loud；正确实现应在 core 中生成合法 child quantity，而不是在 LocalSIM adapter 静默截断 target。SELL residual 例外只适用于 child 等于该 order 全部 authoritative remaining quantity 的一次性清仓；非最终 child 仍必须满足板块 minimum/increment。不得仅凭 `side=SELL` 与 `child_qty < min_qty` 推断为合法 residual，否则诸如主板 300 股、`split_count=6` 会错误产生 50 股 partial child 并与 ledger authority 冲突。

legacy runner 的 run-level 状态不能把非终态 order 包装成成功。只要任一 handle 为 `pending` 或 `partial_filled`，必须返回 `terminal=false`、列出 `pending_handle_ids` 并写 `paper.daemon.run_pending`；只有全部 handle terminal 且所有 intent 已完成处理时才写 `paper.daemon.run_completed`。pending 不是 failure，也不是 completed，不新增人工确认或恢复门禁。

VWAP 必须消费显式、非空、finite、非负且总和大于零的 authoritative `volume_profile`；profile 缺失、为空、类型非法、含负值/NaN/Infinity、总和为零或在 remaining quantity 完成前耗尽，统一通过 `ExecutionAlgoError` 携带 `VWAP_VOLUME_PROFILE_INVALID`、algo/order/symbol/bar context fail loud。禁止退化为“第一根 bar 全量成交”、TWAP、均匀拆分或其它默认业务 fallback；在真实 profile 接入前，显式选择 VWAP 的 LocalSIM run 必须拒绝该 order 并保留完整 cause context。

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

mark/NAV 是经济事实提交后的派生估值，不得反向成为已发生 fill/state 的事务提交门禁。首根 causal bar 等待已按上一节的 no-mark generation 实施；BUG-796 进一步把任一 active symbol 或 passive holding 的 transient mark 缺失纳入同一“经济事实先提交、估值显式 pending、后续 cadence 自动补齐”契约：orders/fills/order-events/cash/states、position lots 及其 canonical hashes 先在 single-writer transaction 中进入 `local_sim_valuation_pending_economic_facts_v1` 与原 generation outbox；run/Paper run 保持 running，`local_sim_persistence.status=INTRADAY_VALUATION_PENDING`、`nav=null`，缺失 symbol/reason 明确可见。后续 cadence 必须先对 exact economic receipt、durable state hashes、outbox identity 以及 Paper orders/fills/order-events/cash 执行独立 readback，全部闭合后才能恢复 pending outbox，不得以 outbox 已存在代替 economic readback，也不得先推进新的 minute event；连接类 readback 失败自动有界重试，事实缺失、schema/hash/identity 冲突立即 fail loud。mark 恢复后以同一 outbox/generation 写 position snapshot、account snapshot、performance/TCA 和 `local_sim_valuation_completion_v1`，再独立 readback。重启恢复以已提交 economic facts 中的 position/cash 为 authority；若调用方同时提供当前 account evidence，则必须 exact hash/cash 相等，否则 `LOCALSIM_DUPLICATE_ECONOMIC_STATE_CONFLICT` fail loud。禁止旧 mark、计划价、0 价、通用 price map 或 manifest fallback。

只允许注册的连接中断、serialization/deadlock/lock timeout 做有界重试，单个 outbox 最多执行 3 次 projection attempt（首次 + 2 次自动重试）；第 3 次失败写 `local_sim_projection_terminal_failure_v1` 并进入 `FAILED_TERMINAL`。schema/hash/CAS/idempotency/business conflict 第一次即写同一 terminal failure receipt，不重试。projection 已提交后的独立 readback 使用单独的 `attempt_count`，最多自动复核 3 次；耗尽后保留 `PROJECTED` 事实、写 `local_sim_projection_readback_terminal_failure` 并 fail loud，不得重放 projection side effects。

P0-B 的现行持久化 schema 固定为：

- `local_sim_economic_receipts_v1`：按 generation 保存 `LocalSimEconomicReceiptV1`，其 `economic_hash` 覆盖 order/fill/order-event/cash/state/position/mark/account snapshot 的 canonical hash；
- `local_sim_projection_outbox_v1`：当前 generation 的 `LocalSimProjectionOutboxV1`，状态仅允许 `PENDING`、`PROJECTION_RETRYABLE`、`PROJECTED`，payload/hash/identity 不因重试改写；
- `local_sim_projection_receipts_v1`：成功重放后的 `LocalSimProjectionReceiptV1`，与 outbox、economic hash 和 projection hash 一一关联；
- `local_sim_valuation_pending_economic_facts_v1` / `local_sim_valuation_pending_projection_payload_v1`：只在明确 `LOCALSIM_MARK_PRICE_MISSING` 或 `LOCALSIM_REALTIME_MARKET_DATA_UNAVAILABLE` 时使用；冻结 order/fill/event/cash/state/position hashes、cash reference、缺失 mark evidence 和最终 projection contract，不包含伪造 mark/account hash；
- `local_sim_valuation_completion_v1`：同一 outbox/generation 的 mark hashes、position hashes、account snapshot hash、performance hash 与 completion hash；schema/identity/hash/receipt 任一冲突都 fail loud，不进入 availability pending；
- `local_sim_projection_terminal_failure`：非重试错误或第 3 次 projection attempt 失败的 durable typed terminal receipt；保留 outbox/economic facts，不允许 scheduler 再自动执行 projection；
- `local_sim_economic_generation` 与 `local_sim_projection_generation`：run、Paper snapshot、performance/TCA 的共同单调 generation。

生命周期 scheduler 通过 `PaperTradingV2Repository.local_sim_economic_transaction` 取得 single-writer advisory lock 和唯一写连接，Paper facts 与 `SimulationRuntimeRepository.stage_local_sim_economic_commit` 共享该连接。事务提交后必须分别 readback economic receipt/state/outbox 和 Paper facts。projection 在第二个 single-writer 事务中重放；若 projection 已提交而独立 readback 失败，保持 `PROJECTED` 事实并写 `local_sim_projection_readback_failure`，下一次自动复核 readback，禁止重写经济事实或重复 run event。

Paper PostgreSQL writer 是 LocalSIM durable JSON 的最终 schema 边界，不能假设 scheduler 已计算 canonical hash 就代表原对象可被 `psycopg2.extras.Json` 序列化。`order.metadata`、`fill.metadata`、`fill_market_context`、`order_event.metadata`、`order_event.fill_json` 和同一经济事务内的 `run_event.context` 必须在 SQL execute 前递归规范化：`Mapping`/`MappingProxyType` 复制为按原生 string key 排序的普通 object，tuple/list 规范为 array，date/datetime 规范为 ISO-8601，finite Decimal 规范为精确字符串，Enum 规范为其 value；finite float、int、bool、string 和 null 保持 JSON 语义。禁止 `default=str`、未知对象字符串化、非 string key 强制转换、NaN/Infinity、静默删除字段或只修 scheduler hash payload。

writer 发现非法 key、non-finite number 或未知类型时，必须在首条对应 fact INSERT 前抛出带 `reason_code/fact_type/fact_id/field/path` 的 typed failure，并由 `local_sim_economic_transaction` 回滚整批；不得泄漏裸 `TypeError`、提交半套 Paper facts 或返回成功。直接验收必须实际调用 psycopg2 JSON adapter 的 `getquoted()` 并覆盖事务 commit/rollback；仅 InMemory repository、mock-only payload hash 或 `json.dumps(default=str)` 不能作为实现证据。scheduler 的 canonical economic hash 与 writer readback JSON 必须保持字段级同义，不得为修复序列化改变策略信号、方向、数量、算法或 broker route。

### 5.6 Market mark contract

账户估值只能使用与 snapshot time 对齐的权威行情：

- 当日：已接受的 realtime minute close/quote mark；
- 历史日：权威 EOD/minute close；
- 停牌：上一合法交易日 close，必须标注 `SUSPENDED_PREV_CLOSE` provenance；
- 缺失：`LOCALSIM_MARK_PRICE_MISSING`，不得生成成功快照。

`reference_price`、`limit_price`、order price、fill-independent plan price 不能作为 position mark fallback。

持久化 mark 使用 `LocalSimMarketMarkV1`，必须带 `source`、实际行情 `as_of_time`、`provenance` 和 `mark_hash`。支持的 provenance 为 `REALTIME_MINUTE_CLOSE`、`HISTORICAL_MINUTE_CLOSE`、`SUSPENDED_PREV_CLOSE`；非有限值、非正值、未知 source、晚于 account snapshot 的 mark 或无法由上一交易日 close 证明的停牌价格均 typed failure。scheduler 只能调用 LocalSIM broker 的 `load_authoritative_position_marks`，由 broker 从本次执行使用的同一 `PaperV2MinuteMarketDataProvider` 读取截至 snapshot time 的最后一个真实分钟 close；停牌时必须调用 authoritative `PreviousCloseProvider`。通用 `current_prices`、`price_by_symbol` 仅用于 target/order 构建，不得转译成 position mark 或伪造 provenance。

当 realtime source 发生已注册的 transient failure 且存在同交易日、同 symbol/source/provenance、时间不晚于 snapshot 的前一 durable mark 时，可以复用其价格，但必须生成新的 mark hash，并完整记录 `reuse_reason_code=LOCALSIM_REALTIME_MARK_REUSED_AFTER_TRANSIENT_SOURCE_FAILURE`、`source_error_reason_code` 与 `reused_from_mark_hash`；不得原样返回旧 mark 造成无痕 fallback。`local_sim_projection_outbox_v1` 完全缺失表示尚无 previous marks；一旦 key 存在，其 outbox、`projection_payload`、`marks` 任何层级 malformed 都必须 `LOCALSIM_PREVIOUS_MARK_SCHEMA_INVALID`，不得当作空 marks 静默继续。

### 5.7 MiniQMT control assignment contract

每个 `binding_id + trade_date + parent_id` 只允许一个冻结 control revision。目标态为 `B0_QUOTE_V2`；active parent 不能在运行中切换 revision。`LEGACY_B0` 仅允许读取历史事实和完成迁移前已存在的 active parent，不接纳新 parent。

### 5.8 MiniQMT route uniqueness contract

所有产品下单、撤单、replace、reconcile 操作必须进入：

```text
SimulationLifecycleScheduler
  -> MiniQMTExecutionRuntimeClient
  -> MiniQMTExecutionRuntime
  -> durable event delivery
  -> ExecutionAlgoPluginV2
  -> durable command outbox
  -> OMS/Gateway
```

`backend/routers/qmt.py` 的 raw order/batch/cancel 产品入口必须退役，或改为调用同一 runtime operator command；不得直接 `XtQuantQMTClient.place_order`。`MiniQMTSimBackend` 和旧 Paper v2 day runner 不得拥有 broker side effect。

event-loop retry 若发现同一 runtime 已持久化 parent intent，只能在 incoming request、durable batch、remark index 和 parent intent 构成完整精确所有权链，且 `broker_called=false/broker_call_pending=true` 时恢复原 batch。quote 导致的动态 preflight price 变化不得把原 batch 自身 remark 或 pending sell reservation 当成外部重复；恢复必须复用 durable request/preflight，不得再次扣减同一 strategy-slot lot。任一 parent 集合、业务字段、runtime identity、remark owner、batch link 或 submit 状态不一致必须 typed fail loud，不能放宽外部 duplicate-order 防线。

event-loop durable batch 的 request/result 关联 identity 是唯一、非空的 parent intent ID，不是 JSON 数组位置。request/result cardinality 相同、两侧 identity 均唯一且集合完全相同但排列不同，属于合法 permutation：replay 必须按 identity 恢复，并按 durable request 的 canonical 顺序返回和重新持久化；禁止分别排序后 positional zip。null/empty identity、request/result duplicate、missing/extra、cardinality drift、identity alias 冲突、success 缺 `qmt_order_id`、`broker_called` 与 accepted/rejected carrier 矛盾均立即 typed fail loud，错误上下文保留 batch/runtime/binding（可获得时）、两侧完整 identity、duplicates/missing/extra 和 expected/actual。一个 parent 的多个 child slice 以 runtime child facts 为完整 authority；parent batch projection 保留首次已接受 broker identity，后续 rejected 或 accepted slice 不得抹掉、迁移或重复累计既有 side effect。restart/replay 复用同一 batch 和 child facts，不重复 broker call、child、accepted/submitted count；single-writer update 后按 request canonical order readback，持久化失败不得返回 ACK。

MiniQMT kernel/plugin 的详细实施契约由 `miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md` 定义。产品 runtime 必须保持一个 durable kernel：event ingress 只负责规范化和持久化，delivery transaction 使用 `SELECT ... FOR UPDATE`/等价 single-writer 语义加载插件 state，插件只接收 immutable context 并返回 `AlgoTransitionReceiptV1`，state、delivery result、transition 和 `BrokerCommandOutboxV1` 在同一事务提交；broker dispatcher 只能消费 committed outbox，并以 `command_id/idempotency_key` 经唯一 OMS/Gateway 去重。插件不得持有 DB、gateway、scheduler、strategy package 或 mutable runtime service 引用，不得自行启动 EventEngine、线程、订阅或 broker side effect。vn.py compatibility façade 只映射算法 API/对象语义，不成为第二 runtime、OMS、Gateway、时钟或数据源 owner。

该 kernel contract 还必须同时满足：algo 创建具有 deterministic `ALGO_START` event→delivery sequence 1→initial transition→outbox 身份链；TICK/TIMER/ORDER/TRADE/ACCOUNT/SESSION/EOD/RECONCILE/OPERATOR 按事件类型和 durable owner 精确路由并冻结 delivery-set hash；每个 algo 使用无间隙 delivery sequence/predecessor 与 state CAS，`SKIP LOCKED` 或 row lock 本身不构成顺序证明。插件异常必须在同一 transaction 写 failure receipt、FAILED state、timer cancellation 和 active-child cancel outbox，后续 delivery 只写可重建 SKIPPED receipt。

K2-B 对callback与quantity进一步只有一套authority：ORDER/TRADE/RECONCILE，以及K3 additive generic `COMMAND_OUTCOME`，均以strict `KernelCallbackMappingUpdateV1`和同一repository transaction把mapping、existing child projection、FAILED algo active-child closure、event/receipt/delivery-set和runtime sequence闭合，禁止ACK后第二writer或按payload猜归属；COMMAND_OUTCOME只承载已持久化outbox outcome且source为execution kernel，不冒充OMS RECONCILE。state的`parent_quantity`必须等于durable target，`traded_quantity`必须严格、非负、不回退且不超过target，remaining只由kernel派生，`FILLED`只有在traded等于target时成立。stale CLAIMED recovery保持同一attempt，只推进exact lease successor；attempt 5后不存在第6次plugin调用。当前三算法pure façade属于K4，K2-B对不兼容旧binding保留durable fail-loud evidence，不fallback legacy route。

dispatcher 提交 `DISPATCHING` 后的 `broker_called` 必须为 unknown/null；stale lease、重启或 timeout 一律自动转 `OUTCOME_UNKNOWN -> RECONCILING`，在 MiniQMT 未证明 `IDEMPOTENT_SUBMIT_BY_CLIENT_REF` 时禁止重提 SUBMIT。capability 必须分为静态 route unsupported、当前合法 observation 暂缺、已提供但非法/冲突：分别执行创建前 typed refusal、durable wait+EOD residual、observation typed rejection；不得让实现任意选择“等待或拒绝”，也不新增人工 acknowledge。vn.py compatibility 仅对 manifest 登记且通过 pinned exact method signature/DTO/return/error characterization receipt 的算法成立，不得用方法子集、动态 no-op 或默认字段宣称 source-compatible。

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

LocalSIM `causal_bar_lag_seconds` 定义为 diagnostics observation clock 到 active state 最新 `last_processed_bar_time` 的 wall-clock age；尚无已处理 bar 时使用 `causality_cursor`。TDX naive timestamp 必须按 `Asia/Shanghai` 解释，aware timestamp 按其 timezone 比较；不得再计算 `causality_cursor - last_processed_bar_time`，因为首根合法 bar 晚于 cursor 时该表达式会永久归零。reference 晚于 observation 必须以 `SIMULATION_PLATFORM_LOCAL_SIM_BAR_TIME_IN_FUTURE` fail loud，不得归零。仅 `WAITING_FOR_CAUSAL_BAR/WAITING_FOR_MARKET_DATA/WAITING_FOR_MARKET_STATE/WAITING_FOR_CAPITAL/ACTIVE` 计入 active/lag，terminal state 不得制造陈旧告警；bar-progress alert 只在连续竞价 `OPEN_AM/OPEN_PM` 触发，午休不形成 false alarm。observability 必须接受 scheduler 实际产生的 `INTRADAY_WAITING_FOR_CAUSAL_BAR/INTRADAY_PERSISTED/PERSISTED/PERSISTED_WITH_CAPACITY_RESIDUAL/PERSISTED_WITH_RESIDUAL/PERSISTED_WITH_TERMINAL_FAILURE`，不得以缩窄 allowlist 把合法 run 判为 schema invalid。该指标和告警始终只读，不是 execution gate。

## 8. Risks / Failure Modes / 风险与失败模式

| Failure mode | Required behavior | Forbidden behavior |
| --- | --- | --- |
| LocalSIM 只取得早盘 bars | 保持 ACTIVE，后续 bar 继续 schedule | 部分成交后把全天 run 报成功 |
| LocalSIM plan 遇到临时 quote/停牌/涨跌停 | 保留 broker-neutral intent，执行层进入 wait/no-fill/market state | 计划阶段删除 intent、写 PRE_TRADE_BLOCKED 成功或重新选股 |
| LocalSIM 单 symbol 行情不可用 | 该 symbol `WAITING_FOR_MARKET_DATA`，健康 symbol 继续 | 整批 rollback 或吞错后报成功 |
| LocalSIM 同 cadence 多 intent/symbol | 每 unique symbol 一次 snapshot/validation，marks 复用；下一 cadence 刷新 | 每 intent 重拉全日或永久复用旧 snapshot |
| LocalSIM BUY 暂无足够现金 | SELL-first，按 ledger 可负担量成交，remaining `WAITING_FOR_CAPITAL` 并自动恢复 | 计划期估价删除 BUY、BrokerRejected 整批回滚或默认现金 |
| symbol-aware 分钟算法合法科创板数量 | 保留 order total 并按统一板块手数生成 child | 硬编码 100 股改写 201 等合法数量 |
| LocalSIM commit 中途失败 | 事务回滚或 outbox retry，typed failure | 留下半套事实后报 PERSISTED |
| LocalSIM immutable fact 进入 PostgreSQL JSON adapter | SQL execute 前按 strict durable JSON schema 规范化；非法值携带 fact identity/path typed fail loud 并回滚 | 依赖 InMemory 假绿、泄漏 mappingproxy/unknown-type `TypeError`、`default=str` 或部分提交 |
| mark 缺失 | loud missing，不能生成成功快照 | 用 reference/limit/0/成本价代替 |
| MiniQMT tick stale/invalid | 当前 symbol/revision fail closed 并可观测 | 回退 LEGACY/minute/旧缓存 |
| MiniQMT quote 缺少 openInt | 视为可选字段缺失；存在时才校验 registered phase | 把普通股票 quote 判为 capability/tradability invalid |
| MiniQMT 单边盘口/零占位档 | 零价零量规范为空档，按交易方向消费有效对手盘 | 从 last/另一侧合成深度或把合法单边盘整体拒绝 |
| MiniQMT durable result 仅顺序不同 | 按唯一 parent identity 重建并以 request canonical order 投影 | positional zip、分别排序后拼接或把一个 parent 结果迁移给另一个 parent |
| MiniQMT durable identity 真冲突 | duplicate/missing/extra/alias/cardinality/broker carrier typed fail loud | 忽略不匹配、padding、过滤坏行或以 pending/fallback 掩盖 corruption |
| MiniQMT 等价 authority refresh | 复用 compatible context/generation | 仅 observed_at/load time 变化就推进 generation 并拒绝 observation |
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

`BUG-824` 补齐 `F-034` 的 PostgreSQL serializer 真实边界：

- Paper repository 在 order/fill/order-event/run-event 的每个 JSON 参数进入 psycopg2 adapter 前执行 strict canonical conversion，并保留 fact type、fact identity、field 和递归 path；合法 immutable mapping、tuple、date/datetime、Decimal 和 Enum 可持久化，non-string key、non-finite number 与未知类型 typed fail loud；
- order-event nested fill 改用 Python-mode payload 后由同一 converter 处理，避免 Pydantic JSON mode 在 mappingproxy 处先抛裸异常；transaction/single-writer/upsert/readback 语义不变，非法首 fact 触发原事务 rollback；
- production-equivalent direct tests 调用真实 `psycopg2.extras.Json.getquoted()`，覆盖 order/fill/fill-market-context/order-event nested fill/run-event、合法 commit、未知类型 rollback 及 key/number negative matrix；既有 scheduler immutable hash 与 pre-commit truth 节点继续作为共享契约回归；
- 本项不新增数据库对象或依赖，不执行 DDL/DML/config，不调用 broker，不启停或重启服务。source PR/CI/merge、用户部署重启和正常交易日 runtime readback 分别记录。

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

`BUG-697` 收敛正常交易日 readback 暴露的两个同源阻断：unattended roll-forward 在创建新 release 前必须 exact 解析历史 MiniQMT source binding 的 `B0_QUOTE_V2` control；缺失、非法或 `LEGACY_B0` 只持久化该 source binding 当日 `FAILED_RETRYABLE` 和稳定 `MINIQMT_B0_QUOTE_V2_BINDING_REQUIRED`，`broker_called=false/legacy_fallback=false`，不得创建 partial release/binding，也不得阻断有效 LocalSIM 或其他有效 MiniQMT binding。manifest rebase 与 expired-binding roll-forward 两条预处理路径共享同一 isolation 语义，`raise_on_error=true` 仍向调用方抛出。平台 diagnostics 必须 exact 消费 `last_result.has_blocking_result/errors/processed`；即使 daily run 为零，也将当前 scheduler blocking result 投影为 process/lifecycle `BLOCKED`、低基数 metric 和自动解除 alert，并从 canonical scheduler window 映射 market phase。该修复不合成 quote-control、不静默跳过坏 binding、不改变 Selection、计划、方向数量、tick authority 或 broker route，也不新增审批、RBAC、人工 acknowledge 或 execution gate。

`BUG-698` 收敛 `F-020/F-023/F-024` 的非阻断测试与 CI 假绿：`TWAP_LITE_MINIQMT` 首次 dispatch 尚未形成 child 时，client 必须返回并持久化 exact `SUBMITTING` batch、`event_loop_pending=true`、`pending_count=1`、active algo、零 child 和 `broker_called=false`；该状态不是成功成交、不是失败吞错，也不得恢复为旧 `MINIQMT_EVENT_LOOP_NO_CHILD_ORDER` 立即终止语义。后续真实 tick 驱动和 child 去重继续由既有 B0 controller direct test 验证。完整目录首次纳入回归后暴露的同源 lifecycle 偏移也必须闭合：BEST_LIMIT 等待 broker cancel callback 后仍保持 running 以便下一真实 tick requote；TWAP 子单 fill 未达到 parent target 时仍保持 running 等待后续 timer slice；只有 rejection，或 vn.py core 已明确 `finished`，才能由普通 child terminal callback 终止 algo，operator/recovery 显式 override 保持原语义。LEGACY plan 旧断言改为 exact B0 assignment/hash，operator fixture 显式注入 in-memory qmt_strategy account/ledger authority。新增 `miniqmt_execution_runtime` 模块所有权和专属 `miniqmt_execution_runtime_l2` 计划，完整运行 `backend/tests/miniqmt_execution_runtime`；`ci_change_classifier` 还必须对 service/test 两个精确目录选择该 session，目录与 catalog 同时变化时与 `validation_center_backend` 并列调度，不得只生成计划而实际 CI 不执行。该修复恢复 vn.py 多阶段生命周期，不改变 Selection、方向数量、tick authority、child/order broker fact 或 broker route，也不新增审批、RBAC、人工 acknowledge 或 execution gate。

严格代码审核后的补充修复证据：五 BUG 最终小矩阵 `10 passed`，另有 durable wait fingerprint tamper 反例 `1 passed`；覆盖 projection-context 并发交付、current-clock eligibility、controller reconstruction 去重、hash 冲突 fail-loud、durable `FAILED+STALE` 非假绿，以及既有 release isolation/owned retry。该补充不新增执行 gate、审批或人工确认。

### P0-E：运行期过度门禁与执行可达性修复

承接 `F-026` 至 `F-033`。本 slice 不放宽真实数据完整性、T+1、涨跌停、停牌、板块手数或 frozen identity；它只移除与权威业务语义不一致的二次/交叉门禁，并补齐明确等待、逐 symbol 隔离和 cadence snapshot：

- `BUG-706`：MiniQMT `openInt/open_interest` 缺失不再阻断普通股票 quote；字段存在但未知/冲突仍 fail loud；
- `BUG-707`：calendar/symbol authority 等价 refresh 复用原 context 与 generation，仅 timestamp/load-time 变化不再制造 context drift；
- `BUG-709`：zero-price/zero-quantity 五档占位规范为空档，BUY/SELL 分别按 ask/bid 对手盘判定，非法零价正量仍拒绝；
- `BUG-711`：LocalSIM plan 不再用 transient same-day quote 删除 intent；运行时由 `WAITING_FOR_MARKET_DATA`/`WAITING_FOR_MARKET_STATE` 接管；停牌无 bar 盘中等待、收盘形成 `SUSPENDED_AT_CLOSE` residual，非停牌缺 close bar 继续 fail loud；
- `BUG-712`：单 symbol provider/integrity failure 不回滚健康 symbol，确定性冲突形成 symbol-level terminal fact；
- `BUG-714`：每 cadence 生成一个 `LocalSimMarketSnapshotV1`，冻结 active execution + passive position symbol 并集，每 unique symbol provider/stream validation 一次，intent 与 mark 共用且禁止 same-cadence lazy expansion；hash 只接受严格 canonical/finite schema；malformed previous outbox fail loud，transient mark reuse 写完整原因与 lineage；
- `BUG-715`：计划保留全部 BUY，SELL-first 后依据 authoritative ledger cash 部分成交或 `WAITING_FOR_CAPITAL`，卖出回款后自动恢复；同 side 保持 plan 相对顺序且恢复不按字典序重排；历史闭日仅以 broker cash-fit 形成显式 residual；legacy runner 对 pending/partial handle 写 `RUN_PENDING`，不再伪报 `RUN_COMPLETED`；
- `BUG-717`：symbol-aware 分钟算法和 participation cap 使用统一 board-lot authority，不再把合法科创板 201 股改写为 200；VWAP 缺失 authoritative volume profile 时 typed fail loud，禁止第一 bar 全量成交 fallback。
- `BUG-718`：`execution_algos`、`execution_algo_adapter.py`、`minute_execution.py` 及其 direct tests 必须共同映射到既有 `paper_v2_backend`；unmapped executable code 继续阻断，不能通过缩窄 changed-files 或跳过产品 session 假绿；
- `BUG-719`：simulation L2 fixtures 必须显式使用当前 event-loop route、in-memory OMS/account/StrategyPackage lifecycle、LocalSIM mark authority 与 EOD schema；测试修复不得向产品代码注入 DB/broker/mock fallback。

本批次不执行 DDL/DML/config，不调用生产 broker，不重启服务。source implementation、PR/CI/merge、用户重启和正常交易日 LocalSIM/MiniQMT readback继续独立记录；任何 direct test 通过都不冒充 runtime activated。

原批次 direct matrix 覆盖 MiniQMT 三文件、LocalSIM broker 全文件、Trading Core minute/V25 契约及 13 个 scheduler fix-point，共 `169 passed`。严格复核补充修复后的最终本地 related matrix 为 `107 passed`，并在 mark reuse schema 最终收紧后重跑对应 nodeid `1 passed`；覆盖 LocalSIM broker、Trading Core minute、daemon lifecycle/outbox 及 scheduler mark/cash fix-point。Ruff、`git diff --check`、F2 validator 33/33、`l0`、`validation_module_registry_l0` 已重新通过。广泛 `paper_v2_backend`、simulation/MiniQMT 与跨模块 business-flow 由 PR CI/Validation Center/nightly 按任务卡委派执行。

### P0-F：durable truth、逐 symbol tick、成交恢复与投影一致性

承接 `F-034` 至 `F-039`。本 slice 修复真实运行暴露出的 durable truth 与执行可达性缺口，不改变 Selection/Target、V25、T+1、涨跌停、停牌、B0 tick authority、共享 batch account preflight 或唯一 broker route：

- `BUG-779`：LocalSIM economic snapshot 必须支持 Pydantic、dataclass、immutable `Mapping`/`MappingProxyType` 和严格 JSON value；JSON object key 必须原生为 string，禁止以 `str(key)` 合并不同业务 key，float/Decimal 的 `NaN`、`Infinity` 必须在 hash/持久化前以 `LOCALSIM_FACT_JSON_KEY_INVALID` / `LOCALSIM_FACT_JSON_NUMBER_INVALID` fail loud。只有 durable economic transaction 已 staged/committed 才可声明 broker/economic side effect。pre-commit 失败必须持久化 `economic_commit_staged=false`、`broker_called=false` 和完整失败数量；历史 run 若曾声称提交但无 receipt/state/outbox，可读 diagnostics 必须标记 `LOCAL_SIM_DURABLE_FACTS_UNRECONSTRUCTIBLE`，不得补写或伪造经济事实；
- `BUG-780`：B0 parent preflight 使用冻结 parent reference，不得预取普通 quote 代替 controller 的真实 callback tick。每个真实 quote 只捕获一次完整 strict JSON-compatible payload并校验 hash；缺 quote、空对手盘或 transient directional depth 只写该 symbol durable wait，健康 symbol 继续。zero-price/positive-volume、未知 source/identity 或 payload/hash 冲突仍 typed fail loud；共享账户/仓位/资金 batch preflight 继续保持全批原子失败；
- `BUG-781`：BUY 与普通 SELL child 继续遵守 symbol-aware board-lot；SELL 仅在 child quantity 等于 authoritative available whole position 时允许低于最小手数或带 odd-lot residual，任意 partial odd-lot child 必须在 gateway 前拒绝。runtime、vn.py core、managed-order preflight 共用同一语义，不用 `min_volume=1` 放宽所有 SELL；qmt strategy ledger preflight 与 vn.py asset direct test 必须精确映射到既有 MiniQMT/Paper execution sessions，不能因 unmapped classifier 跳过真实依赖测试；submit exception 后 broker status probe 自身失败必须在 cash unfreeze 和 intent rejection 后以 `MINIQMT_BROKER_STATUS_PROBE_FAILED` 向上抛出，不得静默解释为 disconnect；
- `BUG-782`：恢复必须先把 runtime-owned broker trade snapshot 与 archived durable trade event 按 exact trade identity 合并、去重和冲突校验，再通过同一 `record_trade_event` 路径重放。BUY 原冻结按 order price 释放并写 cash/T+1 lot，SELL 写 cash/lot/realized facts；event 已 append 但 settlement/child/vn.py core update 失败时，后续同 identity retry 必须按 durable stage checkpoint 续做未完成阶段且不重复经济事实。多 fill 必须用规范化 broker time 稳定排序，同秒 fill 保留 durable event/broker snapshot 原相对顺序；`cumulative_quantity` 是 child progress 派生事实，不得纳入单笔 trade identity hash 冲突判定。缺失/非法/冲突 identity、owner、quantity、price、time 均 fail loud；
- `BUG-783`：daily run、qmt batch、order intent 与 runtime journal 必须形成精确可重建投影。`broker_called` 必须来自 gateway ack 的 exact boolean，并同步到 child metadata、runtime event、batch result、order intent metadata 和 diagnostics；pre-broker rejection 必须保持 false，broker 调用后的 reject 保持 true，任意 rejected event 不得被事件类型本身推断为已调用 broker。order intent status 与 `broker_called/broker_call_pending/qmt_order_id` projection metadata 必须在同一 repository update 中提交；parent outcome count 与实际 child count 分离；同 parent 后续 rejected slice 不得覆盖既有 accepted broker fact；restart recovery 必须把全部 recovered child/trade 投影回原 batch/intent。read-only diagnostics 比较 runtime child/event/trade/count/hash 与 run/batch carrier，陈旧或冲突显示 `MINIQMT_RUNTIME_PROJECTION_STALE`、低基数 metric 和自动解除 alert，不自动 repair、不形成 execution gate；
- `BUG-784`：runtime trade date 由 frozen runtime authority 决定；broker date/time 只作为原始证据及 mismatch 字段，不得反向改写 runtime identity。同一 broker trade 的 `trade_date/traded_date`、`traded_time_iso/trade_time_iso/traded_time/trade_time` 必须分别规范化并交叉校验；alias 或显式日期与规范化时间日期冲突必须以 `MINIQMT_RUNTIME_BROKER_TRADE_TIME_CONFLICT` fail loud。epoch seconds/milliseconds、14 位 compact calendar、compact fractional、HHMMSS、ISO 各自精确判型；compact/time-only 按 Asia/Shanghai exchange local 解释后转 UTC，歧义长度或非法日期必须拒绝，不得由通用 timestamp 猜测落入错误年份。
- `BUG-820`：event-loop durable request/result 以唯一 parent intent ID 关联；set-equal permutation 安全恢复并按 request canonical order 序列化，duplicate/missing/extra/null/alias/cardinality 与 broker carrier corruption fail loud。写入与 readback 使用同一 strict schema：identity 必须为非空字符串，request aliases 在 upsert 前闭合，success order ID 必须为非空白字符串；任意 JSON 类型的 malformed identity 只进入可 JSON 序列化 diagnostics，不参与 duplicate/set/join，且不得使 error builder 产生二次异常。submit producer、durable replay、child sync、restart 和 multi-slice parent aggregate 共享同一 identity contract；后续 rejected slice 不覆盖既有 accepted broker identity，runtime evidence 保留全部 child counts。scheduler 现有 per-binding isolation 以 exact MiniQMT replay failure → later LocalSIM continuation direct test 固化，不修改 scheduler 产品语义。

本 slice 的 shared batch preflight、per-symbol quote wait、broker-called truth、TCA observation-only 和 diagnostics read-only 边界必须同时成立；不得为了让测试通过而删除共享账户一致性检查、合成 tick/trade identity、吞掉 settlement failure、改写历史 run 或增加人工 acknowledge/审批。DDL/DML/config、生产 broker、服务重启均为 `noop`；source/PR/CI/merge 与正常交易日 readback 分别记录。

### P0-G：LocalSIM 首分钟恢复与全链代码审计遗留

2026-07-21 的 BUG-794/runtime 复盘证明，broker 组件单测通过不等于 scheduler→mark→transaction→outbox→Paper projection 全链闭合。本 slice 按以下三条独立事实追踪，禁止用其中一条的修复冒充其它两条完成：

- `BUG-794`：首根 causal bar 前必须持久化 no-mark waiting generation；projection retry/readback 和失败 run 恢复不得重建 parent；state/order/frozen intent 必须一对一闭合；diagnostics bar lag 使用真实 observation age。该 source slice 只修复这些 contract，不改 Selection/Target、方向、数量、T+1、涨跌停、停牌、execution policy 或数据源。
- `AUDIT-LS-LOT-001 / BUG-795 IMPLEMENTED_VERIFIED`：统一 board-lot helper 继续保留 SELL residual 能力，但 `BaseExecutionAlgo` 的 child rounding 默认禁止 residual；仅 parent 初始化和算法已证明的 final child 可显式启用。TWAP/VWAP/AC/POV/SBB/V24 通过该权威边界，V25 P0/final branch 显式传入 final identity，V25.1 保留既有 `step_qty == remaining` 规则。主板 300 股配 `TWAP split_count=6` 不再产生 50 股 partial SELL，LocalSIM 实际输出三个合法 100 股 fills；50 股历史持仓仍仅在最后 child 一次性清仓。ledger/StepFill 校验未放宽。
- `AUDIT-LS-MARK-001 / BUG-796 IMPLEMENTED_VERIFIED`：BUG-796 已实现 economic-first、`INTRADAY_VALUATION_PENDING`、同 generation completion、restart/outbox recovery、economic readback reproof、projection retry/readback recovery 和 diagnostics stale/auto-clear 语义。仅明确 transient availability reason 可进入 pending；schema/hash/identity/source conflict 继续 fail loud。直接测试覆盖 healthy fill + unavailable passive holding、pending 重放不推进 state/不重复 fill、restart 后同代完成、pending 前置 economic readback 失败后必须重新读回、account drift conflict、projection connection retry、projected readback recovery；未使用旧 mark、计划价、0 价或通用 price map。
- `AUDIT-LS-POLICY-001 / BUG-797 IMPLEMENTED_VERIFIED`：BUG-797 已删除 production context 的 release→portfolio fallback 和 `LocalSimBackend` 的 manifest/flat-policy fallback；shared snapshot validator 要求恰好一个已登记 ID 字段、exact schema、完整 policy JSON、normalized hash 以及 release ID/SHA 一致。binding admission 仍只检查 package lifecycle，不新增 policy/package 内容门禁；LocalSIM run context 只校验实际运行必需的冻结 component，明确不复核 package alpha/model/assets，single alpha、multi-alpha parent、需要/不需要 model code 的 package 走同一路径。历史不完整 release typed fail loud 并退休/忽略，不补数据、不增加审批或人工恢复。Paper v2 daemon demo/E2E 作为精确调用依赖改为显式 snapshot wiring，不在 broker 内恢复 manifest fallback。

架构审计同时确认 `simulation_runtime/scheduler.py` 已承担规划、提交、LocalSIM/MiniQMT 两套恢复、事务、投影、诊断 carrier 等过多职责，单文件规模和多 truth plane 是近百个 seam bug 反复出现的放大器。阻断性 contract 修复完成后，应在不改变业务语义的独立 F2 设计中抽取 LocalSIM economic coordinator/projection projector，并保留现有 repository transaction 与 public scheduler contract；该重构不是本轮放行前置门禁，也不得与上述 P0 修复混合成一次不可审核的大改。

### P0-H：MiniQMT durable execution kernel 与算法插件 SPI

承接 `F-043` 至 `F-052`，详细契约见 `miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md`。本 slice 把当前 runtime 中算法专用分支、缺失的真实 timer、弱版本 state 和 submit helper 收敛为一个 execution kernel，不改变 `B0_QUOTE_V2`、Selection/Target、方向数量、A 股交易规则、OMS/Gateway 或 broker route：

1. K0/K1 固定 `RuntimeEventEnvelopeV2`、exact routing、per-algo sequence/predecessor、`ExecutionAlgoPluginManifestV2`、`ExecutionAlgoPluginV2`、capability 三分法、state/version/migration、pinned façade surface 和 import boundary；K1 的 strict schema/canonical hash/registry/deterministic context/current-three manifests/compatibility receipt 以 `miniqmt_execution_kernel_k1_contracts_registry_f2_detailed_design_20260722.md` 为唯一实施级下位合同。code-owned durable descriptor facts 不保存 live callable，factory/config-validator/state-codec 只存在于 process binding 并按冻结 ref/signature/source fail-loud 校验；route receipt 普通 readback 仅证明 structural hash，K2 消费前必须以 exact plugin catalog 与 strict gateway catalog 调用同一 pure evaluator 的 authority-aware readback，重算全部 facts、failure set、status/hash；
2. K2 实现 ALGO_START、durable ingress、per-algo delivery/failure、`ExchangeSessionClock`、transition transaction、command outbox、nullable broker-called、dispatcher retry/dedupe/unknown-outcome readback；
   - K2-B 子切片由 `kernel_ingress.py/kernel_creation.py/kernel_delivery.py/kernel_materializer.py/kernel_repository_k2b.py`实现shadow-only ALGO_START、exact routing、callback single-transaction、pure delivery/failure/skip、bounded retry与stale claim recovery；不启动产品worker、不调用broker、不接线legacy/current-three runtime。
3. K3 把 Sniper、BestLimit、TWAP Lite 迁移到同一 SPI，并以旧/新 trace parity 固定既有业务行为；实施级合同见 `miniqmt_execution_kernel_k3_current_three_runtime_migration_f2_detailed_design_20260727.md`，固定 pure plugin 3.0.0 exact factory/class/binding、transition-first construction、SUBMIT/CANCEL pending/unknown与terminal-trade-pending state、strict ORDER/TRADE/OMS RECONCILE/COMMAND_OUTCOME payload、mapping/outbox lifecycle projection、deterministic outcome ingress、TWAP raw/effective due authority、committed-fact shadow source、visible transport suppression、legacy zero-write inventory和K3/K4/K6边界；
4. K4交付不拥有第二套runtime/OMS/Gateway、且对每个注册算法生成exact compatibility receipt的`VnpyAlgoEngineFacadeV1`；实施级合同为`miniqmt_execution_kernel_k4_vnpy_facade_f2_detailed_design_20260729.md`，严格限定initialize/transition-scoped façade、通用adapter、existing K2 optional invocation/read-only authority seam、K1/K2/K3 authority复用、current-three shadow conformance和Iceberg/Stop characterization-only；K4-A V1永久observation-only，K4-B positive path必须使用V2 full input/actual trace/source-execution/conformance authority与fresh readback；不注册Iceberg/Stop、不改current-three binding、不进入K6 cutover；K2 V1仅允许单command exact shadow materialization，多command collector trace必须完整但不得复用receipt或冒充产品闭环，generic per-command product authority aggregate仍由K6独立F2拥有；
5. K5 以 Iceberg、Stop 两个不同事件需求的插件证明新增算法不增加kernel算法分支、状态机或产品route；K5只允许对K4 conformance builder抽取一个由原current-three writer和exact full-five writer共享的pure evaluator，既有receipt/hash与K4输出不变，不形成平行authority或任意plugin-set入口；
   - `display_volume`在durable plane只允许strict non-negative integer或非整数canonical decimal string，拒绝binary float与整数字符串双重identity；process-local source-setting bridge不得改变target/side/quantity/round语义。
   - writer/readback必须真实执行catalog-bound validator/factory probe；允许的共享代码修复只限K4 adapter无active child时既有`CLEAN`终态闭包与K2通用typed façade reason/context保真，`kernel*.py/client.py`不得出现Iceberg/Stop分支、产品route或额外门禁。
   - pure `ExecutionAlgoPluginV2` Protocol归属既有catalog/process-binding authority，kernel与conformance只引用这一份SPI；不得反向import kernel、复制Protocol或放宽K1 import-boundary。
6. K6按[`miniqmt_execution_kernel_k6_product_cutover_f2_detailed_design_20260801.md`](miniqmt_execution_kernel_k6_product_cutover_f2_detailed_design_20260801.md)实施：K6-A/C0/BUG-953既有contracts、migration、deterministic lineage与production schema readback保持闭合。K6-C1已通过PR #3080实现并合入唯一pure per-command evaluator、full creation/timer envelope、0/1/N与MIXED atomic materializer、typed pre-call reject、deferred coordination、same-authority idempotency、commit-unknown和fresh readback，继续复用K2 transition/mapping/outbox/child/algo/delivery。之后K6-B直接实现最终KERNEL_V2 coordinator/invocation seam，只把同一physical mapping row/command id由product RESERVED显式交接给现有K2 outbox/dispatch lifecycle；K3 inventory不得作为candidate source、legacy parity、迁移oracle或验收输入。K6-D完成new-instance唯一KERNEL_V2 cutover、冻结旧实例自然drain、旧产品caller删除与正常交易日验收。legacy代码不得再获得功能、修复、backport、parity、bridge或shadow研发；只增加现有K2缺失的coordination/product-authority事实，不复制K2 event/transition/outbox/OMS/Gateway，不进入算法plugin，不允许dual route/fallback。

K1 route authority validation 必须先分别完成 plugin catalog 与 gateway catalog strict readback，再重建并比较 receipt；supplied invalid gateway 保留 `MINIQMT_GATEWAY_CAPABILITY_CATALOG_INVALID` 和完整 context，不能包装成 receipt corruption 或 static unsupported。`MINIQMT_PLUGIN_ROUTE_COMPATIBILITY_RECEIPT_INVALID` 仅用于两个 authority 有效后的 durable receipt drift；有效 authority 的能力不满足仍是隔离的 per-plugin/per-route FAILED receipt，不形成全局执行门禁。

当前状态为 `IMPLEMENTATION_IN_PROGRESS`：K1/K2/K3/K4/K5 overall及K6-A/C0/C1/B/D均已`implemented_verified + merged`；K6-B successor production DDL=`applied_and_verified`、`9/9 true`。K6-D唯一详细设计PR #3129及source PR #3146 / merge `33c09049e82c11cdbae7cd9b596b3666cb481349`均已合入，`source_merge=merged_pr_3146`：final product coordinator、V3 evidence/materialization、真实QMT snapshot callback、scheduler-driven no-quote clock/EOD、per-binding failure isolation、跨日runtime release、legacy product caller物理删除、five-root source capability及additive read-only K6-D diagnostics均已进入main，K6源码切片合入口径=`5/5`。runtime cutover、用户restart及正常交易日观察=`0%/not_run`，产品runtime未切换。后续预算仍只归最终KERNEL_V2，不修复或扩展legacy/C2/parity/bridge/backport/shadow route。K6-D new DDL/DML/config/binding=`noop`，dependency gate=`noop`，broker/restart/runtime activation仍未授权/未执行。

K2-A-M1 PR #2753 initial run `30190525169`暴露Windows CRLF raw-byte断言与Linux LF checkout不一致；implementation HEAD `92c391011e4dc233587b5cc0201103cca25ee0a6`改用canonical-LF checksum后，run `30190835098`的MiniQMT L2=`723 passed,21 skipped`且CI verdict全绿；pre-review docs-closeout checkpoint `75d9398a28952b6a634df19d8f6430c30c0f488a`的required CI run `30191268890`同样全绿，最终 source HEAD `df10123bf39cd7f03ead2dce62ba6a2fae268e92`及required CI run `30193156930`已通过PR #2753 / merge `024bcf70537c2f1b267417c72f8539937dd21a3f`闭合。M1为`implemented_verified + merged`、`source_merge=merged_pr_2753`。

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
- LocalSIM transient quote 不删 intent、停牌无 bar 的等待/收盘 residual、非停牌 close-bar missing fail-loud、per-symbol failure isolation；
- 每 cadence active+position symbol 并集只读一次、same-cadence missing-symbol 不 refetch、snapshot canonical hash/unsupported/non-finite 反例、mark reuse lineage 与 malformed outbox 反例；
- LocalSIM SELL-first、同 side plan-relative order、restart restore order、BUY `WAITING_FOR_CAPITAL`、卖出回款自动续跑、历史 cash residual terminalization；
- legacy runner `RUN_PENDING`/`RUN_COMPLETED` 互斥及 pending handle identity；
- 主板/创业板/科创板与 SELL residual 的 TWAP/VWAP/AC/POV/SBB/V24/V25 init、child fill、participation board-lot 一致性；VWAP missing/invalid/exhausted profile typed failure和 valid profile 正路径；
- MiniQMT real tick projection、B0_V2 revision、no minute synthesis；
- MiniQMT runtime ALGO_START/event schema、event-owner routing、delivery-set hash、per-algo sequence/predecessor、plugin manifest/config/capability 三分法/state version 正反路径；
- `ExchangeSessionClock` 上午/午休/下午/EOD、无 quote timer、TWAP restart 后不重复/不丢 slice；
- plugin import boundary、非法 side effect dependency、pinned vn.py façade method signature/return/error/DTO characterization、未声明 surface 与第二 runtime owner 反例；
- MiniQMT openInt missing/present-invalid、等价 authority refresh generation、zero placeholder/单边盘口正反路径；
- MiniQMT 单 symbol quote/depth wait 与健康 symbol continuation，同时覆盖共享账户/仓位 batch preflight 全批拒绝且 broker call=0；
- 主板/科创板 SELL child：整仓 odd-lot residual 允许、partial odd-lot 在 vn.py/runtime/managed-order 三层 gateway 前拒绝；
- LocalSIM immutable mapping snapshot、pre-commit failure side-effect truth、历史不可重建 run 的只读 BLOCKED projection；
- strict invalid count/time/price/error contract。

### 10.2 Repository and transaction tests

- 每个写点 fault injection，证明 LocalSIM economic facts 原子提交；
- CAS conflict、idempotent retry、duplicate economic hash；
- commit success/readback failure 和 outbox replay；
- process crash at before-commit/after-commit/before-projection；
- migration preflight、apply、重复 apply、readback、rollback；
- late order/trade/fill 和 archive read path；
- MiniQMT BUY/SELL broker fill restart replay、partial snapshot + archived union、event-before-settlement retry、cash/lot/child/algo exactly-once；
- runtime journal 与 daily-run/batch/intent parent/child/trade count、identity/hash stale/conflict diagnostics 及自动解除。
- MiniQMT durable request/result 第 5 项 permutation、canonical request-order serialization、重复 replay、restart、multi-slice accepted→rejected、duplicate/missing/extra/null/alias/order-id/broker-called corruption matrix；
- plugin state/transition/delivery/failure/outbox 同事务 commit/rollback、N+1 抢先 claim refusal、delivery lease takeover、active-child cancel/SKIPPED closure、command ACK before/after crash、stale DISPATCHING nullable truth、unknown outcome broker readback 和 exactly-once side effect；
- additive schema preflight/apply/readback/重复 apply/rollback，以及旧 state migration 成功/失败不半迁移；

### 10.3 Restart and trading-day tests

- 上午部分执行 → 进程退出 → 下午恢复；
- 午休期间不误判行情 stale；
- 收盘 residual terminalization；
- 次日 roll-forward 不改昨日 identity；
- MiniQMT pending algo 在 restart 后由 tick 驱动且不重复 child；
- stale DISPATCHING 在 restart 后只能 reconcile，不以 false 重提；plugin FAILED state 的 active child 继续 cancel/reconcile 且后续 delivery 只写 SKIPPED receipt；
- MiniQMT 多 fill 以规范化 broker time 排序；time-only/compact/epoch/ISO 与交易日 mismatch 均有正反测试；
- 单 binding 失败不影响其它 LocalSIM/MiniQMT binding。
- 多 slot、同 symbol、多 algo callback 并发仍保持 per-runtime/per-algo strict sequence，单插件失败只影响其 owner chain；

### 10.4 Route uniqueness tests

- 产品源代码中 direct `place_order` owner 仅允许 Gateway；
- router/Paper v2/day runner 无 broker side effect；
- MiniQMT quote 来源没有 minute-bar adapter；
- execution plugin 不 import scheduler/client/gateway/repository/StrategyPackage，且新增 Iceberg/Stop 不修改 kernel dispatcher；
- TICK/TIMER/ORDER/TRADE/ACCOUNT/SESSION/EOD/RECONCILE/OPERATOR owner routing 无 broadcast fallback；
- vn.py compatibility façade 不创建第二 EventEngine、OMS、Gateway、quote subscription 或 broker session；
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

P0-H 真实路径还必须记录：上午启动、午休无误触发、下午恢复、真实 timer cadence、插件 state/replay、outbox→OMS/Gateway ACK、order/trade callback、EOD terminalization；mock/fake broker 测试不能替代正常交易日 MiniQMT SIM readback。

K1-C 的 F-053/F-058 实施边界已固定为两层 import proof：source-isolated loader 不执行 parent initializer；standard package import 通过 `backend.execution_algos` 与 `backend.services.miniqmt_execution_runtime` 的 lazy explicit exports，证明直接导入 compatibility package 不自动注册算法、加载 legacy adapter、初始化 runtime/repository/DB/Gateway/network/thread。显式请求既有公开 API 时仍加载同一模块集合，不改变产品 route。

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
| `F-026` | MiniQMT openInt 是可选交叉证据，缺失不阻断普通股票 quote，已提供非法值 fail loud |
| `F-027` | MiniQMT 等价 authority refresh 复用 compatible context/generation，不因 timestamp 重建制造漂移 |
| `F-028` | MiniQMT 零价零量盘口占位规范为空档，交易方向仅消费真实对手盘且不合成深度 |
| `F-029` | LocalSIM transient quote/market state 不作为 broker-neutral intent admission gate |
| `F-030` | LocalSIM 单 symbol 数据故障隔离，健康 symbol 继续且完整性冲突显式终态 |
| `F-031` | LocalSIM 每 cadence 冻结 active execution + passive position symbol 并集；每 unique symbol 一次 immutable snapshot/validation，execution/mark 共用且禁止 lazy expansion；hash/schema/previous outbox/mark reuse evidence fail loud |
| `F-032` | LocalSIM dependent BUY 保留、等待真实卖出回款并自动恢复；SELL-first 且同 side 保持 plan/restart 相对顺序；pending runner 不得伪报 completed；停牌/普通 residual 只在权威终结点形成 |
| `F-033` | symbol-aware 分钟算法与 participation 使用统一板块手数 authority，不硬编码 100 股改写合法订单；VWAP 无 authoritative profile 不得降级为全量或其它算法 |
| `F-034` | LocalSIM immutable economic snapshot 可规范化且 commit 前后 side-effect truth 精确；缺 durable receipt/state/outbox 的历史 run 不得伪装可重建 |
| `F-035` | MiniQMT B0 只消费真实 callback tick；单 symbol quote/depth wait 不阻断健康 symbol，且共享账户 batch preflight 原子性不被放宽 |
| `F-036` | MiniQMT SELL child 仅允许规范板块手数或 exact whole-position odd-lot residual；任意 partial odd-lot 在 broker 前拒绝 |
| `F-037` | MiniQMT broker trade restart replay 以 exact identity/time 合并 durable+snapshot facts，并 exactly-once 恢复 BUY/SELL cash、lot、child、algo |
| `F-038` | MiniQMT parent outcome、child、trade 与 daily-run/batch/intent projection 可重建；stale/conflict 只读可见且自动恢复，不自动 repair |
| `F-039` | runtime trade date、broker raw date/time 与 TCA UTC 规范化 authority 分离；numeric/compact/time-only/ISO 不歧义猜测 |
| `F-040` | MiniQMT Phase 1 physical generation 由同一 `data_session_key` 的 subscriber process lifecycle 单调拥有；last-lease release、failed prepare、rebuild、shutdown 后不得复用已分配或 fenced generation，迟到 callback 永久拒绝且 successor bootstrap/callback 继续由同一 single writer 接受 |
| `F-041` | MiniQMT durable batch 以唯一 parent intent identity 关联；合法 permutation 按 request canonical order 幂等恢复，写入/readback strict schema 一致且 malformed JSON identity 的 typed diagnostics 不产生二次异常；真实 corruption fail loud，multi-slice/restart 不覆盖或重复 side effect，scheduler 保持 per-binding isolation |
| `F-042` | LocalSIM Paper PostgreSQL durable fact writer 在 SQL execute 前对 order/fill/order-event/run-event JSON 执行与 economic hash 同义的 strict canonical conversion；合法 immutable mapping 可提交，非法 key/number/type 带 fact identity/path fail loud 并原子回滚，不得泄漏裸 serializer 异常、静默转换或形成半套事实 |
| `F-043` | MiniQMT 保持唯一 execution kernel；算法插件 side-effect-free，Selection/Execution、Gateway、OMS ownership 不漂移 |
| `F-044` | runtime event ingress、exact owner routing、per-algo predecessor sequence、真实 timer/session/EOD 与 single-writer 语义完整 |
| `F-045` | `ExecutionAlgoPluginV2` manifest/config/capability/state/version/migration、ALGO_START 和 failure contract 可直接实现 |
| `F-046` | `MarketDataViewV2` 按插件 capability 投影同一 B0 authority；静态 unsupported/当前暂缺/非法 observation 精确分离，缺字段不合成、不 fallback |
| `F-047` | transition/state/command outbox 在明确事务边界持久化；stale DISPATCHING 进入 unknown reconcile，retry/dedupe 不重复 broker side effect |
| `F-048` | registered pinned vn.py algorithm 使用 exact façade signature/DTO/return/error compatibility receipt，且不引入第二 EventEngine/OMS/Gateway owner |
| `F-049` | Sniper、BestLimit、TWAP Lite 迁移到同一 SPI，行为、A 股规则、timer/restart 和 attribution 不漂移 |
| `F-050` | Iceberg、Stop 只新增插件/manifest/tests 即可接入，证明 kernel 不依赖具体算法 |
| `F-051` | restart/replay、multi-slot、same-symbol、callback concurrency、plugin failure、diagnostics 与 event→delivery→transition→command→broker 完整 identity chain 可重建 |
| `F-052` | additive migration、旧 route 退役、rollout/rollback、生产 gates 与真实 SIM 验收完整且无人工门禁 |
| `F-053` | K1 模块/依赖/import boundary 固定，插件不能越权到信号、runtime owner、DB 或 broker |
| `F-054` | K1 strict DTO、public-marker-safe recursive deep immutability、JSON Schema definition/instance validation、canonical raw-digest/hex hash、exact event identity、identity/type/time/decimal/error evidence writer/readback contract 完整 |
| `F-055` | K1 route-independent code-owned catalog、serializable descriptor/process callable 分层、creation binding、aggregate build failure 与 per-route compatibility 语义完整 |
| `F-056` | K1 deterministic logical time、algo/delivery/local-order/command/timer/diagnostic exact keyed ID 与 effect hash、ordinal、raw-digest u53 random 在 retry/restart/readback 下稳定；通用 ID helper 不与 persisted DTO identity 形成双 authority |
| `F-057` | current-three exact manifest/config/state/event/capability/source、TWAP exchange-active seconds 与 legacy config shadow projection 完整且不改变现有 runtime |
| `F-058` | pinned vn.py source/method/DTO/enum/return/error lock 与 immutable compatibility receipt 精确 |
| `F-059` | K1 direct/negative/parity/import tests、changed-file test routing 与 coverage 可直接执行 |
| `F-060` | K1 rollout/rollback、K2-K4 边界、无 fallback/人工门禁/平行 route 与生产状态分离完整 |
| `F-061` | K2 当前直接副作用链、K1/K2/K3边界和信号/执行隔离事实完整 |
| `F-062` | K2 event/algo/delivery/transition/projection/mapping/outbox/worker-incarnation/timer/session/diagnostic schema、identity/hash与CHECK/unique/composite FK实施级合同完整；algo status不偏离父蓝图 |
| `F-063` | K2 ALGO_START、event routing、ordered delivery set和durable ACK事务完整 |
| `F-064` | K2 predecessor、lease/fence/CAS与state/effect transaction完整 |
| `F-065` | K2 failure/skip/active-child cancel/bounded retry/DB failure语义完整 |
| `F-066` | K2 command-child-broker mapping、outbox、nullable broker-called、callback race和unknown reconcile不重复broker side effect |
| `F-067` | K2 exact calendar/session authority、ExchangeSessionClock、durable timer、午休/catch-up/EOD/restart完整 |
| `F-068` | K2 DEV-first migration、幂等readback、legacy inventory与rollback完整 |
| `F-069` | K2 diagnostics/metrics/alerts/retention/runbook有界且无人工acknowledge |
| `F-070` | K2 direct/crash/concurrency/migration测试、coverage、routing和生产状态分离完整 |
| `F-071` | K3 current legacy side-effect chain、dependent-BUY coordinator carrier缺口、K2/K3/K4/K6边界、信号/执行隔离和唯一route事实完整 |
| `F-072` | exact factory/class/binding refs、transition-first construction、v3 pending command state及mapping/outbox lifecycle projection可直接实施，禁止临时身份回填或partial catalog |
| `F-073` | Sniper BUY/SELL/active-cancel/depth/fill/EOD exact行为与state lineage完整 |
| `F-074` | BestLimit quote/replace/deterministic draw ordinal/restart exact且无global random |
| `F-075` | TWAP plugin raw due与K2 effective exchange-active due authority分离，午休、PM、duration、slice、missing view、EOD/restart语义完整 |
| `F-076` | strict ORDER/TRADE/OMS RECONCILE/COMMAND_OUTCOME payload、pending command、terminal-trade-pending、outbox outcome、active-order/mapping/outbox和traded quantity transaction closure完整 |
| `F-077` | legacy policy/state/dependent-BUY read-only inventory和ALGO_LOCAL immutable parity receipt schema/hash/truncation/readback完整 |
| `F-078` | committed legacy repository facts驱动production-shape K2 public seam shadow orchestration，无algo branch/direct broker/第二runtime/route/fallback，且不把cross-parent coordination塞入plugin |
| `F-079` | typed failure、diagnostics/metrics/retention、concurrency/retry/rollback和无人工门禁完整 |
| `F-080` | K3 direct/parity/restart/integration/DEV repository shadow测试、visible cancel suppression、coverage/routing、K6 prerequisite和生产状态分离可执行 |
| `F-081` | K4 current facts、K1/K2/K3/K5/K6边界、信号/执行隔离与唯一runtime/OMS/Gateway/route事实完整 |
| `F-082` | K1-C authority复用与五算法+`round_to` helper source manifest exact，Iceberg/Stop只表征不注册、utility只AST提取不执行 |
| `F-083` | initialization/transition authority input、K2 optional invocation、通用adapter、collector lifecycle、ordinal/freeze/retry/restart合同可直接实施；K2 V1单/多command shadow边界精确，K5 shadow不增加kernel算法分支/状态机/route，产品aggregate仍由K6拥有 |
| `F-084` | 六个AlgoEngine方法、template helper与四个callback的shadow return/error/diagnostic/zero-command语义精确，含not-running/missing/rounded-zero空返回；product OMS同步reject语义由K6 disposition拥有 |
| `F-085` | Tick/Order/Trade/Contract DTO、selected Exchange/Status enum与event routing逐字段映射，TIMER durable TICK cutoff及missing/unsupported无合成/fallback |
| `F-086` | durable state envelope、constructor-once/restore/extract/freeze、active-child terminal mapping与façade effect复用existing K1/K2 identity；单command shadow链可重建，多command trace完整且不伪装V1 materialization，zero direct broker |
| `F-087` | implementation/method/state/terminal/DTO/isolated-module contract、runtime binding disposition、command authority disposition、conformance failure/receipt/set exact schema/hash/truncation/writer/readback/zero-partial publication完整 |
| `F-088` | current-three复用K3 parity，Iceberg/Stop source-isolated characterization-only，Iceberg TIMER lineage、K5边界与确定性输入完整 |
| `F-089` | typed error、concurrency/retry/restart、diagnostics/cardinality/retention、无人工门禁与无previous/latest fallback完整 |
| `F-090` | K4 direct/negative/integration/fresh-process/coverage/routing/F2/rollout/rollback/生产状态分离可执行 |
| `F-091` | K5 current facts、K1-K4复用、K5/K6边界、信号/执行隔离和禁止kernel算法分支/状态机/product-route改动完整 |
| `F-092` | Iceberg/Stop exact plugin identity、manifest、config、source attribution、event/capability合同及K4 `CHARACTERIZATION_ONLY_K5`到K1 shadow registration桥接可直接实施 |
| `F-093` | K4 conformance hardcode缺口以前后一致的单一pure evaluator闭合，source disposition保持不变，无平行schema/receipt/任意plugin-set authority |
| `F-094` | code-owned exact V2 binding literal、fresh sealed K4 binding equality与exact full-five descriptor/catalog/compatibility/creation/conformance composition、zero-partial publication和fresh-process readback完整 |
| `F-095` | config-only factory的唯一binding authority、constructor、strict config/state codec、restore/active-order/lineage closure完整，Iceberg pointer与cancel-pending mappings不混同 |
| `F-096` | Iceberg exchange-active TIMER、sequence-cutoff native B0 quote、cancel-pending resubmit/late callback、exact `traded >= target`、restart/multi-command语义精确 |
| `F-097` | Stop native TICK、signed price_add、limit bound、exactly-once trigger、exact `traded == target`、ORDER/TRADE/restart语义精确 |
| `F-098` | K2 shadow invocation、transaction/retry/concurrency/failure/diagnostics/metrics/retention完整且无人工门禁 |
| `F-099` | direct/negative/DEV PostgreSQL/fresh-process/coverage/changed-files routing/F2验收可执行 |
| `F-100` | source rollout/rollback、K6 prerequisite与source/production/runtime状态分离完整 |
| `F-101` | K6 current facts、K1–K5复用、signal/execution/LocalSIM隔离、两个真实缺口与K6/non-goal边界完整 |
| `F-102` | dependent-BUY V2 proceeds/dependency/trigger/ledger/decision/coordination strict schema、identity/hash、状态转换、bounds及V1 product拒绝可直接实施 |
| `F-103` | candidate、DEFER authority/mapping、SELL TRADE/ORDER/ACCOUNT/EOD、ledger cash authority、same-command release/block/residual/late/restart语义精确 |
| `F-104` | generic per-command V3 command_json/item/set/lifecycle/materialization、0/1/N、materialize/reject/defer与writer/readback authority闭合 |
| `F-105` | K6 additive表/列、deferred mapping status、composite FK/CHECK/UNIQUE/index/comment、pg_catalog fingerprint、successor migration/readback/guarded rollback完整且不复制K2表 |
| `F-106` | single-writer、lock order、CAS/fence、atomic product transaction与same-command release、commit-unknown、retry/restart/reconcile和no-double-release完整 |
| `F-107` | SIM/runtime-binding-account exact closure、route first/retry/allowed-successor、ALGO_START V2 product lineage、新实例唯一KERNEL_V2、禁止dual route/fallback及capability-bound rollback边界精确 |
| `F-108` | legacy helper/direct dependent-BUY/synchronous timer/adapter产品route退役的source-before/after与process-drain双inventory、source capability及唯一route证据可执行 |
| `F-109` | typed errors、bounded evidence、read-only diagnostics、低cardinality metrics、auto-clear alerts、retention和runbook完整且无人工门禁 |
| `F-110` | direct/negative/DEV PostgreSQL/migration/concurrency/final-route integration/route uniqueness/frozen-input/full-five source matrix/existing-binding normal-day/coverage/changed-files测试计划可执行，禁止legacy parity验收 |
| `F-111` | `K6-C0 -> K6-C1 -> K6-B -> K6-D`优先级、最终架构only/no-C2/no-legacy-R&D约束、依赖、工期、source/DDL/config/restart/runtime/normal-day状态分离与rollout/rollback完整 |
| `F-112` | DESIGN-COMPLIANCE-001、no simplification/silent error/business drift/unapproved gate及K6完成定义闭合 |

## 14. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-001` | §0.1 authority table and subordinate-doc banners | artifact: docs/architecture/simulation_platform_unified_authoritative_blueprint_20260715.md；authority keyword/link scan | design_ready | none |
| `F-002` | §5.11 same-PR progress contract and §15 ledger | artifact: docs/architecture/simulation_platform_unified_authoritative_blueprint_20260715.md；changed-simulation-PR blueprint diff review | design_ready | none |
| `F-003` | §0.2、§16 | artifact: docs/architecture/simulation_platform_unified_authoritative_blueprint_20260715.md；DESIGN-COMPLIANCE-001 four-control review | design_ready | none |
| `F-004` | §2.2 one-time package admission；§5.1 LocalSIM runtime component freeze | backend/tests/simulation_runtime/test_strategy_runtime_release.py；single/multi Alpha、model-code-required/optional active package binding 仍只检查 lifecycle；runtime policy tests 断言 `strategy_package_revalidation_performed=false` | implemented_verified | none |
| `F-005` | §5.1 `FrozenSimulationIdentityV1`；§9 P0-G BUG-797 exact LocalSIM policy authority | `validate_frozen_execution_policy_snapshot`；`StrategyRuntimeRelease` frozen fields；`ProductionSimulationRunContextProvider._resolve_local_sim_execution_policy`；`LocalSimBackend._resolve_execution_policy`；backend/tests/simulation_runtime/test_strategy_runtime_release.py、backend/tests/simulation_runtime/test_lifecycle_scheduler.py、backend/tests/paper_trading_v2/test_localsim_backend.py policy authority/hash/alias/release-ID-SHA/portfolio/manifest direct tests | implemented_verified | none |
| `F-006` | §4.1 shared main chain | backend/tests/simulation_runtime/test_target_rebalance_shared.py；selection-target-execution isolation tests | design_ready | none |
| `F-007` | §4.3、§5.4 LocalSIM event model；BUG-794 first causal wait generation；BUG-984 durable continuation authority；BUG-992 exact durable authority closure | backend/tests/paper_trading_v2/test_localsim_backend.py；backend/tests/simulation_runtime/test_lifecycle_scheduler.py first-wait→same-cursor replay→first-bar resume、`broker_called=false` 但 exact state/outbox/receipt closure 存在时 fresh-process continuation，以及 receipt/generation/current-run/state-hash public authority negative matrix | implemented_verified | none |
| `F-008` | §4.3、§5.4 | backend/tests/paper_trading_v2/test_localsim_backend.py；partial fill and remaining schedule tests | design_ready | none |
| `F-009` | §5.2、§10.3；BUG-794 exact failed-run recovery；BUG-984 active-run restart continuation；BUG-992 writer/readback shared authority | backend/tests/paper_trading_v2/test_localsim_backend.py；backend/tests/simulation_runtime/test_lifecycle_scheduler.py projection retry、exact receipt/outbox/generation/state/Paper readback、extra-active rejection、terminal-history repeatable readback、no-parent-resubmit、`broker_called` observation drift、missing outbox 和 mismatch terminal tests | implemented_verified | none |
| `F-010` | §5.3 | backend/tests/simulation_runtime/test_lifecycle_scheduler.py；historical/current-day/lunch/non-session freshness tests | design_ready | none |
| `F-011` | §5.3、§10.1 | backend/tests/trading_core/test_v25_execution_contract.py；V25 market-state/lot/T+1/limit/suspend regression tests | design_ready | none |
| `F-012` | §5.4 terminal contract；BUG-984 post-close state-first closure；BUG-992 lifecycle-incarnation authority | backend/tests/simulation_runtime/test_lifecycle_scheduler.py；post-close active durable state 必须先由既有 minute loop 消费 EOD；receipt-authoritative generation 之外仅同 run/plan/intent 的 terminal history 可审计保留，extra non-terminal state typed fail loud；run terminal 与 per-intent terminal closure direct tests | implemented_verified | none |
| `F-013` | §5.5；`PaperTradingV2Repository.local_sim_economic_transaction`；`SimulationRuntimeRepository.stage_local_sim_economic_commit`；BUG-794 no-mark generation；BUG-796 valuation-pending economic facts | backend/tests/simulation_runtime/test_lifecycle_scheduler.py；PostgreSQL single-connection commit/rollback、InMemory cross-repository rollback、first-wait 与 missing-held-mark order/state/receipt/outbox transaction、position hash 和 independent readback tests | implemented_verified | none |
| `F-014` | §5.2、§5.5；`LocalSimEconomicReceiptV1`、`LocalSimProjectionOutboxV1`、`LocalSimProjectionReceiptV1`；BUG-794/796 automatic recovery；BUG-992 strict generation/readback closure | backend/tests/simulation_runtime/test_lifecycle_scheduler.py；same-bar/restart dedupe、CAS、continuous `1..N` receipt history、strict non-boolean generation/high-watermark、receipt/economic-fact/outbox identity、connection retry max-3、wait/valuation projection retry、projected readback recovery、same-generation completion、account drift conflict、no-parent-resubmit tests | implemented_verified | none |
| `F-015` | §5.5-§5.6；`LocalSimMarketMarkV1`；`LocalSimBackend.load_authoritative_position_marks`；`_local_sim_position_marks`；BUG-796 transient-only pending classifier/completion evidence | backend/tests/paper_trading_v2/test_localsim_backend.py；backend/tests/simulation_runtime/test_lifecycle_scheduler.py；generic price/plan fallback rejection、realtime/historical/suspended provenance/hash、missing mark pending、schema/identity/source conflict fail-loud、completion hash/readback tests | implemented_verified | none |
| `F-016` | §4.4、§5.8；`B0QuoteV2ControllerFactory.prepare_assignment_transition` | backend/tests/miniqmt_execution_runtime/test_b0_quote_v2_lifecycle.py；real `WHOLE_QUOTE_CALLBACK` scheduler integration、empty/non-empty assignment transition、route owner tests | implemented_verified | explicitly approved production-state separation：正常交易日 production runtime evidence 仍按 `F-024` 单独记录；不表示 source 未完成或 runtime 已激活 |
| `F-017` | §5.7、§9 P0-C；`RebalanceIntentService`、`ExecutionPlanCompiler`、`PaperTradingV2Service.create_live_approval_candidate` | backend/tests/miniqmt_execution_runtime/test_b0_quote_v2_binding.py；frozen target/dropped-position reference identity、new/live-candidate binding、new parent、no LEGACY fallback tests | implemented_verified | explicitly approved production-state separation：production binding migration DML 尚未执行；不表示 source 未完成或 migration 已执行 |
| `F-018` | §6.2、§6.4..6.9 migration sequence；`MiniQMTRouteMigrationService` | backend/tests/simulation_runtime/test_miniqmt_route_migration.py；active parent/child/open-order/conflict/overflow zero-write、transaction/rollback/retry/readback/idempotency tests | implemented_verified | explicitly approved production-state separation：operator 仅完成 source；production dry-run/apply/readback 尚未执行 |
| `F-019` | §5.8、§6；retired router/Paper/day-runner/client paths | backend/tests/simulation_runtime/test_miniqmt_path_uniqueness.py；direct broker static guards、410/typed retirement、historical read-model tests | implemented_verified | explicitly approved production-state separation：source merge 后仍需正常交易日唯一路径观察；不表示观察已完成 |
| `F-020` | §5.9；`SimulationLifecycleScheduler._validated_miniqmt_tick_driver_result/_validated_miniqmt_tick_driver_batch_results`；`MiniQMTExecutionRuntimeClient._event_loop_existing_batch_result`；`_event_loop_requests_from_batch/_event_loop_results_from_batch`；`QmtClientMiniQMTEventLoopGateway`；`MiniQMTExecutionRuntime.record_order_event/record_trade_event/_terminalize_algo_if_all_children_terminal`；`MiniQMTOmsLedger.prepare_trade_fill`；`TcaCaptureDataError`、TCA exact parsers、lifecycle/client durable projection | backend/tests/miniqmt_execution_runtime/test_runtime.py；BUG-680/682/683/698 exact numeric、batch、TCA、pending/lifecycle matrices and dedicated L2 receipt | implemented_verified | none |
| `F-021` | §5.10、§7.2；`SimulationLifecycleBackgroundScheduler._record_loop_exception/_record_loop_success`；`SimulationRuntimeOpsService._scheduler_loop_health/_current_trade_date_blockers/platform_diagnostics`；`SimulationPlatformObservability`；BUG-794 LocalSIM active/lag projection；BUG-981 LocalSIM durable carrier authority closure；BUG-992 bounded typed corruption context | backend/tests/simulation_runtime/test_ops_api.py；BUG-681/687/697 health direct tests；naive Asia/Shanghai last-bar age、waiting-state active count/bar-lag；BUG-981 authoritative state-id map、legacy list、malformed schema 与 map-key identity；BUG-992 reason/run/generation/receipt/state/hash/identity JSON-safe context direct matrix | implemented_verified | none |
| `F-022` | §7；`/api/v1/simulation-runtime/platform-diagnostics`；`simulation_platform_diagnostics_v1`；`simulation_platform_operator_runbook_20260717.md` | backend/tests/simulation_runtime/test_ops_api.py；read-only/no-side-effect、bounded metrics/alerts、recovery auto-clear and runbook direct tests | implemented_verified | none |
| `F-023` | §10.1、§10.4；BUG-719 current-authority fixtures | backend/tests/simulation_runtime/test_lifecycle_scheduler.py；in-memory fixture no-DB/no-broker isolation and current event-loop/OMS/account/package/mark/EOD schema tests | implemented_verified | none |
| `F-024` | §5.11、§10.5、§15；BUG-687 runbook §6 与 progress ledger | artifact: docs/architecture/simulation_platform_unified_authoritative_blueprint_20260715.md；code/CI/dependency/DDL/config/restart/binding/runtime observation 独立状态 | implemented_verified | explicitly approved production-state separation：production runtime 仍需用户重启后按 runbook 独立只读核对，不表示 source contract 未完成或 runtime 已激活 |
| `F-025` | §9 P1 sequencing | artifact: docs/architecture/simulation_platform_unified_authoritative_blueprint_20260715.md；Phase 0B/Adaptive acceptance mapping and boundary validator | design_ready | none |
| `F-026` | §4.4 MiniQMT optional cross-evidence contract；`quote_eligibility.py` | backend/tests/miniqmt_execution_runtime/test_quote_eligibility.py；missing openInt ready、present unregistered/mismatch invalid direct tests | implemented_verified | none |
| `F-027` | §4.4 authority refresh；`miniqmt_quote_context.py` | backend/tests/simulation_runtime/test_miniqmt_quote_context.py；repeated equivalent preload preserves calendar/symbol object and generation direct test | implemented_verified | none |
| `F-028` | §4.4 directional depth；`quote_normalizer.py` | backend/tests/miniqmt_execution_runtime/test_quote_normalizer.py；zero/zero empty、zero/positive invalid、BUY ask-only/SELL bid-only direct tests | implemented_verified | none |
| `F-029` | §4.3、§5.3 validation ownership；scheduler broker-neutral planning | backend/tests/simulation_runtime/test_lifecycle_scheduler.py；transient quote blocked holding retains SELL intent and no pre-trade deletion direct test | implemented_verified | none |
| `F-030` | §4.3、§5.5、§9 P0-G symbol isolation；`LocalSimExecutionStateV1`；BUG-796 economic-first valuation pending | backend/tests/paper_trading_v2/test_localsim_backend.py；backend/tests/simulation_runtime/test_lifecycle_scheduler.py healthy fill + unavailable held symbol + restart/same-generation completion；§9 `AUDIT-LS-MARK-001` | implemented_verified | none |
| `F-031` | §5.3 `LocalSimMarketSnapshotV1`；LocalSim broker snapshot builder；§5.5-§5.6 valuation pending mark/outbox | backend/tests/simulation_runtime/test_lifecycle_scheduler.py；cadence union/no-refetch、canonical hash、mark reuse lineage、malformed outbox、transient-only pending、later cadence completion direct tests | implemented_verified | none |
| `F-032` | §4.3、§5.4 capital dependency/terminal/runner；LocalSim ledger cash-fit | backend/tests/paper_trading_v2/test_localsim_backend.py；realtime partial/wait/resume、plan-relative cash、restore order、close residual and RUN_PENDING direct tests | implemented_verified | none |
| `F-033` | §5.4、§9 P0-G Trading Core board-lot/VWAP authority；execution algos/adapter/minute engine；BUG-718 CI mapping；BUG-795 final-child residual contract | backend/tests/trading_core/test_minute_execution.py small-position multi-slice matrix、V24/V25 non-final 50-share reject + final exact remaining、V25.1 exact-remaining helper；backend/tests/paper_trading_v2/test_localsim_backend.py LocalSIM 300-share SELL direct test | implemented_verified | none |
| `F-034` | §5.5、§7、§9 P0-F/P0-G；`SimulationLifecycleScheduler._persist_local_sim_execution_result/_validate_local_sim_execution_states`；`SimulationPlatformObservability` | backend/tests/simulation_runtime/test_lifecycle_scheduler.py；backend/tests/simulation_runtime/test_ops_api.py immutable snapshot、cross-plan/action-chain、pre-commit truth、unreconstructible projection and exact recovery direct tests | implemented_verified | explicitly approved production-state separation：normal trading-day runtime observation remains separate |
| `F-035` | §4.4、§5.9、§9 P0-F；`MiniQMTExecutionRuntimeClient._event_loop_parent_request`、strict tick snapshot、per-symbol quote wait | backend/tests/miniqmt_execution_runtime/test_runtime.py；two-symbol missing/depth isolation、strict full snapshot、shared batch preflight atomic negative and no synthetic quote | implemented_verified | explicitly approved production-state separation：normal trading-day callback observation remains separate |
| `F-036` | §5.9、§9 P0-F；vn.py base、runtime child validator、`QmtManagedOrderService._sell_board_lot_error/_is_broker_disconnect_exception`；file ownership/nox existing-session mapping | backend/tests/qmt_strategy_ledger/test_order_service_preflight.py；backend/tests/trading_core/test_vnpy_style_execution_assets.py；backend/tests/scripts/test_ci_change_classifier.py；whole-position allow、partial residual reject、status-probe fail-loud、changed-file session selection | implemented_verified | none |
| `F-037` | §4.4、§5.9、§9 P0-F；`MiniQMTExecutionRuntime.recover/record_trade_event`；`MiniQMTOmsLedger` | backend/tests/miniqmt_execution_runtime/test_runtime.py；BUY/SELL replay twice、reserve release、T+1 lot、partial snapshot union、settlement retry and conflict negatives | implemented_verified | explicitly approved production-state separation：production broker replay observation remains separate |
| `F-038` | §7、§9 P0-F；client recovery projection、scheduler parent/child carriers、`SimulationRuntimeOpsService`、`SimulationPlatformObservability` | backend/tests/simulation_runtime/test_ops_api.py；backend/tests/simulation_runtime/test_lifecycle_scheduler.py；parent/child/trade projection stale/conflict and auto-clear | implemented_verified | explicitly approved production-state separation：diagnostics readback after merge/restart remains separate |
| `F-039` | §5.9、§9 P0-F；runtime broker time parsers；`tca_capture.py` | backend/tests/simulation_runtime/test_tca_capture.py；backend/tests/miniqmt_execution_runtime/test_runtime.py；epoch/compact/time-only/ISO and mismatch matrix | implemented_verified | none |
| `F-040` | §4.2 MiniQMT quote ownership、§4.4 tick model、§5.9 fail-loud health；`RealtimeQuoteSubscriber._next_phase_one_generation_locked`、callback capture/fence critical section、`QuoteIngressWorker` active/fenced diagnostics | backend/tests/infra/test_realtime_quote_subscriber_leases.py last-release/reacquire、failed candidate、late callback race、multi-session isolation；backend/tests/miniqmt_execution_runtime/test_quote_ingress.py failed-candidate callback/fence linearization、same-supervisor reacquire/normalized projection/active-failure auto-clear；backend/tests/miniqmt_execution_runtime/test_b0_quote_v2_lifecycle.py factory close/later runtime | implemented_verified | none |
| `F-041` | §5.8 durable batch identity contract；`_event_loop_results_from_batch`、`_event_loop_durable_identity_context`、`_canonical_event_loop_result_objects`、`_sync_event_loop_triggered_children_to_batches` | backend/tests/miniqmt_execution_runtime/test_runtime.py fifth-item permutation、arbitrary-JSON corruption diagnostics、strict pre-upsert carrier/alias validation、合法 pending/rejected/deferred、canonical serialization/replay/restart/multi-slice；backend/tests/simulation_runtime/test_lifecycle_scheduler.py exact replay failure per-binding isolation | implemented_verified | none |
| `F-042` | §5.5 PostgreSQL durable JSON boundary；`PaperTradingV2Repository.save_order/save_fill/save_order_event/save_run_event`、`local_sim_economic_transaction` | backend/tests/paper_trading_v2/test_repository_json_contract.py real psycopg2 adapter mappingproxy commit、unknown-type rollback、non-string key/non-finite negative matrix；backend/tests/simulation_runtime/test_lifecycle_scheduler.py immutable economic payload/pre-commit truth | implemented_verified | explicitly approved production-state separation：normal trading-day PostgreSQL runtime readback remains separate |
| `F-043` | §4.1-§4.2、§5.8、§9 P0-H；target `backend/services/miniqmt_execution_runtime/kernel.py` | artifact: docs/architecture/miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md；target backend/tests/miniqmt_execution_runtime/test_plugin_import_boundaries.py | design_ready | none |
| `F-044` | §4.4、§5.8、§9 P0-H；target event/routing/delivery/timer services | target backend/tests/miniqmt_execution_runtime/test_runtime_event_dispatcher.py exact owner routing + N/N+1 predecessor race；backend/tests/miniqmt_execution_runtime/test_exchange_session_clock.py | design_ready | none |
| `F-045` | §9 P0-H；target plugin contracts/registry/state/failure codec | target backend/tests/miniqmt_execution_runtime/test_algo_plugin_manifest.py；backend/tests/miniqmt_execution_runtime/test_algo_state_codec.py；ALGO_START/failure receipt direct tests | design_ready | none |
| `F-046` | §4.4、§9 P0-H；target B0 capability projector | target backend/tests/miniqmt_execution_runtime/test_market_data_capabilities.py static unsupported/current wait/invalid observation matrix | design_ready | none |
| `F-047` | §5.8、§9 P0-H；target repository/outbox/dispatcher | target backend/tests/miniqmt_execution_runtime/test_command_outbox_repository.py stale DISPATCHING/null truth/callback-before-ACK/no-resubmit；backend/tests/miniqmt_execution_runtime/test_algo_delivery_repository.py | design_ready | none |
| `F-048` | §5.8、§9 P0-H；target backend/execution_algos/vnpy_compat/ | target backend/tests/miniqmt_execution_runtime/test_vnpy_compat_facade.py pinned method signature/return/error/DTO characterization for all registered plugins | design_ready | none |
| `F-049` | §9 P0-H K3；K3 detailed design §1–§15 current-three plugin migration/ALGO_LOCAL parity/policy-state-dependent-BUY inventory | artifact: `docs/architecture/miniqmt_execution_kernel_k3_current_three_runtime_migration_f2_detailed_design_20260727.md`；target `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_parity.py`；existing `backend/tests/miniqmt_execution_runtime/test_miniqmt_vnpy_algo_parity_sniper.py` | design_ready | none |
| `F-050` | §9 P0-H K5；K5 detailed design §0–§13 exact Iceberg/Stop manifests/plugins/full-five shadow catalog | `backend/tests/miniqmt_execution_runtime/test_vnpy_plugin_extensibility.py`、`test_vnpy_k5_adapter_lifecycle.py`、`test_vnpy_k5_shadow_postgres.py`；MiniQMT=`1127/31`、Paper=`1050/2/2` | implemented_verified_local | none |
| `F-051` | §9 P0-H、§10.2-§10.3；runtime/repository/OMS/diagnostics | target backend/tests/miniqmt_execution_runtime/test_plugin_restart_recovery.py；backend/tests/miniqmt_execution_runtime/test_plugin_multi_slot_concurrency.py；plugin failure/active-child cancel/SKIPPED chain direct tests | design_ready | none |
| `F-052` | §9 P0-H K6、§10.5、§11-§12 | target backend/tests/miniqmt_execution_runtime/test_algo_plugin_migration_postgres.py；artifact: docs/architecture/miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md | design_ready | none |
| `F-053` | §9 P0-H K1；K1-A/B/C source-isolated + standard package import boundary；same-package transitive helper closure、single parent/worker authority、bounded repo-relative `PluginImportBoundaryReceiptV2` | `backend/tests/miniqmt_execution_runtime/test_plugin_import_boundaries.py` = 65 passed；新增 helper SQLite、wall-clock/global-random、`sys.modules`、nested forbidden owner、cycle 与 root-independent identity RED/GREEN；既有 raw FileIO、external dependency、malformed carrier、truncation 与 standard package evidence 保持；implementation `52e1c5a2`、CI run `30119335529` green | implemented_verified | none |
| `F-054` | §9 P0-H K1；`plugin_canonical.py`、`plugin_contracts.py` recursive FrozenJson marker validation、JSON Schema definition/instance authority、exact event/delivery/state/config/effect/error writer/readback closure | `backend/tests/miniqmt_execution_runtime/test_algo_plugin_contracts.py` caller-marker mutation、broken exception renderer、schema/type/hash/source identity/authority-time/readback negative matrix | implemented_verified | none |
| `F-055` | §9 P0-H K1；`backend/services/miniqmt_execution_runtime/plugin_registry.py` immutable catalog、descriptor/process binding、canonical persisted readback、exact creation/restore、non-empty bounded aggregate；route authority validation 三阶段保留 supplied authority typed failure，并仅在 authority 有效后比较 receipt | `backend/tests/miniqmt_execution_runtime/test_algo_plugin_registry.py` = 55 passed；authority-classification 4 RED 后 GREEN，覆盖 stale/hash/identity gateway、non-gateway、invalid snapshot、valid FAILED/PASSED 和 receipt drift；gateway reason/context JSON-safe 原样传播；PR #2655 final HEAD `97f7a030` required CI green，merge `ae1035a1` | implemented_verified + merged | none |
| `F-056` | §9 P0-H K1；`deterministic_context.py` 与 `plugin_contracts.py` exact context/algo/delivery/local-order/command/timer/diagnostic/effect identities | `backend/tests/miniqmt_execution_runtime/test_deterministic_execution_context.py` raw-digest u53/retry/restart matrix；`backend/tests/miniqmt_execution_runtime/test_algo_plugin_contracts.py` same-ID/different-payload、readback identity、logical-time negative matrix | implemented_verified | none |
| `F-057` | §9 P0-H K1；`backend/execution_algos/vnpy_style/plugin_manifests.py` current-three exact AISTOCK_DERIVED schema/source/executable behavior trace/active-order parent/local/broker/command/decimal/quantity/native-market-data/order-trade lineage state/TWAP/legacy projection；code-owned build tables recursive immutable；config/state schema failures 共用 K1-A bounded typed authority；shadow-only | `backend/tests/miniqmt_execution_runtime/test_current_three_plugin_manifests.py` 覆盖 top-level/nested/table mutation、fresh-process hash、31/32/>32 bounded schema evidence 与既有 current-three closure；K1-B-REVIEW-FIX RED 后 GREEN；PR #2655 final HEAD `97f7a030` required CI green，merge `ae1035a1` | implemented_verified + merged | none |
| `F-058` | §9 P0-H K1；exact dual-upstream V2 requirement/source/surface/receipt 与 K1-B component seam | `backend/tests/miniqmt_execution_runtime/test_vnpy_compatibility_receipts.py` = 49 passed；八 source、双 license/repo/tag/commit/path/hash/size、core object/enum AST、coordinated drift、fresh-process 与 current-three zero-publication；implementation HEAD `683cbd40` required CI green | implemented_verified | none |
| `F-059` | §9 P0-H K1；ownership/test routing/coverage | `backend/tests/miniqmt_execution_runtime/test_vnpy_compatibility_receipts.py`、`test_plugin_import_boundaries.py`、`test_algo_plugin_contracts.py`、`test_algo_plugin_registry.py`、`test_current_three_plugin_manifests.py` direct matrix=`268 passed`；import/surface/receipt line/branch=`88.27/77.88`,`89.61/81.34`,`88.46/70.00`；full classifier=`29 files`、`unmapped_code_files=[]`；CI run `30119335529` MiniQMT=`682/1`、Paper=`1042/2/1 deselected`，static/verdict green；本地 L0/registry PASS | implemented_verified | none |
| `F-060` | §9 P0-H K1；state-separated rollout/rollback | artifact: `docs/architecture/miniqmt_execution_kernel_k1_contracts_registry_f2_detailed_design_20260722.md`、`docs/architecture/miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md`、`docs/architecture/simulation_platform_unified_authoritative_blueprint_20260715.md`；三份 F2、Feature Workflow、DESIGN-COMPLIANCE-001 与 final CI run `30119969033` 闭合，implementation `52e1c5a2`；PR #2685 / merge `e4faeb53663cb4d19eb4e07d833953725a40fdc1`，K1-C `source_merge=merged_pr_2685`、K2/K3/K4 `not_started`、production/runtime gates `noop` | implemented_verified | none |
| `F-061` | §9 P0-H K2 detailed §1–§3 | artifact: `docs/architecture/miniqmt_execution_kernel_k2_durable_dispatch_f2_detailed_design_20260725.md`；K2-A shadow-only，无产品runtime wiring | design_ready | none |
| `F-062` | §9 P0-H K2 detailed §4.0–§4.10、§9、§12 K2-A-M1；strict carriers、唯一public repository façade、private responsibility modules与migration triplet | `backend/tests/miniqmt_execution_runtime/test_kernel_repository_structure.py`=`4 passed`、`test_kernel_contracts.py`=`33 passed`、`test_kernel_repository_postgres.py` DEV=`12 passed`、`test_kernel_migration_postgres.py` DEV=`11 passed`；public signature SHA-256=`7045f3c2...`，migration canonical-LF bytes=`e2a244d0.../24b4e189.../cb408aaf...` | design_ready | none |
| `F-063` | §9 P0-H K2 detailed §4.1、§4.6、§5–§6.2、§12 K2-B | `backend/tests/miniqmt_execution_runtime/test_kernel_ingress.py`、`test_kernel_creation.py`、`test_kernel_repository_postgres.py`闭合exact routing、ALGO_START authority、sequence、callback transaction与public bypass；focused/DEV/single-process=`50/14/794 passed`；PR #2773 / merge `db81b27e...` | implemented_verified | none |
| `F-064` | §9 P0-H K2 detailed §4.2–4.4、§6.3、§7、§12 K2-B | `backend/tests/miniqmt_execution_runtime/test_kernel_delivery.py`、`test_kernel_repository_k2b_validation.py`、`test_kernel_repository_postgres.py`闭合projection lineage、failure mapping/outbox、retry/reclaim/readback；L2=`772 passed,23 skipped`，核心line/branch=`85.99%/70.38%` | implemented_verified | none |
| `F-065` | §9 P0-H K2 detailed §4.2–§4.4、§6.3、§7.2、§12 K2-B | `backend/tests/miniqmt_execution_runtime/test_kernel_delivery.py`与`test_kernel_repository_postgres.py`覆盖bounded retry/no sixth call、pre-broker terminalization、accepted child CANCEL、outcome unknown、last-good state与queued successor SKIPPED；classifier仅MiniQMT | implemented_verified | none |
| `F-066` | §9 P0-H K2 detailed §4.5–§4.7、§6.4–6.5、§12 K2-D | `backend/tests/miniqmt_execution_runtime/test_kernel_outbox.py`、`test_kernel_repository_postgres.py`覆盖claim/fence、pre-call watermark、ACK/reject/unknown、safe retry、callback race、真实event lineage、append-only reconciliation history与post-commit readback | implemented_verified | none |
| `F-067` | §9 P0-H K2 detailed §4.8–§4.9、§8、§12 K2-C；`kernel_clock.py`与timer/session repository唯一authority、atomic claim/finalize/reclaim及additive migration | `backend/tests/miniqmt_execution_runtime/test_kernel_clock.py`=`16 passed`、line/branch=`90.10%/80.85%`；`test_plugin_import_boundaries.py`/`test_kernel_repository_structure.py` direct合计=`122 passed`；`test_kernel_migration_postgres.py` DEV=`13 passed`及`test_kernel_repository_postgres.py` atomic claim/reclaim/finalize/guarded rollback；`python -m nox -s miniqmt_execution_runtime_l2`=`789 passed,25 skipped`；final source `c87748cd...`、PR #2794 / merge `801dc3c9...`、CI run `30235878200` | implemented_verified + merged | none |
| `F-068` | §9 P0-H K2 detailed §9 | `backend/tests/miniqmt_execution_runtime/test_kernel_migration_postgres.py`验证K2-D DEV clean first/second apply、独立catalog readback、CHECK/UNIQUE/composite FK/index fingerprint与有durable rows时guarded rollback | implemented_verified | none |
| `F-069` | §9 P0-H K2 detailed §10 | `backend/tests/miniqmt_execution_runtime/test_kernel_diagnostics.py`覆盖NOT_APPLIED/NOT_ACTIVATED/NOT_FOUND、reason family、lag、lineage pending/closed、低cardinality metrics与auto-clear alerts；artifact `docs/operations/simulation_platform_operator_runbook_20260717.md` | implemented_verified | none |
| `F-070` | §9 P0-H K2 detailed §11–§13 | `python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_outbox.py backend/tests/miniqmt_execution_runtime/test_kernel_diagnostics.py -q`=`42 passed`；changed-files classifier只选择`miniqmt_execution_runtime_l2`=`832 passed,26 skipped`与`simulation_core_l2`=`438 passed`；DEV repository/migration走真实PostgreSQL disposable schema；核心line/branch均满足`>=80%/>=70%`；classifier=`unmapped_code_files=[]`；production/runtime gates=`noop` | implemented_verified | none |
| `F-071` | §9 P0-H K3 detailed §1–§3.4 current facts/boundaries/dependent-BUY carrier gap | `backend/tests/miniqmt_execution_runtime/test_miniqmt_vnpy_algo_import_boundary.py`精确pure-contract allowlist通过；产品route无修改 | implemented_verified_k3a | none |
| `F-072` | §9 P0-H K3 detailed §3–§5.1.3 exact factory/class/binding、transition-first construction、v3 pending command与lifecycle projection | `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_plugins.py`、manifest/contracts/registry direct tests进入MiniQMT L2 | implemented_verified_k3a | none |
| `F-073` | §9 P0-H K3 detailed §5–§6 Sniper exact behavior | `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_parity.py`与`test_current_three_kernel_plugins.py` | implemented_verified_k3a | none |
| `F-074` | §9 P0-H K3 detailed §5、§7 BestLimit deterministic behavior | `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_parity.py` deterministic vectors | implemented_verified_k3a | none |
| `F-075` | §9 P0-H K3 detailed §5、§8 plugin raw due + K2 clock effective due | `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_restart.py`覆盖session/restart vectors | implemented_verified_k3a | none |
| `F-076` | §9 P0-H K3 detailed §5.1.1–§5.4、§11 strict payload/lifecycle/outbox-outcome ingress | `backend/tests/miniqmt_execution_runtime/test_kernel_callback_events.py`、`test_kernel_outbox_outcome_ingress.py`及DEV repository exact node | implemented_verified_k3a | none |
| `F-077` | §9 P0-H K3 detailed §9–§10 policy/state/dependent-BUY inventory与ALGO_LOCAL parity contracts | `backend/tests/miniqmt_execution_runtime/test_current_three_contract_readback.py`、`backend/tests/miniqmt_execution_runtime/test_current_three_legacy_inventory.py`、`backend/tests/miniqmt_execution_runtime/test_current_three_parity_contracts.py` | implemented_verified_k3b | none |
| `F-078` | §9 P0-H K3 detailed §3–§3.4、§10.1.1、§13 committed legacy snapshot→strict event adapter→K2 public seams | `backend/tests/miniqmt_execution_runtime/test_current_three_shadow_source.py`证明snapshot锁覆盖普通/evidence写入与JSONL durable append及orphan/ambiguous/cross-parent/cross-slot callback typed fail；`backend/tests/miniqmt_execution_runtime/test_current_three_parity_contracts.py`证明`CHILD_ORDER_SUBMITTED` same-step/effect authority；两个DEV PostgreSQL测试验证真实source、committed parity与K2 durable shadow，zero dispatch attempt | implemented_verified_k3b | none |
| `F-079` | §9 P0-H K3 detailed §11–§12、§15 failure/diagnostics/rollback | `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_restart.py`、`backend/tests/miniqmt_execution_runtime/test_current_three_parity_contracts.py`、`backend/tests/miniqmt_execution_runtime/test_current_three_durable_shadow_postgres.py` | implemented_verified_k3 | none |
| `F-080` | §9 P0-H K3 detailed §10.2、§14–§16 visible transport suppression、validation/routing/gates/K6 prerequisite | review-fix direct=`116 passed,2 skipped`；`python -m nox -s miniqmt_execution_runtime_l2`=`988 passed,29 skipped`；`python -m nox -s paper_v2_backend`=`1050 passed,2 skipped,2 xfailed`；DEV=`2 passed`；变更K3-B核心模块line≥91%/branch≥77%；classifier选择MiniQMT/Paper且`unmapped_code_files=[]` | implemented_verified_k3 | none |
| `F-081` | §9 P0-H K4 detailed §0–§2 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k4_scope_boundaries.py`与changed-file review | implemented_verified | none |
| `F-082` | §9 P0-H K4 detailed §3、§12.1 | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_source_execution_v2.py`与conformance matrix | implemented_verified | none |
| `F-083` | §9 P0-H K4 detailed §4–§5 | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_kernel_invocation.py`与kernel delivery matrix | implemented_verified | none |
| `F-084` | §9 P0-H K4 detailed §5.4.1、§6 | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_source_execution_v2.py`的81-vector actual trace | implemented_verified | none |
| `F-085` | §9 P0-H K4 detailed §6.3–§8、§15 | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_repository_postgres.py` DEV=`2 passed` | implemented_verified | none |
| `F-086` | §9 P0-H K4 detailed §5.4.1、§5.6、§9–§10、§15 | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_lifecycle.py`与kernel invocation matrix | implemented_verified | none |
| `F-087` | §9 P0-H K4 detailed §4.1、§5.1、§11–§12 | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_conformance_authority_v2.py` | implemented_verified | none |
| `F-088` | §9 P0-H K4 detailed §3.2、§6.3、§10、§12 | artifact: `backend/execution_algos/vnpy_compat/characterization_artifacts/facade_characterization_vectors_v2.json`；`backend/tests/miniqmt_execution_runtime/test_current_three_plugin_manifests.py::test_source_attribution_hash_is_checkout_eol_independent`、`backend/tests/miniqmt_execution_runtime/test_algo_plugin_registry.py::test_registry_callable_source_hash_is_checkout_eol_independent`、`backend/tests/miniqmt_execution_runtime/test_vnpy_facade_source_execution_v2.py` | implemented_verified | none |
| `F-089` | §9 P0-H K4 detailed §13–§17 | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_source_execution_v2.py::test_trace_collector_preserves_primary_failure_when_reason_code_property_breaks`、`backend/tests/miniqmt_execution_runtime/test_vnpy_facade_source_execution_v2.py::test_exception_summary_sanitizes_before_bounded_truncation`、`backend/tests/miniqmt_execution_runtime/test_vnpy_facade_conformance_authority_v2.py::test_k3_preflight_failure_records_active_characterization_failure`、`backend/tests/miniqmt_execution_runtime/test_vnpy_facade_diagnostics.py::test_characterization_success_does_not_clear_active_failure_before_conformance` | implemented_verified | none |
| `F-090` | §9 P0-H K4 detailed §18–§20 | 非DB direct=`202 passed,1 skipped`；DEV=`2 passed`；coverage八核心line/branch均`>=80/>=70`；`python -m nox -s miniqmt_execution_runtime_l2`=`1099 passed,30 skipped`；`python -m nox -s paper_v2_backend`=`1050 passed,2 skipped,2 xfailed`；`python -m nox -s l0`、`python -m nox -s validation_module_registry_l0`及三份F2 validator通过 | implemented_verified | none |
| `F-091` | §9 P0-H K5 detailed §0–§3 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_scope_boundaries.py`、K1 import-boundary exact nodeid通过 | implemented_verified_local | none |
| `F-092` | §9 P0-H K5 detailed §4 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_plugin_manifests.py`=`4 passed` | implemented_verified_local | none |
| `F-093` | §9 P0-H K5 detailed §2.3、§4.6、§5.3 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_facade_conformance.py`通过 | implemented_verified_local | none |
| `F-094` | §9 P0-H K5 detailed §5 | `backend/tests/miniqmt_execution_runtime/test_vnpy_plugin_extensibility.py`、`backend/tests/miniqmt_execution_runtime/test_vnpy_k5_shadow_catalog.py`、`backend/tests/miniqmt_execution_runtime/test_vnpy_k5_facade_conformance.py`通过 | implemented_verified_local | none |
| `F-095` | §9 P0-H K5 detailed §5.2、§5.4、§6 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_plugin_manifests.py`=`4 passed`、`backend/tests/miniqmt_execution_runtime/test_vnpy_k5_adapter_lifecycle.py`=`15 passed` | implemented_verified_local | none |
| `F-096` | §9 P0-H K5 detailed §7 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_adapter_lifecycle.py`=`15 passed` | implemented_verified_local | none |
| `F-097` | §9 P0-H K5 detailed §8 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_adapter_lifecycle.py`=`15 passed` | implemented_verified_local | none |
| `F-098` | §9 P0-H K5 detailed §9–§11 | `AISTOCK_RUN_MINIQMT_K2_DEV_DB=1 python -m pytest backend/tests/miniqmt_execution_runtime/test_vnpy_k5_shadow_postgres.py -q`=`1 passed` | implemented_verified_local | none |
| `F-099` | §9 P0-H K5 detailed §12 | coverage aggregate=`38 passed`；`python -m nox -s miniqmt_execution_runtime_l2`=`1127/31`；`python -m nox -s paper_v2_backend`=`1050/2/2` | implemented_verified_local | none |
| `F-100` | §9 P0-H K5 detailed §13 | artifact: K5 detailed design、父蓝图、统一蓝图；PR #2978 / merge `4bf54cf2`、final required CI run `30640380170`，source/production/runtime状态分离 | implemented_verified | none |
| `F-101` | §9 P0-H K6 detailed §0–§3 | target `backend/tests/miniqmt_execution_runtime/test_kernel_product_cutover.py` scope/owner/no-diff matrix | design_ready | none |
| `F-102` | §9 P0-H K6 detailed §4.1；`kernel_product_contracts.py` | `python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_product_contracts.py -q`：V2 proceeds/ledger/coordination initial/successor与strict readback；PR #3032 / merge `2a3622a3ba63585e3dfe12ef7ccb3f33b00dcb63` | implemented_verified + merged | none |
| `F-103` | §9 P0-H K6 detailed §7 | target `python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_dependent_buy.py -q` | design_ready | none |
| `F-104` | §9 P0-H K6 detailed §4.2、§8；V3 envelope/evaluator/materializer与BUG-953 lineage/lifecycle closure | `backend/tests/miniqmt_execution_runtime/test_kernel_product_contracts.py`、`test_kernel_product_authority.py`、`test_kernel_product_materialization_postgres.py`：contracts/authority/DEV PostgreSQL direct=`105 passed`，complete disposable DEV PostgreSQL=`20 passed`；PR #3080 / merge `c0ca2e7e356a891ee5b2aad270d391071c942ae4` | implemented_verified + merged | none |
| `F-105` | §9 P0-H K6 detailed §5；K6-A immutable migration + `miniqmt_execution_kernel_k6c_20260802.*` | `AISTOCK_RUN_MINIQMT_K2_DEV_DB=1 python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_k6_migration_postgres.py -q`；PR #3032 / merge `2a3622a3ba63585e3dfe12ef7ccb3f33b00dcb63` | implemented_verified + merged | none |
| `F-106` | §9 P0-H K6 detailed §6；versioned preflight、BUG-953 exact mapping CHECK与C1 atomic writer/readback | `AISTOCK_RUN_MINIQMT_K2_DEV_DB=1 python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_product_materialization_postgres.py -q`=`20 passed`，覆盖rollback、commit-unknown、same-authority concurrency、drift、CLAIMED/FAILED_TERMINAL及released DEFER lifecycle | implemented_verified + merged | none |
| `F-107` | K6 detailed §4.3/§9；K6-D detailed §3–§5 | `backend/tests/miniqmt_execution_runtime/test_kernel_product_cutover.py`、`backend/tests/miniqmt_execution_runtime/test_kernel_product_runtime_integration.py` + DEV route/ALGO_START matrix；PR #3146 / merge `33c09049e82c11cdbae7cd9b596b3666cb481349` | implemented_verified + merged | none |
| `F-108` | K6 detailed §9.2；K6-D detailed §2.2/§6 | `backend/tests/miniqmt_execution_runtime/test_kernel_legacy_route_retirement.py` exact symbol absence + five-root AST/fresh-process source capability；PR #3146 / merge `33c09049e82c11cdbae7cd9b596b3666cb481349` | implemented_verified + merged | none |
| `F-109` | K6 detailed §10；K6-D detailed §7 | `backend/tests/miniqmt_execution_runtime/test_kernel_diagnostics.py`、`backend/tests/simulation_runtime/test_ops_api.py` + DEV route readback；artifact: `docs/operations/simulation_platform_operator_runbook_20260717.md`；PR #3146 / merge `33c09049e82c11cdbae7cd9b596b3666cb481349` | implemented_verified + merged | none |
| `F-110` | K6 detailed §11；K6-D detailed §5/§8 | `python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_product_runtime_integration.py backend/tests/miniqmt_execution_runtime/test_kernel_clock.py -q`；callback/scheduler clock/EOD/binding isolation/direct/DEV/coverage；PR #3146 / merge `33c09049e82c11cdbae7cd9b596b3666cb481349` | implemented_verified + merged | none |
| `F-111` | K6 detailed §12–§13；K6-D detailed §9–§10 | artifact: `docs/architecture/miniqmt_execution_kernel_k6d_final_route_cutover_f2_detailed_design_20260804.md`；source/production/runtime分离receipts；PR #3146 / merge `33c09049e82c11cdbae7cd9b596b3666cb481349` | implemented_verified + merged | none |
| `F-112` | K6 detailed §16–§17；K6-D detailed §13–§14 | artifact: `docs/architecture/miniqmt_execution_kernel_k6d_final_route_cutover_f2_detailed_design_20260804.md`；formal DESIGN-COMPLIANCE-001；PR #3146 / merge `33c09049e82c11cdbae7cd9b596b3666cb481349` | implemented_verified + merged | none |

## 15. Current Implementation Progress Ledger / 当前实现进度账本

状态枚举：`IMPLEMENTED_VERIFIED`、`REPAIR_REQUIRED`、`EVIDENCE_REFRESH_REQUIRED`、`DESIGN_ONLY`、`HISTORICAL_RETIRED`。本表记录当前摘要；详细历史以 Git/PR/BUG/CI 为准。

| Progress ID | Acceptance IDs | Current state after this PR（PR #2685 base `main@7b642fd6`） | Evidence | Status | Next implementation slice |
| --- | --- | --- | --- | --- | --- |
| `SIM-P-001` | `F-004..006` | 单/多 Alpha 策略包一次准入、冻结 identity 和 broker-neutral selection/target 已建立 | `localsim_strategy_package_single_admission_f2_design_20260714.md`、PR #2103 | IMPLEMENTED_VERIFIED | 持续防止 runtime 二次 package 校验 |
| `SIM-P-002` | `F-005,016,017` | BUG-654/657 已修复 B0 context 发布、lot/tradability authority、失败持久化和安全恢复 | commits `02e73de6`、`f4392711`；本设计核对 2026-07-15 相关 direct tests 7 passed | IMPLEMENTED_VERIFIED | 纳入唯一路径退役验证 |
| `SIM-P-003` | `F-005` | BUG-658 已允许 unchanged authoritative manifest roll-forward 且拒绝虚假变更 | commit `43ce19de`；本设计核对 2 direct tests passed | IMPLEMENTED_VERIFIED | 保持 frozen identity contract |
| `SIM-P-004` | `F-007..012` | BUG-660 已实现 durable per-intent state、batch CAS/readback、realtime incremental minute loop、partial continuation、restart cursor/bar-hash 去重和 close residual；历史闭市日同步路径保持不变 | PR #2174、merge `02cc8bde`；direct LocalSIM/scheduler tests | IMPLEMENTED_VERIFIED | P0-B source merge 后由用户重启并补正常交易日 runtime evidence |
| `SIM-P-005` | `F-013,014` | BUG-661 已实现 Paper facts/state/run/outbox 同连接事务、single-writer、canonical receipt/hash、projection replay、same-bar dedupe、commit/readback 分离恢复；仅连接类错误执行最多 3 次 projection attempt，business/schema/CAS 冲突立即 terminal，readback 独立有界复核 | PR #2187、merge `f74fdf3b`；close-sync PR #2189、merge `7d7d1434`；direct transaction/restart/retry-exhaustion/nonretryable/readback tests | IMPLEMENTED_VERIFIED | source 已合入；production DDL/config 为 noop，restart/runtime observation 尚未核验；补正常交易日 runtime evidence |
| `SIM-P-006` | `F-015` | BUG-661 已删除计划价和通用 price map 的 mark fallback；broker 从执行使用的同一 minute provider 读取最后一个 causal close，`LocalSimMarketMarkV1` 强制真实 source/as-of/provenance/hash 与 authoritative previous-close 停牌证明 | PR #2187、merge `f74fdf3b`；close-sync PR #2189、merge `7d7d1434`；generic-price negative、realtime/historical/suspended provenance、LocalSIM partial/restart tests | IMPLEMENTED_VERIFIED | source 已合入；restart/runtime observation 尚未核验；核对真实历史日/当日 mark provenance |
| `SIM-P-007` | `F-016` | MiniQMT canonical runtime 已有真实 tick callback、bootstrap、durable event loop | Phase 1 design/PR #2019、BUG-604/614 tests | IMPLEMENTED_VERIFIED | 唯一路径静态与真实 SIM 持续验证 |
| `SIM-P-008` | `F-016..019` | BUG-662 将 MiniQMT 新 binding/new parent 收敛到 exact `B0_QUOTE_V2`，补齐 LEGACY binding 原子迁移/readback operator，永久退役 Paper/day-runner/raw-router/client broker side-effect，并以真实 callback 驱动 scheduler 回归；production DML/config/restart/broker 均未执行 | BUG-662 / issue #2190；migration 13 tests、route/Paper core 50 tests、scheduler 158 passed + 6 exact-main baseline failures、assignment transition 5 tests | IMPLEMENTED_VERIFIED | source merge/close-sync 后，单独执行 production migration dry-run/readback；用户授权 DML 与重启后再做正常交易日证据 |
| `SIM-P-009` | `F-020,021` | BUG-680 已移除 MiniQMT tick-driver/recovery/pending/submitted count 的 pass/归零并收紧 callback/OMS canonical fact；BUG-681 已消除 scheduler thread-alive false green；BUG-682 已消除 TCA quote/preflight 静默归一；BUG-683 已移除 durable request/result 过滤、错配、padding 以及 raw batch/status/total/default carrier；BUG-687 在 `SIM-P-011/030` 独立闭合全平台聚合 health；BUG-698 在 `SIM-P-032` 闭合 no-child pending、BEST_LIMIT/TWAP lifecycle 和专属 CI 选择。 | BUG-680 / issue #2232 direct 31 passed；BUG-681 / issue #2236 related 20+1 passed；BUG-682 / issue #2239 related 37 + lifecycle 2 passed；BUG-683 / issue #2243 direct 34 passed、related 18 + 24 passed；BUG-698 / issue #2282 direct 7 passed | IMPLEMENTED_VERIFIED | 用户重启后分别只读核对 tick-driver/batch 与平台 diagnostics，不把 source tests 写成 runtime activated |
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
| `SIM-P-029` | `F-020,024` | BUG-683 为 event-loop durable batch replay 和 scheduler tick-driver batch persistence 建立 exact request/result/carrier/status/cardinality contract；非法 current/foreign batch 在覆盖 run 前 typed fail loud，合法 no-child tick 保持空 batch_results；不再过滤、错配或伪造 pending result | BUG-683 / issue #2243；new direct 34 passed；scheduler `-k miniqmt_tick_driver` 18 passed；client `-k event_loop` 24 passed；旧 no-child 测试语义由 BUG-698 独立闭合 | IMPLEMENTED_VERIFIED | source PR/CI/merge 后由用户重启并只读核对 batch/status/total/readback；DDL/DML/config/broker call 均为 noop |
| `SIM-P-030` | `F-021,022,024` | BUG-687 建立 `/platform-diagnostics` 和 `simulation_platform_diagnostics_v1`：五类 identity、六层 facts、LocalSIM/MiniQMT exact durable/business projection、低基数 metrics、自动解除 alerts、read-only side-effect contract 与固定 operator runbook；异常扫描/cardinality/payload/identity 及 live-admission bool/count 均 typed fail loud | BUG-687 / issue #2254 / PR #2269；platform direct 17 passed、run-detail exact 1 passed、ops related matrix 33 passed、live-admission related 3 passed；new observability statements 85.80%、branches 71.85%；Ruff/format/pycompile/diff-check、CodeGraph/UA 4/4、F2 25/25、l0/registry pass；PR CI green | IMPLEMENTED_VERIFIED | source merge 后由用户重启；分别补正常交易日 LocalSIM/MiniQMT platform readback，production DDL/dependency/config/binding/broker 均为 noop |
| `SIM-P-031` | `F-005,016,021,022,024` | BUG-697 将 invalid historical MiniQMT source 的 expired roll-forward/manifest rebase 在创建 release 前 exact fail-loud 并持久化 binding-scoped failure，继续有效 LocalSIM/MiniQMT；platform diagnostics 在零 run 时也消费 scheduler current blocking result，生成 BLOCKED、metric、alert 与 recovery auto-clear | BUG-697 / issue #2279；2026-07-17 production readback：scheduler `processed=[]` 且 `MINIQMT_B0_QUOTE_V2_BINDING_REQUIRED`，platform false-green `NOT_YET_RUN/alerts=0`；修复后 direct 2 passed、related roll-forward 6 passed、platform 18 passed | IMPLEMENTED_VERIFIED | source PR/CI/merge 后由用户重启；确认两个有效 binding 恢复推进，invalid source 仅自身失败；DDL/DML/config/broker call 均为 noop |
| `SIM-P-032` | `F-020,023,024` | BUG-698 将过期 no-child 立即失败测试改为 exact durable pending 契约；普通 callback 不再把仍 running 的 BEST_LIMIT/TWAP 多阶段 algo 提前终止，rejection/core-finished/operator override 保持 exact；LEGACY/OMS fixtures 对齐当前 authority；并建立 critical module ownership、专属 runner-enabled L2 plan、CI classifier 精确选择与完整目录回归 | BUG-698 / issue #2282；原 nodeid 与首次完整矩阵 4 failures 复现；修复 direct 7 passed；ownership 定向 3/3；`miniqmt_execution_runtime_l2` 293 passed、1 skipped；classifier service/test direct；L0/registry/catalog pass；Validation Center 476 passed 后唯一新增-plan 预期漂移 nodeid 补修 1 passed | IMPLEMENTED_VERIFIED | source PR/CI/merge；正常交易日仍按既有 runbook 观察真实 callback/child，不执行 DDL/DML/config/restart/broker activation |
| `SIM-P-033` | `F-026,024` | BUG-706 已将 MiniQMT openInt 收敛为 optional cross-evidence；缺失不再拒绝普通股票 quote，已提供未知值仍明确 invalid | BUG-706 / issue #2307；MiniQMT 三文件 direct matrix 73 passed；Ruff/F2/L0/registry/diff-check pass | IMPLEMENTED_VERIFIED | 创建 PR/CI；合入和重启后再观察真实 quote |
| `SIM-P-034` | `F-027,024` | BUG-707 已使等价 calendar/symbol authority refresh 复用原 context/generation，仅 observation timestamp 变化不再制造漂移 | BUG-707 / issue #2308；MiniQMT 三文件 direct matrix 73 passed；Ruff/F2/L0/registry/diff-check pass | IMPLEMENTED_VERIFIED | 创建 PR/CI；正常交易时段只读观察 generation 稳定性 |
| `SIM-P-035` | `F-028,024` | BUG-709 已把 exact zero-price/zero-quantity 深度规范为空档，保留 zero-price/positive-quantity invalid，并按 BUY ask/SELL bid 方向判定 | BUG-709 / issue #2311；MiniQMT 三文件 direct matrix 73 passed；Ruff/F2/L0/registry/diff-check pass | IMPLEMENTED_VERIFIED | 创建 PR/CI；正常交易时段观察真实单边盘口 |
| `SIM-P-036` | `F-029,024` | BUG-711 已删除 LocalSIM plan 阶段 transient quote intent gate；broker-neutral intent 保留；停牌无 bar 使用自动等待/收盘 residual，非停牌缺 close bar 继续 fail loud | BUG-711 / issue #2313；补充 related matrix `107 passed`，含停牌/普通 close 及 canonical suspension schema 正反 direct tests | IMPLEMENTED_VERIFIED | 更新 PR #2325/CI；合入和用户重启后核对当日 plan/intents 与停牌 state |
| `SIM-P-037` | `F-030,024` | BUG-712 已把 LocalSIM provider/stream failure 隔离到 symbol state；健康 symbol 继续，确定性 payload conflict 形成 `FAILED_TERMINAL` order/state/event | BUG-712 / issue #2314；原批次 direct matrix `169 passed`，补充 related matrix `107 passed` | IMPLEMENTED_VERIFIED | 更新 PR #2325/CI；合入和用户重启后核对多 symbol run 隔离事实 |
| `SIM-P-038` | `F-031,024` | BUG-714 已建立 immutable `LocalSimMarketSnapshotV1`；每 cadence 冻结 active execution + passive positions 并集，每 unique symbol 只读/验证一次并由 intent/mark 共用；same-cadence 禁止扩容；hash、previous outbox 和 mark reuse evidence 均严格可重建 | BUG-714 / issue #2316；补充 related matrix `107 passed`，含 cadence union/no-refetch、canonical hash、unsupported/non-finite、mark lineage、malformed outbox 正反 direct tests | IMPLEMENTED_VERIFIED | 更新 PR #2325/CI；合入和用户重启后核对 provider cadence、snapshot identity 与 mark lineage |
| `SIM-P-039` | `F-032,024` | BUG-715 已保留全部 BUY，SELL-first 后按 ledger cash 部分成交或等待卖出回款；同 side 保持 plan 顺序且 restart restore 不按字典序重排；legacy runner 对 pending/partial 写 `RUN_PENDING`，不再伪报 completed | BUG-715 / issue #2317；补充 related matrix `107 passed`，含 plan-priority cash competition、wait/resume、restore-order source、RUN_PENDING/full lifecycle 正反 direct tests；广泛 `paper_v2_backend` 委派 PR CI/Validation Center/nightly | IMPLEMENTED_VERIFIED | 更新 PR #2325/CI；合入和用户重启后核对 WAITING_FOR_CAPITAL/自动恢复/收盘 residual，不执行人工确认 |
| `SIM-P-040` | `F-033,024` | BUG-717 已让 symbol-aware legacy minute algorithms 与 participation cap 使用统一板块手数 authority，合法科创板 201 股不再被改写；VWAP 缺少/非法 authoritative volume profile 时 typed fail loud，不再首 bar 全量 fallback | BUG-717 / issue #2323；补充 related matrix `107 passed`，含 TWAP/VWAP/AC/POV/SBB main-board+STAR init、VWAP profile 正反、LocalSIM rejection、V24 missing-asset fail-loud；V25 contracts 继续由既有 direct/CI 覆盖 | IMPLEMENTED_VERIFIED | 更新 PR #2325/CI；不执行 DDL/DML/config/restart/broker call |
| `SIM-P-041` | `F-033,024` | BUG-718 已将 minute execution algorithm、adapter、engine 与 direct tests 精确映射到既有 `paper_v2_backend`，保留 unmapped executable-code fail-closed；本次严格复核补齐遗漏的 `execution_algo_adapter.py` ownership/classifier | BUG-718 / issue #2328；PR #2325 CI run `29580550883` 精确复现 adapter unmapped；classifier direct、full PR classification、ownership/module registry 验证 | IMPLEMENTED_VERIFIED | 更新 PR #2325 并等待完整 `paper_v2_backend`/static/Semgrep/CodeQL CI，不缩窄 session |
| `SIM-P-042` | `F-023,024` | BUG-719 已把 simulation L2 陈旧 fixtures 对齐当前 event-loop、OMS/account、StrategyPackage lifecycle、LocalSIM mark 与 EOD schema，产品 fail-fast 语义不变 | BUG-719 / issue #2329；旧 main 7 failures 复现，修复 exact 7 passed、`simulation_core_l2` 395 passed | IMPLEMENTED_VERIFIED | 更新 PR #2325/CI；正常交易日 runtime readback 继续独立记录 |
| `SIM-P-043` | `F-034,024` | BUG-779 规范化 immutable LocalSIM snapshot，并让 pre-commit failure、broker_called、durable receipt/state/outbox truth 一致；strict JSON 禁止 key coercion collision 与 non-finite number；不可重建历史 run 只读 BLOCKED，不补写经济事实 | BUG-779 / issue #2488；mappingproxy canonical、non-string key collision、NaN/Infinity、scheduler persistence failure、ops/platform unreconstructible direct tests | IMPLEMENTED_VERIFIED | PR/CI/merge 后由用户重启并只读核对；DDL/DML/config/broker call 均为 noop |
| `SIM-P-044` | `F-035,024` | BUG-780 取消 B0 普通 quote 预取，真实 callback tick 完整 capture/hash；transient quote/directional depth 逐 symbol wait，健康 parent 继续；共享 account/position/cash batch preflight 保持原子失败 | BUG-780 / issue #2489；two-symbol quote/depth isolation、strict snapshot、shared preflight regression direct tests | IMPLEMENTED_VERIFIED | PR/CI/merge 后观察真实 callback；不执行 binding/config/DML 或 broker 激活动作 |
| `SIM-P-045` | `F-036,024` | BUG-781 统一 vn.py/runtime/managed-order SELL board-lot：仅 exact whole available position 可携带 odd-lot residual，partial odd-lot child 在 gateway 前拒绝；补齐 qmt strategy ledger/vn.py asset ownership与既有 execution sessions，禁止 classifier unmapped 导致测试 skipped；broker status probe failure 不再静默转换为 disconnect | BUG-781 / issue #2490；order preflight、vn.py asset、runtime child validator、status-probe cash/intent fail-loud 正反 direct tests；classifier existing-session direct test；changed-file L0 0 HIGH findings | IMPLEMENTED_VERIFIED | PR/CI/merge 后正常交易日观察 child quantity；DDL/DML/config/restart/broker activation 均分离 |
| `SIM-P-046` | `F-037,039,024` | BUG-782 以 exact broker trade identity/time 合并 snapshot 与 archived event，重放 BUY/SELL cash/lot/child/algo；同秒 fill 稳定保持权威相对顺序，cumulative 不冒充 trade identity；event-before-settlement/child-before-vn.py-core retry 可续做且 exactly-once | BUG-782 / issue #2491；BUY/SELL double recovery、partial snapshot union、same-second reverse-trade-id、settlement fault injection、child-before-core restart checkpoint、identity/time negative direct tests | IMPLEMENTED_VERIFIED | PR/CI/merge 后由真实 broker snapshot readback 验证；本 slice 未调用生产 broker |
| `SIM-P-047` | `F-038,024` | BUG-783 分离 parent outcome 与 child/trade count，restart 回投影 batch/intent，accepted child 不被后续 reject 覆盖；gateway ack 到 child/event/result/intent/diagnostics 保留 exact broker_called，intent status 与 projection metadata 原子更新；只读 diagnostics 对 stale/conflict 生成 metric/auto-clear alert | BUG-783 / issue #2492；scheduler one-parent/multi-child、client recovery、missing-place_order false chain、accepted intent metadata、ops pre-broker rejection、repository metadata update、platform stale/clear direct tests | IMPLEMENTED_VERIFIED | PR/CI/merge 和用户重启后读取 platform diagnostics；不自动 repair、不新增 execution gate |
| `SIM-P-048` | `F-039,024` | BUG-784 固定 runtime trade-date authority，保留 broker raw date/time/mismatch；broker date/time aliases 先规范化再交叉校验；TCA numeric/compact/time-only/ISO 精确解析，exchange-local 转 UTC，歧义或冲突输入 fail loud | BUG-784 / issue #2493；TCA parser full matrix、runtime broker-time recovery 与 date/time alias conflict direct tests | IMPLEMENTED_VERIFIED | PR/CI/merge 后只读核对实际 broker/TCA 时间；DDL/DML/config/restart 均为 noop |
| `SIM-P-049` | `F-007,009,013,014,021,022,034,024` | BUG-794 已实现 first-causal-bar no-mark economic generation、wait outbox projection/retry、exact projected-generation recovery、state/order/intent action-chain closure、cross-plan fact fail-loud，以及真实 observation-age bar lag；失败 recovery 不再误归类为 pre-run，不重建 parent | BUG-794 / issue #2524；first wait/replay/resume、wait projection retry、normal projection readback recovery、exact durable recovery/refusal、cross-plan/action-chain、diagnostics lag direct tests；20 项相关 small matrix、DESIGN-COMPLIANCE-001、F2 39/39、changed-file Ruff/pycompile/diff-check、L0/registry 均已通过 | IMPLEMENTED_VERIFIED | 创建 PR 并检查 CI；合入后由用户重启并补正常交易日只读 evidence；DDL/DML/config/broker 均为 noop |
| `SIM-P-050` | `F-033,024` | BUG-795 已统一全部注册分钟算法的 non-final board-lot/final exact residual 语义；Base child 默认拒绝 residual，TWAP/POV/V25 只在可证明 final 时显式启用，V24 等继承统一默认；ledger/StepFill authority 保持不变 | BUG-795 / issue #2530；Trading Core TWAP/VWAP/AC/POV/SBB 300-share SELL matrix、50-share final residual、V24/V25 non-final/final exact direct test、V25.1 helper direct test，以及 LocalSIM 300-share TWAP direct test | IMPLEMENTED_VERIFIED | 更新本地 gate 与 PR/CI；合入并由用户重启后在正常交易日只读核验，不执行 DDL/DML/config/broker call |
| `SIM-P-051` | `F-030,031,013,014,015,021,022,024` | BUG-796 将 LocalSIM economic commit 与 mark/NAV projection 解耦：明确 transient mark gap 先提交 order/fill/event/cash/state/position hashes，run 保持 `INTRADAY_RUNNING + INTRADAY_VALUATION_PENDING`；next cadence 在推进新 minute event 前必须重新证明 exact economic receipt/state/outbox 与 Paper economic facts，闭合后才恢复同 outbox/generation，mark 可用后生成 exact completion/account/performance receipt；economic/projection/readback 失败均保留显式 retry/terminal evidence且不重复经济事实 | BUG-796 / issue #2531；healthy fill + missing passive holding、restart pending/recovery、same generation/outbox、pending economic readback fault injection、account drift conflict、transient-only classifier、projection connection retry、projected readback recovery、active/stale diagnostics direct tests | IMPLEMENTED_VERIFIED | 更新修复 PR 并检查 CI；合入后由用户重启并补正常交易日只读 evidence；DDL/DML/config/broker 均为 noop；BUG-795 child sizing 已合入，BUG-797 frozen policy 仍独立追踪 |
| `SIM-P-052` | `F-004,005,024` | BUG-797 删除 scheduler release→portfolio 与 broker manifest/flat-policy fallback；LocalSIM run context 验证 persisted release snapshot exact schema/normalized hash/ID-SHA，binding admission 仍只检查 package lifecycle；Paper daemon 显式 wiring；历史 incomplete release typed fail/retire | BUG-797 / issue #2532；single/multi/model-code required/optional binding、scheduler release-over-portfolio、incomplete release no-fallback、release 顶层 ID/SHA 与 snapshot 冲突、broker missing/empty/hash/alias、daemon E2E direct tests | IMPLEMENTED_VERIFIED | 更新修复 PR 并检查 CI；合入后由用户重启并只读核对实际 release policy reason；DDL/DML/config/broker 均为 noop；BUG-795/796 已合入 |
| `SIM-P-053` | `F-016,017,020,021,022,024,040` | BUG-806 将 Phase 1 physical generation authority 从可删除 active feed 提升为 subscriber-owned per-session high-watermark；candidate 建立前预留 generation，failed prepare/last release/rebuild/shutdown 均保留 fenced history；callback immutable capture 与 generation fence 在 subscriber 内线性化；successor generation 严格递增，旧 callback 继续 fail loud，多个 logical consumers 与 single writer 语义不变 | BUG-806 / issue #2566；subscriber last-release/reacquire、failed candidate non-reuse、late callback race、multi-session isolation；failed-candidate in-flight callback/fence serialization；same-supervisor raw/normalized capture；factory close/later runtime；worker active/fenced/current diagnostics 与 recovery auto-clear direct tests | IMPLEMENTED_VERIFIED | `source_merge=pending_pr`；`production_ddl=noop`；`production_config=noop`；`restart=pending_user_after_merge`；`binding_migration=noop`；`runtime_observation=pending_normal_trading_day`。合入并由用户重启后，按只读 diagnostics 核对实际生命周期触发的 last-release、后续 generation successor/bootstrap/callback；本 BUG 不调用 broker |
| `SIM-P-054` | `F-020,021,024,038,041` | BUG-820 将 durable result 从 positional identity 改为唯一 parent identity association：set-equal permutation 按 request canonical order 恢复/序列化；submit、replay、child sync、restart 与 multi-slice aggregate 共用完整性检查，后续 rejected slice 保留既有 accepted broker fact。独立审核补修使任意 JSON malformed identity 的 error context 可序列化且不会二次 TypeError，并在 upsert 前拒绝非字符串 result identity、conflicting request aliases 和空白 success order ID，使 write/readback schema 完全一致。生产只读 row 为 35/35、无空值/duplicate/missing/extra、集合相同但顺序不同；repository 整体 upsert 未自行排序，submit producer 按原 parent order 覆盖了 result order。scheduler core 已证明 per-binding isolation，LocalSIM 在故障后继续 durable 推进；当前 background TypeError、reconciliation/OMS warnings 作为独立观察，不并入本修复 | BUG-820 / issue #2591；原 RED fifth-item permutation + cross-tick accepted→rejected 2 failed；审核补修 RED 5 failed/2 passed；GREEN arbitrary-JSON diagnostics、strict persistence、合法 carrier、核心 replay/scheduler nodeids 与 L2/CI/F2 evidence 在 PR 更新后刷新 | IMPLEMENTED_VERIFIED | `source_merge=pending_user_authorization`；`production_ddl=noop`；`production_config=noop`；`restart=pending_user_after_merge`；`binding_migration=noop`；`runtime_observation=pending_deploy_readback`。本 PR 不修改/重放生产 batch，不调用 broker |
| `SIM-P-055` | `F-013,014,015,024,034,042` | BUG-824 将 LocalSIM immutable durable fact 的最终 JSON schema enforcement 落到真实 Paper PostgreSQL writer：order/fill/fill-market-context/order-event nested fill/run-event 全部在 execute 前 canonicalize；unknown type、non-string key 和 non-finite number 带完整 fact identity/path 拒绝并触发 economic transaction rollback，不再依赖 InMemory serializer 假设或泄漏 mappingproxy TypeError | BUG-824 / issue #2607；真实 psycopg2 `Json.getquoted()` RED 2 failed，修复后 direct contract matrix 8 passed，含合法 fact 已执行后后续 event schema failure 的整事务 rollback；scheduler immutable payload/pre-commit truth direct nodes 与 changed-file catalog gate 在 PR 前复核 | IMPLEMENTED_VERIFIED | `source_merge=pending_pr`；`close_sync=not_started`；`production_ddl=noop`；`production_dml=noop`；backend/frontend dependency gate `noop`；`restart=not_authorized_not_run`；`runtime_observation=pending_merge_and_user_restart`。本 BUG 不调用 broker |
| `SIM-P-056` | `F-043..070` | MiniQMT统一kernel/plugin F2蓝图保持唯一权威；K2-A strict durable contracts、K2-A-M1 repository职责拆分、K2-B shadow ingress/delivery、K2-C shadow clock/timer和K2-D shadow outbox/reconcile/observability均已合入；未启动任何worker或产品route | K2 design PR #2714 / merge `50dbcbf6...`；K2-A PR #2729 / merge `0b46f781...`；M1由`SIM-P-062`记录；K2-B由`SIM-P-063`记录；K2-C PR #2794 / merge `801dc3c9...`；K2-D PR #2804 / merge `fc4170fa...`、required CI run `30269640126` | IMPLEMENTED_VERIFIED_MERGED | K2-A、K2-A-M1、K2-B、K2-C、K2-D均=`implemented_verified + merged`；K2 overall=`implemented_verified + merged`；K3/K4=`not_started`；production DDL/DML/dependency/config/broker/restart/runtime均`noop` |
| `SIM-P-057` | `F-053..060` | 完成 K1 实施级详细设计并修复正式审核问题：父子 exact manifest/requirement schema 同步；recursive FrozenJson 深度不可变；serializable descriptor 与 process callable 分层；route-independent plugin catalog、hash-covered creation binding、aggregate build failure receipt、per-plugin/per-route compatibility isolation；raw digest/hex 与 keyed identity/effect/u53 公式；current-three exact active-order/lineage/state schema；TWAP exchange-active seconds、午休不累计、EOD/restart 与 legacy alias drift receipt；K1 保持 shadow-only，不切换现有 runtime | `miniqmt_execution_kernel_k1_contracts_registry_f2_detailed_design_20260722.md`；parent/umbrella F2 validator；DESIGN-COMPLIANCE-001；PR #2628 / merge `5ddfb100bddfbc53c6dddb0e2b2ef7153c201ecd` | DESIGN_READY | `source_merge=merged_pr_2628`；`close_sync=not_applicable_feature_design`；production DDL/DML/dependency/config/binding/broker/restart/runtime 均 `noop`。详细设计已合入，不能写成 implementation completed |
| `SIM-P-058` | `F-054,F-056` | 实现 K1-A strict/frozen contracts、recursive FrozenJson、canonical JSON/raw digest/hex、decimal/time/error evidence、完整 runtime event composite DTO、state/start/initialization/effect closure，以及 logical-time keyed deterministic ID/raw-digest u53/ordinal helpers。正式审核补修公开 FrozenJson marker 深冻结、JSON Schema definition/config/state instance authority、exact source key set 与 ALGO_START tuple、delivery/local-order/command/timer/diagnostic readback identity、state/diagnostic logical-time authority及 error-renderer 二次异常；same command ID/different quantity 不再共享 effect hash；schema violation evidence 有界且显式标记截断；generic helper 收窄为内部 ACTION，消除第二 identity authority。保持 K1-A shadow-only 和产品 runtime 零接线 | `plugin_canonical.py`、`plugin_contracts.py`、`deterministic_context.py`；RED audit matrix 5 failed；GREEN direct 67 passed；coverage 94/85/97%；PR #2637 / merge `e69e72dbdd6e5fd8d414721adaa86ccec7fafc2f` | K1_A_IMPLEMENTED_VERIFIED | `source_merge=merged_pr_2637`；K1-B/K1-C 均 `implemented_verified + merged`；K1-C PR #2685 / merge `e4faeb53663cb4d19eb4e07d833953725a40fdc1`；production DDL/DML/dependency/config/binding/broker/restart/runtime 全部 `noop` |
| `SIM-P-059` | `F-055,F-057,F-059,F-060` | K1-B 已实现并经多轮独立审核补修 route-independent immutable catalog、durable descriptor/process binding、canonical persisted snapshot、exact creation/restore PluginKey、non-empty deterministic bounded aggregate failure、gateway strict readback/B0/exact-cancel per-route evidence、三阶段 authority-aware route receipt validation、current-three exact manifests/strict state/source/behavior、recursive immutable code-owned durable authority、bounded schema evidence、active-order/TWAP/legacy projection；保持 shadow-only，未修改产品 runtime wiring | `plugin_registry.py`、`plugin_manifests.py`、shared `plugin_contracts.py`；registry/current-three/shared=55/39/60 passed；current registry coverage 88.06% line/73.45% branch；PR #2655 / merge `ae1035a1ab4916427f7b72443c6235d544eb4c8e` | K1_B_IMPLEMENTED_VERIFIED_MERGED | `source_merge=merged_pr_2655`；K1-C `implemented_verified + merged`，PR #2685 / merge `e4faeb53663cb4d19eb4e07d833953725a40fdc1`；K1 overall `implemented_verified + merged`；K2/K3/K4 `not_started`；production DDL/DML/dependency/config/binding/broker/restart/runtime activation 全部 `noop`，产品 runtime 未切换 |

每次更新本表必须使用当时最新 `origin/main` 和可重复证据；不得把旧运行快照写成当前事实。若只完成代码而没有生产授权，状态说明必须明确 `source merged`，不能写成 runtime activated。

| `SIM-P-060` | `F-053,F-058,F-059,F-060` | K1-C C1/C2 dual-upstream V2 authority 保持闭合；final-review follow-up implementation `52e1c5a2` 修复 target-only AST 与 worker 第二份手写 denylist 导致的 transitive helper SQLite、wall-clock/global-random、dynamic module 与 forbidden owner 假 PASSED。public validator 现于 root execution 前递归验证同 package repo-owned Python helper closure，`checked_modules` 保存 canonical closure，cycle 有界，dependency failure 阻止执行；parent/worker 共用同一 code-owned forbidden/nondeterminism authority。该能力仅保障模拟盘 deterministic/plugin ownership，不扩展为网络安全沙箱、运行门禁或审批；current-three generator、K1-B aggregate zero-publication 与产品 runtime route 均不变 | `backend/execution_algos/vnpy_compat/import_boundary.py`、`backend/tests/miniqmt_execution_runtime/test_plugin_import_boundaries.py`；新增 6 个初始 RED，最终 import direct `65 passed`、完整 direct `49/65/60/55/39=268 passed`；import coverage line/branch=`88.27/77.88`；full classifier `29 files`、`unmapped_code_files=[]`；final CI run `30119969033` MiniQMT=`682/1`、Paper=`1042/2/1 deselected`、static/verdict green；L0/registry PASS；PR #2685 / merge `e4faeb53663cb4d19eb4e07d833953725a40fdc1` | K1_C_IMPLEMENTED_VERIFIED_MERGED | `source_merge=merged_pr_2685`；K1 overall `implemented_verified + merged`；K2/K3/K4 `not_started`；产品 runtime 未切换；`close_sync=not_applicable_feature`；production DDL/DML/dependency/config/binding/broker/restart/runtime activation gates `noop` |
| `SIM-P-061` | `F-061..070` | K2-A第二轮审核补修已闭合：strict initial carriers + repository first-write/progressed-view retry、CANCEL ACK后later callback mapping/algo atomic closure、complete pure scalar projection/exact recovery readback、independent `pg_catalog`与post-COMMIT function proof；保持K1唯一DTO authority、shadow-only且K3前不切换产品route | `plugin_contracts.py`、`kernel_repository.py`、K2 migration triplet、四份MiniQMT tests（含import-boundary）；contracts=`33 passed`、DEV repository=`12 passed`、DEV migration=`11 passed`、import=`66 passed`、coverage=`87.21/72.04`、MiniQMT/Paper/L0/registry/F2/final CI run `30172230466`全绿；final source HEAD `fc261aaf47a6fade01b1037efd5c8cb8ccda5235`、PR #2729 / merge `0b46f7819f4147c97a36908e25ca948ce5450661` | IMPLEMENTED_VERIFIED_MERGED | `source_merge=merged_pr_2729`；K2-A=`implemented_verified + merged`；K2-B也已由`SIM-P-063`闭合，K2-C/D、K3/K4=`not_started`；`close_sync=not_applicable_feature`；production gates全部`noop` |
| `SIM-P-062` | `F-061..070` | K2-A-M1将repository从3102行单文件拆为38行唯一public façade、shared DB owner、唯一projection/schema authority和event/delivery、transition/outbox/callback、timer/session/recovery私有模块；27个public方法、30个private helper、public signature、identity/hash、SQL/lock/CAS/fencing、transaction/readback、migration与业务语义不变，无第二repository或fallback | `test_kernel_repository_structure.py` RED=`3 passed,1 failed`后GREEN=`4 passed`；contracts/DEV repository/DEV migration/import=`33/12/11/66 passed`、combined=`49 passed`、aggregate line/branch=`87.83%/72.04%`、MiniQMT L2=`723 passed,21 skipped`；public signature SHA-256=`7045f3c2...`，migration canonical-LF triplet=`e2a244d0.../24b4e189.../cb408aaf...`；classifier仅MiniQMT且ownership=`11 mapped,0 unmapped,0 ambiguous`；最终 source HEAD `df10123bf39cd7f03ead2dce62ba6a2fae268e92`及required CI run `30193156930`已通过PR #2753 / merge `024bcf70537c2f1b267417c72f8539937dd21a3f`闭合 | IMPLEMENTED_VERIFIED_MERGED | `source_merge=merged_pr_2753`；K2-A、K2-A-M1、K2-B均=`implemented_verified + merged`；K2 overall=`implementation_in_progress`；K2-C/D、K3/K4=`not_started`；shadow-only且产品runtime未切换；production gates全部`noop` |
| `SIM-P-063` | `F-063..065,F-070` | K2-B实现shadow-only ALGO_START、code-owned exact event routing、callback mapping single transaction、pure delivery/materialization、failure/skip、provider-only bounded retry与same-attempt stale reclaim；最终审核闭合direct-writer bypass、sequence drift、failure mapping/outbox、projection lineage、malformed exception evidence与ALGO_START plan/release/policy authority；保持K4 façade/K2-C clock/K2-D dispatcher及产品runtime边界 | final source `84ce557ccb533452b8dcb08e0747398b94cd88c6`；focused/DEV/single-process/L2=`50/14/794/772 passed`；changed files=`20`、classifier仅MiniQMT、`unmapped_code_files=[]`；六个核心模块line/branch=`85.99%/70.38%`；required CI run `30225853616`全绿 | IMPLEMENTED_VERIFIED_MERGED | PR #2773 / merge `db81b27e84c9c82bed26e8d8e66b44d80b44def4`、`source_merge=merged_pr_2773`；K2-C/D、K3/K4=`not_started`；shadow-only且产品runtime未切换；production DDL/DML/dependency/config/binding/broker/restart/runtime activation全部`noop` |
| `SIM-P-064` | `F-067,F-068,F-070` | K2-C实现shadow-only ExchangeSessionClock：strict durable CalendarSnapshotSet/session authority、确定性SESSION/TIMER/EOD identity、AM/PM exchange-active seconds、午休零TIMER与PM恢复、三种显式catch-up、EOD residual、event-commit后atomic finalize及same-occurrence stale reclaim；K2-C additive migration扩展CLAIMED epoch/version closure且guarded rollback，不改写K2-A migration | final source `c87748cd...`；direct/import/structure=`122 passed`；clock=`16 passed`、line/branch=`90.10%/80.85%`；DEV migration=`13 passed`且真实PostgreSQL atomic claim/reclaim/finalize通过；MiniQMT L2=`789 passed,25 skipped`；classifier仅MiniQMT、`unmapped_code_files=[]`；required CI run `30235878200`全绿；PR #2794 / merge `801dc3c9...` | IMPLEMENTED_VERIFIED_MERGED | `source_merge=merged_pr_2794`；`close_sync=not_applicable_feature`；K2-D、K3/K4=`not_started`；shadow-only且产品runtime未切换；production DDL/DML/dependency/config/binding/broker/restart/runtime activation全部`noop` |
| `SIM-P-065` | `F-066,F-068,F-069,F-070` | K2-D实现shadow-only durable outbox/reconcile/observability：唯一dispatcher/reconciler与gateway adapter执行PENDING/FAILED_RETRYABLE→CLAIMED→DISPATCHING→ACK/reject/unknown；broker boundary前必须验证底层place/cancel method，缺失时以broker_called=false显式pre-call failure进入既有有界重试，optional diagnostic reader失败写入ACK evidence而不改变broker事实；last-release后successor generation对已fenced writer执行两秒有界join且仍拒绝live stale writer，QuoteContractError保持只读业务字段但允许Python traceback lifecycle，避免负载竞态和FrozenInstanceError覆盖primary reason；repository event sequence随outbox DISPATCHING原子持久化且只允许单次赋值，post-call全链不可改写并与unknown/non-acceptance receipt identity闭合；expired CLAIMED显式pre-call retry、expired DISPATCHING显式转OUTCOME_UNKNOWN且不调用broker；safe retry除exact capability与watermark变化外还必须证明区间内零matching callback；pre-call cadence固定1/2/4/8秒，持久化exchange-clock EOD触发fresh final snapshot；reconcile history由(command_id,runtime_id) composite FK与完整scalar/carrier readback闭合；sync ACK不伪造mapping event lineage；只读diagnostics提供稳定keyset cursor、精确lag阈值、expired lease/unknown/readback critical alert及自动清除；additive migration含每列注释、独立catalog readback（包括复合UNIQUE backing index）和有数据rollback保护；无人工ack、审批或额外业务门禁 | direct outbox/diagnostics/ops=`111 passed`，direct+structure=`115 passed`，DEV migration=`15 passed`；MiniQMT/Paper/Simulation=`852 passed,27 skipped` / `1050 passed,2 skipped,2 xfailed` / `438 passed`；F2=`10/10,28/28,70/70`，required CI run `30269640126` 全绿；final source `82c69fbf7e7245e0af76262ddc7b7f59ce7d996b` 已通过 PR #2804 / merge `fc4170faa10847c0b58aa8088b4a8b6d0ca26b29` 合入 | IMPLEMENTED_VERIFIED_MERGED | `source_merge=merged_pr_2804`；`close_sync=not_applicable_feature`；K2 overall=`implemented_verified + merged`；K3/K4=`not_started`；shadow-only且产品runtime未切换；production DDL/DML/dependency/config/binding/broker/restart/runtime activation全部`noop` |
| `SIM-P-066` | `F-071..080` | K3 current-three runtime migration两个shadow-only切片均已实现。K3-A已通过PR #2840合入三个pure `ExecutionAlgoPluginV2`与strict lifecycle；K3-B从单事务committed legacy snapshot构建strict policy/state/dependent-BUY zero-write inventory与ALGO_LOCAL parity，完成visible transport suppression、stable runtime identity及真实K2 creation/ingress/delivery/materializer/mapping/outbox broker-neutral shadow。最终审核补修将in-memory/file mutation、evidence append与JSONL durable append纳入同一snapshot锁；callback owner missing/multiple/cross-parent/cross-slot全部typed fail；legacy/kernel共用QMT status/cumulative/trade alias authority；child-command由committed `CHILD_ORDER_SUBMITTED` sequence闭合same-step/effect。未增加algo分支、第二route、fallback或人工门禁。dependent-BUY durable coordinator和产品route cutover仍由K6拥有 | review-fix direct=`116 passed,2 skipped`、JSON restart/evidence=`11 passed`；DEV disposable PostgreSQL source+durable shadow=`2 passed`且包含committed parity，repeated ingress/restart不重复mapping/outbox且dispatch attempt=0；MiniQMT=`988 passed,29 skipped`；Paper=`1050 passed,2 skipped,2 xfailed`；K3-B变更核心line≥91%/branch≥77%；classifier选择MiniQMT/Paper且`unmapped_code_files=[]`；K3-A PR #2840 / merge `aa155222a1072d6c1110f4cc8a11b4f501d8dd1b`；K3-B PR #2848 / merge `38434e10d530edd883fa75f904de5b025158f918` | K3_IMPLEMENTED_VERIFIED_MERGED | `source_merge=merged_pr_2848`；K3-A/K3-B/K3 overall=`implemented_verified + merged`；K4/K6=`not_started`；产品runtime未切换；production DDL/DML/dependency/config/binding/broker/restart/runtime activation全部`noop` |
| `SIM-P-067` | `F-081..090` | K4-A保持V1 observation-only/zero-positive-publication；K4-B已实现81项V2 full-input/full-actual-trace vector、六项K3 committed parity material、fresh spawned writer/readback、sealed conformance authority、真实CLAIMED pre-call transition identity、existing K2 locked transaction中的ALGO_START/latest prior native B0 TICK read、current-three live K3重放与Iceberg/Stop characterization-only。2026-07-31正式审核补修canonical-LF跨平台authority、primary failure保留、bounded omitted hash/path evidence及characterization active-failure build-cycle；不修改current-three算法语义、不注册Iceberg/Stop、不新增DDL/产品command authority或route | artifact semantic/vector/file=`37dc70e5.../4a3117fa.../ec7bc3c7...`、live K3 binding=`123b3349...`；非DB direct=`202 passed,1 skipped`、DEV=`2 passed`、MiniQMT=`1099 passed,30 skipped`、Paper=`1050 passed,2 skipped,2 xfailed`，八核心coverage line/branch均`>=80/>=70`；final source HEAD `3b740721...`与required CI run `30573914476`已通过PR #2953 / merge `cbb5f128...`闭合 | K4_IMPLEMENTED_VERIFIED_MERGED | `k4a_source_merge=merged_pr_2883`；`k4b_design_source_merge=merged_pr_2914`；`k4b_code_source_merge=merged_pr_2953`；K4 overall=`implemented_verified + merged`；K5 design source已合入、implementation本地验证状态由`SIM-P-068`记录；K6=`not_started`；产品runtime未切换；production gates全部`noop` |
| `SIM-P-068` | `F-091..100` | K5 exact Iceberg/Stop manifest/config/source、K4 provenance到K1 full-five shadow bridge、code-owned/fresh V2 binding、config-only factory、shared conformance evaluator、strict state codec、Iceberg TIMER/cancel/late callback/`>=`、Stop TICK exactly-once/`==`、K2 broker-neutral shadow均已实现并合入。正式代码审核闭合fractional durable carrier、factory real probe、typed error、terminal CLEAN、public binding readback、SPI ownership/import-boundary、DEV并发/ingress、branch coverage及Python 3.12/3.13 AST与checkout-path authority确定性，不增加产品route、algo kernel分支或门禁 | Windows/Python 3.13 K5 direct=`12 passed`、Linux/Python 3.12 exact authority=`3 passed`、lifecycle=`15 passed`、DEV=`1 passed`；MiniQMT=`1129 passed,31 skipped`、Paper=`1050 passed,2 skipped,2 xfailed`；classifier=`targeted_ci_required`、20 files、MiniQMT+Paper、unmapped=[]；final source HEAD=`894c22ff`，required CI run `30640380170`全绿；PR #2978 / merge `4bf54cf2` | K5_IMPLEMENTED_VERIFIED_MERGED | `k5_design_source_merge=merged_pr_2968`；`implementation_source_merge=merged_pr_2978`；K5 overall=`implemented_verified + merged`；K6=`not_started`；product runtime未切换；production gates全部`noop` |
| `SIM-P-069` | `F-101..112` | K6-A/C0/C1/B与production schema保持既有闭合。K6-D同一source slice实现SIM/runtime-binding-account exact closure、code-owned KERNEL_V2 first/retry/successor cutover、V2 creation双lineage、full-five统一ALGO_START和后续V3 event delivery、真实QMT snapshot ORDER/TRADE callback、scheduler-driven SESSION/TIMER/EOD、K2 outbox/reconcile、dependent-BUY、legacy product caller物理删除与five-root fresh-process capability。正式复审补齐完整四段native calendar及auction/continuous精确投影、compiler-owned frozen policy唯一闭包、commit-unknown与non-sequence conflict原typed failure保真、stale-source runtime拒绝、quote runtime exact symbol/object owner及primary+rollback双失败证据；callback/clock failure按binding隔离，terminal late TRADE只扩展lineage不重开状态；跨日final tick后释放旧runtime lease。现有platform endpoint additive投影durable route/live source/current active count，alerts自动clear且无ack/force/replay。无新DDL/DML/config/binding、legacy fallback/parity/bridge或审批门禁 | product integration+clock=`55 passed`；quote activation=`16 passed`；DEV PostgreSQL exact writer/materialization=`25 passed`；MiniQMT/Paper/Simulation=`1346 passed,68 skipped` / `1050 passed,2 skipped,2 xfailed` / `407 passed`；product composition coverage line/branch=`88.17%/78.57%`；设计PR #3129、source PR #3146 / merge `33c09049e82c11cdbae7cd9b596b3666cb481349`已合入；changed-files路由选择MiniQMT/Paper/Simulation，ownership catalog补齐shared adapter test | K6_SOURCE_IMPLEMENTED_VERIFIED_MERGED_RUNTIME_PENDING | `source_merge=merged_pr_3146`；K6 source slices=`5/5`；runtime cutover/用户restart/正常交易日=`not_run`；K6 overall=`implementation_in_progress`；K6-D new DDL/DML/config/binding/dependency=`noop` |
| `SIM-P-070` | `F-021,F-022` | BUG-981 修复 platform diagnostics 与 LocalSIM repository durable carrier 的 schema 漂移：只接受唯一权威的 `state_id -> LocalSimExecutionStateV1` map，复用 repository reader 完成 schema/hash/key identity closure；legacy list、malformed state 与 identity conflict 均 typed fail loud，不做转换或 fallback | `backend/tests/simulation_runtime/test_ops_api.py -k platform_diagnostics`=`30 passed,22 deselected`；新增 authoritative map 正例及 list/schema/identity 负例；public `SimulationRuntimeOpsService.platform_diagnostics` fresh-process path；用户重启后 runtime identity 与 business smoke 已通过 | BUG_981_IMPLEMENTED_VERIFIED_MERGED_RUNTIME_EFFECTIVE | source PR #3151、final close-sync PR #3156 / merge `a6b6d20a...`；`post_restart_effective_gate=passed`；DDL/DML/dependency/config/binding/broker/service-control 全部 `noop` |
| `SIM-P-071` | `F-007,F-009,F-012,F-014` | BUG-984 修复 LocalSIM durable continuation 与 EOD terminal closure：`broker_called` 仅为观察事实，不再拥有 active minute-loop lifecycle；`INTRADAY_RUNNING` continuation 必须从 frozen plan、完整 per-intent states、explicit non-terminal persistence、PROJECTED outbox、economic/projection receipts 与 Paper facts strict readback 获得唯一 authority。post-close summary terminalizer 遇到 `terminal=false` durable generation 必须让既有 binding loop 先按 15:00 后 exchange time 驱动；state/plan、active-state/terminal-flag 或 all-terminal/non-terminal persistence 矛盾 typed fail loud，禁止 terminal run 遗留 ACTIVE state | RED=`2 failed`（`REUSED_EXISTING_PLAN`、ACTIVE state 被提前 terminalize）；GREEN fix-point=`2 passed`，新增 all-terminal/persistence 与非布尔 schema nodeid=`1 passed`；既有 restart/first-causal/partial/post-close=`4 passed`；`simulation_core_l2=411 passed`、L0 blocking=0、registry=8 passed/14-of-14 mapped、classifier仅`simulation_core_l2`且unmapped=[]、F2=`112/112` warnings=0 | BUG_984_SOURCE_VERIFIED_READY_FOR_PR | GitHub Issue #3158；`source_merge=pending_pr`；`post_restart_effective_gate=pending_user_restart`；production DDL/DML/dependency/config/binding/broker/service-control 全部 `noop` |

`SIM-P-068` 的 PR #2978 initial required CI run `30630489853` 在 Linux/Python 3.12 显式拒绝了 Windows/Python 3.13 生成的 K5 binding。根因不是算法或 route，而是 K4 AST hash 依赖 Python minor-version 的 empty-field 默认值，且 source executor signature 绑定绝对 checkout 路径与 `WindowsPath/PosixPath`。复审补修将 AST 固定为 Python 3.12 full-field canonical shape，将 executor signature 固定为 repo-relative structured payload；Windows/Python 3.13 K5 direct=`12 passed`，Linux/Python 3.12 exact authority=`3 passed`且 fresh binding相同，final required CI run `30640380170`全绿。该补修不改变 Iceberg/Stop、signal/target/side/quantity、B0、OMS/Gateway、K2 shadow 或产品 route，也不新增 gate/审批；source merge已通过PR #2978 / merge `4bf54cf2`闭合。

### 15.1 运行阻断修复增量账本

| Progress ID | Acceptance IDs | Current state | Evidence | Status | Remaining runtime state |
| --- | --- | --- | --- | --- | --- |
| `SIM-P-072` | `F-021,F-107,F-109` | BUG-987 修复 KERNEL_V2 pre-run 的父 benchmark schema 权威漂移与无 broker 调用时的 run 状态闭合：父表只读取已由 Phase 0A DDL 声明的 plan/binding/release/package/trade-date 字段，release hash 继续由唯一 `strategy_runtime_release` 与 binding strict readback authority 持有；product-root factory、coordinator 与 worker-incarnation 的 pre-broker failure 均在返回前持久化 `FAILED_RETRYABLE`，重复 scheduler tick 不得把同一 run 留在 `SUBMITTING`。不新增 DDL、route、fallback、审批或人工恢复 | DEV PostgreSQL 无 `execution_parent_benchmark.release_hash` 的真实 disposable schema、七类 frozen-field drift、factory/coordinator/worker pre-broker failure、restart idempotency 与双 binding isolation direct matrix=`6 passed`；GitHub Issue #3169 | BUG_987_SOURCE_VERIFIED_READY_FOR_PR | `source_merge=pending_pr`；`post_restart_effective_gate=pending_user_restart`；production DDL/DML/dependency/config/binding/broker/service-control 全部 `noop` |
| `SIM-P-073` | `F-007,F-009,F-021` | BUG-988 修复大规模 LocalSIM cadence 的串行实时分钟取证和 watchdog owner 丢失：同一 cadence 对 active-order/passive-position symbol union 使用 code-owned 16-worker 上限并发读取，结果仍按 canonical symbol order 聚合且 per-symbol typed failure 隔离；binding watchdog 以 `(binding_id,trade_date)` single-flight owner 保留原 worker/result/exception，后续 tick 不启动第二 writer，返回显式非失败 `IN_PROGRESS` 并继续其他 binding，owner 完成后只消费一次原 result/exception。未提高 timeout、未删除 watchdog、未写假成功或增加人工 gate | RED：并发峰值=`1`、超时 tick=`FAILED_RETRYABLE`且 second writer 可启动；GREEN：20-symbol bounded concurrency、same-binding single writer、peer binding isolation、late failure reason preservation；LocalSIM direct=`52 passed`、scheduler direct=`3 passed`；`paper_v2_backend=1051 passed,2 skipped,2 xfailed`；`simulation_core_l2=413 passed`；GitHub Issue #3170 | BUG_988_SOURCE_VERIFIED_READY_FOR_PR | `source_merge=pending_pr`；`post_restart_effective_gate=pending_user_restart`；production DDL/DML/dependency/config/binding/broker/service-control 全部 `noop` |
| `SIM-P-074` | `F-007,F-009,F-012,F-014,F-021` | BUG-992 closes LocalSIM lifecycle authority and economic readback with one pure contract over current run/binding/trade-date/plan identity, strict continuous receipt generations and high-watermark, exact receipt/outbox identity, and the latest committed `state_id -> state_hash` set. Empty authority is legal only for exact initial facts (`generation` absent/exact integer zero, `broker_called=false`, `submitted_intents=0`) when every economic/projection carrier and generation identity is absent. Orphan pending/PROJECTED outbox, projection receipt/generation identity, persistence/valuation/performance carrier, broker-side-effect carrier, missing/forged receipt authority, generation corruption, missing/hash-drift states, identity drift and extra non-terminal states fail loud. The earlier PostgreSQL-labelled branch backed by an InMemory getter is withdrawn: PostgreSQL completion evidence now comes only from a committed DEV row read through real `SimulationRuntimeRepository` connections and independent post-commit readback. | Orphan-carrier RED=`5 passed,6 failed`, including scheduler false terminalization; final authority/readback matrix=`50 passed`, including committed independent DEV PostgreSQL legal-history/repeat-readback and extra-active/orphan/forged negative cases; lifecycle branch-coverage process=`239 passed`; `simulation_core_l2=466 passed,1 explicitly gated DEV skip`; registry=`8 passed,14/14 mapped`; L0 blocking=`0`; classifier `unmapped_code_files=[]`; changed-code coverage against `origin/main`=`90.71% line (166/183),87.23% branch (82/94)`; real DEV target=`aistock_dev`, writer/readback backend PIDs disjoint, exact cleanup readback=`0` disposable packages; F2=`112/112,warnings=0`; GitHub Issue #3184 / PR #3186. | BUG_992_FINAL_REVIEW_REPAIR_VERIFIED_PR_OPEN | `source_merge=pending_pr_3186`; `post_restart_effective_gate=pending_user_restart`; current production run cannot recover from source changes before user-authorized merge/restart/readback; production DDL/DML/dependency/config/binding/broker/service-control all `noop`; DEV-only disposable row DML was committed, independently read back, and exactly cleaned |

BUG-987 source verification closure：`miniqmt_execution_runtime_l2=1346 passed,69 skipped`、`simulation_core_l2=414 passed`、L0 blocking=`0`、ownership registry=`8 passed/14-of-14 mapped`、F2=`112/112,warnings=0`。2026-08-06 生产只读证据确认当前 MiniQMT 已选择唯一 `KERNEL_V2` route，但旧进程仍包含本 BUG；source merge、用户 restart、runtime identity match 与修复后正常交易日业务闭环继续分开记录。

## 16. DESIGN-COMPLIANCE-001 设计复核

| Control | Review result | Design evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | LocalSIM/K2 durable contracts保持完整；K2-A-M1从真实public façade进入structure/contract/DEV矩阵并覆盖全部既有repository职责，单一实现路线与完整DEV transaction/schema证据保持闭合 |
| `no_silent_error` | pass | §5.9、§8 和 `F-020..023` 明确 typed failure、readback 和 false-green 反例 |
| `no_business_semantic_drift` | pass | §3、§4、§5 固定 Selection/Execution 隔离、数据源、V25 与 B0 语义；变化必须先更新蓝图 |
| `no_unrequested_gate_or_approval` | pass | §0.3 区分技术条件与审批；禁止 RBAC/ack/confirm-run，自动恢复 |
| design-to-implementation traceability | pass | §13 稳定索引、§14 验收矩阵、§15 当前进度和 same-PR 更新契约 |
| production state separation | pass | §10.5、§11、§12、`F-024`分离merge/DDL/config/restart/binding/runtime evidence；K2-A与K2-A-M1 source merge分别记录为`merged_pr_2729`和`merged_pr_2753`，M1全部production/runtime gates为`noop` |

P0-H MiniQMT execution kernel/plugin F2 设计的逐项复核：

| Control | Review result | Design evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | K1–K5完整authority保持；K6修订闭合strict command_json、V2 ledger/proceeds/coordination、0/1/N MATERIALIZE/REJECT/DEFER、same-command release、single-writer事务、最终cutover、旧调用删除和正常交易日验收；legacy parity/bridge/shadow过渡物不得冒充实现 |
| `no_silent_error` | pass | stale/unknown既有规则保持；K6增加typed bounded evidence、terminal broker_called=false reject、commit-unknown独立readback、K4/K5 V1与K6-A V2 product拒绝、same-authority writer/readback与zero-partial materialization，既有Gateway/OMS primary reason不被catch-all覆盖 |
| `no_business_semantic_drift` | pass | exact owner/TWAP/Iceberg/Stop语义保持；K6不改Selection/Target、策略包、方向数量、A股规则、native B0、OMS/Gateway、五个算法或LocalSIM，只把既有dependent-BUY结果迁入durable owner |
| `no_unrequested_gate_or_approval` | pass | route/session/ledger/capability校验是运行合同，不是人工门禁；alerts自动clear；未新增RBAC、人工acknowledge、审批、confirm-run、manual recovery、全局stop或永久enable flag |
| `production state separation` | pass | K1–K5及K6-A/C0/C1/B/D source merge已闭合；K6-D design PR #3129与source PR #3146 / merge `33c09049e82c11cdbae7cd9b596b3666cb481349`均已合入，`source_merge=merged_pr_3146`。predecessor与K6-B successor production DDL=`applied_and_verified`；K6-D new DDL/DML/config/binding/dependency=`noop`，broker/restart/runtime activation/normal-day observation=`not_run`，产品runtime未切换 |

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
| `no_silent_error` | pass | scheduler boolean/string coercion、LocalSIM authoritative state-id map/schema/hash/key identity、legacy list、MiniQMT runtime identity、batch result cardinality/count/top-level conflict、非 mapping、非法时间、bounded scan 和 metric cardinality 均稳定 typed fail loud；不使用 `_safe_int`、truthy bool、坏行过滤、carrier 自动转换、默认成功或截断假成功 |
| `no_business_semantic_drift` | pass | projection 只读取 scheduler/repository/runtime journal；不启动 feed、不写 run/DB、不重放 parent/child/order、不调用 broker，不改变 Selection、V25、B0 tick authority、方向数量、T+1/lot/limit/suspend、reconcile 或 TCA 语义；单 binding failure 保持隔离 |
| `no_unrequested_gate_or_approval` | pass | metrics/alerts/freshness/lag 均是只读通知，所有层显式 `execution_gate=false`，alerts 显式 `acknowledge_required=false/auto_clears_on_recovery=true`；未新增 RBAC、审批、confirm-run、人工 ack 或业务开关 |
| `production state separation` | pass | 本 slice 只改 source/test/runbook/蓝图/BUG 元数据；未执行 dependency/DDL/DML/config、未调用 broker、未重启服务。platform direct 17 passed、run-detail exact 1 passed、live-admission related 3 passed 和 coverage 不冒充生产已激活；merge/CI 与用户重启后的 LocalSIM/MiniQMT readback 分开记录 |

`BUG-697` source implementation 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | expired source 与 same-day manifest rebase 两条 roll-forward 预处理路径均在创建 release 前 exact 校验；失败持久化为 binding-scoped daily run，并继续真实有效 binding，不是只 catch/log 或测试 mock |
| `no_silent_error` | pass | invalid source 保留 `MINIQMT_B0_QUOTE_V2_BINDING_REQUIRED`、binding/strategy/stage、`legacy_fallback=false`；diagnostics exact 校验 blocking flag、errors、processed、window，flag/evidence 冲突 typed fail loud，不归零、不吞错 |
| `no_business_semantic_drift` | pass | 不合成 B0、不迁移或退休生产 binding、不改变 Selection/plan/order/tick/方向数量；合法 LocalSIM/MiniQMT 继续既有路径，invalid source 只阻断自身且 broker call=0 |
| `no_unrequested_gate_or_approval` | pass | isolation、metric 和 alert 均为技术失败/只读观测；`execution_gate=false`、恢复自动清除，未增加审批、RBAC、人工 acknowledge、confirm-run 或业务开关 |
| `production state separation` | pass | 仅执行生产 GET/只读 binding readback；source 修改不执行 DDL/DML/config、不调用 broker、不重启服务。合入与用户重启后的运行恢复继续单独记录 |

`BUG-698` test-contract/CI implementation 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | 陈旧 nodeid 不只改名或删除：直接断言 `SUBMITTING`、pending count、preflight、active algo、durable batch metadata/result、零 child/零 broker call；B0 later-tick、BEST_LIMIT cancel-requote、TWAP partial-fill、SNIPER reject、operator override 和 normal guard 全部直接覆盖；专属 L2 plan 运行完整 runtime 测试目录，CI classifier 对 service/test 两类变更实际选择该 plan |
| `no_silent_error` | pass | pending 不是假成功：child result `success=false`、batch `success=true` 仅表示 lifecycle 已接受并保持 active，`succeeded=0/failed=0/pending=1`、`broker_called=false` 和 durable facts 全部显式闭合；非 active/非法 no-child 与 rejection 路径仍 typed fail loud，完整目录失败不得通过缩窄计划隐藏 |
| `no_business_semantic_drift` | pass | 产品修改只恢复 vn.py core authority：core 仍 running 时普通 CANCELLED/FILLED child 不提前终止 parent，FAILED rejection、core `finished`、operator/recovery override 保持原语义；Selection、方向数量、preflight、child/order fact、tick authority 和 broker route 均未改变 |
| `no_unrequested_gate_or_approval` | pass | 新增的是无服务、无 DB、无业务写入的 runner-enabled L2 回归计划，不是运行时门禁；未新增审批、RBAC、人工 acknowledge、confirm-run 或 execution gate |
| `production state separation` | pass | 本 slice 只改 test/nox/validation catalog/蓝图/BUG 元数据；未执行 DDL/DML/config、未调用 broker、未重启服务，source/CI/merge 与正常交易日 runtime readback 分开记录 |

`BUG-706/707/709/711/712/714/715/717/718/719` 运行期门禁、可达性与验证闭环批次的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | MiniQMT optional cross-evidence、stable authority generation、directional depth 与 LocalSIM 停牌无 bar 状态机、active+passive cadence snapshot、strict canonical hash、mark reuse lineage、plan-relative cash competition、daemon pending lifecycle、VWAP authoritative profile 和统一板块手数均有产品实现与正反 direct oracle；adapter/engine/algo 使用完整 Paper v2 session，simulation L2 使用当前 authority fixtures；未用删除 intent、默认 quote/cash/profile、mock-only、缩窄 CI、首 bar 全量或一次性成功代替 |
| `no_silent_error` | pass | 已提供但未知 openInt、零价正量、bar identity/payload/duplicate/order conflict、snapshot scope/schema/hash、malformed previous outbox、非 boolean suspension、mark transient reuse、cash residual、pending handle、VWAP profile 和 algorithm quantity mismatch 均有明确 typed/state/event evidence；provider/persistence/runner 异常不被 `pass`、空集合、truthy 转换、旧 mark 原样返回或 `RUN_COMPLETED` 假成功吞掉 |
| `no_business_semantic_drift` | pass | Selection/Target、方向、目标数量、T+1、涨跌停、停牌、B0 tick source 和唯一 broker route 不变；停牌仍不产生 synthetic bar，SELL-first 不变且 BUY 恢复 plan 相对顺序，VWAP 只在真实 profile 存在时执行；删除的是 transient quote 二次 intent gate、symbol 字典序资金偏移、硬编码 100 股和无 profile 全量 fallback |
| `no_unrequested_gate_or_approval` | pass | 未新增 RBAC、审批、人工 acknowledge、confirm-run、业务开关或 execution gate；`WAITING_FOR_MARKET_STATE`、`WAITING_FOR_CAPITAL`、`RUN_PENDING` 和 diagnostics 均由既有 scheduler/runner 自动推进或在下一 cadence 恢复，不要求 operator 解锁 |
| `production state separation` | pass | 本批次只修改 source/test/蓝图/BUG workflow task-card；未执行 DDL/DML/config、未调用生产 broker、未重启服务，PR #2325/CI/merge 与用户重启后的正常交易日 readback 分开记录 |

`BUG-779..784` durable truth、tick isolation、成交恢复与投影一致性批次的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | LocalSIM immutable snapshot/transaction truth、B0 完整 tick capture、逐 symbol wait、三层 SELL odd-lot、BUY/SELL cash-lot replay、parent/child/trade projection 与精确 broker/TCA time 均有产品实现和正反 direct oracle；未用 reduced L1 snapshot、合成 quote/trade ID、mock-only settlement、单层 count 或日志告警替代完整链路 |
| `no_silent_error` | pass | non-string JSON key、key coercion collision、unsupported/non-finite snapshot、unapplied no-id trade、source/hash/depth conflict、partial odd-lot、trade owner/identity/time alias conflict、settlement/core checkpoint failure、broker_called carrier 缺失、projection schema/count/hash conflict和歧义 timestamp 均 typed fail loud；同秒 fill 不按 trade_id 改写顺序，cumulative 派生值不冒充 identity；仅 transient per-symbol quote/depth 形成有 durable reason 的自动 wait，不伪报 broker call 或成功 |
| `no_business_semantic_drift` | pass | Selection/Target、V25、T+1、涨跌停、停牌、frozen B0 tick authority、shared account/position/cash batch preflight 和唯一 gateway route 均保持；健康 symbol continuation 只发生在 quote/depth 运行阶段，未放宽共享资金/仓位一致性；odd-lot 仅允许 exact whole-position liquidation |
| `no_unrequested_gate_or_approval` | pass | 新增 diagnostics/metrics/alerts 均只读且自动解除，明确不 repair、不要求 acknowledge；未新增 RBAC、审批、confirm-run、业务开关、人工恢复或 execution gate |
| `production state separation` | pass | 本批次只修改 source/test/蓝图/BUG 元数据；未执行 DDL/DML/config、未调用生产 broker、未重启服务。PR/CI/merge、用户重启、正常交易日 LocalSIM/MiniQMT、broker replay 与 TCA readback 继续分别记录 |

`BUG-794` source implementation 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass for BUG-794/795/796/797 scope；platform remains open | 首分钟 durable recovery、SELL child sizing、mark/economic 解耦与 frozen policy authority 均有产品实现和正反 direct tests；BUG-795 的 legacy/V24/V25/V25.1 residual、BUG-796 的 pending economic generation/readback/recovery、BUG-797 的 single/multi/model-code variants 与 release ID/SHA conflict 均有直接 oracle；P0-G 三项均完成，但正常交易日 runtime evidence 与后续 Phase 0B/Adaptive 仍独立，不把已修 slice 冒充整个平台完成 |
| `no_silent_error` | pass | cross-plan fact、state/order/intent、receipt/generation/state hash、Paper economic fact、wait projection schema/count/performance、bar time/schema 冲突均 typed fail loud；recovery readback 失败写显式 `local_sim_failed_run_recovery_failure_v1`，不误归为 pre-run、不返回成功 |
| `no_business_semantic_drift` | pass | Selection/Target、frozen plan intent、方向数量、执行算法、T+1、涨跌停、停牌、数据源和 broker route 未改变；无 mark 时不合成 NAV/performance，恢复不重提 parent |
| `no_unrequested_gate_or_approval` | pass | waiting/projection/recovery 均由既有 scheduler cadence 自动推进；diagnostics metric/alert 只读且 `execution_gate=false`；未新增 RBAC、审批、人工 acknowledge、confirm-run、业务开关或重启门禁 |
| `production state separation` | pass | 本 slice 只改 source/test/蓝图/BUG 元数据；未执行 DDL/DML/config、未调用生产 broker、未重启服务；PR/CI/merge 与用户重启后的正常交易日 runtime evidence 继续分开记录 |

`BUG-796` economic-first valuation-pending implementation 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass for BUG-796 scope；platform remains open | 不是 catch-and-ignore 或只保留内存：orders/fills/order-events/cash/states/position hashes、economic receipt 与 outbox 在 single-writer transaction 先提交；pending、same-generation completion、restart、pending economic readback reproof、projection retry、projected readback recovery、account drift conflict 和 diagnostics 均有产品实现与正反 direct tests；BUG-795/796/797 保持独立验收映射 |
| `no_silent_error` | pass | 仅 `LOCALSIM_MARK_PRICE_MISSING`/`LOCALSIM_REALTIME_MARKET_DATA_UNAVAILABLE` 可进入 `INTRADAY_VALUATION_PENDING`；恢复前必须重新读回 exact economic generation，失败不能被 mark pending 清除；schema/hash/identity/source/account conflict 继续 typed fail loud；pending 明确 `nav=null`、missing symbols/reason/outbox/generation，投影或 readback 失败保留 durable retry/terminal evidence，不伪报成功 |
| `no_business_semantic_drift` | pass | Selection/Target、frozen plan、方向数量、算法、T+1、涨跌停、停牌、数据源和 broker route 不变；不使用旧 mark、计划价、0 价、current-price map 或 manifest fallback；pending cadence 先完成已提交 generation，禁止先推进新经济事实 |
| `no_unrequested_gate_or_approval` | pass | valuation pending、mark retry、projection/readback recovery 由既有 scheduler cadence 自动推进；diagnostics 仅增加只读 `IN_PROGRESS/DEGRADED` 事实与自动解除 alert，`execution_gate=false`；未新增 RBAC、审批、人工 acknowledge、confirm-run 或业务开关 |
| `production state separation` | pass | 本 slice 只改 source/test/唯一蓝图/BUG 元数据；未执行 DDL/DML/config、未调用生产 broker、未启停或重启服务；source PR/CI/merge 与用户重启后的正常交易日 readback 分开记录 |

`BUG-797` LocalSIM frozen policy authority implementation 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass for BUG-797 scope；platform remains open | 不只在 scheduler 删除一行 fallback：release frozen identity、production context、broker constructor、roll-forward、Paper daemon wiring 与直接/恢复路径统一消费 exact snapshot；binding admission 保持 lifecycle-only；missing/empty/hash/alias/release-ID-SHA conflict 和 single/multi/model-code variants 均有正反 direct tests |
| `no_silent_error` | pass | snapshot 缺失、字段不精确、多 ID alias、空 policy JSON、normalized hash drift、release 顶层 ID/SHA 与 nested snapshot drift 均以 `LOCALSIM_EXECUTION_POLICY_*` / `SIMULATION_RELEASE_EXECUTION_POLICY_HASH_CONFLICT` typed fail loud；不返回 portfolio/manifest/flat policy，不把缺字段解释为“未冲突” |
| `no_business_semantic_drift` | pass | Selection/Target、策略信号、方向数量、算法行为、数据源、T+1、涨跌停、停牌和 broker route 不变；new binding 只核对已冻结 runtime policy component，不复核 package alpha/model/assets；single/multi/model-code-required/optional 共享同一路径 |
| `no_unrequested_gate_or_approval` | pass | 未新增 RBAC、审批、人工 acknowledge、confirm-run、execution gate 或 package 二次完整性检查；历史 incomplete release 的 typed refusal 是既有 immutable release contract，不要求补数据或人工解锁 |
| `production state separation` | pass | 本 slice 只修改 source/test/唯一蓝图/BUG/GitHub scope；未执行 DDL/DML/config、未调用生产 broker、未启停或重启服务；source PR/CI/merge 与用户重启后的 runtime readback 分开记录 |

`BUG-984` LocalSIM durable continuation 与 EOD state-first closure 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass for BUG-984 source scope；runtime activation pending | 不是删除一个条件后直接推进：每次 active continuation 都从 frozen plan、完整 per-intent state set、explicit non-terminal persistence、PROJECTED outbox、economic/projection receipts 与 Paper economic/projection facts重新读回同一 generation；fresh scheduler instance、same-bar dedupe、generation advance、stable order identity、EOD terminal/residual均走真实 `run_once` 与 LocalSIM broker path |
| `no_silent_error` | pass | missing/malformed outbox、outbox run/plan/status drift、state cardinality/identity/hash、receipt/Paper readback、active-state/terminal flag 与 all-terminal/non-terminal persistence conflict 均以稳定 reason code 和 run/binding/plan/state context typed fail loud；post-close 不把 active durable generation 静默解释为 summary failure或成功 |
| `no_business_semantic_drift` | pass | `broker_called` 仍保留为实际 broker-call observation，但不再拥有 durable lifecycle；Selection、Target、frozen intents、方向数量、执行算法、TDX causal minute authority、T+1/涨跌停/停牌、Paper repository transaction 与 MiniQMT route 均未改变；continuation不重提 parent、不重建 plan、不复制 order |
| `no_unrequested_gate_or_approval` | pass | exact durable readback是既有 restart/CAS contract的自动执行，不是新业务 admission gate；合法 run 由既有 scheduler cadence 自动续跑和收盘闭合，未新增 RBAC、审批、人工 acknowledge、人工恢复、confirm-run、业务开关或额外重启门禁 |
| `production state separation` | pass | 本 slice 只修改 source/test/唯一蓝图/BUG 元数据；未执行 DDL/DML/dependency/config、未调用生产 broker、未启停或重启服务。source PR/CI/merge、用户执行 backend restart、post-restart identity/business smoke与下一正常交易日 runtime evidence分别记录 |

`BUG-806` MiniQMT quote generation lifecycle 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass | generation high-watermark 由 subscriber 按 session/process lifecycle 持有，并覆盖 successful/failed prepare、last release、partial release、rebuild、shutdown、same-supervisor reacquire、factory recreate、failed-candidate callback/fence 线性化和迟到 callback；不是清空 worker fence、永久订阅或 mock-only 绕过，也不虚构 scheduler 午休自动释放语义 |
| `no_silent_error` | pass | 旧 generation 仍以 `ADAPTIVE_IS_QUOTE_ORDERING_REJECTED` fail loud；capture/prepare/publish 显式 ack 规则不变；health 同时呈现 current/active/preparing/fenced/high-watermark generation，worker 保留 last failure 并仅在成功 successor publication 后清除 active failure |
| `no_business_semantic_drift` | pass | 多 logical consumer 共用一个 physical feed/single writer、symbol union、bootstrap all-or-nothing、watchdog bounded rebuild、B0 controller registry、Selection/策略信号/方向数量/执行算法/LocalSIM 和唯一 event-loop broker route均未改变；未引入 LEGACY_B0、普通 quote、minute/compiler fallback |
| `no_unrequested_gate_or_approval` | pass | 修复仅恢复自动 lifecycle successor；未新增 RBAC、审批、人工 acknowledge、人工恢复、业务开关或 runtime gate，合法下午/失败重试/次日 runtime 均沿既有 scheduler tick 自动继续 |
| `production state separation` | pass | 本 slice 只修改 source/test/唯一蓝图/BUG 元数据；`production_ddl_gate=noop`、前后端 dependency gate 均为 `noop`；未执行 DDL/DML/config、未调用 broker、未启停或重启服务。source PR/CI/merge 与用户重启后的正常交易日只读观察分开记录 |

`BUG-820` durable batch identity replay 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass for BUG-820 scope；platform remains open | 不是只放宽第 5 项检查：submit producer、durable parser、canonical persistence、existing-batch replay、child sync、restart 和 multi-slice aggregate 使用同一 parent identity contract；正路径 permutation 与完整 corruption matrix 均走生产函数，scheduler isolation 走真实 `run_once` seam |
| `no_silent_error` | pass | null/empty/object/list/number/bool、request/result duplicate、missing/extra、cardinality、alias、success/order-id 和 broker-called carrier 冲突保留 batch/stage/conflict field、两侧 identities、duplicates/missing/extra、expected/actual 及可用 runtime/binding 后 typed fail loud；diagnostics 仅对有效字符串计算 duplicate/set，context 可 JSON 序列化且 error builder 不会二次失败；未过滤、强制字符串化、padding、忽略或返回假 ACK |
| `no_business_semantic_drift` | pass | 不改变 Selection、策略信号、选股、资产、方向、数量、执行算法、B0 quote authority、broker route 或 child runtime facts；只把 durable parent projection 与 canonical identity 对齐，accepted side effect 不被后续 slice 覆盖 |
| `no_unrequested_gate_or_approval` | pass | 未新增 RBAC、审批、人工 acknowledge、人工恢复、execution gate、业务开关或 fallback；合法 permutation 和独立 binding 继续沿既有自动 replay/scheduler cadence 推进 |
| `production state separation` | pass | 本 slice 只修改 source/test/唯一蓝图/BUG 元数据；`production_ddl_gate=noop`、前后端 dependency gate 均为 `noop`；未执行 DDL/DML/config、未调用 broker、未启停或重启服务、未修改或人工重放生产 batch。source/CI/merge、部署重启和当前 row runtime recovery 分开记录 |

`BUG-992` LocalSIM post-close state-authority repair 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass after final-review repair; prior PostgreSQL-labelled InMemory claim withdrawn | The shared closure now validates current durable identity, every receipt generation, the high-watermark, exact state authority, empty-authority carrier absence and terminal-history rules across authoritative listing, post-close, restore, continuation and readback. PostgreSQL evidence uses committed DEV rows plus real `SimulationRuntimeRepository` reads and independent connections; no monkeypatched InMemory row is presented as PostgreSQL completion. |
| `no_silent_error` | pass after final-review repair; prior PASS withdrawn | Empty states/receipts no longer bypass orphan pending/PROJECTED outbox, projection receipt/generation identity or other economic/projection carriers. Corruption raises stable typed JSON-safe context with reason, run, carrier, outbox/receipt identity and expected/actual generation; scheduler preserves the damaged active run while independent bindings continue. No deletion, ignore, fallback or empty-success return is used. |
| `no_business_semantic_drift` | pass | Frozen plan intents, Selection/signals/strategy package/assets, side, quantity, algorithm policy, state identity/hash, broker route, CAS/single-writer/transaction/outbox/projection and terminal status semantics are unchanged. Exact initial `broker_called=false` and integer `submitted_intents=0` remain legal; legal terminal history remains auditable. Changed-code line/branch coverage is `90.71%/87.23%`. |
| `no_unrequested_gate_or_approval` | pass | No RBAC, acknowledgement, manual recovery, new runtime gate, global stop, broker call, config/binding change or service control was added. Valid independent bindings continue on the existing scheduler cadence. |
| `production state separation` | pass | Source, tests, BUG metadata and this authoritative blueprint are the only tracked changes. The PostgreSQL test used guarded `aistock_dev` disposable rows, committed for independent readback, then exactly cleaned; production DDL/DML remained `noop`. Merge, user restart, post-restart runtime readback and any current production-run repair remain separate pending states. |

`BUG-824` LocalSIM PostgreSQL durable fact serializer 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass for BUG-824 scope；platform remains open | 修复位于真实 Paper PostgreSQL writer 和 economic transaction seam，覆盖 order、fill、fill market context、order-event metadata/nested fill、run-event context、adapter commit/rollback 与 negative matrix；不是只改 hash helper、InMemory/mock-only 或捕获一个 mappingproxy 特例 |
| `no_silent_error` | pass | unknown type、non-string key、NaN/Infinity/Decimal NaN 均以稳定 `PAPER_V2_DURABLE_FACT_JSON_*` reason、fact type/id、field/path 抛出 `DataUnavailableError` 并 rollback；没有 `except: pass`、字段删除、`default=str`、固定成功或裸 serializer TypeError |
| `no_business_semantic_drift` | pass | canonical value 语义与 scheduler economic hash 一致；未改变 Selection、策略信号、选股、资产、方向、数量、算法、T+1、行情 authority、MiniQMT 或 broker route；Paper upsert、single-writer、receipt/outbox/readback identity 不变 |
| `no_unrequested_gate_or_approval` | pass | 未增加 RBAC、审批、人工 acknowledge、人工恢复、业务开关或 execution gate；合法 immutable payload 沿既有自动 scheduler transaction 提交，非法数据只是既有 durability contract 的 typed failure |
| `production state separation` | pass | 本 slice 只修改 source/test/唯一蓝图/BUG 元数据；未执行 DDL/DML/config、未调用 broker、未启停或重启服务；source PR/CI/merge、用户重启和正常交易日 runtime readback 分开记录 |

`BUG-988` LocalSIM cadence/single-writer closure 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass for BUG-988 source scope；CI/runtime verification pending | 同时修复生产 `LocalSimBackend` 的 symbol-union取证和真实 scheduler `run_once` owner lifecycle；不是单纯提高 600 秒、删除 watchdog、mock-only 并发或忽略 lag；Paper/Simulation 两个 ownership 模块计划均已通过 |
| `no_silent_error` | pass | per-symbol connectivity failure仍进入 snapshot errors；unexpected worker/result carrier、owner drift、result missing、late exception均 typed fail loud；完成/异常 outcome只消费一次，不返回固定成功或清除原 failure identity |
| `no_business_semantic_drift` | pass | selection、target、side、quantity、算法、causal minute cursor/bar identity、TDX authority、T+1、涨跌停/停牌、Paper transaction与MiniQMT route均未改变；只并发读取独立 symbol输入并串行应用既有 canonical结果 |
| `no_unrequested_gate_or_approval` | pass | 未增加 RBAC、审批、人工 acknowledge、人工恢复、confirm-run或新业务开关；single-flight是既有 durable writer ownership的自动调度实现，合法 binding沿scheduler cadence自动继续 |
| `production state separation` | pass | 当前只修改source/test/唯一蓝图/BUG元数据；未执行DDL/DML/dependency/config/binding、broker调用或服务控制；source merge、用户restart、runtime identity和正常交易日readback分开记录 |
`BUG-987` KERNEL_V2 pre-run schema 与 run-state closure 的逐项复核：

| Control | Review result | Implementation evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass for BUG-987 source scope；runtime activation pending | 修复同时闭合真实 DEV PostgreSQL parent/binding/release/plan readback 与 scheduler 重试状态，不以删除单列、mock repository 或仅捕获异常代替生产路径；七个 declared parent frozen fields 均有 conflict 反例 |
| `no_silent_error` | pass | 未声明列不再被查询；factory、缺失 coordinator、缺失 worker incarnation 分别保留精确 stage/type/message/context，先写 `FAILED_RETRYABLE` 和 `broker_called=false` 后向 scheduler 抛出原异常；持久化失败不会返回成功 |
| `no_business_semantic_drift` | pass | 未改变 selection、target、side、quantity、执行算法、B0 quote、OMS/Gateway、broker route 或 KERNEL_V2 ownership；release hash 仍由既有 release/binding authority 唯一持有，未复制到 parent 表 |
| `no_unrequested_gate_or_approval` | pass | 未增加 RBAC、审批、人工 acknowledge、人工恢复、运行开关或额外准入；合法 run 仍由既有 scheduler cadence 自动重试 |
| `production state separation` | pass | 当前只修改 source/test/唯一蓝图/BUG 元数据；DEV 测试只使用 disposable schema；未执行生产 DDL/DML、broker、config/binding 或服务控制；source merge、用户 restart 与正常交易日 runtime readback 分开记录 |

## 17. Definition of Done

本蓝图本身完成的条件是：权威关系、目标架构、契约、迁移、失败模式、测试、发布/回滚、生产边界、稳定索引和当前进度均完整，并通过 F2 validator 与 DESIGN-COMPLIANCE-001。

模拟盘平台整体完成必须是 §15 所有 `REPAIR_REQUIRED` 项均由真实实现和直接证据更新为 `IMPLEMENTED_VERIFIED`，Phase 0B/Adaptive 阶段按各自批准范围完成；在此之前，只能汇报具体 slice 的完成状态，不能宣称整个平台已经完成。

MiniQMT 插件化架构只有在 `F-043..F-112` 全部更新为 `implemented_verified`，现有五算法、dependent-BUY coordinator、generic per-command product authority、restart/outbox exactly-once、唯一KERNEL_V2 cutover、旧 algorithm-specific route 退役及正常交易日 MiniQMT SIM 证据均闭合后，才可宣称实现完成；蓝图或任何K1–K6详细设计合入本身不代表代码或生产 runtime 已具备该能力。
