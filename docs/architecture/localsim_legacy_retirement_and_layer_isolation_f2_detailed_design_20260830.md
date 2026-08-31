# AIstock LocalSIM 旧业务逻辑退役与三层隔离 F2 详细设计

> 文档状态：`sim_lr_c_runtime_profile_contract_merge_ready`
>
> 上位权威：[`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md)
>
> Feature Workflow：`F2 / T3 design-driven`
>
> 本文只冻结后续实施权威，不代表源代码、数据库、运行进程或生产环境已经切换。

## 1. Background / 背景与结论

2026-08-30 对 LocalSIM 生产入口、调用链、前端、MCP、脚本和数据库模型的只读审计确认：统一 `simulation_runtime` 已经成为当前日级生命周期 owner，但旧 Paper v2 仍同时承担创建、session tick、单日执行、历史 replay、auto-run、readiness、dashboard 和部分 broker/data/repository 基础能力。当前状态是“新调度器依赖旧底座并与旧控制面并存”，不满足上位蓝图的唯一 owner、单 writer 和信号/执行隔离要求。

已确认的关键事实：

1. `backend/routers/paper_trading_v2.py` 仍暴露 `run-day`、`replay`、`sessions`、`tick`、auto-run 和 coldstart sentinel 等可写入口；
2. `backend/services/paper_trading_v2/{day_runner,replay,session,live_session,scheduler,auto_run}.py` 仍保存完整替代编排链；
3. `backend/services/simulation_runtime/scheduler.py` 仍直接导入 Paper v2 broker、market-data、model 和 repository；
4. Selection Center 仍反向依赖 `PaperTradingV2PortfolioService`，并可从信号流程直接创建 Paper portfolio；
5. `PaperV2MinuteMarketDataProvider` 同时承载实时 TDX、历史分钟、日级静态事实、旧 previous-close 和 V25 day-feature；
6. 前端 LocalSIM 创建仍执行“portfolio -> session -> tick”多请求链，统一 runtime 没有完整创建、配置、回放和切换 API；
7. `simulation_runtime/scheduler.py` 同时承担 planning、两个 broker、事务、projection、恢复和 diagnostics，已成为多 truth-plane 缺陷的放大器。

因此，目标不是继续给旧路径增加 410、feature flag 或 compatibility adapter，而是先补齐新控制面和三层实现，再原子切流，最后删除旧业务代码和无引用数据库对象。

### 1.1 2026-08-30 实施基线事实

本轮实施规划绑定 `origin/main@93e5f02ad`；上位蓝图与本文初版已由 PR #3983 合入，merge commit=`4ab2f333e1bee63718c466a81baef25e71ac07e5`。只读代码盘点得到：

| 范围 | 当前规模/事实 | 规划影响 |
| --- | --- | --- |
| `backend/services/paper_trading_v2/**` | 43 files / 25,862 lines | 不能按单文件搬运；必须按 owner 迁出有效合同后整体删除产品包 |
| `backend/services/simulation_runtime/**` | 24 files / 45,676 lines；`scheduler.py` 约 1.9 万行 | 不能继续向 scheduler 填入 data/signal/execution；必须先抽出 LocalSIM economic owner |
| Paper/Simulation backend tests | 72 files / 46,651 lines | 有效合同测试迁往新 owner；验证退役行为的测试在最终删除阶段移除 |
| Paper frontend app/API | 24 files / 11,320 lines | 创建、session、tick、auto-run、scheduler 控件必须在同一产品切流阶段迁移 |
| 当前 router | Paper router 仍公开 account/portfolio、run-day、replay、session/tick、auto-run、scheduler mutation；Simulation router 主要是 read API、MiniQMT operator 与公开 scheduler mutation | control-plane 不能只增加新 endpoint；必须以一次 route inventory 原子替换 |
| 跨模块直接依赖 | Simulation Runtime 直接导入 Paper broker/data/model/repository；MiniQMT client、Selection Center、Strategy Package、Trading Core SimGateway 和清理脚本仍导入 Paper contract | contract owner 迁移必须先于 Paper 包删除，并执行 MiniQMT/Selection/Strategy Package no-drift 验证 |
| MCP/backend lifecycle | `paper_v2_monitoring` 仍读取 session/scheduler，`backend/main.py` 仍持有旧 scheduler shutdown hook | 产品切流必须同时修改 router、frontend、MCP、script 和 application lifecycle |

上述规模决定实施采用四个阶段、最多六个源 PR；`LR-0..LR-9` 保留为验收工作包编号，不再等同于十个独立 PR。只有 contract owner、economic writer、产品切流、生产 schema 这四类边界允许拆分，禁止为了“一个文件一个 PR”制造重复 CI、临时 adapter 或长期中间态。

## 2. Scope / 范围

### 2.1 范围内

- LocalSIM 数据层、信号层、执行层的物理模块和依赖方向；
- `simulation_runtime` 作为唯一 lifecycle/control-plane orchestrator；
- StrategyPackage alpha core 与可版本化 runtime config 的边界；
- LocalSIM account、runtime release、binding、历史回放和实时切换 API；
- TDX 当前日 causal minute 与历史 DB minute 的来源隔离；
- TWAP-only frozen execution policy authority；
- LocalSIM order/fill/cash/position/state/outbox 的单 writer；
- router、frontend、MCP、script 和 backend lifecycle 的切流；
- MiniQMT KERNEL_V2 client/quote context 对 Paper v2 broker/data共享DTO的迁移，仅改变contract owner/import，不改变MiniQMT产品route或算法；
- 旧 Paper v2 LocalSIM 代码、测试、init schema 和数据库对象的退役顺序；
- import boundary、route uniqueness、query budget、正常交易日和 replay-to-live 验收。

### 2.2 Non-Goals / 非目标

- 不改变 StrategyPackage 的 alpha、模型权重、因子或 package admission 语义；
- 不恢复 V25 execution algorithm，也不把旧 V25 policy 自动翻译为 TWAP；
- 不改变 Selection 选股结果、target、side、quantity、T+1、板块手数、涨跌停或成交模型；
- 不把 LocalSIM 和 MiniQMT 合并为同一 broker、行情源或经济账本；
- 不修改 MiniQMT KERNEL_V2 的quote、algorithm、parent/child、OMS/Gateway、order/trade或reconcile业务语义；
- 不删除已执行的历史 migration 文件；历史 migration 是审计记录，不是可执行旧业务入口；
- 不在设计 PR 中执行 DDL/DML、清理历史数据、启停或重启 backend/scheduler；
- 不长期保留双写、shadow product route、translator、re-export shim 或 legacy fallback；
- 不允许为了快速删目录而把旧 Paper v2 代码复制到新命名空间。

## 3. Architecture / 目标架构

### 3.1 四个物理边界

```text
backend/services/simulation_data/
  contracts.py
  daily_context.py
  trading_calendar.py
  tdx_causal_minute.py
  historical_minute.py

backend/services/simulation_signal/
  contracts.py
  strategy_package_selection.py
  target_portfolio.py
  rebalance.py

backend/services/simulation_execution/
  contracts.py
  planning.py
  broker.py
  localsim/runtime.py
  localsim/twap.py
  localsim/economic_repository.py
  localsim/projector.py

backend/services/simulation_runtime/
  control_plane.py
  lifecycle.py
  replay.py
  repository.py
  queries.py
  scheduler.py
```

`simulation_runtime` 只编排，不拥有行情解析、alpha inference、撮合算法或第二经济账本。每个下层包必须能以纯 contract/in-memory fixture 独立测试，不得通过 import 副作用启动 scheduler、连接 broker 或访问生产数据库。

### 3.2 允许的依赖方向

```text
simulation_runtime orchestrator
  |-- calls simulation_data ----------> frozen input/context/minute
  |-- calls simulation_signal --------> selection/target/rebalance
  `-- calls simulation_execution -----> plan/runtime/economic facts

simulation_execution
  |-- imports simulation_data.contracts
  `-- imports simulation_signal.contracts

simulation_signal
  `-- reads StrategyPackage admission/assets only
```

允许：

- `simulation_signal` 导入 StrategyPackage 的只读 runtime/assets contract；
- `simulation_execution` 导入 `simulation_data.contracts` 和 `simulation_signal.contracts`；
- `simulation_runtime` 导入三层公开 contract/service；
- MiniQMT KERNEL_V2 client/quote modules可导入迁移后的`simulation_data.contracts`与`simulation_execution.broker`共享contract，但不得反向引入LocalSIM runtime或改变KERNEL_V2 route；
- router/frontend/MCP 只访问 `simulation_runtime` public API/query service。

禁止：

- data 导入 signal、execution、runtime、Paper v2 或 broker；
- signal 导入 execution、runtime、Paper portfolio/session/repository 或 broker；
- execution 调用 signal service 重新选股，或直接查询 market 日级规则表；
- 下层反向导入 runtime scheduler/router；
- runtime、router、UI、MCP 绕过 economic coordinator 直接写订单、成交、现金或持仓；
- 新三层中的任何生产文件导入 `backend.services.paper_trading_v2`。

### 3.3 唯一主链

```text
atomic LocalSIM create
  -> SimulationAccount
  -> StrategyRuntimeRelease
  -> SimulationReleaseBinding
  -> scheduler claims binding/day
  -> data layer freezes SelectionInputSnapshot + DailyTradingContextV2
  -> signal layer produces DailySelectionEvidence + TargetPortfolio
  -> execution layer compiles immutable ExecutionPlan
  -> LocalSimMinuteExecutionRuntime consumes frozen facts + causal minute
  -> LocalSimEconomicCoordinator commits state/order/fill/cash/position/outbox
  -> LocalSimProjectionProjector updates read models
```

router 请求只能创建 command/identity，不能同步执行一个交易日或 tick。后台 scheduler 是实时生命周期唯一 owner；历史 replay 使用独立 job owner，但调用同一个日级 data/signal/execution contract，并写入独立 account/binding 范围，不能成为第二实时 writer。

## 4. Contracts / 契约

### 4.1 StrategyPackage 与 runtime config

StrategyPackage 只冻结不可在模拟盘中修改的 alpha core：

- package/manifest identity；
- 模型代码与模型权重；
- 因子集合和必要 artifact；
- alpha mode 与 admission receipt。

以下配置属于 `StrategyRuntimeRelease`，用户修改时必须创建 immutable successor release，禁止原地改 manifest 或 active release：

- 日频 selection strategy、top-k、行业/板块过滤；
- HMM enablement、snapshot/version/preset；
- risk、fee、tail/unfilled policy；
- approved runtime variant；
- LocalSIM execution policy snapshot。

LocalSIM 当前算法继续唯一为 `localsim_twap_only_v1 / TWAP`。control-plane writer 必须把完整 normalized policy snapshot 冻结进 release；runtime 只验证 ID/schema/hash 并消费，不读取 `manifest.minute_execution_policy`，也不得在 scheduler 中用代码内置 policy 覆盖另一份 release identity。策略包或创建请求携带的历史/V25 policy若需要保留，只能进入`requested_execution_policy_audit` metadata，明确`consulted_for_execution=false`，不得占用release的effective `execution_policy` component、触发LocalSIM运行准入或被自动翻译成TWAP。任何未来 TWAP 参数业务变化必须先修改上位蓝图，不能借本次重构改变。

### 4.1.1 `SimulationAccountV1`

LocalSIM账户不再以`PaperPortfolio`/session作为运行身份。新control plane建立逻辑`SimulationAccountV1`，至少冻结：

- `account_id/account_hash/schema_version`；
- `account_name`、`broker_backend=LOCAL_SIM`；
- `package_id/manifest_sha256/admission_receipt_id`；
- `initial_capital`和fee/risk配置的release引用，不保存可变策略副本；
- `status=ACTIVE|PAUSED|RETIRED`、CAS version、created/updated actor/time；
- 当前effective binding由binding lifecycle查询得到，account行不得保存可漂移的第二份release/policy快照。

account、initial release和binding在同一数据库事务创建并独立readback；任一identity/hash/FK冲突整体rollback。successor release/binding使用append-only identity和effective window，pause/resume/retire通过显式CAS lifecycle transition，不恢复session状态机。

### 4.1.2 `LocalSimRuntimeProfileV1` 配置权威

`SIM-LR-C`不能继续把`paper_v2.runtime_profile/runtime_profile_version`作为新账户配置权威：旧profile外键绑定`paper_v2.portfolio`，复用它会让native `SimulationAccountV1`重新依赖旧账户truth。C bridge migration必须新增中性、package-scoped且不可变版本的：

- `paper_v2.localsim_runtime_profile_v1`：`profile_id`、`package_id/manifest_sha256`、profile name、`ACTIVE|RETIRED` CAS lifecycle、created/updated actor/time；不含account、portfolio、cash、position或active release副本；
- `paper_v2.localsim_runtime_profile_version_v1`：content-addressed `profile_version_id/hash`、profile/package/manifest identity、严格`config_json`、`validation_status=VALIDATED|INVALID|RETIRED`、bounded validation evidence、immutable version number/created metadata；同profile+hash幂等且已被release引用的version不得更新或删除。

`config_json`只允许以下顶层配置，并对嵌套字段执行`extra=forbid`：

- `daily_strategy`：daily strategy id/version、top-k、明确的行业/板块过滤和selection参数；
- `hmm`：`enabled`、明确snapshot/model version、preset和状态映射；关闭时仍冻结`enabled=false`，不能用字段缺失表达；
- `risk_policy`、`fee_policy`；
- 可空`runtime_variant_id/hash`，只能引用同package且`VALIDATION_PASSED`的既有Strategy Package runtime variant；其variant_config只允许`strategy_config/portfolio_policy/notes`，在profile version创建时验证并物化进canonical config；包含`execution_policy/minute_execution_policy/risk_policy`或任何core/HMM字段的variant拒绝，runtime不做第二次动态merge；
- 非执行语义的bounded notes/metadata。

禁止出现或嵌套携带`alpha_components`、alpha weight/combination、factor set、model code/weight/artifact、manifest、package identity覆盖、broker/account/ledger、order/fill/cash/position、execution algo/policy、tail/unfilled handler或行情数据。HMM snapshot/version、runtime variant和所有外部引用必须由server repository解析并绑定hash；缺失/retired/cross-package/hash drift时version保持`INVALID`且不能创建release。

LocalSIM execution/tail继续由同package的`strategy_pkg.validated_execution_policy`权威解析，且effective algo必须是TWAP；`tail_policy_version_id/hash`从该validated policy的unfilled/tail snapshot确定性派生。`daily_strategy_profile_version_id`从validated LocalSim profile version的daily strategy identity派生。product request只提交`runtime_profile_version_id + execution_policy_version_id`，不能提交四个component hash、daily/tail衍生id或effective JSON。

统一API增加：

- `POST /api/v1/simulation-runtime/localsim/runtime-profiles`：创建package-scoped profile；
- `POST /api/v1/simulation-runtime/localsim/runtime-profiles/{profile_id}/versions`：append-only创建并验证version；
- `POST /api/v1/simulation-runtime/localsim/runtime-profiles/{profile_id}/retire`：CAS停止新release引用，不影响已冻结release；
- profile/version stable-cursor read-only GET。

旧Paper profile/activation route在C删除mutation caller并转只读历史；D1删除Python DTO/repository，D2再迁移或退役旧物理表。不得把旧profile复制成native默认配置；retained account继续使用其已冻结release，只有用户创建successor release时才明确选择新的LocalSim profile version。

### 4.2 Data layer contract

数据层只输出以下不可变对象：

- `TradingCalendarSnapshot`：来自全局 Trading Calendar Service；
- `SelectionInputSnapshot`：信号层所需 PIT 数据及其 cutoff/source/hash；
- `DailyTradingContextV2`：broker-specific 日级交易事实；
- `CausalMinuteBatch`：symbol、trade_date、observed_until、ordered bars、source/hash；
- `HistoricalMinuteBatch`：仅允许 `trade_date < current_trading_date` 的已完成历史日。

当前交易日 LocalSIM：

- planning owner 在 09:10 后只对 exact symbol set 执行一次 dataset readiness 和 set-based `stk_limit/stock_st/suspend_d` 读取；
- 允许的缺失条件只对缺失集合执行一次逻辑 TDX reference batch；
- plan freeze 后盘中只读取 TDX causal minute；
- cadence/recovery/reconcile/post-close 不查询 market 日级表或历史分钟表；
- ordinary minute/quote 不写交易数据库。

历史 replay：

- 仅 historical data source 可以读取 `market.kline_minute_raw`；
- 历史 provider 不得向当前日自动 fallback；
- TDX source 不得在不可用时 fallback 到历史分钟；
- source mismatch typed fail loud。

V25 day-feature、`kline_daily_raw` previous-close 和通用 `require_day_features` 开关不得出现在新 data layer。

### 4.3 Signal layer contract

信号层输入为 frozen release identity、StrategyPackage alpha assets、`SelectionInputSnapshot` 和显式 runtime profile；输出为 immutable `DailySelectionEvidence`、`TargetPortfolio` 与 `RebalanceIntent`。

信号层不得：

- 创建或修改 simulation account/portfolio/session/binding；
- 读取 broker account、cash、positions、fills 或 execution state；
- 选择 execution algo、调用 broker 或构造 order；
- 在 execution failure 后重新选股、补位或重分配 target；
- 再次验证 StrategyPackage 完整性。

### 4.4 Execution layer contract

execution planning 只消费 frozen release/binding、DailySelectionEvidence、TargetPortfolio、current account facts 和 DailyTradingContextV2，输出 immutable `ExecutionPlan`。LocalSIM runtime 只消费 plan、frozen daily facts 和 causal minute batches。

执行层不得：

- 调用 Selection service 或改变 alpha evidence；
- 逐 symbol/cadence 查询 `stk_limit/ST/suspend/calendar/audit/daily/V25` 表；
- 从 portfolio、manifest 或 flat JSON 推断 execution policy；
- 使用默认价格、默认资金、旧 mark 或历史 close 掩盖缺失；
- 因单 symbol 不可交易而给其它 symbol 补位或重分配资金。

### 4.5 Economic single-writer contract

`LocalSimEconomicCoordinator` 是以下事实的唯一 writer：

- execution state；
- order/order event；
- fill；
- cash ledger；
- position lot/snapshot；
- market mark；
- economic receipt；
- projection outbox/receipt。

所有写入继续遵守 existing CAS、canonical hash、幂等 key、transaction/readback 契约。projector 只消费 committed outbox，不重新执行 broker 或 signal。router、replay controller、scheduler 和 diagnostics 都不得复制 writer。

#### 4.5.1 Successor ledger-scope bridge

`SIM-LR-C`不能把`SimulationAccountV1.account_id`直接写入仍外键引用`paper_v2.portfolio`的经济表，也不能为新account创建一条影子Paper portfolio。C源PR必须新增DEV-first migration与唯一`SimulationLedgerScopeV1` repository authority：

- 新表物理位于既有`paper_v2` schema但使用中性名称`simulation_ledger_scope_v1`，冻结`ledger_scope_id/hash`、`scope_kind=LEGACY_PORTFOLIO|SUCCESSOR_NATIVE`、唯一source identity、可空且immutable的native `account_id`、created/readback identity；legacy scope的source identity是原portfolio id且`account_id=NULL`，native scope的source/account identity均为`SimulationAccountV1.account_id`；表本身不是account、release、position或portfolio truth；
- migration为每个历史`paper_v2.portfolio_id`确定性回填一个`LEGACY_READ_ONLY` scope，并验证orders/fills/cash/positions/plans/runs/snapshots等全部distinct scope均有覆盖；只把successor runtime仍会写入且当前有旧FK的`paper_v2.run.portfolio_id`与`paper_v2.intraday_snapshots.portfolio_id`两个约束以同一锁事务改指向`simulation_ledger_scope_v1(ledger_scope_id)`。execution-policy activation、runtime-profile/config activation、trade-session/session-day、reset-audit以及MiniQMT broker binding等legacy/product-specific FK在C不扩大变更，继续只读并由D2 inventory退役；已有runtime column可暂时继续名为`portfolio_id`，但successor Python/API一律称`ledger_scope_id`；
- retained account只通过immutable `LegacyLocalSimAccountLineageV1(account_id, ledger_scope_id, economic_facts_sha256)`引用既有`LEGACY_PORTFOLIO` scope，scope row自身逐字段不变；new/replay account bundle在同一transaction插入`SUCCESSOR_NATIVE` scope，且`ledger_scope_id=source_identity=account_id`；不更新任何历史order/fill/cash/position/run/outbox行；
- account create transaction实际为`account + ledger scope + release + binding`，replay create为上述四项再加`replay job`；任何scope/hash/FK/readback冲突整体rollback；
- runtime composition按account/lineage解析唯一ledger scope，再把该scope传给execution/economic repository；不得调用`PaperTradingV2Repository.get_portfolio`、把旧portfolio row当配置/资金truth，或在缺scope时自动创建Paper row；
- migration preflight必须枚举2个exact runtime-active FK、另外6个legacy-only Paper FK和MiniQMT binding FK保持不变、所有distinct legacy scope覆盖率、orphan/duplicate/cross-account冲突和锁预算；post-commit readback证明`run/intraday_snapshots`旧FK为零、新FK为2、未纳入约束逐字不变、历史row count/hash不变、每个native account唯一scope、每个retained lineage唯一legacy scope且scope hash不变。rollback仅在无successor native scope/retained lineage引用且约束/row hash可逆时允许，否则fail closed。

该bridge是删除旧Python业务逻辑前的物理兼容层，不授权保留Paper产品route/model/repository。`SIM-LR-D/PR-D2`在源码退役和运行验收后再迁移剩余physical column/table name；历史migration仍保留。

### 4.6 Control-plane API/UI/MCP contract

新增统一 API 至少包括：

- `POST /api/v1/simulation-runtime/localsim/accounts`：原子创建 account + release + binding；
- `POST /api/v1/simulation-runtime/localsim/accounts/{id}/successor-releases`；
- `POST /api/v1/simulation-runtime/localsim/replays`；
- `POST /api/v1/simulation-runtime/localsim/replays/{id}/cancel`；
- `POST /api/v1/simulation-runtime/localsim/accounts/{id}/pause|resume|retire`；
- `POST /api/v1/simulation-runtime/localsim/accounts/bulk-lifecycle`：保留现有批量pause/resume/retire能力，每项必须携带`account_id + expected_version`；
- account/release/binding/replay/run/ledger 的 read-only GET。

创建请求失败必须整体零写或事务回滚，不得留下只有 portfolio、没有 release/binding 的孤儿记录。UI 不再创建 session 或主动 tick；MCP 只提供统一 read-only monitoring，不调用 readiness POST 或旧 scheduler status。产品router不再公开scheduler `start/stop/tick` mutation；scheduler进程内生命周期只由backend application lifecycle owner管理，保留read-only status/verification/diagnostics。

#### 4.6.1 Product request authority

产品 API 只能接收用户可配置字段，不能让客户端提交或覆盖服务端权威身份。所有 request model 均为 `extra=forbid`，日期使用 `YYYY-MM-DD`，金额进入 service 前按 canonical decimal 规范化；`created_by` 来自当前既有调用身份/固定 application actor，不为本阶段新增 RBAC、审批或人工确认。

`LocalSimAccountCreateRequestV1` 固定包含：

- `account_name`、`package_id`、`initial_capital`；
- `runtime_profile_version_id`、`execution_policy_version_id`；daily strategy与tail component id/hash由服务端从两个validated version确定性派生；
- `effective_from`、可空 `effective_to`、可空 `created_reason`；
- 可空 `requested_execution_policy_audit` 只作为未参与执行的审计输入，service 必须覆盖写入 `consulted_for_execution=false`。

客户端不得提交 `manifest_sha256`、`admission_receipt_id`、任何 component SHA-256、daily/tail衍生id、`release_id/hash`、`binding_id/hash`、`account_id/hash`、effective execution JSON、broker account、ledger scope 或 lifecycle status/version。router 必须通过 Strategy Package admission、LocalSim runtime profile/version、daily/HMM引用、同package validated TWAP policy 和派生tail authority在一个一致快照内解析这些字段；`admission_receipt_id`由当前durable package/manifest/status-event/asset-eligibility facts的canonical payload生成，bounded receipt payload/hash同时冻结到release validation evidence，后续可独立重算，不以瞬时warning/governance展示字段作为identity。缺失、退役、跨 package、hash 漂移或非 TWAP effective policy 均 typed fail loud，禁止信任客户端副本、manifest execution policy、旧 portfolio config 或默认策略。

`LocalSimSuccessorReleaseRequestV1` 固定包含 `base_release_id`、`base_binding_id`、`runtime_profile_version_id`、`execution_policy_version_id`、`effective_from`、可空 reason/audit；服务端读取 account/package/admission/hash/ledger scope并派生daily/tail component，以 source binding hash CAS 原子关闭旧 window 后插入 successor。单项 `pause|resume|retire` 只接收 `expected_version`；bulk lifecycle包含1..200个exact item并在一个transaction按稳定account id顺序锁定，任一CAS/state冲突整体零更新，禁止部分成功列表冒充成功。不新增 complete、delete、run-day、readiness、session、tick、auto-run 或 scheduler mutation。

#### 4.6.2 Historical replay product command

`LocalSimReplayCreateRequestV1` 固定包含 account create 的用户可配置字段、`start_trade_date`、`end_trade_date` 和服务端可解析的 `historical_source_id`；不能接收 source/calendar SHA-256、day-engine id、cursor、status、failure、live release/binding 或 safe-boundary decision。产品创建必须由新增的 repository transaction 一次提交：

```text
SimulationAccountV1
  + SimulationLedgerScopeV1
  + historical StrategyRuntimeRelease
  + closed historical SimulationReleaseBinding
  + LocalSimReplayJobV1
```

任一 insert/readback/identity/FK 冲突整体 rollback，禁止先提交 account bundle 再保存 job。calendar snapshot、historical source hash、统一 day-engine contract 和 exact completed-day range 全由服务端冻结。创建成功后 replay job owner 自动按 bounded batch 推进；产品不公开 `run_next_batch`、manual tick、mark-ready 或 activate-live mutation。`POST .../cancel` 只接收 `expected_version`，caught-up、live successor 与 safe-boundary activation 均由 lifecycle owner 依据 durable facts 自动推进。

#### 4.6.3 Response, query and error envelope

所有成功写响应使用版本化 envelope，禁止返回旧 `portfolio/session` DTO：

```json
{
  "ok": true,
  "schema_version": "localsim_control_response_v1",
  "account": {},
  "ledger_scope": {},
  "release": {},
  "binding": {},
  "replay": null
}
```

不同命令只填充实际产生的 entity；entity 使用 §4.4/§4.5.1/§4.7 frozen model 的 JSON representation，保留 `version` 供下一次 CAS。GET 固定包括：

- `/localsim/accounts` 与 `/localsim/accounts/{account_id}`；
- `/localsim/accounts/{account_id}/releases`、`bindings`、`runs`、`ledger`；
- `/localsim/replays` 与 `/localsim/replays/{replay_job_id}`；
- `/localsim/cutover-readiness`、既有 scheduler status/verification 和 platform diagnostics 的 read-only projection。

列表使用稳定 `(created_at, id)` cursor、`limit=1..200` 和 exact account/status/date filters；不以 offset page 或前端拼接旧 portfolio/session 形成第二 query truth。run/ledger 读取现有 committed economic facts，只做 account/lineage scope 投影，不调用 broker、signal、historical provider 或 writer。

错误统一为既有 FastAPI detail envelope：`code/message/context/retryable`。schema/field 为 `422`，不存在为 `404`，CAS、identity、state/window/idempotency 冲突为 `409`，cutover schema/readiness/source 暂不可用为 `503`，确定性损坏保持 typed `409/422`；任何错误均不得返回 `ok=true`、旧 DTO、默认 account/policy、自动重建或 compatibility fallback。

#### 4.6.4 Retained-account inventory and cutover preparation

`LegacyLocalSimAccountInventoryV1` 不是公开 request model。`SIM-LR-C` 源 PR 必须提供 task-owned、可复跑、fail-closed 的 `scripts/prepare_localsim_successor_cutover.py`（最终名称以实现 catalog 为准），模式固定为 `inventory -> preflight -> apply -> readback`：

- `inventory` 只读列出当前统一 Simulation Runtime 仍认领的 LocalSIM account/release/binding/ledger scope、经济事实 hash、in-flight、claim、legacy session/auto-run/sentinel owner；terminal/failed/orphan 不得成为 retained candidate；
- retained 集合由明确的 exact legacy account id 输入冻结；工具自己从权威 repository 构造 inventory，调用方不能提供 hash、capital、release/binding、status、runtime-owned 或 in-flight 值；
- `preflight` 验证 B2 production schema与C ledger-scope bridge的comment/FK/index readback、source merge identity、零 legacy writer/claim/in-flight、每个 retained candidate 唯一且经济事实 hash 稳定；
- `apply` 仅在用户对精确 production target、B2 migration 和 lineage DML 明确授权后执行，并逐 account 调用同一 `LocalSimControlPlaneService.prepare_legacy_lineage` transaction；不复制/更新 economic rows；
- `readback` 重新计算 candidate、lineage、release/binding、ledger row-count/hash 和 owner inventory，生成不含敏感值的 commit-bound receipt；重复 apply 只能得到 exact same lineage，任何 drift 整体 fail loud。

公开 router 不提供 lineage prepare/activate endpoint。lineage `PREPARED -> ACTIVATION_PENDING_SAFE_BOUNDARY -> ACTIVE` 只由 application lifecycle owner读取服务端 inventory与自动 safe-boundary decision 推进，客户端不能提交 `eligible`、market phase、in-flight 或 writer-claim 事实。

#### 4.6.5 Frontend and shared UI route inventory

唯一 LocalSIM 产品 route 固定为：

- `/simulation/localsim`：account list/create 和 cutover/readiness 摘要；
- `/simulation/localsim/accounts/{accountId}`：account/release/binding/run 摘要；
- `/simulation/localsim/accounts/{accountId}/ledger` 与 `/performance`：只读经济事实投影；
- `/simulation/localsim/replays` 与 `/simulation/localsim/replays/{replayJobId}`：创建、进度、cancel 和自动 live-transition 状态。

UI 不显示或调用 session、manual tick、run-day、readiness POST、auto-run、scheduler start/stop/run-once；运行控制只保留 account lifecycle CAS、successor release 和 replay cancel。LocalSIM pages/client/types 从 `frontend/src/app/paper-v2/{portfolios,running}`、`frontend/src/lib/paper-v2` 迁到 `frontend/src/app/simulation/localsim` 与 `frontend/src/lib/simulation/localsim`，旧 LocalSIM path 不保留 redirect、410 page 或 compatibility import。`paper-v2` 目录下当前承载的 Strategy Package、Selection、HMM、Advisory 和 MiniQMT 页面不在本 PR 偷换业务语义；其 caller 只能改指向新 LocalSIM route，物理命名空间拆除按 `SIM-LR-D/PR-D1` 的完整 inventory 执行。

通用 card/table/error/notice/status 组件迁到不含 Paper product 语义的共享 UI owner，并更新所有现有 caller；不得复制组件留两份。frontend contract test 必须从 network request inventory 直接断言零 `/paper-v2/(portfolios|sessions|replay|auto-run|scheduler)` 和零 `/simulation-runtime/scheduler/(start|stop|tick)` mutation。

#### 4.6.6 MCP, script and application lifecycle

MCP 替换为 `simulation_runtime` read-only monitoring，只暴露 account/replay/run/diagnostics projection；删除 session/scheduler/readiness mutation tool/profile/catalog entry。`scripts/paper_v2_live_validation.py`由新的LocalSIM产品验证脚本取代，使用account/replay/query API且不创建session/manual tick；`paper_v2_coldstart_sanity.py`与`r6_prod_cutover_e2e_wrapper.py`的sentinel mutation能力删除；历史失败清理脚本迁到 successor account/query repository，继续要求 exact account ids、dry-run inventory、保留 package、物理删除授权与 readback，不通过 Paper service 构造业务对象。历史migration helper只作为审计/已执行migration入口处置，不能成为旧产品运行脚本。

`backend/main.py` 删除旧 Paper scheduler shutdown hook；统一 Simulation lifecycle 仍由既有 application owner start/shutdown，不新增公开启动开关。fresh process 构造 `LocalSimCutoverReadinessV1`：schema未应用、retained lineage缺失、经济 hash 漂移、旧 writer/reference 非零时，新 mutation endpoint统一 `503` 且不执行写入，旧 mutation endpoint仍不存在；这是自动技术 fail-closed，不是 feature flag、人工 approval 或旧路径 fallback。

#### 4.6.7 Selection and Strategy Package product seam

现有 `/selection-center/runs/{run_id}/create-paper-portfolio` 与 `SelectionPaperPortfolioController` 是旧产品 mutation caller，必须在 `SIM-LR-C` 同步替换为 `/selection-center/runs/{run_id}/create-localsim-account` 和 simulation-runtime owned controller。新controller先读取既有 successful selection evidence，再调用同一server-resolved LocalSIM account bundle transaction；不得让 Selection service 创建 account、读取 broker/ledger 或重新选择股票。响应返回 account/release/binding和selection link，不返回旧 portfolio/runtime_config DTO。

Selection provenance 不能因切流丢失。C阶段把 Python DTO/repository API 改为 neutral `SelectionSimulationAccountLink`，底层已存在的 `selection.paper_portfolio_link(portfolio_id)` 可在D2 schema retirement前作为物理兼容列保存 `SimulationAccountV1.account_id`，因为该表/列没有Paper portfolio FK；新代码和API不得继续暴露paper/portfolio命名。D2再通过独立DEV-first migration将table/column改为successor命名并保持历史link row identity/readback。Strategy Package usage由 account/package与selection link权威查询得出，不再调用 `mark_paper_portfolio_created` 或写第二份package lifecycle truth。

Strategy Package、Selection、Multi-Alpha页面中“创建LocalSIM”的链接只改到`/simulation/localsim?package_id=...`并保留其已有package/top-k输入；不得迁移或改变这些模块自身的训练、promotion、selection、watchlist、HMM或MiniQMT语义。direct no-drift tests必须覆盖 selection result逐字段不变、package admission/hash不变、创建失败零account/link orphan，以及MiniQMT create route完全不受影响。

### 4.7 Historical replay and live transition contract

每个 replay 使用独立 `replay_job_id + simulation_account_id + binding_id`，不得复用当前运行账户的 ledger 或 writer lock。

状态机：

```text
CREATED -> RUNNING_HISTORICAL -> CAUGHT_UP -> READY_FOR_LIVE
        -> FAILED_RETRYABLE | FAILED_TERMINAL | CANCELLED

READY_FOR_LIVE
  -> successor live release/binding created atomically
  -> ACTIVATION_PENDING_SAFE_BOUNDARY
  -> LIVE_ACTIVE
```

平滑切换规则：

- 盘前追赶至前一交易日时，可为当日 pre-open 创建 live successor；
- 已进入当日交易时段后才追赶完成，不允许从中途插入当前日执行，最早在下一安全交易日激活；
- historical provider 在 `CAUGHT_UP` 后必须关闭；live binding 只允许 TDX current-day source；
- 切换不得复制历史订单或重放已完成日；
- 当前已运行 LocalSIM 不受新 replay 的锁、资金、持仓或生命周期影响。

### 4.8 Cutover and deletion contract

新路径可以在 task branch/DEV 中先构建，但生产只允许一次原子切流：

1. 新 control-plane、data/signal/execution 和 UI 在非生产隔离环境完成验证；
2. cutover release 同时启用新 LocalSIM create/replay/lifecycle route，并删除旧 mutation route/caller；
3. 禁止生产双写、shadow order、legacy fallback、translator 或长期 410 façade；
4. 旧实例若存在 active economic owner，只允许按原版本自然 drain；源代码删除前必须证明 active owner/reference 为零；
5. 完成 cutover 后删除旧代码，而不是保留“deprecated but runnable”类。

历史 migration 文件保留。`paper_v2`可暂时作为既有共享runtime事实的物理schema名称，schema名称本身不构成Python业务依赖，也不授权保留旧session/auto-run语义；`SimulationAccountV1`可以先落在该既有schema的新增规范表中，避免无业务收益的全schema改名。旧 init schema 创建语句、runtime repository 方法和无引用表/列应在后续 successor migration 中停止创建或删除，DEV 与生产状态分别记录。

### 4.8.1 Existing active LocalSIM continuation

切流不能要求用户删除或重建当前正常LocalSIM，也不能把旧portfolio/account与新account同时视为economic owner。`SIM-LR-B/PR-B2`必须交付一次性、可重放且fail-closed的`LegacyLocalSimAccountLineageV1`迁移合同：

- inventory只选择当前统一Simulation Runtime仍认领、状态合法且用户明确保留的LocalSIM account/binding；terminal、failed、orphan、legacy session和auto-run记录只读保留，不被重新激活；
- 每个保留账户建立唯一`legacy_account_id -> SimulationAccountV1.account_id -> release_id -> binding_id` lineage及canonical hash；重复、交叉账户、package/admission、capital、ledger scope或binding冲突整体失败；
- successor account继续引用原有order/fill/cash/position/economic receipt范围，不复制、重写或重放经济行，也不重置现金、持仓、T+1 lot、run、plan或outbox；
- 新旧identity切换只允许在自动判定的non-trading safe boundary、无in-flight economic transaction且旧claim已释放时执行；这是技术一致性条件，不是人工审批；
- cutover transaction完成后，旧account/session identity仅作read-only lineage，不能再次claim、写入或通过fallback恢复；
- DEV和生产readback分别证明account/release/binding/ledger引用闭合、economic row count/hash不变、唯一writer owner已转移。

新创建的LocalSIM直接使用`SimulationAccountV1`，不得经过lineage迁移。当前运行账户延续与新账户创建共享同一control-plane/economic contract，但有不同的creation evidence。

## 5. 旧能力处置清单

### 5.1 新路径具备替代能力后直接删除

| 旧能力/文件 | 当前问题 | 最终处置 |
| --- | --- | --- |
| `paper_trading_v2/day_runner.py` | router 可直接执行整日并写 ledger | 删除；由统一 lifecycle/day engine 取代 |
| `paper_trading_v2/replay.py` | 循环调用旧 day runner | 删除；由 `simulation_runtime/replay.py` 取代 |
| `paper_trading_v2/session.py` | session create/tick/pause/resume 第二状态机 | 删除 |
| `paper_trading_v2/live_session.py` | 第二实时 minute executor | 删除 |
| `paper_trading_v2/scheduler.py` | 禁用 autostart 但完整 runnable | 删除，同时移除 `backend/main.py` shutdown import |
| `paper_trading_v2/auto_run.py` | portfolio config + session recovery 第二控制面 | 删除 |
| `paper_trading_v2/runner.py` | 无生产 caller 的替代撮合入口 | 删除 |
| `paper_trading_v2/day_features.py` | V25 多表数据 loader | 删除 |
| `paper_trading_v2/readiness.py` | 复制 data/policy/session 校验 | 删除；统一 query/preflight 取代 |
| `paper_trading_v2/coldstart_sentinel.py` | 绕过 runtime 直接写模拟订单/成交 | 删除 |
| `paper_trading_v2/daemon/**`、`poc/**` | 可执行旧 broker/gateway/SQLite 路径 | 删除 |
| `trading_core/sim_gateway/**` | 仅旧 daemon/test 使用 | 删除 |
| `paper_trading_v2/broker/minqmtsim.py` | 旧Paper MiniQMT直接broker；当前scheduler仅借用quote normalizer | normalizer迁入MiniQMT/data owner后删除，禁止恢复旧broker route |
| `paper_trading_v2/execution/minqmt_live_algo_adapter.py` | 已标记legacy的替代adapter | 删除；KERNEL_V2不经该adapter |
| `paper_trading_v2/execution/minqmt_order_state.py` | 无当前product caller的旧order-state helper | 零caller确认后删除 |
| `risk_targets.py`、`selection_cutoff.py` | 无新主链 production owner | 迁移仍需纯函数后删除原文件；无 caller 则直接删除 |

### 5.2 先迁移有效能力，再删除原文件

| 混合文件 | 必须迁出的有效能力 | 目标 owner |
| --- | --- | --- |
| `broker/base.py` | broker contract、handle/status DTO | `simulation_execution/broker.py` |
| `broker/localsim.py` | durable minute runtime、TWAP、ledger effects | `simulation_execution/localsim/*` |
| `market_data.py` | DailyContextV2、TDX causal minute、historical minute、calendar adapter | `simulation_data/*` |
| `models.py` | 仍有效的 account/run/ledger query DTO | C迁到各层neutral contracts并让product path零Paper model；session DTO在D1删除 |
| `repository.py` | economic transaction/readback、portfolio query | C迁出successor product实际使用的economic repository + runtime query service并切断`get_portfolio` truth；D1删除剩余旧实现 |
| `service.py` | account/release/binding 必要创建语义 | `simulation_runtime/control_plane.py`；manifest policy/session/auto-run 逻辑删除 |
| `symbol_names.py` | 通用 symbol display enrichment | 独立 read-only market metadata service |
| `live_dashboard.py` | 有价值的只读 projection | `simulation_runtime/queries.py`；scheduler/session projection 删除 |
| `canonical_pit_control.py` | Paper runtime-profile历史迁移/read-only inventory | 必要inventory迁入统一control-plane后删除 |
| `execution/minqmt_execution_report.py` | 仍有价值的MiniQMT只读质量投影 | MiniQMT/runtime read-only query owner；迁移后删除原文件 |

完成LR-7后，`backend/services/paper_trading_v2/` Python产品包及`backend/routers/paper_trading_v2.py`必须整体不存在；允许继续存在的只有历史migration、历史文档和数据库schema中的尚待LR-8处理的只读事实。若inventory发现任何未分类文件或production caller，LR-7 fail closed并先更新本文处置表，不得保留未说明例外。

### 5.3 旧产品包外部表面

| 外部旧表面 | 当前事实 | 最终处置/阶段 |
| --- | --- | --- |
| `backend/services/strategy_package/multi_alpha_paper_admission.py`、`multi_alpha_paper_dry_run.py`及`asset_eligibility.py`中的Paper broker contract | Strategy Package仍从Paper model取得broker identity，paper-named admission可能被误当LocalSIM产品owner | `SIM-LR-A`迁为broker-neutral simulation admission/eligibility contract并更新全部caller；旧paper-named module和import删除，package alpha/admission结果不变 |
| `backend/services/selection_center/**`与`backend/routers/selection_center.py`的Paper data/service import | 信号服务反向创建portfolio并依赖Paper data DTO | `SIM-LR-A`迁至data/signal contract；产品创建编排暂存controller，`SIM-LR-C`改用统一control plane |
| `backend/services/miniqmt_execution_runtime/client.py`、Simulation Runtime与Trading Core SimGateway的Paper broker/data import | current KERNEL_V2和统一runtime仍借用旧contract owner | data DTO在`SIM-LR-A`迁移，broker DTO/runtime在`SIM-LR-B/PR-B1`迁移；SimGateway在`SIM-LR-D/PR-D1`删除；MiniQMT业务语义不变 |
| `frontend/src/app/paper-v2/**`、`frontend/src/lib/paper-v2/**` | 旧portfolio/session/tick/auto-run产品UI/API仍可达 | `SIM-LR-C`迁到`/simulation/localsim`产品route和统一API；旧business page/API/types删除，不保留redirect或410 |
| `frontend/src/components/paper-v2/**`及其他模块对其import | 多个非模拟盘页面复用通用card/table/error视觉组件 | 通用组件在`SIM-LR-C`迁到共享UI owner并更新caller；组件行为保留，但旧paper命名空间在`SIM-LR-D/PR-D1`必须不存在 |
| `backend/mcp/modules/paper_v2_monitoring.py`、tool manifest/profile和Research Assistant MCP catalog | 仍投影session/scheduler/readiness旧语义 | `SIM-LR-C`以simulation-runtime read-only monitoring替换；旧module/tool/profile/catalog entry删除 |
| `scripts/paper_v2_coldstart_sanity.py`、`paper_v2_live_validation.py`、R6旧cutover wrappers | 仍调用sentinel/session/tick和旧route | 有价值验证迁到统一control-plane/runtime smoke；旧脚本在`SIM-LR-D/PR-D1`删除，禁止保留可执行旧入口 |
| `scripts/prune_localsim_failed_history.py` | 有效维护能力仍直接构造Paper repository | `SIM-LR-C`迁到统一account/query repository并改为新命名脚本；保留精确删除安全合同，不保留Paper import |
| `backend/main.py`旧scheduler hook、Validation/CI/Nightly ownership/module/test-plan中的Paper产品分类 | application lifecycle和质量路由仍承认旧产品owner | hook在`SIM-LR-C`删除；catalog/test-plan/changed-file分类在`SIM-LR-D/PR-D1`切到新三层与runtime owner，历史receipt文本不改写 |

fresh-process AST/import、OpenAPI、frontend route/build manifest、MCP registry、script entry和ownership catalog必须共同证明这些外部表面已迁移或删除；只对`backend/services/paper_trading_v2/`执行`rg`不构成退役证据。

### 5.4 不属于“旧业务代码删除”的对象

- 已执行 migration 文件；
- immutable historical run/receipt/audit evidence；
- StrategyPackage alpha/model/factor assets；
- Trading Core 通用 OMS、fee、board-lot 和 minute execution pure engine；
- `DailyTradingContextV2`、LocalSIM durable state、economic receipt/outbox 等当前权威契约。

## 6. Implementation Plan / 分阶段实施与 PR 边界

任何实现阶段都从开始时最新 `origin/main` 建独立 worktree，并引用 `F-133..F-148` 中的适用项。阶段是业务可验收单元，不是文件清单；默认一个阶段连续开发、统一复核，只有下表列出的风险边界才拆成两个源 PR。总预算最多六个源 PR，不因目录、类或测试数量继续细分。

| 阶段 | 合并的 LR 工作包 | 默认源 PR | 阶段目标 | 不允许进入本阶段的动作 |
| --- | --- | --- | --- | --- |
| `SIM-LR-A` Layer Foundation | `LR-0..LR-2` | 1 | 建立依赖/route/query/single-writer基线；交付`simulation_data`和`simulation_signal`；迁移共享data DTO；解除Selection Center创建Paper portfolio的反向依赖 | 不迁移经济writer，不开放新产品route，不改当前运行账户 |
| `SIM-LR-B` Successor Core | `LR-3..LR-5` | 2 | PR-B1交付`simulation_execution/localsim`与唯一economic coordinator/projector；PR-B2交付account/release/binding control plane、现有正常账户lineage、隔离replay、safe-boundary live successor和additive successor schema source/DEV receipt | 不注册新生产create/replay mutation route，不新增dormant UI/MCP，不删除旧产品包；生产DDL/DML仍需精确授权 |
| `SIM-LR-C` Atomic Product Cutover | `LR-6` | 1 | 同PR交付ledger-scope bridge、server-resolved product contract、neutral economic/query composition、preparation/readiness工具，并让router/frontend/MCP/scripts/application lifecycle一次切换；删除旧mutation调用和公开scheduler mutation；source merge后只有在production B2/C schema+lineage readback通过才允许用户重启激活 | 不保留dual route、translator、shadow writer、feature flag、影子Paper portfolio或长期410 façade |
| `SIM-LR-D` Physical Retirement & Acceptance | `LR-7..LR-9` | 2 | PR-D1物理删除旧Python产品包/router/旧测试并更新ownership/catalog；PR-D2只提交DEV已验证的legacy schema/init退役变更；随后分别完成用户重启、生产授权迁移和正常交易日验收 | 不把源码删除当作DDL授权，不删除历史migration/审计事实，不在正常交易日证据前宣布完成 |

### 6.1 `SIM-LR-A`：Layer Foundation

同一源 PR 完成以下闭环：

- 先建立 production-entry inventory、AST/import boundary、route uniqueness、current-day query budget 和 single-writer 断言；这些测试在迁移期间允许精确列出尚未完成项，但禁止宽泛 allowlist；
- 建立 `simulation_data`，原子迁移 DailyContextV2、calendar adapter、TDX causal minute、historical minute 与通用 quote/metadata contract；同时建立纯`simulation_execution` broker identity/handle contract owner，current MiniQMT、Strategy Package、Selection、router和Trading Core caller同步改 import owner，业务 payload/hash/route/admission不变；
- 建立 `simulation_signal`，迁移 selection/target/rebalance contract；Selection service只产出evidence/target，不再创建或修改Paper portfolio/session；现有创建流程的产品编排暂时上移到既有Paper controller/service边界，因此本阶段API结果、账户创建副作用和用户流程不变，待`SIM-LR-C`整体替换；
- 删除已迁出的 V25 day-feature、legacy previous-close 和 current-day DB-minute capability；若仍有 caller，本阶段失败而不是保留 re-export shim；
- 完成 data/signal direct tests、Selection/Strategy Package contract tests和MiniQMT共享contract no-drift矩阵。

退出条件：新data/signal owner可独立测试；当前运行路径仍只有旧economic writer；新三层没有Paper反向import；本阶段未改变生产route、账户、订单、成交、资金或持仓。

### 6.2 `SIM-LR-B`：Successor Core

该阶段只按“经济事实风险”拆两个源 PR，不再细分：

1. **PR-B1 — execution/economic owner**：迁移 broker contract、LocalSim durable minute runtime、TWAP、planning、economic transaction、outbox 与 projector；从`simulation_runtime/scheduler.py`移出LocalSIM planning/economic/projection责任；MiniQMT client切换到唯一broker contract owner并运行KERNEL_V2直接回归。合入后仍由既有产品入口调用，不增加第二writer或第二policy truth。
2. **PR-B2 — control/replay owner**：交付`SimulationAccountV1`、immutable successor release/binding、CAS lifecycle、原子零orphan repository、现有正常LocalSIM的`LegacyLocalSimAccountLineageV1`、隔离replay job/resume/cancel/day cursor和safe-boundary live successor；同PR提交additive successor schema/mapping migration/init source并先在DEV验证transaction/FK/readback/economic-hash unchanged；只交付内部command/query service和纯request/response contract tests，不新增未注册router、未链接UI、MCP tool或其他dormant product surface。PR-B2合入后，生产successor DDL/DML必须另获精确target/migration授权、应用并readback，不能由source merge推定。

退出条件：旧入口与新内部服务只汇合到同一economic coordinator；六个月replay使用独立account/binding/writer lock并可重启恢复；当前运行LocalSIM账户的ledger、资金、持仓、run和锁逐事实不变且lineage映射唯一；successor schema/mapping已有DEV receipt，生产应用/readback若未授权则明确为pending并阻断`SIM-LR-C`用户重启/产品激活而不阻断B2或C source merge；仓库不存在提前暴露或无法验收的第二产品表面。

### 6.3 `SIM-LR-C`：Atomic Product Cutover

该阶段固定一个源 PR，禁止拆成“先加新route、以后再删旧route”：

- 前置只读inventory必须证明旧Paper session/auto-run/sentinel writer、旧scheduler active owner和未归属legacy process reference均为零；当前由统一Simulation Runtime持有的LocalSIM binding/run不属于legacy owner，必须通过`LegacyLocalSimAccountLineageV1`、新repository identity和economic hash/readback保持连续；
- C 源码可以在生产迁移前合入，但用户重启/产品激活前，production successor schema及保留账户lineage mapping必须已按精确授权应用并readback，account/release/binding/replay transaction smoke通过；若未满足，fresh process 新 mutation 统一 fail-closed 为 `503`，绝不恢复旧route；
- cutover command只在non-trading safe boundary、零in-flight economic transaction执行；若条件未满足保持`ACTIVATION_PENDING_SAFE_BOUNDARY`并自动重试，不启用新route、不部分切换owner；
- 注册统一 LocalSIM account/release/binding/replay/lifecycle/query API；
- frontend创建/配置/运行/回放页面和API一次改用统一control plane，不再创建session或主动tick；
- MCP只保留统一read-only monitoring；移除Paper session/scheduler monitoring与readiness POST；
- scripts改用统一API/repository，`backend/main.py`删除旧Paper scheduler shutdown hook；
- 删除Paper `run-day/replay/session/tick/auto-run/coldstart` mutation route以及Simulation Runtime公开`scheduler start/stop/tick` mutation；
- 新frontend产品route固定为`/simulation/localsim`；通用视觉组件迁往共享UI owner，旧`/paper-v2`business route/API/types不保留redirect、410或compatibility import；
- fresh-process OpenAPI、frontend route/build manifest、MCP registry、script entry、import graph和writer inventory必须同时证明唯一route/owner。

源 PR 必须同时交付 §4.5.1 ledger-scope migration、§4.6.4 cutover preparation/readback 工具和 §4.6.6 automatic readiness owner，使顺序可以安全保持为：`source merge -> 精确授权 production B2 DDL + C bridge DDL/lineage DML/readback -> 用户重启 -> fresh-process route/runtime verify`。源码合入本身不授权生产写入，也不允许在 production readback 前建议用户重启。

合入后状态必须按真实前置写为`source_merged_pending_production_cutover_preparation`或`source_merged_pending_user_restart`。只有 production DDL/lineage/readback 已闭合才进入后者；用户重启和只读route/runtime identity验证通过前，不进入物理删除阶段；失败只在新路径forward-fix，不恢复旧route。

> 2026-08-31 状态回写：`SIM-LR-C` source PR #4035 已 squash 合入为 `f2eb8ed03694fd1eb856052a79a25499be74910b`，规范根同步与源工作树/分支清理已完成。当前状态严格为 `source_merged_pending_production_cutover_preparation`：B2 successor migration、C bridge migration、retained-account lineage DML及其经济事实/owner readback均尚未获得精确生产授权，用户重启、fresh-process route/runtime identity、正常交易日和六个月 replay capacity 证据均未开始；因此不得进入 `SIM-LR-D`。

### 6.4 `SIM-LR-D`：Physical Retirement & Acceptance

该阶段按“源码退役”和“生产schema授权”拆两个源 PR，并把运行验收作为独立状态：

1. **PR-D1 — physical source retirement**：只在`SIM-LR-C`用户重启后fresh-process active owner/reference为零时，按§5物理删除`backend/services/paper_trading_v2/`Python产品包、`backend/routers/paper_trading_v2.py`、旧frontend/MCP/script caller、daemon/POC/SimGateway和只验证旧行为的测试；有效contract tests迁到新owner；ownership/module/test-plan catalog同步更新。
2. **PR-D2 — legacy schema retirement source**：基于只读row/FK/retention inventory和DEV PostgreSQL验证，停止init schema创建旧session/auto-run/sentinel对象并提交精确legacy-retirement migration；不得重复创建PR-B2已交付的successor对象。生产DDL/DML仍等待用户对具体target/migration授权，历史migration和仍属审计权威的经济事实保留。
3. **运行验收**：在用户重启后分别闭合六个月replay capacity、盘前create/plan、开盘TDX、午间恢复、partial fill、EOD、query/write budget、单writer和零旧route/import；只有全部完成才把`SIM-P-086`标为`IMPLEMENTED_VERIFIED`。

### 6.5 开发效率与质量控制

- 每阶段只登记一个主任务/epic和一个共享Context Pack；阶段内第二PR复用同一事实盘点，但分别保存commit-bound validation receipt；
- review先跑直接fix-point/contract，再跑该阶段一个相关矩阵；跨模块全量回归委托CI/Validation Center/nightly，禁止每个文件迁移都重复全矩阵；
- 同一阶段内发现的同根因缺陷直接修复并复审，不新增微型BUG；只有跨阶段、不同生产门禁或独立业务语义才拆BUG/PR；
- 任何阶段 scope 扩大到MiniQMT业务语义、StrategyPackage alpha、Selection结果、执行算法或生产DDL时立即停止并重新审核，不用“重构顺手修改”吸收；
- 每阶段退出时更新父蓝图`SIM-P-086`，分别记录source、merge、DEV、production、restart、runtime和cleanup，不把其中任一状态代替整体完成。

## 7. Verification Plan / 验证方案

### 7.1 静态边界

目标测试：

- `backend/tests/simulation_architecture/test_layer_import_boundaries.py`
- `backend/tests/simulation_architecture/test_legacy_localsim_absence.py`
- `backend/tests/simulation_architecture/test_localsim_route_uniqueness.py`

至少断言：

- 新三层生产代码无 `paper_trading_v2` import；
- data/signal/execution/runtime 依赖方向闭合；
- 旧类、router mutation、frontend API、MCP tool、startup/shutdown hook 和脚本 caller 不存在；
- `backend/services/paper_trading_v2/` Python产品包与`backend/routers/paper_trading_v2.py`不存在，current MiniQMT shared-contract imports已迁移且KERNEL_V2 direct tests保持通过；
- `day_features`、`V25DayFeatureProvider`、`PaperTradingSessionRunner`、`PaperTradingDayRunner` 等 exact symbols 不存在。

### 7.2 Data layer direct tests

- `backend/tests/simulation_data/test_daily_context.py`
- `backend/tests/simulation_data/test_tdx_causal_minute.py`
- `backend/tests/simulation_data/test_historical_minute.py`
- `backend/tests/simulation_data/test_query_budget.py`

覆盖 current/historical source isolation、exact symbol coverage、TDX causal cursor、future/duplicate/conflicting bar、盘中零 market SQL、历史 DB minute 只对已完成日可用以及零行情写入。

### 7.3 Signal layer direct tests

- `backend/tests/simulation_signal/test_strategy_package_selection.py`
- `backend/tests/simulation_signal/test_target_rebalance_isolation.py`

覆盖 single/multi-alpha、HMM on/off、runtime successor config、package no-revalidation、相同 frozen input 确定性、broker/account facts 不可达、execution failure 不改变 selection。

### 7.4 Execution and repository tests

- `backend/tests/simulation_execution/test_localsim_runtime.py`
- `backend/tests/simulation_execution/test_localsim_economic_transaction.py`
- `backend/tests/simulation_execution/test_localsim_projection.py`
- `backend/tests/simulation_execution/test_twap_policy_authority.py`

覆盖 TWAP-only release snapshot、causal minute、partial fill、cash competition、T+1、limit/suspend、CAS/idempotency、rollback、commit-unknown/readback、restart/outbox、单 writer 和 projection no-broker/no-signal。

### 7.5 Control plane and replay tests

- `backend/tests/simulation_runtime/test_localsim_control_plane.py`
- `backend/tests/simulation_runtime/test_localsim_replay.py`
- `backend/tests/simulation_runtime/test_localsim_replay_live_transition.py`
- `frontend/tests/paper-v2/localsim-unified-control-plane.spec.ts`

覆盖原子创建失败零 orphan、successor release、六个月 replay、独立账户、重启 resume、盘前切换、盘中延迟至下一安全日、当前运行账户逐字节/逐事实不变以及前端不再调用旧 session/tick API。

共享contract迁移还必须运行current MiniQMT client/quote/KERNEL_V2最小直接矩阵，证明只改变import owner，未改变quote、parent/child、algo、OMS/Gateway、order/trade或reconcile语义。

### 7.6 DEV、capacity 与正常交易日证据

- DEV PostgreSQL 验证 migration、transaction、FK、readback 和 old-reference zero inventory；
- historical replay 至少六个月，记录 day count、runtime、memory 和 DB transaction budget；
- 当前日 1m cadence 记录 market SQL=0、minute DB read=0、raw bar/quote write=0；
- 正常交易日验证 plan、TDX causal minute、order/fill/cash/position、EOD 和重启恢复；
- Validation Center/CI/nightly 承担跨模块完整矩阵，本地只保留直接 contract 与 changed-file gate。

## 8. Rollout / Rollback / 发布与回滚

### 8.1 Rollout

1. `SIM-LR-A/B` 可以按§6最多三个源 PR合入，但不得让尚未启用的新路径与旧路径同时写生产经济事实；
2. `SIM-LR-C` 是唯一产品切流点，必须绑定 exact merge commit、route inventory 和 fresh-process evidence；
3. `SIM-LR-D/PR-D1` 仅在 `SIM-LR-C` source merge、用户重启和生产 active-reference zero readback 后执行；
4. `SIM-LR-D/PR-D2` 数据库迁移与源码删除保持独立状态；
5. 最终正常交易日证据失败时保持 source/DDL/runtime 各自真实状态，不伪报整体完成。

### 8.2 Rollback

- `SIM-LR-C`之前使用阶段内forward-only PR修复，不为新路径增加生产shadow writer；
- `SIM-LR-C`之后不得恢复旧 Paper day/session/auto-run route；问题通过新路径 forward fix 或暂停受影响的新 binding admission 处理；
- 已提交经济事实不回写、不删除、不用旧 ledger 覆盖；
- schema rollback 只处理尚未被新 carrier 引用的 additive 对象，并遵守 DEV-first 和生产授权；
- 旧代码删除后不允许从 Git history cherry-pick 整体恢复旧产品路径。

## 9. Risks / Failure Modes / 风险与失败模式

| 风险 | 设计控制 |
| --- | --- |
| 先删 Paper v2 导致统一 runtime import 失败 | `SIM-LR-A/B`先迁移有效能力，`SIM-LR-D/PR-D1`后删除 |
| 新旧 route 同时写订单/成交 | `SIM-LR-C`原子切流；route/import/single-writer fresh-process tests |
| 仅改目录名，复制旧 monolith | 模块 LOC/责任检查、exact owner matrix、禁止旧 symbol 复制 |
| scheduler 继续变成总控巨石 | economic coordinator/projector/control plane/replay 分离，scheduler 只编排 |
| signal 为执行失败重新选股 | immutable evidence/hash，execution 无 signal-service import |
| 当前日 TDX 失败回落 DB minute | source type 和日期 fail-loud，跨源负向测试 |
| replay 影响当前 LocalSIM | account/binding/writer-lock 隔离和逐事实不变测试 |
| 追赶中途切入当日造成未来/漏 bar | safe-boundary activation，盘中完成延至下一交易日 |
| runtime config 又写回 package | successor release only，package manifest unchanged assertion |
| hardcoded TWAP 与 release policy 双 truth | control-plane 冻结 exact snapshot，runtime strict consume |
| 旧数据库表被代码删除后继续由 init schema 创建 | `SIM-LR-D/PR-D2`同步 init schema 和 successor migration |
| 旧历史数据被误删 | active/reference/retention inventory，DML 精确授权，历史 migration 保留 |
| 长期保留 410/compatibility façade | `SIM-LR-D/PR-D1` Definition of Done 要求旧 module/route/symbol 物理不存在 |

## 10. Production Gates / 生产门禁

| Gate | 本设计 PR | 后续实现要求 |
| --- | --- | --- |
| backend dependency | noop | 仅依赖文件实际变化时进入独立 gate |
| frontend dependency | noop | 仅 lockfile 实际变化时进入独立 gate |
| production DDL | noop | `SIM-LR-B/PR-B2` additive successor schema与`SIM-LR-D/PR-D2` legacy retirement分别DEV-first、分别绑定精确生产migration授权/readback；前者是`SIM-LR-C`前置 |
| production DML | noop | 历史模拟盘数据删除/迁移需精确目标和单独授权 |
| config/binding/broker | noop | `SIM-LR-B/SIM-LR-C/最终运行验收`分别记录 |
| backend restart | noop；owner=user | runtime source 合入后由用户决定目标重启 |
| runtime verification | not started | `SIM-LR-C`和最终运行验收在用户重启后只读验证 |
| client sync | noop | 本设计不修改 `.codex/**` 或 `.claude/**` |

## 11. Design Acceptance Index / 设计验收索引

| ID | 验收条款 |
| --- | --- |
| `F-133` | LocalSIM 最终生产路径、router、frontend、MCP、scripts 和 backend lifecycle 对旧 Paper day/session/replay/auto-run/sentinel 的可达引用为零；Paper v2 Python产品包与router整体删除 |
| `F-134` | `simulation_data`、`simulation_signal`、`simulation_execution`、`simulation_runtime` 物理边界和单向依赖完整，无反向 import、旁路 writer 或复制 contract；current MiniQMT共享contract迁离Paper且业务语义不变 |
| `F-135` | 数据层独立输出 calendar/selection input/DailyContextV2/current TDX causal minute/historical minute；当前日与历史源严格隔离，盘中零 market SQL/历史 minute read/行情写入 |
| `F-136` | 信号层只消费 package alpha assets、frozen input 和 runtime profile，只输出 immutable selection/target/rebalance evidence，不创建模拟盘、不读 broker/ledger、不重选补位 |
| `F-137` | 执行层只消费 frozen release/binding、signal/target、daily context 和 causal minute；TWAP-only、方向数量、T+1、limit/suspend、失败隔离不漂移 |
| `F-138` | LocalSimEconomicCoordinator 是 state/order/fill/cash/position/mark/receipt/outbox 唯一 writer；account/lineage只解析唯一`SimulationLedgerScopeV1`，不读写影子Paper portfolio，projector/read API 无 broker/signal/第二写路径 |
| `F-139` | StrategyPackage仅冻结alpha/model/factor；package-scoped `LocalSimRuntimeProfileV1/version`权威管理可修改的日频/HMM/risk/fee/runtime variant，validated TWAP policy派生execution/tail，全部通过immutable successor release冻结；新配置不依赖Paper portfolio/profile，requested policy仅进audit metadata |
| `F-140` | `SimulationAccountV1`与统一control-plane按§4.6冻结server-resolved request/response/error/query合同；普通创建原子写account+release+binding，replay创建原子写四实体bundle，失败零orphan/双快照；UI/MCP不再创建session或主动tick，产品router不公开scheduler start/stop/tick mutation |
| `F-141` | historical replay 使用独立 account/binding/job 和同一日级 engine；六个月追赶、restart resume、safe-boundary live successor、当前运行账户隔离完整 |
| `F-142` | 产品切流一次完成，新旧路径无生产双写/shadow/translator/fallback；retained inventory只由服务端权威生成，现有正常LocalSIM通过lineage复用唯一ledger scope且经济事实hash不变，native account不创建影子Paper portfolio；production preparation/readback缺失时新mutation自动503且旧route仍不存在 |
| `F-143` | §5 所列旧 LocalSIM product files、classes、tests、daemon、POC、gateway 和 startup/shutdown hook 在替代证据闭合后物理删除，不保留 runnable deprecated code |
| `F-144` | 历史 migration 保留；旧 init schema、session/auto-run/sentinel objects 和无引用字段按 inventory、DEV、授权生产 migration/readback 顺序退役 |
| `F-145` | import boundary、legacy absence、route uniqueness、query budget、single writer、source isolation 和 no-orphan 具有直接静态/contract tests |
| `F-146` | DEV PostgreSQL、六个月 replay、capacity、用户重启和正常交易日 LocalSIM 证据分别闭合，source/merge/DDL/runtime/cleanup 状态不混写 |
| `F-147` | rollout按`SIM-LR-A..D`四个可验收阶段、最多六个源PR forward-only实施；LR编号是工作包而非逐项PR；切流后不恢复旧产品route，失败不改写经济事实或私增人工门禁 |
| `F-148` | DESIGN-COMPLIANCE-001 四项逐条通过；设计 PR 不冒充实现，后续只有全部删除和运行证据闭合才能宣布 LocalSIM legacy retirement 完成 |

## 12. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-133` | §3、§4.6、§5、`SIM-LR-C/D` | artifact: `docs/architecture/localsim_legacy_retirement_and_layer_isolation_f2_detailed_design_20260830.md`; target `backend/tests/simulation_architecture/test_legacy_localsim_absence.py` | design_ready | explicitly approved design-only stage; implementation evidence follows cutover and physical retirement |
| `F-134` | `backend/services/simulation_data/`、`backend/services/simulation_signal/`、`backend/services/simulation_execution/broker.py`、Selection/Paper controller owner migration | `pytest backend/tests/simulation_architecture/test_layer_import_boundaries.py`; direct matrix `337 passed`; `python -m nox -s paper_v2_backend` = `1129 passed, 3 skipped, 2 xfailed` | stage_a_source_complete | explicitly approved Stage A source boundary; source PR、merge、restart/runtime evidence remain pending |
| `F-135` | data contracts、global calendar adapter、DailyContext owner、TDX causal provider、completed-day historical provider | `pytest backend/tests/simulation_data` included in Stage A direct contract matrix `20 passed`; current-day DB rejection and zero-DB TDX assertions pass | stage_a_source_complete | explicitly approved Stage A source boundary; normal trading-day runtime/query receipt remains pending |
| `F-136` | signal evidence owner、StrategyPackage selection owner、immutable target/rebalance services、Selection/Paper side-effect separation | `pytest backend/tests/simulation_signal/test_target_rebalance_isolation.py`; Selection/StrategyPackage cases included in `337 passed` matrix | stage_a_source_complete | explicitly approved Stage A phased boundary: implementation owner is uniquely `simulation_signal`; an import-only `simulation_runtime.selection` compatibility surface remains temporarily for out-of-scope DEV-onboarding consumers and must be removed by `SIM-LR-D/PR-D1`; unified successor runtime consumption remains pending `SIM-LR-B` scope |
| `F-137` | `backend/services/simulation_execution/localsim/runtime.py`、`planning.py`、execution-owned models、neutral minute provider | PR #4015 / `6ec349a19`; `pytest backend/tests/simulation_execution`=`66 passed`; lifecycle scheduler与直接execution合计`382 passed, 2 skipped`; Paper v2与MiniQMT current-head CI全绿 | pr_b1_merged | explicitly approved phased gap：PR-B1 source已合入并清理；用户重启与正常交易日证据仍按`SIM-LR-C/D`独立记录 |
| `F-138` | `backend/services/simulation_execution/localsim/economic.py`、`persistence.py`、`projection.py`、`valuation.py`；§4.5.1 C ledger-scope composition | PR #4015 / `6ec349a19`; `pytest backend/tests/simulation_execution/test_localsim_economic_transaction.py backend/tests/simulation_execution/test_localsim_projection.py`与current-head Simulation Core CI全绿；C target `backend/tests/simulation_runtime/test_localsim_ledger_scope_bridge_postgres.py` | pr_b1_merged | explicitly approved phased gap：PR-B1唯一writer source已合入并清理；C仍须迁出neutral repository/query composition并闭合native/retained ledger scope，production DDL/DML、restart和runtime activation仍未执行 |
| `F-139` | B2 control source；§4.1.2 C `LocalSimRuntimeProfileV1/version`与server authority resolver | B2 control=`9 passed`; C target `backend/tests/simulation_runtime/test_localsim_runtime_profile.py`、`test_localsim_product_authority.py`和DEV PostgreSQL profile/version transaction/readback | pr_b2_source_complete | explicitly approved phased gap：B2 release snapshot完成；C必须交付neutral profile schema/API、HMM/daily/risk/fee validation、same-package TWAP/tail派生和旧Paper profile零新引用 |
| `F-140` | 同上；§4.6 versioned product contract、additive migration `localsim_successor_core_20260831.sql`及bootstrap owner | B2 schema/control证据同前；C target `backend/tests/simulation_runtime/test_localsim_product_control_plane.py`、`test_localsim_replay_product_transaction.py`和frontend request inventory | pr_b2_source_complete | explicitly approved phased gap：内部control plane与DEV schema闭合；C须实现server-resolved authority、replay四实体原子bundle、cursor GET与统一error，不得让router信任客户端hash/inventory或暴露manual replay tick |
| `F-141` | `backend/services/simulation_runtime/localsim_replay.py`、`successor_repository.py` | `pytest backend/tests/simulation_runtime/test_localsim_replay_live_transition.py`=`5 passed`; 126交易日独立回放、restart resume、失败日精确重试、source/calendar/current-day隔离、atomic live successor与safe-boundary覆盖 | pr_b2_source_complete | explicitly approved phased gap：replay/safe-boundary source完成；真实六个月capacity、用户重启及正常交易日live transition证据仍按`SIM-LR-D`独立验收 |
| `F-142` | §4.5.1、§4.6.4、§4.8、§4.8.1、`SIM-LR-C`；PR #4035 / `f2eb8ed03` | `pytest backend/tests/scripts/test_localsim_product_validation.py backend/tests/test_validation_ui_target_catalog.py backend/tests/test_validation_catalog_integrity.py backend/tests/simulation_runtime/test_localsim_product_control_plane.py backend/tests/simulation_runtime/test_localsim_product_authority.py backend/tests/scripts/test_ci_change_classifier.py::test_github_workflow_wires_workflow_validation_fast_lane backend/tests/scripts/test_aistock_issue_workflow.py::test_merge_finalizer_detects_close_sync_from_origin_main_when_root_is_stale`=`30 passed`；`python -m nox -s validation_center_backend`=`795 passed,2 skipped`；`python -m nox -s localsim_successor_core_dev_db`=`1 passed`；`python -m nox -s ra_phase5_agent_teams`=`13 passed`且catalog findings=`0` | sim_lr_c_source_complete | explicitly approved phase boundary：source merge/cleanup已闭合；production B2/C schema、lineage DML、economic-hash/owner readback、用户重启与runtime evidence仍严格后置，缺前置时新mutation 503且无旧route fallback |
| `F-143` | §5、`SIM-LR-D/PR-D1` | target `backend/tests/simulation_architecture/test_legacy_localsim_absence.py` | design_ready | explicitly approved design-only stage; physical deletion occurs only after replacement evidence |
| `F-144` | §4.8、`SIM-LR-D/PR-D2` | target `backend/tests/simulation_runtime/test_localsim_legacy_schema_retirement_postgres.py` | design_ready | explicitly approved design-only stage; production DDL/DML require separate authorization |
| `F-145` | `backend/tests/simulation_architecture/`、`backend/tests/simulation_data/`、`backend/tests/simulation_signal/`、`simulation_core_l2` direct-plan mapping | `python -m nox -s simulation_core_l2` = `718 passed, 2 skipped` + `63 passed, 1 skipped`; `python -m ruff check`、compileall、`git diff --check` pass locally | stage_a_source_complete | explicitly approved Stage A source boundary; CI receipts bind to final PR head before merge approval |
| `F-146` | §7.6、`SIM-LR-D`运行验收 | artifact: future commit-bound DEV/replay/capacity/restart/normal-day validation receipts | design_ready | explicitly approved production-state separation; all runtime evidence pending implementation |
| `F-147` | §6、§8 | artifact: future `SIM-LR-A..D` source/merge/DDL/runtime/cleanup receipts | design_ready | explicitly approved four-stage implementation; no rollback to legacy route |
| `F-148` | §13、§14 | artifact: this design review record and `scripts/aistock_feature_workflow.py validate` receipt | design_ready | explicitly approved design-only stage; implementation completion is not claimed |

## 13. DESIGN-COMPLIANCE-001

| Control | 设计结论 | 证据 |
| --- | --- | --- |
| `no_simplified_delivery` | pass for design | 覆盖四层物理边界、配置权威、control plane、replay-to-live、单 writer、产品切流、精确删除清单、数据库退役和正常交易日证据；不以改名、410 或部分路由下线代替物理退役 |
| `no_silent_error` | pass for design | source/date/hash/identity/import/route/writer/orphan/reference 冲突均要求 typed fail loud；禁止 DB-minute fallback、policy override、默认价格/资金和假成功 |
| `no_business_semantic_drift` | pass for user-requested architecture revision | 保持 package alpha、Selection、target、side、quantity、TWAP-only、T+1、limit/suspend、经济事实和 broker 语义；只改变 owner、依赖和产品入口 |
| `no_unrequested_gate_or_approval` | pass for design | import/route/query/single-writer 是自动技术合同；未新增 RBAC、人工 acknowledge、confirm-run 或人工恢复 |
| state separation | pass for design-only revision | 当前只更新蓝图/详细设计；源码、PR merge、DEV/生产 DDL/DML、用户重启、runtime、正常交易日和 cleanup 后续分别记录 |

### 13.1 `SIM-LR-A` source implementation review

| Control | Stage A source conclusion | Direct evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass：交付完整 Stage A owner 迁移、data/signal/execution contract、Selection 副作用上移、旧 V25/previous-close/current-day DB-minute 能力删除和直接边界测试；未用空目录、可运行 facade 或 allowlist 代替迁移。`simulation_runtime.selection`仅为无 class/def 的临时重导出，真实实现唯一属于`simulation_signal`，并绑定`SIM-LR-D/PR-D1`物理删除 | `pytest backend/tests/simulation_architecture backend/tests/simulation_data backend/tests/simulation_signal`，含 import-only compatibility 反向断言 |
| `no_silent_error` | pass：source/date/timezone/hash/identity 缺失或篡改均 fail closed；TDX 当前日不 fallback DB，historical provider 不 fallback TDX，缺少 frozen daily fact 不发起行情请求 | `backend/tests/simulation_data/test_query_budget.py`、`test_tdx_causal_minute.py`、`test_frozen_contracts.py` |
| `no_business_semantic_drift` | pass：现有 API route、Paper 创建副作用、MiniQMT KERNEL_V2、StrategyPackage admission payload/schema、订单/成交/资金/持仓 writer 均未变；只迁移 owner 和 import | 核心 no-drift matrix `337 passed`；`python -m nox -s paper_v2_backend` = `1129 passed, 3 skipped, 2 xfailed` |
| `no_unrequested_gate_or_approval` | pass：未增加人工确认、RBAC、运行门禁或第二产品 route；未执行 DDL/DML、服务控制、restart 或 runtime mutation | architecture route/writer assertions；production gates=`noop` |

### 13.2 `SIM-LR-B/PR-B1` source implementation review

| Control | PR-B1 source conclusion | Direct evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass：实际迁移minute provider、broker runtime、execution state/model、TWAP planning、三类economic commit、outbox projector、valuation completion与failed-retryable recovery；scheduler只保留composition/compatibility delegate，不以空壳wrapper代替owner迁移 | `backend/services/simulation_execution/localsim/{runtime,planning,economic,persistence,projection,valuation,models}.py`；scheduler AST/source boundary assertions |
| `no_silent_error` | pass：缺失frozen daily context、minute/mark/cash、receipt/outbox/hash、CAS generation或readback均保留typed fail-closed；不新增默认价格、默认资金、旧mark、DB minute或成功fallback | direct execution=`66 passed`；lifecycle/execution=`382 passed, 2 skipped`；rollback、commit-unknown、bounded retry与tamper cases全绿 |
| `no_business_semantic_drift` | pass：现有route/account/package identity不变；TWAP-only、T+1、limit/suspend、partial fill、sell-first cash dependency、Paper ledger与MiniQMT KERNEL_V2均保持 | Paper v2=`396 passed, 1 skipped, 2 xfailed`；MiniQMT execution runtime=`1469 passed, 82 skipped` |
| `no_unrequested_gate_or_approval` | pass：未新增RBAC、人工确认、运行门禁、第二route或第二writer；未执行DDL/DML、依赖安装、服务控制、restart或runtime activation | production gates=`ddl:no-op, dependency:no-op, restart:no-op`；changed-files/ownership/CI仍绑定最终PR head |

### 13.3 `SIM-LR-B/PR-B2` source and DEV implementation review

| Control | PR-B2 source/DEV conclusion | Direct evidence |
| --- | --- | --- |
| `no_simplified_delivery` | pass：交付真实`SimulationAccountV1`、immutable release/binding、原子create、CAS lifecycle、legacy lineage、126交易日isolated replay、durable cursor/resume/cancel、atomic live successor、safe-boundary与additive schema/preflight/rollback/bootstrap；所有状态均有repository和直接事务测试 | control=`9 passed`；replay/schema=`10 passed`；Simulation Runtime=`716 passed,3 skipped`；DEV PostgreSQL=`1 passed` |
| `no_silent_error` | pass：account/release/binding/lineage/replay/source/calendar/economic hash、CAS、FK、writer window、current-day source与safe-boundary均fail closed；migration post-commit回读必需列、9个FK、唯一open-binding index与table comments | direct tamper/mismatch/restart tests；DEV migration双次幂等、bootstrap与业务行零残留readback |
| `no_business_semantic_drift` | pass：StrategyPackage alpha core不变，LocalSIM effective execution policy仍唯一TWAP；requested V25只进入`consulted_for_execution=false`审计；当前运行账户与replay account隔离，economic rows不复制/重写，MiniQMT/Paper route均未修改 | 126-day current-account byte-equivalent assertion；DEV lineage economic digest before/after unchanged；router source absence测试 |
| `no_unrequested_gate_or_approval` | pass：safe boundary是零in-flight、writer claim、historical-provider-close的自动技术条件；未增加RBAC、人工确认、第二产品route、UI或MCP；仅DEV 5433执行已授权DDL验证，production DDL/DML、service control、restart与activation均未执行 | `_dev_dsn`强制`127.0.0.1:5433/*dev*`；production gate=`pending exact target/migration authorization`；restart=`user owned` |

## 14. 多轮审核记录

| 轮次 | 审核重点 | 发现与修订 | 结论 |
| --- | --- | --- | --- |
| R1 | 现状可达性、旧文件处置、三层依赖、control-plane 缺口 | 初稿固定“先补齐新路径、一次切流、后删除”；区分直接删除、迁移后删除和历史 migration 保留 | findings fixed |
| R2 | runtime config、TWAP authority、replay-to-live、单 writer、共享contract与数据库边界 | 发现MiniQMT KERNEL_V2仍借用Paper broker/data DTO、统一router仍公开scheduler mutation、旧包处置表未覆盖minqmtsim/execution/canonical PIT；已补充共享contract迁移、MiniQMT不漂移测试、scheduler owner与Paper Python包整体删除条件 | findings fixed |
| R3 | policy双truth、account identity、物理schema边界与反向业务语义 | 发现requested policy审计与effective release component容易混同、PaperPortfolio替代身份未定义、schema命名可能被误解为必须全量迁库；已固定audit-only metadata、`SimulationAccountV1`事务/CAS/单快照契约，并允许保留既有物理schema名称但删除旧session/auto-run语义 | findings fixed |
| R4 | 反向语义、F2完整性、changed-files guardrail、module ownership与diff hygiene | 逐项复核不存在dual route、translator、silent fallback、旧scheduler mutation、第二份policy/account truth或以目录存在冒充切流；F2=`16/16,warnings=0`，guardrail=`findings=0,blocking=0`，ownership=`mapped=2,unmapped=0,ambiguous=0`，`git diff --check`通过 | zero findings; design merge-ready |
| R5 | 阶段粒度、successor schema时序、Selection当前流程和cutover前置 | 发现十个LR逐项PR过细、successor schema若留到最终阶段会阻断cutover、Selection提前解耦可能改变当前副作用、legacy owner归零检查过晚；已合并为四阶段/最多六PR，把additive schema/DEV移到PR-B2、产品编排暂存controller并把legacy owner zero提前到cutover前 | findings fixed |
| R6 | dormant product surface、现有正常LocalSIM连续性与安全边界 | 发现B2提前提交未注册router/UI/MCP会形成死产品线，现有账户迁移缺少资金/持仓/ledger连续性；已限制B2为内部command/query，新增`LegacyLocalSimAccountLineageV1`、economic hash unchanged、零in-flight和safe-boundary原子owner切换 | findings fixed |
| R7 | 旧包外部业务表面、反向依赖与最终门禁 | 发现Strategy Package paper admission、frontend paper-v2、MCP、scripts、backend lifecycle及Validation/CI/Nightly分类未被精确处置；已加入§5.3并固定`/simulation/localsim`唯一UI、共享组件迁移和外部旧命名空间删除。最终F2详细=`16/16,warnings=0`、父蓝图=`148/148,warnings=0`、guardrail=`0/0`、ownership=`2/2 mapped`、diff-check通过 | zero findings; revision merge-ready |

| R8 | Stage A 物理 owner、import、route、query/writer 边界 | 发现初版静态测试根路径少一层 `backend` 导致空跑；修正后扫描真实源码，并补齐 calendar/selection/minute content-addressed contract | findings fixed |
| R9 | immutable evidence 与 causal source semantics | 发现 Pydantic `frozen=True` 未深冻结 payload/weights/bars，且 UTC cutoff 被直接剥离时区；改为 JSON-safe 深冻结、batch hash readback、Asia/Shanghai 归一化和篡改反例 | findings fixed |
| R10 | F2 workflow、changed-files、ownership、direct no-drift | Feature Workflow 初次拒绝非标准 acceptance status，已改为 Stage A source-scoped standard completion evidence；F2=`16/16,warnings=0`，ownership=`115/115 mapped`，guardrail=`blocking=0`，最终直接矩阵全绿 | zero blocking findings; source PR-ready pending latest-main integration and CI |
| R11 | PR CI classifier 与真实 L2 计划执行 | CI 首轮发现 12 个跨模块/新测试缺少 direct-plan mapping；新增 `simulation.layer_foundation` catalog ownership，并让 `simulation_core_l2` 分两个 pytest 进程执行全部目标，避免同名测试模块 collection 冲突；catalog=0 findings、classifier=`unmapped=[]`、计划=`717+63 passed` | findings fixed; rerun CI required |
| R12 | superseded CI Paper 全矩阵 | 旧 head 在取消前发现 controller 过早构造 Paper service 破坏 Selection fail-fast，且 StrategyPackage 静态测试仍引用已删除 owner；改为 prepare 成功后懒构造并更新权威路径，精确 nodeids=`2 passed`，`paper_v2_backend`=`1129 passed, 3 skipped, 2 xfailed` | findings fixed; rerun CI required |
| R13 | 最终 CI DEV-DB 分类与跨模块范围 | 所有源码测试通过后，CI verdict 正确发现 PR 还改动 Advisory DEV onboarding 两个文件并要求外部 DEV DML lane。该模块不属于 Stage A；恢复其文件净差异，改以无业务逻辑、仅三个重导出的临时 import surface 保持调用合同，并增加 AST 断言禁止 class/def。该 surface 不是第二 owner，必须由`SIM-LR-D/PR-D1`删除 | findings fixed; final classifier must show `dev_db_required=false` and CI rerun green |
| R14 | PR-B1 owner真实性、反向依赖、单writer与projector边界 | 初版只迁移broker和事务wrapper，scheduler仍解释outbox并生成projection，且runtime models保留第二份dead definitions；逐轮迁移normal/first-causal/valuation/duplicate/recovery/persistence workflow并物理删除重复模型和scheduler内联writer逻辑 | findings fixed |
| R15 | PR-B1全量业务不漂移与失败语义 | 逐项复核TWAP、causal minute、cash competition、partial fill、T+1、limit/suspend、rollback、CAS/idempotency、commit-unknown/readback、KERNEL_V2；execution/lifecycle、Paper v2和MiniQMT广域矩阵全绿，source无Paper/Runtime反向import | zero local findings; ownership/feature validator/current-head CI required |
| R16 | PR-B1修订后最终本地门禁与四项设计符合性 | 第一轮F2校验发现F-137/F-138证据不够具体且阶段缺口未显式标记、Ruff发现三个迁移文件未格式化，均已修订；复跑详细设计=`16/16,warnings=0`、父蓝图=`148/148,warnings=0`、ownership=`7/7 mapped`、execution+StrategyPackage=`114 passed`、`simulation_core_l2`=`718 passed,2 skipped`加跨模块`63 passed,1 skipped`、L0/registry/diff全绿；未发现简化交付、silent fallback、业务语义漂移或新增审批门禁 | zero local findings; current-head CI required |
| R17 | PR-B1首轮current-head CI分类完整性 | Semgrep/CodeQL在安全扫描前共同fail closed，精确根因为Stage A catalog覆盖architecture/data/signal但漏掉同一模块新增的`backend/tests/simulation_execution/**`，导致四个真实执行层测试被分类为unmapped；补齐`simulation.layer_foundation`的唯一test ownership，不改pipeline、测试计划或业务实现，并要求classifier/catalog/current-head CI复跑 | finding fixed; current-head CI rerun required |
| R18 | PR-B2 account/release/binding与lineage真实性 | 初版缺account truth与原子事务；实现content-addressed account、existing immutable release/binding复用、CAS lifecycle和lineage。复审发现canonical hash幂等对象不应因created_at差异冲突，并发现lineage必须校验release/binding/economic digest且用legacy source进入account hash避免误复用普通账户，均已修复 | findings fixed |
| R19 | PR-B2 additive schema、DEV前置与bootstrap事务 | 首轮DEV正确发现既有`account_group_id/strategy_slot_id` migration未应用；加强只读preflight并先在DEV应用已提交前置。复审发现transaction-bearing migration不能原样嵌入rollback-only/autocommit bootstrap，改为从单一migration提取无`BEGIN/COMMIT/SET LOCAL`的DDL body；DEV migration双次、bootstrap和零残留回读全绿 | findings fixed |
| R20 | PR-B2 writer window、replay隔离与safe-boundary | 发现普通successor若不原子关闭旧open binding会被唯一索引正确拒绝；改为source hash/CAS关闭窗口后插入successor。historical replay binding固定在end date关闭，126交易日cursor可跨重启续跑，source/calendar/current-day mismatch fail closed，live successor与job pending在同一事务 | findings fixed |
| R21 | PR-B2 state/schema fail-closed完整性 | 增加小写SHA-256、replay cursor/failure/live字段状态一致性；runner绑定`simulation_daily_engine_v1`及historical source identity/hash；migration post-commit验证必需列、9个FK、唯一open-binding index与table comments，禁止同名空壳表冒充成功 | findings fixed |
| R22 | PR-B2本地fix-point与DESIGN-COMPLIANCE-001 | control=`9 passed`、replay/schema=`10 passed`、Simulation Runtime=`716 passed,3 skipped`、`simulation_core_l2`=`736 passed,3 skipped`加跨模块`63 passed,1 skipped`、DEV plan=`1 passed`、catalog/classifier=`86 passed`、ownership=`21/21`、classifier=`dev_db_required=true`、F2=`16/16,warnings=0`、L0无blocking；无route/UI/MCP、无production DDL/DML、无process control | zero local findings; current-head CI required |
| R23 | SIM-LR-C API authority与原子性设计差距 | 发现原设计只有endpoint名称，未冻结request/response/error/query；若直接绑定B2 service，客户端可上传权威hash/policy/inventory，且replay account bundle与job分事务会留下orphan。已新增§4.6.1-.3，固定server-resolved authority、四实体replay transaction、CAS/cursor/error合同和零manual tick | findings fixed |
| R24 | retained lineage、production时序与fail-closed激活 | 发现B2 migration只建schema，lineage preparation无受控产品前置；若要求production mapping先于C source merge则没有已合入工具可执行。已新增§4.6.4/.6：C同PR交付可复跑inventory/preflight/apply/readback工具和automatic readiness；source可先合入，但production DDL/lineage DML/readback严格先于用户重启，缺失时新mutation 503且旧route不存在 | findings fixed |
| R25 | frontend/MCP/script与跨模块边界 | 发现`paper-v2`前端命名空间同时承载Strategy Package、Selection、HMM、Advisory、MiniQMT，整目录提前删除会扩大业务语义。已冻结C只迁移LocalSIM pages/client/types并更新其caller，通用UI单份迁移；非LocalSIM产品仅改LocalSIM link，完整旧命名空间物理删除仍由D1 exact inventory执行 | findings fixed; final validators and zero-finding review pending |
| R26 | 现有产品能力与Selection provenance反向审核 | 发现旧UI具有bulk lifecycle，且Selection route/controller/link和Strategy Package `mark_paper_portfolio_created`仍把信号结果绑定旧portfolio；若只实现基础account API会丢功能/provenance并保留旧mutation caller。已补充全事务bulk CAS、neutral selection-account controller/link、旧无FK物理列的限期兼容边界与D2迁移责任，并删除第二份package-created truth | findings fixed; cross-module no-drift tests required |
| R27 | successor account到economic ledger/FK的可运行性反证 | 发现B1 scheduler仍读取Paper repository，且多个表FK继续指向`paper_v2.portfolio`；新account若无旧row会首笔run失败，若补旧row会形成第二account truth。已新增§4.5.1 `SimulationLedgerScopeV1`：全历史scope回填、runtime FK改指、retained复用、native原子scope、零economic row rewrite，并要求C composition移除Paper account truth读取 | findings fixed; migration/DEV and runtime direct tests required |
| R28 | ledger-scope migration最小业务影响复核 | 对8个base Paper FK和MiniQMT binding FK逐项分类后，确认successor runtime只需要重定向`run`与`intraday_snapshots`两个active-write FK；其余均属待退役session/config/reset或MiniQMT产品合同。已把migration缩到exact 2 FK，并要求另外约束逐字节不变及D2单独inventory，避免为LocalSIM切流改动MiniQMT/legacy只读schema | findings fixed; exact catalog tests required |
| R29 | ledger-scope identity与lineage replay审核 | 发现把legacy scope从`LEGACY_READ_ONLY`更新为`SUCCESSOR_RETAINED`会让immutable scope身份/哈希漂移。已固定scope只有`LEGACY_PORTFOLIO`/`SUCCESSOR_NATIVE`两种不可变来源；retained关系只由hashed lineage表达，scope/economic row均不更新；native source/account/scope三identity相等且唯一 | findings fixed; tamper/idempotency tests required |
| R30 | SIM-LR-C合同修订最终fix-point与DESIGN-COMPLIANCE-001 | 逐项反证server/client authority、普通/replay/bulk transaction、ledger FK、retained/native identity、Selection provenance、TWAP-only、旧route/script/MCP/lifecycle、MiniQMT不漂移和production/restart时序；未发现剩余dual truth、orphan、silent fallback、影子portfolio或新增人工gate。详细F2=`16/16,warnings=0`、父蓝图=`148/148,warnings=0`、L0=`0 findings/0 blocking`、ownership=`2/2`、diff-check通过 | zero findings; design revision merge-ready |
| R31 | runtime configuration authority可运行性反证 | C实现审计发现B2 release只保存profile version引用，而现有唯一profile/version表仍FK绑定旧Paper portfolio；native account既无法创建可修改配置，复用旧表又会恢复旧account truth。已新增§4.1.2 package-scoped neutral profile/version schema、append-only API与daily/HMM/risk/fee/runtime-variant严格边界 | findings fixed |
| R32 | component解析、TWAP/tail与客户端伪造审核 | 发现此前product request仍让客户端提交daily/tail component id，且admission receipt无独立authority。已收窄为runtime-profile+validated-execution-policy两个version输入；server从package admission summary派生receipt，从validated profile派生daily，从同package TWAP policy派生tail，hash/JSON/身份均不可由客户端覆盖 | findings fixed; final validator/no-drift review pending |
| R33 | runtime variant双配置truth审核 | 发现既有variant允许execution/risk字段，若profile只保存variant引用会与profile/TWAP policy形成运行时merge顺序。已限制可引用variant为同package validated且仅`strategy_config/portfolio_policy/notes`，创建profile version时物化并哈希；execution/minute/risk/core/HMM variant全部拒绝，runtime不动态merge | findings fixed |
| R34 | admission receipt持久可复核性 | 发现直接hash完整`paper_simulation_admission`会纳入可漂移warning/governance，且account只存receipt id不足以重算。已固定identity只含durable package/manifest/status-event/asset facts，并把bounded payload/hash冻结进release validation evidence；展示性诊断不进入identity | findings fixed; final validators pending |
| R35 | runtime-profile修订最终fix-point与四项符合性 | 逐项复核native account零Paper profile FK、profile/version append-only、alpha core不可变、HMM显式状态、variant物化无双merge、same-package TWAP/tail、durable admission receipt、retained release不被默认迁移及production/restart状态分离。详细F2=`16/16,warnings=0`、父蓝图=`148/148,warnings=0`、L0/changed-files=`0/0`、ownership=`2/2`、diff-check通过 | zero findings; design correction merge-ready |
| R36 | SIM-LR-C实现、CI dogfood与合入后状态复核 | PR #4035 同一source slice交付ledger-scope/profile/product authority、原子account/replay、readiness/preparation、统一router/frontend/MCP/scripts/lifecycle并删除旧产品mutation；审核修复runtime identity假收据、UI catalog、L4 evidence policy及三项独立CI缺陷。current-head后端/前端/Semgrep/CodeQL/workflow lane全绿，两个external DEV计划已有commit-bound收据；四项DESIGN-COMPLIANCE-001逐项通过。source merge=`f2eb8ed03`、cleanup完成；production/restart/runtime仍未执行 | source merged; production cutover preparation pending; no runtime completion claim |

## 15. 合入条件

本设计 PR 只有在以下条件同时满足时才可请求用户批准合入：

1. 父蓝图同步加入 `F-133..F-148`、`SIM-P-086`、详细设计引用和 Definition of Done；
2. 本文 Acceptance Matrix 对全部 16 项逐行闭合为设计可实施状态，不冒充 source/runtime 完成；
3. 至少两轮独立审核的 findings 均已修订，最终轮为 zero findings；
4. `python scripts/aistock_feature_workflow.py validate --design <本文> --tier F2` 通过；
5. 父蓝图 F2 validator 通过；
6. `git diff --check`、changed-file scope 和 Markdown UTF-8/LF 检查通过；
7. production DDL/DML、dependency、config、broker、runtime、client sync 和 process control 均明确为 `noop`。
