# AIstock LocalSIM 旧业务逻辑退役与三层隔离 F2 详细设计

> 文档状态：`implementation_plan_revision_ready_for_user_merge_approval`
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

### 4.6 Control-plane API/UI/MCP contract

新增统一 API 至少包括：

- `POST /api/v1/simulation-runtime/localsim/accounts`：原子创建 account + release + binding；
- `POST /api/v1/simulation-runtime/localsim/accounts/{id}/successor-releases`；
- `POST /api/v1/simulation-runtime/localsim/replays`；
- `POST /api/v1/simulation-runtime/localsim/replays/{id}/cancel`；
- `POST /api/v1/simulation-runtime/localsim/accounts/{id}/pause|resume|retire`；
- account/release/binding/replay/run/ledger 的 read-only GET。

创建请求失败必须整体零写或事务回滚，不得留下只有 portfolio、没有 release/binding 的孤儿记录。UI 不再创建 session 或主动 tick；MCP 只提供统一 read-only monitoring，不调用 readiness POST 或旧 scheduler status。产品router不再公开scheduler `start/stop/tick` mutation；scheduler进程内生命周期只由backend application lifecycle owner管理，保留read-only status/verification/diagnostics。

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
| `models.py` | 仍有效的 account/run/ledger query DTO | 各层 contracts；session DTO 删除 |
| `repository.py` | economic transaction/readback、portfolio query | economic repository + runtime repository/query service |
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
| `SIM-LR-C` Atomic Product Cutover | `LR-6` | 1 | 在successor schema生产readback和legacy product owner归零后，router/frontend/MCP/scripts/application lifecycle一次切换；删除旧mutation调用和公开scheduler mutation；fresh process只剩唯一LocalSIM product route/writer | 不保留dual route、translator、shadow writer、feature flag或长期410 façade |
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

退出条件：旧入口与新内部服务只汇合到同一economic coordinator；六个月replay使用独立account/binding/writer lock并可重启恢复；当前运行LocalSIM账户的ledger、资金、持仓、run和锁逐事实不变且lineage映射唯一；successor schema/mapping已有DEV receipt，生产应用/readback若未授权则明确为pending并阻断`SIM-LR-C`而不阻断PR-B2 source merge；仓库不存在提前暴露或无法验收的第二产品表面。

### 6.3 `SIM-LR-C`：Atomic Product Cutover

该阶段固定一个源 PR，禁止拆成“先加新route、以后再删旧route”：

- 前置只读inventory必须证明旧Paper session/auto-run/sentinel writer、旧scheduler active owner和未归属legacy process reference均为零；当前由统一Simulation Runtime持有的LocalSIM binding/run不属于legacy owner，必须通过`LegacyLocalSimAccountLineageV1`、新repository identity和economic hash/readback保持连续；
- production successor schema及保留账户lineage mapping必须已按精确授权应用并readback，account/release/binding/replay transaction smoke通过；
- cutover command只在non-trading safe boundary、零in-flight economic transaction执行；若条件未满足保持`ACTIVATION_PENDING_SAFE_BOUNDARY`并自动重试，不启用新route、不部分切换owner；
- 注册统一 LocalSIM account/release/binding/replay/lifecycle/query API；
- frontend创建/配置/运行/回放页面和API一次改用统一control plane，不再创建session或主动tick；
- MCP只保留统一read-only monitoring；移除Paper session/scheduler monitoring与readiness POST；
- scripts改用统一API/repository，`backend/main.py`删除旧Paper scheduler shutdown hook；
- 删除Paper `run-day/replay/session/tick/auto-run/coldstart` mutation route以及Simulation Runtime公开`scheduler start/stop/tick` mutation；
- 新frontend产品route固定为`/simulation/localsim`；通用视觉组件迁往共享UI owner，旧`/paper-v2`business route/API/types不保留redirect、410或compatibility import；
- fresh-process OpenAPI、frontend route/build manifest、MCP registry、script entry、import graph和writer inventory必须同时证明唯一route/owner。

合入后状态必须明确为`source_merged_pending_user_restart`。用户重启和只读route/runtime identity验证通过前，不进入物理删除阶段；失败只在新路径forward-fix，不恢复旧route。

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
| `F-138` | LocalSimEconomicCoordinator 是 state/order/fill/cash/position/mark/receipt/outbox 唯一 writer，projector/read API 无 broker/signal/第二写路径 |
| `F-139` | StrategyPackage 仅冻结 alpha/model/factor；日频/HMM/risk/fee/tail/runtime variant 与 LocalSIM TWAP snapshot 通过 immutable successor release 配置；requested policy仅进audit metadata，runtime不读manifest policy或硬编码覆盖 |
| `F-140` | `SimulationAccountV1`与统一control-plane原子创建account+release+binding并提供CAS lifecycle/query API；失败零orphan/双快照，UI/MCP不再创建session或主动tick，产品router不公开scheduler start/stop/tick mutation |
| `F-141` | historical replay 使用独立 account/binding/job 和同一日级 engine；六个月追赶、restart resume、safe-boundary live successor、当前运行账户隔离完整 |
| `F-142` | 产品切流一次完成，新旧路径无生产双写/shadow/translator/fallback；现有正常LocalSIM通过唯一lineage在安全边界延续且经济事实hash不变；cutover后旧mutation route/caller不存在 |
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
| `F-134` | §3.1–§3.3、`SIM-LR-A/B` | target `backend/tests/simulation_architecture/test_layer_import_boundaries.py` | design_ready | explicitly approved design-only stage; implementation evidence pending staged PRs |
| `F-135` | §4.2、`SIM-LR-A` | target `backend/tests/simulation_data/test_query_budget.py` and `backend/tests/simulation_data/test_tdx_causal_minute.py` | design_ready | explicitly approved design-only stage; source/runtime evidence pending |
| `F-136` | §4.3、`SIM-LR-A` | target `backend/tests/simulation_signal/test_target_rebalance_isolation.py` | design_ready | explicitly approved design-only stage; source evidence pending |
| `F-137` | §4.4、`SIM-LR-B/PR-B1` | target `backend/tests/simulation_execution/test_localsim_runtime.py` and `backend/tests/simulation_execution/test_twap_policy_authority.py` | design_ready | explicitly approved design-only stage; source/runtime evidence pending |
| `F-138` | §4.5、`SIM-LR-B/PR-B1` | target `backend/tests/simulation_execution/test_localsim_economic_transaction.py` and `backend/tests/simulation_execution/test_localsim_projection.py` | design_ready | explicitly approved design-only stage; DEV/restart evidence pending |
| `F-139` | §4.1、`SIM-LR-B/PR-B2` | target `backend/tests/simulation_runtime/test_localsim_control_plane.py` | design_ready | explicitly approved design-only stage; successor release implementation pending |
| `F-140` | §4.6、`SIM-LR-B/PR-B2`、`SIM-LR-C` | target `backend/tests/simulation_runtime/test_localsim_control_plane.py` and `frontend/tests/paper-v2/localsim-unified-control-plane.spec.ts` | design_ready | explicitly approved design-only stage; API/UI evidence pending |
| `F-141` | §4.7、`SIM-LR-B/PR-B2` | target `backend/tests/simulation_runtime/test_localsim_replay_live_transition.py` | design_ready | explicitly approved design-only stage; six-month replay/runtime evidence pending |
| `F-142` | §4.8、§4.8.1、`SIM-LR-C` | target `backend/tests/simulation_architecture/test_localsim_route_uniqueness.py` and lineage economic-hash unchanged receipt | design_ready | explicitly approved design-only stage; cutover/current-account continuation evidence pending |
| `F-143` | §5、`SIM-LR-D/PR-D1` | target `backend/tests/simulation_architecture/test_legacy_localsim_absence.py` | design_ready | explicitly approved design-only stage; physical deletion occurs only after replacement evidence |
| `F-144` | §4.8、`SIM-LR-D/PR-D2` | target `backend/tests/simulation_runtime/test_localsim_legacy_schema_retirement_postgres.py` | design_ready | explicitly approved design-only stage; production DDL/DML require separate authorization |
| `F-145` | §7.1–§7.5 | target `backend/tests/simulation_architecture/test_layer_import_boundaries.py` and `backend/tests/simulation_data/test_query_budget.py` | design_ready | explicitly approved design-only stage; CI/nightly plans pending implementation |
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

## 15. 合入条件

本设计 PR 只有在以下条件同时满足时才可请求用户批准合入：

1. 父蓝图同步加入 `F-133..F-148`、`SIM-P-086`、详细设计引用和 Definition of Done；
2. 本文 Acceptance Matrix 对全部 16 项逐行闭合为设计可实施状态，不冒充 source/runtime 完成；
3. 至少两轮独立审核的 findings 均已修订，最终轮为 zero findings；
4. `python scripts/aistock_feature_workflow.py validate --design <本文> --tier F2` 通过；
5. 父蓝图 F2 validator 通过；
6. `git diff --check`、changed-file scope 和 Markdown UTF-8/LF 检查通过；
7. production DDL/DML、dependency、config、broker、runtime、client sync 和 process control 均明确为 `noop`。
