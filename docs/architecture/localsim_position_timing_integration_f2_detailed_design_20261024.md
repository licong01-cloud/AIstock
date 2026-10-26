# LocalSIM 分钟择时最小接入与冻结历史账户验证 F2 详细设计

日期：2026-10-24；状态：OFFLINE_VERIFIED_PENDING_FINAL_DELIVERY；Feature tier：F2。
源码基线：9c8db2c420875b1076ea8f2f5ba78e3a9c35e2a2。
上位：simulation_platform_unified_authoritative_blueprint_20260715.md、localsim_legacy_retirement_and_layer_isolation_f2_detailed_design_20260830.md。
消费合同：position_timing_intraday_advice_localsim_blueprint_f2_20261023.md、position_timing_intraday_phase1_f2_detailed_design_20261023.md、position_timing_intraday_research_f2_detailed_design_20261023.md。

## Background / Scope / Non-goals

现有 LocalSIM 的 planner/backend 显式拒绝非 TWAP。择时交接没有覆盖该限制；本任务按用户授权，在自身上位合同增加显式 opt-in 的 `localsim_position_timing_v1 / POSITION_TIMING_INTRADAY_V1`，旧 `localsim_twap_only_v1 / TWAP` 及其默认、参数、历史 release 和现有账户不变。
新策略只消费唯一纯函数 `backend.services.position_timing.intraday.evaluate`，不复制规则，不经提醒 HTTP 下单。不恢复 V25、不改选股、不训练、不连接 MiniQMT/Tick/实盘。

本次一套 LocalSIM 实现批量运行 288 个隔离离线 case，不创建 288 个生产账户、调度器或 UI。24 原计划各冻结一份账户，三个规则和四档延迟共用其初态；不同 case 不共享资金、持仓、订单、最低费用累计状态。四日不滚动复利。初始账户来源在收益读取前冻结；目标 1000 股不是资金或持仓来源。缺账户来源保持 pending，不以 fixture 或默认为正式账户结果。

2026-10-24 用户补充授权：全部新账户从 1000 万元现金开始；先完成上述固定研究，再选择最新加入的策略包，做三规则×四延迟的 12 个年初连续离线账户。现有 500 万元生产账户不修改。按 10-23 原生创建回执，选择 LightGBM＋12项因子（manifest `5de2ec91c088e5cf9a9cc25673498a4165857b761cb9e4a4456151a1e04f0dc9`），选择依据是加入顺序，不读取收益挑选。年初首个交易日从冻结 candidate calendar 验证；不得以日历日猜测。

固定研究 SELL 初态通过同 candidate 前一交易日、1000 万元起始现金、既有 LocalSIM 真实模拟买入形成；保存买入成交与费用实体，T+1 后冻结。准备费用单独披露，不混入比较日费用；不能直接把目标 1000 股写成期初持仓。连续账户从纯现金开始，之后持仓仅由真实模拟成交产生。

12 账户另用独立 campaign/source/plan 身份，不能冒充原 288 case 或改变原消费包。复用原生选股、目标和交易计划；必须有可读的冻结计划及合法价格偏好来源，不把参考价自创成 HARD/PREFERRED 区间。若原策略只有 TWAP 目标、没有 PRICE_TOUCH 所需偏好，输出准确的 owner 合同缺口；账户可以冻结初态，但不得伪造全年计划、联网补数或借生产数据库绕过本轮离线边界。

## Architecture

`LocalSimBackend` 持有既有 `OMS / InMemoryLedger / FeeModel / MinuteExecutionEngine`。新增 timing facade 只负责纯核心调用、待执行意图、后续开盘代理与确认反馈桥接；订单和账户计算继续调用上述既有实现。离线驱动直接消费冻结文件，文件 repository 仅实现既有 `LocalSimEconomicCoordinator` 的保存/事务/readback 接口，不重新实现账户或撮合计算。

新增策略身份只在显式新 policy/release 中选择。旧产品账户默认仍为 TWAP。本任务不更改已有 release，不自动修改生产配置。离线 plan-only 研究使用明确研究来源身份，不捏造 StrategyPackage/model/admission 资产。必要 constructor 接缝仅用于显式 timing 研究输入，不把缺 manifest 的旧入口降级为成功。

### 写入范围 / allowed_write_scope

以下为本任务唯一允许编辑的精确文件；扩展前先修改本表。其他模块只读。

- docs/architecture/localsim_position_timing_integration_f2_detailed_design_20261024.md
- docs/architecture/simulation_platform_unified_authoritative_blueprint_20260715.md
- docs/architecture/localsim_legacy_retirement_and_layer_isolation_f2_detailed_design_20260830.md
- docs/analysis/localsim_position_timing_integration_acceptance_20261024.md
- backend/services/simulation_execution/localsim/runtime.py
- backend/services/simulation_execution/localsim/planning.py
- backend/services/simulation_execution/localsim/timing.py
- backend/services/simulation_execution/localsim/timing_research.py
- backend/services/simulation_execution/localsim/timing_store.py
- backend/services/simulation_execution/localsim/timing_campaign.py
- backend/services/simulation_execution/localsim/timing_continuation.py
- backend/services/simulation_execution/localsim/timing_portfolio.py
- backend/tests/simulation_execution/test_localsim_position_timing.py
- backend/tests/simulation_execution/test_localsim_timing_campaign.py

不可修改 position_timing、StrategyPackage、Advisory、QE、HMM、数据准备或共享规则实现。若必要公共接缝无法在上述 owner 内闭合，记录准确缺口交回用户，不跨模块改写。

## Contracts

### 策略与因果

策略 snapshot 固定身份、规则 policy hash、delay、来源及运行模式；不读取 mutable active profile。facade 适配既有 `init_order/compute_step/is_complete`，其建议不改变 executed/reserved。只有经过 OMS/账本确认的成交或取消反馈推进状态；同反馈重试幂等，旧反馈、修订、撤销及到期不得恢复旧意图。

当时可见的完整分钟前缀才进入 evaluate；执行 bar 的后续 OHLCV 与信号分开。`NEXT_ELIGIBLE_BAR_OPEN / strict-after`：bar.start 必须严格晚于 advice_available_at + delay，并在计划、机会及交易窗口内。D0 仍有分钟网格等待，不是同 bar 成交或收益上界。成交使用 open，不能因 high/low 触价成交。volume 如用于事后区间容量代理，明确标识，不反馈为开盘前特征。

facade 复用 board-lot、risk、cash-fit/ledger、T+1 与父订单累计费。不得超过确认后剩余量；停牌、BUY 涨停、SELL 跌停、零量、价格限制、资金和可卖量逐项处理。旧 residual 不能进入新策略；新策略到期取消剩余，不能突破 HARD 或跨日执行。

分批依核心累计目标比例和确认 batch 推进；本研究严格 allow_split=false。部分成交不是批次完成；重试不重新建立父订单或重收最低额。

### 冻结研究与账户

研究 request/bundle：`1c684e78b94872878608cafc48a7a496540b9a9c8cdf62424d886154398d2062`。
manifest canonical SHA：`8b9128a33c15464c0784b953d658d0666fa2697608e46b073f3a3691ae81b9c1`。
candidate 完整文件 SHA：`f5af5cc0506e502300ef225805b51aaeb7ea1b1bc352fdc81f363f4933f5f8ed`。
candidate/费用/执行合同从 consumption.json 读取，不覆盖旧 artifact。先整批 source/bundle inspect，然后冻结账户，再运行账户计算；比较器读取收益前再次整批 inspect。

固定 000001.SZ/600000.SH/688981.SH，2026-09-21/22/23/24，BUY/SELL 各 1000 股，24×3×4=288。三规则 FIRST_OBSERVABLE_V1 / PRICE_TOUCH_V1 / BOUNDED_REVERSION_V1；延迟 0/60/180/300 秒，主口径 60 秒。不换人口、阈值、价格偏好或删除未成交。

账户输入为只读、完整文件 SHA 绑定的显式 JSON：按原 plan_id 提供 cash、positions（数量、available_quantity、原持有日期、avg_cost）、初始估值与来源回执引用/文件 SHA、目标金额估值依据。不能从未来收益决定资金或持仓。已知无可卖量为真实限制；未知账户为 UNAVAILABLE/pending，不能补造。所有规则/延迟的同计划账户 hash、NAV、target notional 和 14:57 终点必须完全相同。

费用绑定 consumption 的文件：买 0.000095、卖 0.000595、最低 5 元、PER_PARENT_ORDER_CUMULATIVE；不是个人净佣加规费。终值保留原现金和剩余持仓，使用相同分钟 raw 市价合同，不以成本净值或 15:00 价替代 14:57。MDD=`POSITIVE_PEAK_TO_TROUGH_FRACTION`，初始净值计入 peak；缺估值保留 UNAVAILABLE，不填零。

### 两类运行与结果

`FROZEN_OPPORTUNITY_DELAY_DIAGNOSTIC`：主研究使用已冻结机会流；整个 inspect 会调用原唯一 evaluate 重建并核对决策。实际账户反馈单独保存，不能改变冻结流，也不能把 HUMAN_UNKNOWN 当成账户状态。
`CONFIRMED_FEEDBACK_CLOSED_LOOP`：功能/边界验证实时把自身成交反馈送入 evaluate；与纯延迟主研究分别标识，不混报。

主比较是同延迟、同代理 matched B0；B0 已包含于 288 行，不加重复基线。native TWAP 可另作产品对照，不充当 matched B0。

结果严格采用现有 EconomicResults/EconomicRow，schema position_timing_localsim_research_results_v1。每 case 恰一行，FILLED/PARTIAL/NO_FILL/UNAVAILABLE，绑定 producer=LOCALSIM、实际源码 commit、release、study/cost/execution 身份、账户/终值/MDD/费用/等待/receipt hash。receipt 实体可读，保存真实订单/成交/资金/持仓和来源。全文件 SHA 与信封 canonical SHA 分开。

输出由 CLI 显式指定到 X 盘独立 LOCALSIM 研究目录，按 source/account/bundle/policy hash 派生不可变身份；Windows 临时文件仅 X 盘，WSL 直接访问现有 F/X 挂载，不复制大型数据。事务采用既有 coordinator，经 file repository 临时提交完整 generation 后 no-replace 发布。恢复读取已提交 hash 链；exact retry 回读相同结果或 ALREADY_MATERIALIZED，损坏不覆盖、不归零重跑。

## Implementation Plan

1. 冻结本设计/精确 scope 并修正自身上位合同。
2. 实现 LocalSIM facade、backend opt-in 接缝和文件 repository/离线 CLI。
3. 功能、因果、账户/费用及恢复定向测试；两轮以上独立主题审核和修复。
4. 账户来源闭合后，在 WSL 整批 inspect、冻结账户、288 case、比较器、exact retry；不等未来行情或重启。
5. F2、L0、scope/ownership、CI 通过后提交独立 PR，按已有授权合入并只清理本任务。

### 连续账户长任务执行计划（2026-10-24 r2）

用户授权按以下顺序完成；本节是本任务的持久进度表，不改变原 288 场景身份。

| 阶段 | 任务与完成标准 | 当前状态 |
|---|---|---|
| P1 | 核验最新 main、已有原生选股及择时合同；冻结价格偏好消费 schema，形成外部 owner 的精确输入要求 | 已完成：四个冻结文件SHA、原生双端inspect和审计UNKNOWN兼容通过 |
| P2 | 复用 TargetPositionEngine、RebalanceIntentService 和既有 LocalSIM 账本/OMS；实现多股票、跨日账户执行、共同计划和日事务恢复 | 实现及78个直接节点通过；实际948个账户日原生账本复核通过 |
| P3 | 合法价格计划返回后，冻结最新 LightGBM＋12 因子、12 个 1000 万元账户，运行 01-05 至 09-30 的连续研究 | 完成181日/2172账户日，9050个选股目标及原288不变；完整结果SHA acdea8ab... |
| P4 | 比较同延迟 B0、费用/最低额、收益/回撤/等待/未成交；验证完整人口、前缀恢复和 exact retry | 2172账户日独立原生账本/目标核验及36组比较通过；exact retry ALREADY_MATERIALIZED且结果SHA不变 |
| P5 | 多轮审核修复、直接回归、L0、F2 和最终 HEAD CI 全部通过；更新原 PR、合入、只清理本任务 | 源码CI1551通过/22跳过；完整设计证据13项齐全，最终F2/文档HEAD CI/合入与清理由交付收据分别记录 |

P1 缺口不得阻止 P2 中独立开发，但不得借合成功能测试宣称 P3 已完成。年初连续研究明确采用 `CONFIRMED_FEEDBACK_CLOSED_LOOP`，不是原 288 场景的冻结机会流纯延迟实验。日计划的数量和跨日持仓由 LocalSIM 负责；择时 owner 不负责创建账户、撮合、数量计算或资金管理。

#### 价格计划最小消费合同

价格偏好须由 owner 提供不可变文件及完整文件 SHA，明确生产者源码和政策身份、适用的策略包 manifest、candidate/PIT 身份、覆盖日期、来源可见时间、有效窗口和修订/撤销。文件只提供规则无关的 `PriceConstraint` 及版本事件（按日期、股票、BUY/SELL）；LocalSIM 再将同一模板绑定三规则及四延迟，不能由各规则分别挑选偏好。每个事件引用可回读的原始来源文件和 SHA，且来源可见时间不晚于事件可见时间。

当日目标需要 BUY/SELL 模板；可能仍被持有、但已离开当日目标的股票只需要 SELL 模板，不强制提供不会被消费的重新买入偏好。

原 `PREDECLARED_MECHANISM_PLAN_NOT_PRODUCTION` 的 0.5% PREFERRED 工程政策不得自动扩大人口。owner 可以交付正式可复用构造器及适用性合同，也可以直接交付完整冻结模板；LocalSIM 不复制该构造算法、不修改核心参数、不用参考价猜测偏好。

2026-10-24 用户授权消费 PR #6671 的冻结共享价格计划。模板完整 SHA 为 `bce2563f61ee2c3ee64b74d723dfac57baa95e127ed87e137229e86d16b327a1`，policy_document 为 `58e2fddf837f890e7eecadeef08e2907273f7edc3ab9759d62ae51e892e5b779`，其 reference_audit 为 `fe6b1fcb12ffb868a68e95f3e122bbe7969fed533a017afabd3ae23bd02bfde6`。消费侧从已绑定审计建立日期/股票/方向的 UNKNOWN 声明，不在代码写股票或包 ID 白名单。逐一核验类型化原因、来源路径及 SHA；未声明 NONE、来源冲突、AVAILABLE 缺区间、审计人口不一致均 fail closed。整批来源核验按不可变 FileRef 去重，结束时再次读回核验，不能用进程级业务缓存代替。

通用事件校验不再强制 PRICE_TOUCH；NONE 必须空区间，非 NONE 必须有合法区间。B0/C2 可正常消费已声明 NONE。C1 在实际原生意图需要该未知模板时保留 UNAVAILABLE 行、原生请求量/合法量、审计原因和来源实体，不创建非法 Plan、不发订单、不补造价格；仍计算原现金和剩余持仓终值。未使用的备用 SELL 不阻断账户日。本轮14条未知均为单修订 ACTIVE 模板；若未来 C1 的混合修订序列包含 UNKNOWN，需要显式新合同，当前保持失败关闭。估值缺口与计划价格缺口分别处理，不因接受 UNKNOWN 而放宽行情、估值或真实缺价校验。

#### 连续账户和比较合同

各规则/延迟共用同一初始账户、外生选股与价格模板、日历、费用和数据；全部从纯现金 1000 万元起步。连续账户成交分化后持仓、净值及再平衡数量可以分化，不能继续宣称逐日数量完全相同。报告这是连续组合闭环比较；原 288 个共同计划场景仍单独保留。

复用原生目标引擎及 rebalance 服务，不重新实现权重、lot 或可卖量规则。所有同一时点的意图按固定顺序执行，卖出优先、随后 symbol 排序；该顺序在全部账户一致，不用未来成交挑顺序。费用使用既有父订单累计模型。每日开盘前依原生 settle_trade_date 释放 T+1，14:57 撤销未完成意图；估值使用当时已可见的 raw 市价，不能用成本代价或缺价填零。

连续账户采用日原子提交：仅已提交日的订单/成交/现金/持仓和运行状态成为可恢复前缀；中断日无对外发布经济事实，从原提交前缀用相同冻结输入重算。日内部分成交及取消仍走 TimingExecution 和既有账本，不能以提醒数量代替成交。恢复校验 source、全部输入、政策、日期前缀和 receipt 链，不删除失败前缀、不跨源码继续未完成运行。

#### 复杂度及落盘审查（ALGO-COMPLEXITY-001）

原固定研究为288个独立场景，每个场景最多240根分钟线，按场景顺序执行，没有跨股票join。连续研究固定12账户、181日、每天50个目标，181日合计9050目标及615个不同股票；单日行情只装载当日目标与实际持仓的并集，12账户共用只读日行情。以D日、A账户、P日内父订单、C决策时点、B分钟前缀计，保守计算量为O(D×A×P×C×B)，不构造多日股票笛卡尔积；终态父订单不再调用核心。按日提交限制未提交状态，历史摘要和NAV轨迹仍随D增长。

组合日只保存一份账户快照和coordinator现金事实；父订单保留 `localsim_timing_parent_audit_v1`，不在每个父订单重复全组合持仓和现金明细，也不能用该审计片段恢复单账户。旧单计划完整receipt和恢复语义不变。四处P2复杂度提示已逐项审查；原79日/948账户日前缀至05-08阻断实测14,569.20秒、峰值1,585,472KiB、2,526,595,722字节。后继102日正式接续实测25,463.15秒、峰值2,470,624KiB；完整181日结果及36组经济比较已封存，最终run目录6,564,992,048字节。分阶段计时不冒充无中断全年单次基准；独立核验及exact retry性能见验收报告第11.5节。

## Verification Plan / Design Acceptance Index

### 2026-10-26：修复候选的受控接续

用户已授权核验新旧输入、保留兼容的948个已完成账户日并继续原12账户研究。数据窗口的BUG-2089/2090修复已正式激活，但原请求仍绑定不可变的旧candidate；禁止修改请求、账户、账本或断点，也不能用active自动替换。

最小增量登记为上述LocalSIM专属`timing_continuation.py`、原campaign入口、既有campaign定向测试及本任务设计/验收文档；不修改共享择时或数据代码。新增显式`adopt-successor`离线入口，在原run目录追加一次不可变`input_successor.json`。记录原request/旧candidate、新candidate完整FileRef、原价格计划身份、完整研究人口、费用/执行合同、接续源码身份、已提交前缀实体引用及检查证据。原请求和旧产物不重写；新账户日的身份及摘要显式携带后继身份。

采用同一新candidate供全体12账户的后续交易日读取；原共享价格模板仍保留原路径、政策及SHA。接续前必须证明本研究全部历史目标股票的日历、日线prev_close/上下限、分钟OHLCV/amount/factor组件pin及股票报价范围与旧candidate一致，原价格引用来源逐一等价；PIT组件不得发生迁移。其他证券的修复不能被误称为全数据集等价。新停牌组件由唯一共享原生projector解析，UNKNOWN/UNRESOLVED保持拒绝。

先完成原价格计划和新来源的整批inspect，再读取账户前缀。核验12账户有相同连续已提交日数、原生journal/summary/hash链、跨日资金持仓和T+1，并按每一天真实目标及此前真实持仓的并集，证明新旧停牌状态不改变任何已完成输入。任一已完成输入不等价则拒绝adopt，不删除或重做前缀；无合法后继合同则保持BLOCKED。不得把“新数据已激活”当成经济前缀等价证明。

接续仅允许campaign编排与新增接续适配器的审核变更；账户/撮合/费用、择时核心、策略数量和执行政策的冻结源码必须保持一致。新输入读取器及公共停牌authority的实际源码另行绑定。后继记录冻结后，未完成运行仍严格拒绝任何未登记源码或输入变化；恢复重读该记录而非active。后续日事务、成交、费用和账本继续调用原链。原288产物不变；完整181日及36组连续经济比较、exact retry未完成前，F-010/F-013不得声明PASS。

接续复杂度审查：固定615证券、181日、12账户；行情等价检查6152个日历/日线/分钟组件，另检查PIT组件及两份稀疏停牌事实。按组件流式hash，不构造证券×分钟×账户笛卡尔积；停牌表用证券/日期向量筛选，再交唯一projector。前缀遍历948个原生日journal，实际市场键6926个；按原生账本逐日重放确认事实，内存仅保留必要账户和引用。来源hash在前缀核验后再次检查，封存前缀引用以防检查期间漂移。恢复整批检查只做一遍，日内计算继续沿用原日行情共享与原生事务，不增加模型/数据复制。

| ID | 必须验收 |
|---|---|
| F-001 | 自身上位合同显式 opt-in，旧 TWAP/default/release/退役规则不变 |
| F-002 | 唯一 evaluate、三规则真实调用，无 HTTP/规则复制/第二账本 |
| F-003 | 前缀因果、strict-after、四延迟及执行 bar 特征隔离 |
| F-004 | 确认反馈、partial、取消/修订/到期、split 累计目标 |
| F-005 | cash/sellable/lot/T+1/limit/suspend/HARD 与 residual 不越权 |
| F-006 | 冻结原输入/账户/费用/估值、288 全人口、共同账户/终点 |
| F-007 | coordinator 文件保存、receipt 实体、恢复和 exact retry 无重复 |
| F-008 | EconomicResults 兼容、先整批 inspect 后 outcomes_read、matched B0 报告 |
| F-009 | 无 DB/数据集/行情网络/服务操作，交付与运行/经济证据分离 |
| F-010 | 最新加入策略绑定、12 个连续离线账户、1000 万元及年初日历；合法计划缺口不能伪造 |
| F-011 | 连续消费者的价格模板整批校验、原生数量规划、多股票共享账户、确认时钟、日事务恢复及独立比较 |
| F-012 | 审计 UNKNOWN/NONE 精确绑定；B0/C2 合法 NONE；C1 实需缺价保留 UNAVAILABLE；备用 SELL 不阻断且不改变人口 |
| F-013 | 不可变后继绑定；整批source先于outcomes；原生账本与实际输入前缀兼容；跨日接续及重试不重做旧经济事实 |

## Rollout / Rollback / Risks / Production Gates

新增 opt-in 源码不自动激活现有账户；离线 CLI 无需后端重启。未来选择该策略的生产模拟账户仍需正式新 release；本任务不启动账户回放，不操作数据库或用户进程。源码影响 backend 时合入后报告待用户重启，运行态验收独立。
回滚停止选择新策略，保留研究/receipt 和旧 TWAP；不覆盖历史输入或账本。冻结人口为工程机制样本，不外推全市场收益/统计显著性。
database_read/write=false；dataset_mutation=false；live_market_network=false；process_control=false；production_ddl_gate=noop。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | 两份自身上位设计；runtime.py/planning.py | pytest backend/tests/simulation_execution/test_twap_policy_authority.py -q；2项，旧账户默认未修改 | PASS | - |
| F-002 | localsim/timing.py | pytest backend/tests/simulation_execution/test_localsim_position_timing.py -q；三规则调用唯一evaluate | PASS | - |
| F-003 | localsim/timing.py/timing_portfolio.py | pytest backend/tests/simulation_execution/test_localsim_timing_campaign.py -q；未来隔离、strict-after、四延迟和确认时钟 | PASS | - |
| F-004 | localsim/timing.py | pytest backend/tests/simulation_execution/test_localsim_position_timing.py -q；部分成交、取消、修订和到期 | PASS | - |
| F-005 | localsim/timing.py及既有OMS/ledger/engine | pytest backend/tests/simulation_execution/test_localsim_position_timing.py -q；cash、可卖、T+1、lot、HARD及限停牌 | PASS | - |
| F-006 | localsim/timing_research.py | artifact: X:/AIstock_temp/localsim-position-timing/run-metrics-r1.json；288行，215 FILLED、73 NO_FILL，24共同初态 | PASS | - |
| F-007 | localsim/timing_store.py/timing_research.py | artifact: X:/AIstock_temp/localsim-position-timing/recovery-proof-r1.json；retry-metrics-r3.json：原288行SHA不变 | PASS | - |
| F-008 | localsim/timing_research.py及现有只读比较器 | artifact: X:/AIstock_temp/localsim-position-timing/compare-metrics-r1.json；36组matched B0，先inspect后读结果 | PASS | - |
| F-009 | WSL离线验证及审计钩子 | artifact: X:/AIstock_temp/localsim-position-timing/native-quantity-validation-r2.json；数据库/数据集/行情/服务操作均false | PASS | - |
| F-010 | localsim/timing_campaign.py；验收报告第11节 | artifact: X:/AIstock_temp/localsim-position-timing/continuous-validation-r16.json；181日/2172账户日、9050原生选股目标、36组比较；全部日独立核对通过 | PASS | - |
| F-011 | localsim/timing_campaign.py/timing_portfolio.py/runtime.py | artifact: X:/AIstock_temp/localsim-position-timing/direct-validation-r5.json；78个直接节点，12账户×2日恢复；real-prefix-validation-r6.json独立复核948日原生账本 | PASS | - |
| F-012 | localsim/timing_campaign.py/timing_portfolio.py | pytest backend/tests/simulation_execution/test_localsim_timing_campaign.py -q；artifact: X:/AIstock_temp/localsim-position-timing/shared-price-inspection-r4.json：真实181日/73997事件及14 UNKNOWN整批通过 | PASS | - |
| F-013 | localsim/timing_continuation.py/timing_campaign.py | artifact: X:/AIstock_temp/localsim-position-timing/offline-completion-r17.json；948日前缀保留、完整2172日核验、后继exact retry ALREADY_MATERIALIZED；结果SHA不变 | PASS | - |

F-011验收消费能力及功能不变量；F-010由181日真实研究单独验证，F-013由真实后继及exact retry验证，13项设计证据均齐全。源码、离线研究、生产运行激活与经济效果分别报告；年度三规则×四延迟结果不支持宣称择时稳定优于简单执行。最终F2门禁、文档HEAD CI、合入和清理由交付收据记录，不把离线通过当作生产激活。

2026-10-26最终离线验收：`continuous_results.json`完整SHA为`acdea8aba2f7ef3ad4ee26bc3ef4a2b0d389d303b62510b2f0d5d5e351d4f9ba`；年度模型和全部36行指标/状态/初始账户SHA见验收报告第11节。此后续结论覆盖本设计中先前的进展和历史阻断记录；不改写旧产物。原生trade/fee/raw mark模型未新增公司行为现金/数量事件，不据此声称真实全收益或统计超额收益。
