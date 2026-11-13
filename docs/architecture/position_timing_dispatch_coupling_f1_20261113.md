# C2机会到派单耦合：父计划独立操作时钟机制对照

日期：2026-11-13。Owner：position_timing；账户执行owner：LocalSIM。Feature tier：F1。
状态：TIMING_RESEARCH_INTERFACE_IMPLEMENTED_LOCALSIM_NOT_STARTED；设计已由#7237合入，择时侧研究plan/inspect及两臂分析适配已实现，本轮来源验收、CI和源码交付身份由末节回执记录。LocalSIM producer适配、新账户和正式收益结果仍未开始。设计或软件验收不等于账户完成或经济确认。

## Background

终极目标仍是明确买卖计划下改善成本后结果和回撤，而不是增加模型或基础设施。[上轮机制诊断](position_timing_adverse_wait_p2_f1_20261109.md)发现C2的69个预算前机会未转成15交易分钟前派单；C2/W15的期初共同持仓数量全部不同，不能从账户收益差识别纯时点优势。上述观察不证明每次未成交都由队列/TTL造成。

本设计只回答：**在相同C2规则和同一经济期初下，去掉不同父计划之间的操作员容量竞争，完整账户结果与机会兑现率怎样变化？** 这是一个容量机制消融，不是新增择时模型、收益Oracle或可实现人工收益上限；更早执行也可能亏得更多。

## Scope / Non-goals

首轮设计只写本文、日内F2蓝图及P2 F1的历史衔接，证据保留在`X:/AIstock_temp/position-timing-dispatch-coupling-design-20261113/`。本轮允许写入仅为`backend/services/position_timing/intraday/dispatch_coupling_research.py`、同目录`dispatch_coupling_results.py`、`backend/tests/position_timing/test_intraday_dispatch_coupling.py`、本文及日内F2蓝图；本轮机器证据和冻结计划仅在`X:/AIstock_temp/position-timing-dispatch-capacity-20261113/`。不修改其他模块或旧实现closure，不重跑账户、训练/推理、扫参、查数据库、补数据、改active/profile或控制服务。后端重启始终归用户，本轮无需重启。

后续只新增一个研究臂C2L的17账户日，原C2的17日只读复用，不扩为四规则×多延迟矩阵。B0/W15/P2、原13户、288案例及1—9月产物保留，不参与新增显著性比较。本设计不授权择时窗口修改LocalSIM；其owner按本合同做最小离线接缝，不扩展生产配置、策略包或荐股界面。

## Contracts

### 1. 权威输入与冻结人口

首轮设计审计源码基线`76b050e28941d7dda27d9bc31db3c12ca042a5a4`。已有诊断交付回执`X:/AIstock_temp/position-timing-c2-w15-mechanism-20261113/source_delivery_receipt_r1.json`，完整SHA `48a3a62323382bcade17eafe641bb5b8db00d69c77d22de3ae53b14515a9d4d0`。它及其中FileRef为路径/身份权威，不从账户名或文件夹猜来源；首轮设计只核对元数据，没有重跑该诊断的11,150项来源preflight。本轮接口复用原四户完整来源预检，计划closure另加入baseline spec及producer contract引用，共11,146份；用途不同，不以与旧诊断总数相同作为完整性判据。

| 冻结对象 | 完整SHA256 / 身份 |
|---|---|
| 原四户analysis spec | `9b623ffcf15a8635c9741b0f6b0c98187b90b4e1cc145c91685ae80121db2a66` |
| 原四户facts | `81062290ca45bfb7ebb794518448ec351e1b829f8a6eafc031b571ea9c71b2c6` |
| 原producer request | `d25efd225b5f5260cbd213563fb51203de300496bea226203fcf2aaa0e78856b` |
| October candidate manifest | `2242c44261dc0eaaf54cda3416a260cf6712ec960cff8bfbb46a527f6587b958` |
| selection inventory | `d55b4fff01b79f55c223e35b6f185200410290ce9921468f68b90b6aada6b6fc` |
| 共享price manifest / templates | `842cf72d760d3ca1512b2bf6f749a58e76ed4087bec0d097e1b773c3c85d6639` / `5a080214d341996031bd8d1d9636ef3abf314acf48f0b9894d070577db4cda1a` |
| 研究price policy / cost policy | `d50e19ebf66688272f5c0663910e6e46c4058aa782af0fd78741892e9d2d7336` / `d9f8c97ea4593206b0509fcfeb5046a20925c84c618a9a3d3061f7945d9f3041` |
| C2基准account / opening | `lsnatural_918ee66460fb7689bff74509891690c8c293712d` / `4e6c1704dad9784b390b85af93086557314bee199422f89f6ef6e9d2a3786eef` |

日期严格为2026-10-{08,09,12,13,14,15,16,19,20,21,22,23,26,27,28,29,30}，17交易日、850个原生目标，不重选股票/权重。包为`pkg_c8d3d51fb6d24be89cfb3a50c30c80e4`，manifest SHA `5de2ec91c088e5cf9a9cc25673498a4165857b761cb9e4a4456151a1e04f0dc9`。C2基准的opening和有序17个day FileRef直接从上述facts的唯一`arm_id=C2`读取。

price builder按“期初持仓＋截至D已出现目标”覆盖SELL（`price_plan_campaign.py::_build`），理论上可覆盖同人口新路径；后续prepare仍逐日核验C2L所需并集，不冒称本设计已验证新账户持仓。合法NONE/UNKNOWN口径不变，不补造区间或删股票；真缺口交数据owner，不转实时行情或数据库。后续仅消费已交付的本地冻结来源，完整可用性由实际source preflight检查，研究不等待未来交易日。

### 2. 两臂、一个变化

研究身份`C2_PARENT_LANE_CAPACITY_PROBE_V1`；基准臂C2保持`ACCOUNT_SINGLE_OPERATOR`，新增臂C2L为`PARENT_PLAN_INDEPENDENT_OPERATORS`。**唯一经济机制变化是操作员容量时钟的作用域：账户共享→各父计划独立。** C2L仍只有一个共享现金/持仓账本，不为每只股票分配一份资金。

底层C2 policy保持`BOUNDED_REVERSION_V1`、`source_commit=70e459c569020f09880d91c1f16d43e9e5d951f3`，不能换成本轮producer commit。完整快照为schema `position_timing_intraday_policy_v1`、algorithm_version `20261023.v1`、allow_split=false、mean_bars=5、wait_trading_minutes=15、cooldown_trading_minutes=5、max_batches=3、decision_ttl_seconds=90、closure_delay_seconds=5、max_lag_slots=1、session_version `CN_A_CONTINUOUS_WINDOW_V1`；canonical policy SHA `8e456686452cc2e53b0e922cf915b53308e1b40f17358c2fff41b656228d8e79`。语义来源为`timing_campaign.build_timing_plan_events`和`intraday/contracts.py::Policy`；prepare须与基准编译身份核对，不依赖未来共享默认值。

原human policy完整参数取自C2 opening：reaction=60秒、minimum_action_interval=60秒、maximum_actions_per_dispatch=1、CANCEL_THEN_SELL_THEN_BUY_READY_FIFO、不拆单、不自动改价、未知账户不下单、无随机响应。上述数值不变，但**60秒一次/一次一个仅对每条父计划lane成立，C2L全账户可在同一时刻处理多个不同父计划**；不可声称全账户容量参数仍完全不变或这是实际单人操作能力。

原`execution_contract.human.capacity=ACCOUNT_SINGLE_OPERATOR`不能原样用来证明C2L同契约。新研究封套必须显式绑定base execution contract及本容量overlay，生成新的execution/operation identity；原生分钟撮合、strict-after、可见性、费用及终点子契约逐项相同。不得把新模式塞入旧`HumanOperationPolicy.operator_scope`的ACCOUNT Literal，或扩大生产配置枚举。新账户/plan/opportunity ID允许因自有研究身份而不同，不伪造与旧C2字节一致。

### 3. 调度、账户与因果约束

- lane键为`(account_id, parent_plan_id)`，不含revision/opportunity_id，避免修订/新机会获得免费的第二个操作员。每lane至多一个inflight，跨修订继续保留last_action_at；原撤单确认、反应延迟和冷却约束不跳过。
- 优先复用现有`HumanOperationQueue`逻辑的有界实例或等价薄研究适配，不复制队列/核心算法、不建线程池/多操作员平台。“并行”仅指模拟容量拓扑，不是线程/进程并发。实例数只随本日原生父计划数增长，日终释放，跨日仅承接原账户状态；新模式不得冒用旧“账户单队列snapshot”schema。
- 每个原有决策时刻按当前可见输入评估；同一时刻有多个可claim lane时，仍按CANCEL、SELL、BUY，再visible_at、symbol、action_id确定顺序，一lane一次claim/ack后才继续。同钟每lane至多claim一次，循环次数不超过本日lane数；被拒或NO_ORDER的尝试仍消耗该lane的60秒容量。同一时钟只推进一次行情/原订单撮合，不为多claim重跑同一bar。
- 不强制ready即下单。仅在交易时段、计划及机会仍有效、lane容量可用时派单；现有机会续期与派单前复核保持。withdrawn/expired/已派单机会不复活，不重置首次visible/ready或放宽90秒TTL。代码依据`human_operation.py::enqueue/revalidate_opportunity/claim`、`human_portfolio.py`的派单前检查；这些能力已经存在，不另研发“重确认系统”。
- C2L每次决策只用本户当时可见的ExecutionView与确认反馈；不借用旧C2 fills/持仓/事后first-last结果推进新路径，不把未确认反馈提前展示。全体lane共用同一原生现金、持仓和资金预留约束；每次派单使用原生账户的即时预留/数量校验，不能将同钟多次claim视为在同一旧余额上各自通过。新订单仍不能使用当钟刚完成但确认尚不可见的卖出所得；多个BUY不能重复花同一现金。
- 执行仍严格晚于派单的下一合格分钟bar open，原停牌、方向性涨跌停、T+1、费用、成交量/数量约束及14:57终止语义不变；不以提醒或派单视为成交，不读取当刻未知bar/未来价格选调度顺序。

C2L从C2的真实9月末经济期初克隆：现金、持仓数量/成本/可卖日、marks、可用数量时点、原始1000万元及累计费用/NAV高水位/MDD锚点相同。只改变clone-owned身份和本研究封套，不充值、不把NAV重置1000万元。之后各户自然复投并承接自己的期末；目标股票/权重相同，不强制后续数量或账户反馈相同。原C2 opening/day及旧账户一律不覆盖、重算或改名。

恢复单位仍为`ATOMIC_COMPLETED_DAY`，不新增盘中checkpoint服务：已完成日只读复用；未提交日失败后在新研究的隔离临时状态重演，不能把半日状态重复记账。声明的artifact exact retry针对已完成产物，不能把未完成日的真实重演也报告为零重算。lane状态只需满足既有日内确定性/快照核验，不要求开发新跨日操作员状态系统。

### 4. 最小生产者/消费者接缝及owner边界

| Owner | 后续最小交付 | 禁止越界 |
|---|---|---|
| position_timing | 自有研究plan/只读inspect、两臂source adapter及分析；复用FileRef/freeze、CandidateReader和`period_account_rows`/方向诊断 | 不修改LocalSIM、策略包、QE、Advisory、数据准备或共享默认值 |
| LocalSIM | opt-in容量overlay，旧默认仍账户单操作员；新增C2L 17日及原生账户inspect/回执 | 不重放旧C2、原四户68日、1—9月、13户、288案例，不复制账本/撮合或切生产配置 |
| 数据owner | 仅处理实际证据化来源缺口 | 本轮无数据需求，不把未来行情/新数据版本设为前置条件 |

plan schema `position_timing_dispatch_capacity_plan_v1`冻结本设计批准commit `59b1b7ea0edeff932d8a85fcebdb0917871d52db`/文档Git blob `3890ced18ab30a1982a6bb4c795232be7b52e5e5`、实现来源commit、完整FileRef closure、baseline C2映射与opening、17日期/850目标、底层policy快照/hash、base execution及capacity overlay、费用和price identity、唯一比较/统计配置；按canonical内容生成身份，自有不可变路径，不改旧registry/产物。进度说明以后更新不回写该已批准合同身份。

研究模块为`backend/services/position_timing/intraday/dispatch_coupling_research.py`，只读SDK `inspect_dispatch_capacity_plan(FileRef)`在完整来源验证后返回上述合同，无账户执行或新经济读取。LocalSIM消费入口由其owner明确后，只精确登记该文件路径及该只读成员，不开放整模块/内部辅助函数；本轮未预置未知消费者例外。新producer request `localsim_dispatch_capacity_request_v1`绑定plan、新producer commit/实现closure和C2L新账户；新facts `position_timing_dispatch_capacity_facts_v1`精确包含C2旧account/opening/有序17日引用及C2L新opening/17日、request、完整source inspection、artifact retry FileRef。

新day需明确capacity control/hash、有效lane作用域及真实队列状态身份。供旧纯统计函数消费的有界投影只映射NAV、parents、actions、费用等明确经济字段；不能将并行lane状态伪装成可被旧`HumanOperationQueue.restore`认证的账户单队列。原`natural_period_research._fixed_scope`及`NaturalFacts`固定四户，**不能直接把C2L追加给旧四户CLI**。后续用薄适配复用统计，不改旧四户解析器/implementation closure；实现前不得发出虚构消费命令。

#### 4.1 本轮择时交付的最小机器接缝

`dispatch_coupling_research`提供`prepare`与只读`inspect_dispatch_capacity_plan(FileRef)`；prepare spec只引用原四户analysis spec，不从目录搜索新来源。真实入口严格冻结上述17日及身份，不提供放宽范围的CLI。计划的`producer_contracts`以FileRef引用独立不可变文件，导出producer request、两臂facts、source inspection、artifact retry和analysis spec的JSON schema及opening/day/state的必要字段；不内嵌schema，避免其properties中的path/sha256被既有来源收集器误识别。LocalSIM可读取文件而不导入择时内部辅助函数。双方的开仓/逐日账户、现金与撮合仍由LocalSIM生成。

LocalSIM request使用`localsim_dispatch_capacity_request_v1`，绑定plan、producer commit/source identity、同一C2 common opening、新C2L opening、candidate/inventory/价集/成本、完整base execution和新capacity envelope。source identity为非空内容SHA树，不接受无哈希的标签。source inspection使用`localsim_dispatch_capacity_source_inspection_v1`，绑定request/plan、全部新day的有序identity、来源内容FileRef和旧默认串行行为回归证据；原生账本/匹配/费用子身份及代码差异明确披露，不能只写“金额相同”或要求所有聚合源码字节不变。

`legacy_serial_regression`引用生产者现有直接回归的紧凑JSON证据，至少含PASSED状态、producer commit、baseline request、baseline source identity与base execution的SHA、零旧账户重算及非空测试标识列表；不要求重演历史账户或新建测试平台。它属于producer-attested兼容性，不冒称择时独立重做账本。source inspection必须封印新request、opening和全部17日，以及day中的原生receipt/journal引用；消费方在完整来源预检后逐个确认引用均在已验证closure中。

新C2L opening使用`localsim_dispatch_capacity_opening_v1`，除新schema、arm/account及clone-owned portfolio ID、新operation envelope、plan引用外，逐字段保留C2经济opening和历史anchor，不丢额外marks。新day使用`localsim_dispatch_capacity_day_v1`，保留原账户统计字段；`human_operation_state`必须为独立的`localsim_parent_lane_operation_state_v1`，显式包含account/control identity、父计划lane及其action_id集合、真实actions和state hash，不冒充旧单队列快照。旧C2 day只读原schema。两臂分析仅使用这些明确字段；任何投影都不作为旧queue.restore输入。

`dispatch_coupling_results`提供`inspect/analyze`：完整source/hash和C2精确复用、共同opening、producer身份检查完成后才读取新day，随后复用`period_account_rows`、既有等待/方向诊断，新增唯一比较及冻结bootstrap。未来真实C2L结果未交付时，只验收消费者行为与真实来源prepare/inspect，不生成虚构正式收益。回执分别标识择时接口完成、LocalSIM接入/17日回放待完成和经济未确认；不将后两者作为本轮择时源码合入门槛。

这是离线研究预检接口，不是逐bar或逐父计划调用的运行态接口。完整内容校验安排在研究入口和发布前，不在每个分钟poll重复执行；执行循环继续复用既有有界Reader与当前可见前缀，不因此增加第二套缓存/读取器。producer request的identity按完整规范化模型payload（包含schema默认字段、排除identity本身）计算；文件SHA仍绑定实际全部文件字节，两者不可混用。

### 5. 来源、审计与不可变重试

后续prepare在任何新收益之前完整检查来源内容SHA、包/candidate/inventory、基准producer及C2 lineage/日期、底层policy、价集、成本和base execution子项；冻结新容量identity及对旧经济行为兼容的证据。run前再次验证输入，source/manifest/hash或前缀错误fail closed；发布前复验。源码聚合文件因显式注入变化不等于旧经济语义变化，不要求所有旧文件hash与新producer字节相等；也不能只比金额就宣称兼容。

复用原四户完整来源检查及其已批准的producer seal/source-pin supplement，不重新修补或覆盖旧raw inspection。基准price policy的历史可见时间假设仍如实披露，不冒称当时真实接收时刻；不为此次容量消融重建价集或改共享Reader。

复用旧父计划/动作/研究时间投影，报告全人口的首次机会、ready、派单、首成及各自覆盖；不得用中位数相减构造逐笔因果分解。新C2L可追加有界的关键动作状态转换摘要（QUEUED、DISPATCHING、COMPLETED、INVALIDATED，单动作最多4条），含原生observed_at、action/plan/revision/opportunity identity、当时decision身份/状态/有效期和reason；持续续期不展开逐poll日志，不建设通用事件平台。旧C2缺少失效转换时钟继续UNKNOWN，不以补齐旧完整生命周期为运行门槛；摘要缺失不得静默算作0或声称已识别失效原因。

软件/来源、C2L账户17/17完成、投影统计和经济意义分别报告。prepare→run→inspect→同输入artifact exact retry由相应owner完成；retry复用相同不可变request/报告/账户产物，账户重算和新增经济提交均0。producer经济重算未提供记NOT_SUPPLIED，不误判失败或自动重算。

### 6. 唯一比较与解释边界

唯一新增比较 **C2L−C2**。主报告成本后净PnL差及原始1000万元归一bps、同一opening的期间收益、完整分钟NAV正向期间/累计MDD、费用；累计指标继承共同C2历史锚点，不重写1—9月或均分/月平均MDD。全17日及完整BUY/SELL父计划分母、FILLED/PARTIAL/NO_FILL/UNAVAILABLE、取消/失效/派单失败全部保留；各臂父计划数量可自然不同，不以原587强行截断C2L。

机制报告机会→派单/首成转换、t0后不足15交易分钟内的机会/派单/首成计数、ready→派单与派单→首成各自原始样本/分母、同钟不同lane派单与lane内60秒间隔校验、费用/成交名义额、期初/逐日期末持仓路径。日终现金可统计；未发布完整盘中现金投影则时间加权暴露UNAVAILABLE，不用日终均值代替。新增最小状态摘要仅帮助解释本轮新臂，不为旧臂补造转换。

冻结5交易日循环区块、2000次、seed=20261113、family_size=1，报告17日配对每日净PnL差/原始资本的描述性95%区间；不是累计收益/MDD区间，不新增按方向/子集检验，不修改原研究三比较或共享BOOTSTRAP常量。MDE和样本功效仅报告，不作为开跑、完成或继续研发的门禁。正/负/跨零都保留，不因结果再改操作员数、反应/TTL、价格或均线阈值。

`quantity_matched=false; pure_timing_attribution=false; economic_confirmation=false`保持。该结果是给定冻结目标、资金和分钟模型下的**完整容量政策敏感性**，不是C2相对W15的新增确证、单人真实可行性、价格预测、日频择时对长期持有alpha或样本外收益；不自动替换线上规则。双边同量全成价差仅作有选择偏差的诊断。

## Implementation Plan

只设两个交付块：设计块已由#7237完成；当前连续研究块先交付择时plan/inspect/两臂消费者，再由LocalSIM owner接容量overlay并仅运行C2L 17日，最后交回择时完整分析/独立复核/exact retry。本轮源码验收限于完整的择时侧接口与消费者，不将未开始的LocalSIM接入或正式新收益标为完成。代码各归owner，不扩为模型/平台研发，不等未来自然日，不为缺少完整旧生命周期另加放行审批。

## Verification Plan / 可合入标准

初次设计验收由#7237完成。本轮择时侧直接测试覆盖冻结输入、独立capacity/execution identity、共同opening及marks完整保留、跨revision/NO_ORDER仍消耗lane容量、全部来源先于新day解析、发布前漂移、唯一比较/完整分母/MDD及artifact retry；合成投影测试不是LocalSIM账户回放验收。真实prepare/inspect/exact retry在WSL只读消费原冻结文件，不操作旧账户。以下调度和账户场景仍由LocalSIM实现后验收，不记为本轮已执行的队列/撮合测试：

- 两个不同plan同ready可同钟派单，同一plan及其新revision仍相隔至少60秒；CANCEL/SELL/BUY及同钟tie稳定，单bar不重复撮合。
- 仍需60秒反应；机会撤回/失效/修订、未来bar、过期窗口拒绝；续期不重置ready，不把旧终态复活；包含午休/终点。
- 多BUY共用一份现金/预留，不超现金/借用旧fills；原生T+1、停牌/方向性涨跌停、费用及原分钟成交规则保持。
- legacy默认串行路径逐经济字段一致；新C2L从C2 opening归一身份后经济字段相等且后续独立承接；新/旧产物namespace、source hash、前缀或baseline引用错误拒绝。
- 新并行状态不能冒充旧全账户queue restore；crash/retry不丢lane冷却/重复派单。来源先验、发布前漂移、完整17日统计、UNKNOWN覆盖及artifact retry零重算分别验证；广泛回归交CI。

设计校验使用`python scripts/aistock_feature_workflow.py validate --design docs/architecture/position_timing_dispatch_coupling_f1_20261113.md --tier F1`及日内F2校验；直接矩阵为`python -m pytest backend/tests/position_timing/test_intraday_dispatch_coupling.py backend/tests/position_timing/test_isolation.py -q`，完整模块回归交CI。首轮设计回执保留在原design目录；本轮实际源码/输入/测试/CI身份汇总到`X:/AIstock_temp/position-timing-dispatch-capacity-20261113/source_delivery_receipt_r1.json`。原设计回执不能替代新接口实现或账户运行证据。

## Risks / 失败模式

容量放宽可能更快亏损、改变成交数量和后续现金路径，故只报告完整政策敏感性，不称纯时点alpha或人工收益上限。同钟多claim若复用旧余额或重复推进bar会产生虚假收益，须保持原生共享预留、确认可见时间和每钟一次撮合。旧C2缺完整状态转换只能UNKNOWN，不能倒推原因；17日且事后选定机制的区间只能描述，不能晋升生产。以上均由既有来源校验、直接行为验收和报告边界处理，不另建风险平台或审批流。

## Production Gates / 不涉及运行态变更

本轮为纯离线择时研究接口，`database_read/write=false; network_market_access=false; runtime_action_performed=false; account_replay=false`；Git/CI网络与行情访问分别报告。无DDL/DML、依赖安装、active/profile切换、后端重启或运行态激活步骤，LocalSIM未来接入和C2L经济评价仍为未开始。纯离线实现不等待重启、未来交易日或经济正结果；失败的来源完整性不得放宽，但不加人工审批或功效准入门禁。

## Design Acceptance Index

以下ID仅属本文。DESIGN_VERIFIED仍只说明设计闭合；本轮择时侧接口/消费者的实现证据单列在矩阵，不能把它解释为LocalSIM容量overlay、账户调度或17日经济研究已完成。尤其下面的合成lane审计只验证消费约束，不替代真实队列/资金/撮合测试。

| design_item | 要求 |
|---|---|
| F-601 | 固定输入、C2期初/17日人口、旧资产只读及来源先于收益 |
| F-602 | 单一容量作用域变量、底层policy/费用不变及新契约身份诚实 |
| F-603 | lane跨revision、共享资金、因果前缀/机会保护与确定性调度 |
| F-604 | 明确owner/拟议SDK与两臂接缝，不冒称旧四户CLI兼容 |
| F-605 | 有界投影、UNKNOWN及不可变retry，不建事件平台或新门禁 |
| F-606 | 唯一比较、完整分母/分钟MDD和非alpha解释，无收益回选 |
| F-607 | 两个交付块、后续直接验收场景与软件/账户/经济状态分离 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-601 | dispatch_coupling_research._baseline/prepare/inspect_dispatch_capacity_plan；dispatch_coupling_results.source_preflight | backend/tests/position_timing/test_intraday_dispatch_coupling.py::test_every_day_is_checked_before_any_new_outcome_read；冻结source/身份与日序列先验 | DESIGN_VERIFIED | none |
| F-602 | dispatch_coupling_research.frozen_policy/capacity_control/_body | backend/tests/position_timing/test_intraday_dispatch_coupling.py::test_plan_public_inspect_retry_and_frozen_scope；既有规则和新封套分开绑定 | DESIGN_VERIFIED | none |
| F-603 | 本文Contracts-3；dispatch_coupling_results._lane_audit；LocalSIM执行待其owner实现 | backend/tests/position_timing/test_intraday_dispatch_coupling.py::test_revision_cannot_create_a_second_free_lane；同文件test_one_lane_cannot_have_two_inflight_actions；只证明消费方拒绝违约状态，不是账户执行通过 | DESIGN_VERIFIED | none |
| F-604 | dispatch_coupling_research的公共SDK和producer_contracts FileRef；两臂CapacityFacts | backend/tests/position_timing/test_intraday_dispatch_coupling.py::test_plan_public_inspect_retry_and_frozen_scope；backend/tests/position_timing/test_isolation.py；未知LocalSIM入口未放行 | DESIGN_VERIFIED | none |
| F-605 | dispatch_coupling_results.source_preflight/_lane_audit/analyze | backend/tests/position_timing/test_intraday_dispatch_coupling.py::test_source_clone_lane_and_publication_fail_closed；有界摘要/UNKNOWN及发布前漂移/不可变retry | DESIGN_VERIFIED | none |
| F-606 | dispatch_coupling_results.compare/paired_uncertainty/analyze；复用period_account_rows及side_panel | backend/tests/position_timing/test_intraday_dispatch_coupling.py::test_capital_mdd_complete_denominators_and_artifact_retry；非正式新账户收益证据 | DESIGN_VERIFIED | none |
| F-607 | 本文Implementation Plan/Verification Plan及本轮交付状态 | validation-receipt:X:/AIstock_temp/position-timing-dispatch-coupling-design-20261113/design_validation_receipt_r1.json；保留原设计证据，本轮源码/WSL/CI另绑定新交付回执，LocalSIM/经济未完成 | DESIGN_VERIFIED | none |

## 2026-11-13：择时侧接口、冻结输入与交接

研究接口实现commit为`30a2875e878f445df49fdab0e1e0ff5b6af77826`。研究源码在原工作树封印后执行WSL prepare/inspect/exact retry；消费者自审额外修复单lane多inflight状态接纳，使用独立delivery工作树，不改正在验收的研究源码。该修复不改变研究接口、policy/plan identity或producer合同。最终消费者/文档commit、PR/CI/合入、WSL执行证据和完整SHA由`X:/AIstock_temp/position-timing-dispatch-capacity-20261113/source_delivery_receipt_r1.json`记录；不得仅从源码commit推定交付验证通过。

冻结计划Windows路径为`X:/AIstock_temp/position-timing-dispatch-capacity-20261113/artifacts/intraday_v1/dispatch_capacity_plans/6e8f088698cef36007adbb21149e91877b33a7a2baa91910d98629548d17666a.json`，完整文件SHA `6f3549b3fe92c5e30aa4b732499391246de921594055b34193f473edecc52df6`。文件名为canonical研究identity，不是文件SHA；producer contract由plan.producer_contracts的FileRef读取，不能混用两种哈希。计划根下只有择时研究产物，未创建C2L账户、订单或正式收益报告。

WSL公共消费命令：`python -m backend.services.position_timing.intraday.dispatch_coupling_research inspect --input /mnt/x/AIstock_temp/position-timing-dispatch-capacity-20261113/artifacts/intraday_v1/dispatch_capacity_plans/6e8f088698cef36007adbb21149e91877b33a7a2baa91910d98629548d17666a.json --sha256 6f3549b3fe92c5e30aa4b732499391246de921594055b34193f473edecc52df6`。冻结来源使用WSL绝对路径，Windows独立回读只转换物理读路径核SHA，不改写plan引用或重绑profile。

未来LocalSIM从合同文件生成`CapacityProducerRequest/CapacityFacts/CapacitySourceInspection/CapacityArtifactRetry`和`CapacityAnalysisSpec`。实际C2L事实交回后，择时使用`python -m backend.services.position_timing.intraday.dispatch_coupling_results inspect|analyze --input <analysis_spec绝对路径> --sha256 <完整SHA> --timing-root <新的独立分析根>`；inspect不需要timing-root。当前没有可执行的LocalSIM账户命令；入口由owner明确并完成opt-in适配后再交接，不能把schema或构造器当成已接入、已回放或有收益优势。
