# P2：不利方向持续移动时提前结束等待

日期：2026-11-13。Owner：position_timing。Feature tier：F1。状态：C2_W15_NATURAL_MECHANISM_VERIFIED_EXPLORATORY_NO_PROMOTION；本轮Contracts-9诊断、独立复核与不可变重试已完成。

## Background

终极目标是改善明确BUY/SELL计划的成本后结果和回撤。既有181日三户研究中，C2相对B0净增584,422.39元、MDD降低2.8724个百分点；相对W15净增120,234.09元，但区块校正区间跨零且8月贡献超过全期。保留三者，不以收益回选月份或阈值。
初次P2只研究一个假设：价格已经连续朝本次交易的不利方向移动时，继续等固定15交易分钟可能增加机会成本。BUY的不利方向是上涨，SELL是下跌。该假设不承诺盈利，也不预测当日最高最低点。前轮权威结果与完整SHA见[同等待预算设计](position_timing_wait_budget_control_f1_20261106.md)末尾。2026-11-11已按Contracts-8.1消费四户10月事实；本轮按Contracts-9补充机制诊断，不新增策略假设。

## Scope

本设计初次交付研究专用纯函数、可恢复状态、冻结研究plan/只读inspect、定向测试和真实来源验收；LocalSIM owner随后在#7150完成新P2账户181日，择时已完成四户结果与机制诊断。#7174准备输入及#7187/#7191择时价集、软件与精确SDK登记已完成；#7192已交回四户10月68账户日，2026-11-11已完成期间/累计结果消费。当前执行Contracts-9的既有事实机制诊断，不运行账户、不等待未来行情；当前进度见文末2026-11-13记录。
纯函数交付允许写入以下7个文件；该次operator和不可变验收产物仅在`X:/AIstock_temp/position-timing-p2-adverse-wait-20261109/`：

- `backend/services/position_timing/intraday/adverse_wait_control.py`
- `backend/services/position_timing/intraday/adverse_wait_research.py`
- `backend/tests/position_timing/test_intraday_adverse_wait.py`
- `backend/tests/position_timing/test_isolation.py`
- `docs/architecture/position_timing_adverse_wait_p2_f1_20261109.md`
- `docs/architecture/position_timing_wait_budget_control_f1_20261106.md`
- `docs/architecture/position_timing_intraday_advice_localsim_blueprint_f2_20261023.md`

LocalSIM #7150交付后，追加结果消费范围为`backend/services/position_timing/intraday/adverse_wait_results.py`、`backend/tests/position_timing/test_intraday_adverse_results.py`及上述两份设计文档；本次operator和不可变分析证据仅在`X:/AIstock_temp/position-timing-p2-analysis-20261109/`。不修改原7个文件中的算法、来源冻结或统计实现，以免改变既有plan的implementation closure。
2026-11-09的机制诊断及下一轮设计只写`backend/services/position_timing/intraday/adverse_wait_diagnostics.py`、`backend/tests/position_timing/test_intraday_adverse_diagnostics.py`、本设计和日内上位蓝图；该次operator及不可变证据仅写`X:/AIstock_temp/position-timing-mechanism-october-20261109/`。不修改旧结果消费者、冻结来源、LocalSIM或任何其他业务模块。
复用core.evaluate、分钟/日级合同、CandidateReader、交易分钟时钟、freeze、原history来源预检；不修改旧core、W15、原统计函数或其冻结implementation closure，不另建Reader、撮合、账本或研究平台。

本次10月输入续接的允许写入范围为 `backend/services/position_timing/intraday/natural_period_context.py`、`natural_period_prices.py`、`natural_period_results.py`（均在同目录）、`backend/tests/position_timing/test_intraday_natural_period.py`、`backend/tests/position_timing/test_isolation.py`、本设计及上位日内蓝图。operator和不可变交付只写 `X:/AIstock_temp/position-timing-natural-october-consumer-20261109/`。不修改任何旧研究implementation closure或LocalSIM文件。

## Non-goals

不改线上PolicyId/API/UI或已有B0/W15/C2，不接QE/Advisory/HMM/MiniQMT，不训练/扫参，不用Oracle标签。新策略经济回放属于LocalSIM，本窗口不读生产数据库、不抓行情、不改数据集/active/profile、不控服务，不把软件或前缀机制检查称为P2盈利证据。

2026-11-13续接只消费既有四户10月事实，复用现有`natural_period_results.source_preflight`与`adverse_wait_diagnostics.side_panel/matched_panel`。允许写入仅为本设计、上位日内蓝图，以及`X:/AIstock_temp/position-timing-c2-w15-mechanism-20261113/`内的有界operator（`run_diagnostics.py`）、三个独立复核脚本及不可变证据；不新增常驻分析服务，不修改业务代码、旧implementation closure、测试或其他模块。

## Contracts

### 1. 唯一冻结规则

研究身份`ADVERSE_TREND_WAIT_15M_V1`，原始人民币分钟价。共享core FIRST_OBSERVABLE先验证身份、交易日/时段、可见前缀、停牌、方向性涨跌停、账户反馈和HARD区间。
从共享core实际设置的batch_started_at=t0起计时，最大等待仍为15交易分钟，午休按既有时钟剔除。15分钟以前，仅当最近5根已闭合且连续的一分钟bar满足以下条件，提前结束本批等待：5根end均晚于t0、均有成交量，BUY的5个close严格递增、SELL严格递减。5根复用既有core的5-bar结构，不新增幅度阈值、不按收益选择窗口，不使用未来high/low/VWAP或未来成交量。
一旦提前释放即锁定到本plan revision/batch；趋势随后消失不重新开始等待。之后仍逐次执行共享安全、HARD和有效期检查。若无提前信号，15分钟时按W15语义释放；这是策略明确的预算分支，不是数据故障fallback。缺少5根只会继续等待；来源错误、未来bar、修订冲突仍由核心显式拒绝。PREFERRED不构成额外价格门槛，与W15一致；NONE合法。

### 2. 状态和可见时间

`AdverseWaitSpec`显式绑定本策略source_commit、5根和15分钟，不改变底层B0 policy。外层P2的source_commit与底层B0的source_commit分开：后者必须沿`base_history_plan`读出原`timing_source_commit`重建原Policy，不用新的P2 commit覆盖。原Policy的全部Literal参数仍由原Contracts closure绑定；新P2 adapter不得借新account/plan身份改变既有目标或操作员排序。`evaluate_adverse_wait(..., spec=...)`只返回外层decision/state，调用core.evaluate一次。LocalSIM只可用外层status/ID/advice_available_at，不使用底层B0更早的机会。
状态保存base_state及一次释放证据：ADVERSE_PREFIX保存当时5根bar，BUDGET保存预算时刻。恢复时核对spec、t0、历史时钟、已消费bar内容hash和原始可见时间；未来证据、缺失证据或重绑定返回UNAVAILABLE。相同plan/batch拥有稳定且与B0/W15/C2隔离的opportunity_id，decision_id绑定本次输入与前后状态。90秒TTL、14:57终点及原计划有效期保持；释放不是派单或成交，取消/修订必须使用新状态。

### 3. 一户新增、三户冻结对照

人口和日期沿用2026-01-05～09-30的181日、9050原生目标。新增P2独立1000万元现金/空仓账户，60秒反应、原操作员容量/排序、90秒TTL、14:57、费用、价集和分钟来源不变，账户内逐日顺序，未充值、不按月重置。
原B0/W15/C2的543账户日只读复用，不重放或覆盖。共同输入、期初、费用、OMS/账本/撮合和操作员队列经济语义保持；P2自己的adapter和纯策略实现独立绑定，不要求新adapter与旧producer字节一致。plan保留baseline_shared_economic_implementation作来源对照；包含分派器的聚合文件hash不是经济语义本身。允许owner作显式P2分派/注入扩展，记录精确源码差异、独立producer身份及既有三规则固定短窗口的经济字段等价证据，不因分派扩展自动重放543日。若实际改变共享经济行为，明确交回owner处理可比性，不能将不同经济链路冒充同一对照。
固定effective candidate SHA `321a966c55822db5777dda4ef0091422e8a1877b7bcb6c27ffb763e9a2441c73`；旧f5af…仍仅作原价集来源。冻结原analysis_spec和报告FileRef；prepare/inspect先完整验证原plan、来源、opening及543日文件字节，再发布新研究plan，不读任何P2收益、不重建旧模板。

### 4. 最小LocalSIM合同

新plan为`position_timing_adverse_wait_research_plan_v1`，public `inspect_adverse_plan(FileRef)`返回已核验的plan和输入；仅该只读入口及纯P2函数/模型可跨模块import，prepare/发布/内部预检不开放。
LocalSIM新请求schema为`localsim_adverse_wait_history_request_v1`，绑定plan FileRef、producer commit、共享经济实现身份及上述兼容证据、单一P2 profile、共同opening和全部181日期；沿用已有分钟账户引擎和逐日提交，不改旧三户请求人口。返回单户P2的opening、有序181个day FileRef、全来源检查及不可变重试回执；day沿用现有NAV、父计划、动作/费用投影，增加P2释放证据的明确引用。实际数量/资金/撮合全部由LocalSIM负责，禁止使用旧账户执行反馈冒充新P2路径。
初次设计审计时，消费接缝核到`dual_mode_research.py`的research_control分派、`human_portfolio.py::run_human_portfolio_day`及`wait_budget_execution.py::research_facade`，当时仅接受W15。其后LocalSIM owner在#7150通过显式factory/spec注入及最小分派扩展接入P2 facade，保留默认入口与既有三规则；本轮不再把该历史差异当接入阻断。不修改W15常量冒充P2，不猴补丁替换核心，不新建账户引擎。P2外层spec与底层B0 policy须分别传递并绑定。
后续择时在完整来源检查后将新增181日与冻结543日合并评价；总证据724账户日，不产生旧三户的新提交。缺失时间加权暴露/完整生命周期保留UNAVAILABLE/UNKNOWN，不阻止新研究设计或运行，不建监控或ACK平台。

### 5. 评价冻结

主比较P2−W15，次比较P2−C2、P2−B0，共3项新增比较。沿用同初始资本成本后PnL/收益、全分钟NAV正向MDD、费用和BUY/SELL全部父计划分母；月/季按各自上期期末NAV承接。5交易日循环区块bootstrap、2000次、seed=20261107，报告每日配对净PnL差/共同初始资本的nominal 95%和三比较Bonferroni区间；不把它称为累计收益/MDD区间。
不挑月份、指数或成交子集做晋升，未成交保留；预算前机会、实际派单、成交分别报告。数据已被用于前轮机制研究，所有新结果属于探索性历史研究。quantity_matched=false、pure_timing_attribution=false、economic_confirmation=false；软件通过不晋升规则，不设置MDE、自然日等待或盈利准入。

### 6. P2事实消费与不可变分析

LocalSIM交付回执`X:/AIstock_temp/localsim-p2-adverse-wait-20261109/source_delivery_receipt_r1.json`，完整SHA256为`f2af7d04c9bb4c655cbc926552f67fb794dd1b3dd9a21465a05e8d3fcccc9206`；#7150已完成单P2的181日及inspect/artifact exact retry，旧543日只读复用。此事实不代表经济有效。
`AdverseAnalysisSpec`仅绑定该交付FileRef；新入口核验plan、candidate/价集/费用/包/selection身份、原三户及新一户的opening/有序days、producer request、来源检查、共享经济兼容证据、release FileRef和artifact retry。全部来源内容SHA通过后才解析P2 NAV；分析结束前再次核验来源，漂移不发布报告。不导入LocalSIM内部实现、不运行账户或重建模板。
统计复用现有`period_account_rows`、`period_panels`和单户诊断；新模块固定三项P2比较及同规格区块区间，不修改原三户分析器或其closure。全期、月、季报告全部四户和三个比较；各期承接本户上期期末NAV，MDD使用完整分钟观察而非仅日终。BUY/SELL完整父计划分母保留，释放证据、提交/派单/成交区分，缺失暴露/生命周期明确UNAVAILABLE/UNKNOWN。producer经济重算未提供不是失败，不发起重算。
报告绑定spec、全部来源FileRef及分析实现身份；同输入exact retry返回相同报告FileRef，已有产物不覆盖。独立Decimal复算收益、费用、分钟MDD和月季承接；软件及来源验收与探索性经济结论分开记录。

### 7. 四户机制诊断的有界补充（已交付）

本次只消费已冻结四户报告及其原始事实，不重放账户。新增 `adverse_wait_diagnostics` 的 `inspect/analyze` 接缝，输入仅为原 analysis spec 与报告 FileRef；先复用 Contracts-6 的完整 source preflight，再核验父报告实现/人口/日期身份，最后才读取账户收益及明细。发布前重验全部来源，输出自有不可变结果；exact retry 必须复用同一 FileRef。旧结果消费者及其实现 closure 不变。

固定输出全部四户、全期和所有月份，不按收益筛选日期。对 BUY/SELL 分别报告全部父计划、完整/部分/未成交/不可用、费用覆盖及金额、SUBMIT/派单/失效、已派单样本的等待中位数、已成交父计划的首笔成交延迟；分母和缺失覆盖必须显式。预算前机会及15分钟前派单/成交只在可归属的单修订父计划上比较，不能把机会当成交或把不同人口的计数当因果提升。费用差按方向拆分；双方同日同股同方向同目标数量且完整成交的价格比较仅作有选择偏差的条件诊断。

逐月净差必须与原报告逐分一致，费用加回只作算术恒等式；不建立 BUY/SELL 收益归因或假设“不成交的订单本来可以成交”。盘中时间加权暴露仍由原投影覆盖决定，完整失效转换原因仍为 UNKNOWN，不以日终现金均值、first/last 状态或推断 TTL 填补。本轮诊断不新增假设检验、参数搜索、规则、训练、推理或实时输入。
规模固定四户181日，模型上限沿用原每户512日；只保留所需日投影/父计划/操作动作，不保留完整分钟NAV数组、市场bin或原始journal。配对键为`(date,symbol,side,target_quantity)`且必须唯一，仅作两侧键交集，不发生多对多join或笛卡尔扩张。全期和9个月共40个账户面板、80个方向面板、30个比较面板；排队中位数从各期原始已派单样本计算，不能平均每日或方向中位数。

### 8. 10月自然承接的冻结设计（本窗口不运行账户）

范围固定 B0/W15/C2/P2、60秒人工响应、90秒TTL、既有人工排队/费用/数量与交易限制、14:57终点、P2的5根/15交易分钟及0.5%研究价集。沿用已交付10月原生850目标、17个交易日（2026-10-08至10-30），不重新推理、重训或重选股票。新研究绑定四户各自2026-09-30的真实期末 day/receipt/state、现金、持仓、成本和累计NAV锚点；无充值、不清仓、不资本均衡，不把旧共同B0 opening三户研究拼接为自然承接。

允许复用原13户10月中真正同源且本户期初/研究合同完全一致的 B0/C2 结果，但必须逐项核验来源、opening、规则、数量/费用/排队及有序日期；不依据收益决定复用。W15/P2 未有兼容延续产物时由 LocalSIM owner 新建其独立连续账户。本窗口不构造账本或调用账户runner。价格计划覆盖当日BUY/SELL与四户历史可能持仓SELL的并集；既有模板不足时仅追加新研究身份，不覆盖原模板，缺真实来源保留UNKNOWN。

10月来源使用其冻结candidate/inventory并披露相对9月来源的差异，不切换active/profile。期间PnL以各户真实opening为起点；主要比较仍 P2−W15，次比较 P2−C2/P2−B0，报告NAV变化之差，不能把9月末财富差算成10月优势。累计收益保留1月1000万元原点；累计MDD继承本户全分钟高水位和既有MDD，期间MDD从各户opening另算。不得重置锚点后拼接MDD，或把差异期初的期间收益率之差当资本匹配因果效果。全期/月/季、完整BUY/SELL分母及UNKNOWN边界保持原合同；现有历史已被观察，仍是探索性而非sealed holdout。

Contracts-8初次仅交付可复制给LocalSIM owner的设计和精确来源引用，未生成可执行账户request或通过10月新账户验收；随后收到的准备输入及本轮最小适配见8.1。真实账户仍归LocalSIM，不以等待未来行情或经济显著性作为研发门槛。
接口差异须明示：现有`adverse_wait_results`只消费1—9月四户、`wait_control_results`只消费共同opening三户；原13户期间来源校验也不能直接当四户自然承接消费者。下一轮由LocalSIM owner沿既有`period_continuation`及W15/P2 facade准备本户opening、有序days、来源检查和retry FileRef；择时仅补最小的四户期间消费适配，复用`continuous_results.period_account_rows`和现有期间/累计统计。不能将本节声明性设计或历史spec传给不兼容的旧CLI冒充接入完成。

下一轮输入与owner交接合同（本节即权威设计，不另建一套计划文件）：

- 1—9月四户分析报告与spec沿用本文冻结证据段的`5a610f…`/`48b9f3…`；三户facts完整SHA为`1be364909b7ef928ce416725b3c8210de1ee454b5a81b8d80316029e53b9afc5`，P2事实从原LocalSIM交付回执`f2af7d…`读回。四户9月30日day/receipt/state必须从这些有序事实取最后一日，不按账户名称猜路径或只按NAV金额配对。
- 10月准备preflight：`X:/AIstock_temp/localsim-october-continuation-20261106/prepared/465cb1fd201b93e9fda412ec85e8c5219752198c84a6d6a4bf708f23c0c5f706/preflight.json`，完整SHA `12bdae08351b363f9da871b70e20e1a2faee4caafdad897d7146fb7e61636e56`；其`inputs.candidate_manifest`完整SHA为`2242c44261dc0eaaf54cda3416a260cf6712ec960cff8bfbb46a527f6587b958`，`selection_inventory`完整SHA为`d55b4fff01b79f55c223e35b6f185200410290ce9921468f68b90b6aada6b6fc`。本次已核验交接preflight身份；不声称已检查四户新增持仓所需全部10月市场文件。
- 旧13户10月analysis spec：`X:/AIstock_temp/localsim-october-account-replay-20261106/studies/63fb7784ae238513c74d7d3bfed86cf10b822573d0ffddff79fddf5703868bd1/analysis_spec.json`，完整SHA `a48db077873ca0cfd1b3494816eda33d2d0adc086bc960a43beb39429ef8df21`。只作为B0/C2复用候选，不把旧13户全部当新四户输入；该旧研究已有producer经济重试，不能误称其NOT_SUPPLIED。
- LocalSIM交回新四户的opening、原parent映射、有序17日FileRef及新建/复用分类；绑定冻结包、candidate、inventory、价集、费用、基础策略commit和W15/P2 spec、producer身份。完整检查先于新收益；每个opening提供真实现金/持仓/成本、原始资本、此前累计费用、NAV高水位和累计MDD及其来源，不把序列化hash不同等同经济不一致，也不只比较NAV/现金后忽略持仓或成本。
- 需要的历史SELL是四户实际期末持仓与既有10月目标的并集；原13户73只历史SELL并集不保证覆盖W15/P2。LocalSIM先交本户opening/股票并集和必要只读输入预检，择时再冻结所需共享模板。缺价格/资格由数据owner处理，不在任一业务窗口补造或改candidate。数量、资金、原生T+1结算、费用、撮合及账户提交继续归LocalSIM。
- 只在上述owner消费准备就绪后，由LocalSIM执行缺少兼容产物的10月账户；返回inspect和不可变exact retry（零重算、零新增提交）。若另执行producer经济重试必须单独报告，未提供不算失败。本次任务不执行这些后续步骤、不控制其进程；离线准备不等待后端重启。

#### 8.1 本次输入交付后的最小续接

下列准备/软件交付状态记录于2026-11-09，并非当前账户进度；#7192现已交回68/68账户日，当前结果消费及验收见文末2026-11-11记录。本节冻结的接口、人口和统计合同不因此改写。

LocalSIM #7174已交付四户真实期初、17日850目标、53只历史SELL并集，市场阻断0，但尚未运行账户。回执 `X:/AIstock_temp/localsim-october-input-preparation-20261109/source_delivery_receipt_r1.json` 的完整SHA为 `268a60a027ac582afa3bf5a0b50d508a20e2fb79862501cf7ad5a8e94192d662`。旧B0/C2各多24个非持仓marks，其他经济期初字段一致；严格复用未认证。本轮选择既定“不兼容则仅新跑10月”的分支，后续LocalSIM新跑四户共68账户日，旧产物只作来源，不裁剪marks、不改账户命名空间来冒充可复用，不重跑1—9月。差异本身不被解释成经济效果不同。

本窗口只交付以下接缝：

- `natural_period_context.load_context` 消费新的 `localsim_natural_period_preflight_v1`，核验交付、准备提交、全部来源SHA、包/candidate/inventory、四户parent/期初/累计锚点、SELL并集和无充值声明。不开账户、不导入LocalSIM内部实现。
- `natural_period_prices.build/inspect` 用新研究身份复用已有价格构造器及同一CandidateReader；当日目标BUY/SELL，继承持仓和截至当日已观察目标只补SELL，不提前使用未来目标。原0.5%工程政策、raw单位、可见时间假设、UNKNOWN/NONE、schema和旧产物均不变。新spec绑定适配器实现，旧13户validator不被改装成四户validator。
- `natural_period_results` 提供独立四户期间分析合同和 `inspect/analyze` CLI；LocalSIM返回的 `position_timing_natural_period_facts_v1` 必须绑定同一preflight、模板、可执行producer request及source inspection，四户各有序17日；所有源和68个day文件完整检查后才解析新收益。实际账户必须使用准备期初的account_id，不接受旧B0/C2命名空间别名。复用 `period_account_rows` 与原父计划诊断，输出四户期间/累计收益、费用、分钟NAV回撤及三个固定比较，比较主值为NAV变化之差/原始1000万元；差异期初期间收益率差只作描述。10月月/季面板与本期一致，1—9月面板只读引用父报告，累计回撤继承原锚点，不拼接或平均月回撤。
- facts的source inspection同时绑定request、opening、全部day索引、candidate、cost、execution contract和producer commit；request身份由除identity/release_identity外内容计算，显式绑定原规则/control、日期/人口、60秒/90秒/14:57、无充值和本户期初。day接受显式研究导出schema，不能把底层checkpoint冒充导出。artifact exact retry只验证/复用产物，未提供producer经济重算记NOT_SUPPLIED，不发起重算。

本轮验收分为软件合同与真实来源/价集两部分；结果消费者的定向fixture验收不等于真实10月收益已验收。后续LocalSIM必须先交四户事实，择时才执行正式收益分析；时间加权暴露UNAVAILABLE、quantity_matched=false、pure_timing_attribution=false、economic_confirmation=false保持。

#7174的离线准备文件 `backend/services/simulation_execution/localsim/natural_period_preparation.py` 属于本合同的已批准SDK消费者，不是线上反向依赖。隔离测试仅为此精确文件登记现有模块别名导入：`adverse_wait_diagnostics`只可访问`DiagnosticSpec/source_preflight`，`continuous_results`只可访问`PeriodAnalysisSpec/period_source_preflight`。使用AST核查实际成员，其他LocalSIM文件、子目录同名文件、其他业务模块、发布/分析/私有成员及通配符均不获得该例外；不扩大runtime分类、不修改LocalSIM源码。

后续四户离线回放的 `backend/services/simulation_execution/localsim/natural_period_research.py` 仅获两项额外公共函数导入授权：`natural_period_context.load_context`、`natural_period_prices.inspect`（允许本地别名）。授权按相对文件路径、完整模块及导入符号同时匹配；不授权整模块、子目录同名文件、其他消费者、构建/发布/分析/私有成员或通配符，不开放`natural_period_results`。登记只调整本模块`test_isolation.py`及本文，不改变SDK实现、冻结价集或来源身份；LocalSIM消费代码与68账户日仍由其owner交付。隔离合同测试不要求尚未合入的消费者文件存在，后续合入时由既有全仓AST导入检查验证实际入口。

### 9. C2/W15自然期间机制诊断（2026-11-13冻结）

输入固定为上轮正式交付回执`cc17798e45b7b1f7d23867f2a7a4e20cf7d2bcc14ae8777812f31e053dfaf94c`及其中spec、报告FileRef。完整来源preflight及父报告spec/实现/人口身份验证先于本轮明细读取，发布前再验全部来源；旧68日及原报告不覆盖、不重新生成。复用既有共享函数，不新建生产分析器或统计框架；operator、冻结spec、源码seal及结果仅在上述X盘任务目录。

范围为2026-10-08～10-30全部17日，B0/W15/C2/P2作为完整上下文；只对C2−W15增加描述性机制面板，不增加假设检验、区间或原三比较family。全期及全部17日都输出，不按收益筛选。固定维度：BUY/SELL完整父计划状态及数量、费用与覆盖、机会/可见/ready/派单/首笔成交各自样本分母、修订/失效边界、四户真实期初及日终持仓/现金路径。等待分解必须有相应时间戳且可唯一归属；多修订、未释放或未成交不混入单修订已完成样本。时间加权暴露仍依赖原投影，不以日终现金代替。

同日同股同方向同目标数量键唯一，C2/W15仅作键交集、状态转移与双边完整成交的条件价格诊断；数量不同及仅一侧存在的父计划保留在各自完整分母，不强行匹配。条件价差不等于账户收益；预算等待、人工排队和派单后成交延迟不得互相替代。费用加回仅算术恒等式；本户期初财富/持仓不同，`quantity_matched=false`、`pure_timing_attribution=false`、`economic_confirmation=false`。无法分离的BUY/SELL收益、未成交反事实、完整失效转换及盘中暴露继续UNKNOWN/UNAVAILABLE，不新增阈值/模型/政策、不晋升线上。

验收以独立Decimal/计数复算、完整人口/日期和父报告逐项对账、不可变产物exact retry、零账户重算及零新增经济提交为准；重试只重读验证，不发起producer经济重算。缺失必要投影只提出最小只读owner需求，不阻断现有可识别诊断。沿用F-514～F-516及F-521验收条款，本次证据在文末追加，历史验收不改写。

## Implementation Plan

初次一个交付块完成纯函数/plan实现、定向验证、真实冻结来源prepare/inspect/exact retry与LocalSIM交接。#7150账户交付后，一个结果消费块完成来源预检→全期/月/季分析→独立复算及报告exact retry。2026-11-09以单一交付块补齐Contracts-7机制诊断，并冻结Contracts-8自然承接设计，当次不执行10月账户；其后#7192及2026-11-11结果消费已完成该延续。当前以Contracts-9补齐既有10月机制诊断，不新增细碎阶段或基础设施。
Contracts-8.1续接仍为单一交付块：新准备合同适配→同一价格引擎build/inspect/exact retry及独立覆盖复核→四户分析合同/归约软件与owner交接。真实账户回放由LocalSIM后续执行，不把软件测试称为该账户的经济验收。

## Verification Plan

定向测试覆盖BUY/SELL对称、只用t0以后5个连续已闭合close、无趋势预算释放、锁定后安全约束仍有效、午休/缺bar、未来/重述、状态恢复与时钟/spec篡改、机会身份隔离、源缺失fail-closed及不可变重试。复用已有core测试夹具，不复制API/账本fixture。
真实验收只读冻结candidate/原价集和历史来源，先验证所有引用，再对固定预声明的小型真实分钟样本做前缀一致性和尾部扰动检查。工程人口预声明为000001.SZ/600000.SH、2026-01-05/01-06、BUY/SELL，共8条无量机会流；使用明确NONE工程约束，不改原共享价集或新经济研究的0.5%价集。缺数据不换股/日期，合法停牌保留。样本不挑收益，不宣称自然账户目标或新P2账户回测。运行于WSL的离线环境；旧账户/报告实现closure保持可核验。
Contracts-8.1直接测试覆盖candidate相对描述符、源漂移、历史SELL无BUY/无未来目标、四户本户期初/命名空间、完整来源先于收益、发布前漂移拒绝、期间/累计MDD、原始资本归一的区块统计及不可变重试。真实价集独立逐区间复算raw bin与Decimal公式，并在Windows回读21份价格产物SHA；真实四户收益必须另消费owner的68个有序账户日。

## Risks

5根严格单调可能少触发，早释放也可能追涨杀跌或错过回落；本轮只冻结一次定义，是否有价值由后续完整账户结果回答。排队和资金反馈可能超过信号差异，不能用条件成交价格解释全账户收益。原研究已被观察，不是sealed holdout；缺失日志不反推原因。

## Production Gates

database_read=false；database_write=false；dataset_mutation=false；market_network=false；runtime_action=false；account_replay=false（本窗口）；backend_restart_required=false。不注册线上策略，不操作用户服务。

## Design Acceptance Index

| ID | 验收条款 |
|---|---|
| F-501 | 单一假设、固定5根/15分钟/方向对称，无收益回选 |
| F-502 | 共享安全authority且无旧规则/默认值漂移 |
| F-503 | 前缀、t0、闭合/连续/成交量约束，无未来输入 |
| F-504 | 一次释放锁定、证据恢复、时钟/身份拒绝 |
| F-505 | 机会/TTL与外层可见时间独立，提醒不等于成交 |
| F-506 | 原来源、543账户日及结果身份先检查后冻结新plan |
| F-507 | 单一新181日账户与旧三户只读对照，owner边界明确 |
| F-508 | 真实来源及前缀机制验收、inspect和exact retry |
| F-509 | 研究SDK隔离、无订单/服务/DB/旧资产写入 |
| F-510 | 单P2及原三户完整来源先验、身份/顺序/内容漂移fail-closed |
| F-511 | 四户全期/月/季、三个冻结比较与区块区间，无收益回选 |
| F-512 | 全父计划诊断、机会/派单/成交分离，未知不补造 |
| F-513 | 不可变分析exact retry与独立统计复算，无账户重放 |
| F-514 | 机制诊断来源/父报告身份先验，发布前重验及不可变重试 |
| F-515 | 全期/全月份BUY/SELL及机会/派单/成交分母、费用与缺失边界 |
| F-516 | 逐月净差与原报告一致，条件成交价不冒充账户收益/因果alpha |
| F-517 | 10月本户自然承接与可复用旧产物的身份条件；仅设计不重放 |
| F-518 | 四户准备提交、真实期初/累计锚点及完整来源只读适配 |
| F-519 | 新身份共享价集覆盖当日BUY/SELL及历史SELL，UNKNOWN和不可变重试 |
| F-520 | 四户期间消费者先验来源及身份，独立期间/累计归约与三比较 |
| F-521 | 软件/真实价集、账户回放与经济有效性分离，旧研究及跨模块零写入 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-501 | 本文Contracts-1/5；backend/services/position_timing/intraday/adverse_wait_control.py | backend/tests/position_timing/test_intraday_adverse_wait.py | VERIFIED | none |
| F-502 | backend/services/position_timing/intraday/adverse_wait_control.py | backend/tests/position_timing/test_intraday_adverse_wait.py | VERIFIED | none |
| F-503 | backend/services/position_timing/intraday/adverse_wait_control.py | backend/tests/position_timing/test_intraday_adverse_wait.py | VERIFIED | none |
| F-504 | backend/services/position_timing/intraday/adverse_wait_control.py | backend/tests/position_timing/test_intraday_adverse_wait.py | VERIFIED | none |
| F-505 | backend/services/position_timing/intraday/adverse_wait_control.py | backend/tests/position_timing/test_intraday_adverse_wait.py | VERIFIED | none |
| F-506 | backend/services/position_timing/intraday/adverse_wait_research.py | backend/tests/position_timing/test_intraday_adverse_wait.py；artifact:X:/AIstock_temp/position-timing-p2-adverse-wait-20261109/validation_receipt.json | VERIFIED | none |
| F-507 | 本文Contracts-3/4/6 | artifact:X:/AIstock_temp/localsim-p2-adverse-wait-20261109/source_delivery_receipt_r1.json | VERIFIED | none |
| F-508 | backend/services/position_timing/intraday/adverse_wait_research.py | artifact:X:/AIstock_temp/position-timing-p2-adverse-wait-20261109/validation_receipt.json | VERIFIED | none |
| F-509 | backend/services/position_timing/intraday/adverse_wait_control.py；test_isolation.py | backend/tests/position_timing/test_isolation.py | VERIFIED | none |
| F-510 | backend/services/position_timing/intraday/adverse_wait_results.py | backend/tests/position_timing/test_intraday_adverse_results.py | VERIFIED | none |
| F-511 | backend/services/position_timing/intraday/adverse_wait_results.py | artifact:X:/AIstock_temp/position-timing-p2-analysis-20261109/verify_receipt.json；artifact:X:/AIstock_temp/position-timing-p2-analysis-20261109/interval_verification.json | VERIFIED | none |
| F-512 | backend/services/position_timing/intraday/adverse_wait_results.py | artifact:X:/AIstock_temp/position-timing-p2-analysis-20261109/mechanism_analysis.json；backend/tests/position_timing/test_intraday_adverse_results.py | VERIFIED | none |
| F-513 | backend/services/position_timing/intraday/adverse_wait_results.py | artifact:X:/AIstock_temp/position-timing-p2-analysis-20261109/retry_receipt.json；backend/tests/position_timing/test_intraday_adverse_results.py | VERIFIED | none |
| F-514 | backend/services/position_timing/intraday/adverse_wait_diagnostics.py::source_preflight/analyze；Contracts-9复用natural_period_results.source_preflight | backend/tests/position_timing/test_intraday_adverse_diagnostics.py；artifact:X:/AIstock_temp/position-timing-mechanism-october-20261109/inspect_receipt.json及retry_receipt.json；artifact:X:/AIstock_temp/position-timing-c2-w15-mechanism-20261113/source_inspection.json及retry_receipt.json | VERIFIED | none |
| F-515 | backend/services/position_timing/intraday/adverse_wait_diagnostics.py::side_panel/diagnose；Contracts-9有界operator | backend/tests/position_timing/test_intraday_adverse_diagnostics.py；artifact:X:/AIstock_temp/position-timing-mechanism-october-20261109/independent_readback_r2.json；artifact:X:/AIstock_temp/position-timing-c2-w15-mechanism-20261113/independent_readback.json及early_opportunity_readback.json | VERIFIED | none |
| F-516 | backend/services/position_timing/intraday/adverse_wait_diagnostics.py::matched_panel/diagnose；Contracts-9有界operator | backend/tests/position_timing/test_intraday_adverse_diagnostics.py；artifact:X:/AIstock_temp/position-timing-mechanism-october-20261109/independent_readback_r2.json；artifact:X:/AIstock_temp/position-timing-c2-w15-mechanism-20261113/independent_readback.json | VERIFIED | none |
| F-517 | 本文Contracts-8 | artifact:X:/AIstock_temp/localsim-october-continuation-20261106/prepared/465cb1fd201b93e9fda412ec85e8c5219752198c84a6d6a4bf708f23c0c5f706/preflight.json；设计逐项审核，本次非账户验收 | DESIGN_VERIFIED | none |
| F-518 | natural_period_context.py::load_context | test_intraday_natural_period.py；artifact:X:/AIstock_temp/position-timing-natural-october-consumer-20261109/inspect_receipt.json，完整10962项来源闭合 | VERIFIED | none |
| F-519 | natural_period_prices.py::build/inspect；原price_plan_campaign引擎 | test_intraday_natural_period.py；artifact:X:/AIstock_temp/position-timing-natural-october-consumer-20261109/independent_readback.json及retry_receipt.json | VERIFIED | none |
| F-520 | backend/services/position_timing/intraday/natural_period_results.py::source_preflight/analyze | backend/tests/position_timing/test_intraday_natural_period.py；artifact:X:/AIstock_temp/position-timing-natural-october-analysis-20261111/independent_readback.json及retry_receipt.json，实际68日统计验收通过，经济有效性仍未确认 | VERIFIED | none |
| F-521 | 本文Contracts-8.1及三个natural_period模块；backend/tests/position_timing/test_isolation.py | artifact:X:/AIstock_temp/position-timing-natural-october-consumer-20261109/l0.json及ownership.json；既有31项相关回归；test_natural_preparation_exception_is_exact_and_readonly/test_natural_replay_exception_is_exact_and_readonly覆盖精确SDK登记及拒绝边界，旧实现closure零改动 | VERIFIED | none |

## 初次纯函数实施与交接证据

实现源码commit `f02cf9358a463cebd3043bf4813886864360ea51`。旧core/W15/history/统计函数没有修改，其原plan与报告closure仍通过。新增19项定向测试、最终相关小矩阵40项通过；Ruff和等价changed-file L0两项扫描均0 findings。L0直接调用与nox相同的两项scanner，输出到X盘；nox没有RTK wrapper，且默认输出路径在F盘。classifier为targeted_ci_required、workflow_gate=passed、unexecuted_test_files=[]。当前Agent完成正确性及完整性两轮自审，不声称独立review。

- 准备spec：`X:/AIstock_temp/position-timing-p2-adverse-wait-20261109/preparation_spec.json`，完整身份由验收回执绑定。
- 正式plan：`X:/AIstock_temp/position-timing-p2-adverse-wait-20261109/artifacts/intraday_v1/adverse_wait_plans/e93bec5f010854d6342d8cff09fe2f1d2045279cba219b09591c6c42ef6a4c0a.json`，完整SHA `89cc858463058cb7e2445285a7373f4ac13096019766058ef160f57e236a3fa9`。
- 验收回执：`X:/AIstock_temp/position-timing-p2-adverse-wait-20261109/validation_receipt.json`，完整SHA `862f30da976f25bb790b8d259b240d075295ebd3e8aa59f786aeb0dc4ad7a000`。

首次prepare=MATERIALIZED，inspect=VERIFIED（1732个引用），同输入exact retry=ALREADY_MATERIALIZED、plan FileRef完全相同。工程operator初次plan_id含股票代码的点，被已有schema拒绝；只修正工程ID后从原不可变plan恢复，再次inspect和exact retry通过，P2算法/参数没有改变，原错误未用数据替换掩盖。

WSL离线验收覆盖预声明8条机会流、1896次前缀评估、8次未过滤未来输入拒绝，24份实际消费市场文件pin复核通过。所有合法前缀在未来尾部价格扰动下的decision/state完全一致。8条工程流的首次释放均为BUDGET；真实小样本未出现提前信号，不换股或调5根窗口。ADVERSE_PREFIX正向及锁定分支由定向测试覆盖，不宣称已有真实提前成交或收益证据。

截至初次纯函数交付，本窗口没有创建/重跑任何账户；当时新P2完成0/181日、旧三户重算0/543。该历史状态不代表当前进度：LocalSIM #7150已交付181/181日，当前择时执行Contracts-6结果消费；后续经济结论另记，不把当时F-507 DESIGN_VERIFIED称为消费者已接入。

原plan消费命令：`python -m backend.services.position_timing.intraday.adverse_wait_research inspect --spec <上述plan绝对路径> --spec-sha256 89cc858463058cb7e2445285a7373f4ac13096019766058ef160f57e236a3fa9 --timing-root <独立X盘根>`。WSL调用路径使用/mnt/x/，不改冻结JSON/FileRef。公共函数为`adverse_wait_control.evaluate_adverse_wait`及模型、`adverse_wait_research.inspect_adverse_plan`；后者只读返回完整输入和来源身份，不生成请求/账户。LocalSIM已按Contracts-4返回单P2账户181日及兼容/来源/重试回执；择时按Contracts-5/6消费，不等待后端重启。

## 2026-11-09：724账户日经济分析与交付

结果消费源码commit为`26d29a1d48bc16541182637b25b6ba75d5630e3e`，与原P2算法及底层B0 commit分别绑定。只新增结果消费入口与直接测试、更新本设计和日内蓝图；未修改任何旧算法、原分析器/closure或LocalSIM代码。来源检查覆盖8,808个完整内容引用，全部通过后首次解析P2 NAV，发布前再次检查；无账户回放、数据库、行情网络或服务操作。
独立Decimal复算全部724账户日、172,312条已发布分钟NAV观察（冻结终点14:57）、36个月度及12个季度面板的收益/费用/MDD/承接，全部一致；旧三户rows和panels与原报告完全相同。三个区块区间另以独立逐次采样循环和fmean复算，一致至1e-12。报告exact retry为ALREADY_MATERIALIZED、FileRef完全相同，账户重算/新增经济提交为0。

共同期初1000万元、空仓、零追加资金；成本后结果如下。所有回撤均为正向峰谷比例，不是日终抽样回撤。

| 规则 | 全期净PnL（元） | 收益 | 分钟MDD | 费用（元） |
|---|---:|---:|---:|---:|
| B0 | -260,607.82 | -2.6061% | 25.9525% | 381,751.27 |
| W15 | 203,580.48 | 2.0358% | 23.5706% | 372,733.95 |
| C2 | 323,814.57 | 3.2381% | 23.0800% | 323,281.72 |
| P2 | 301,679.90 | 3.0168% | 24.0973% | 380,866.31 |

| 比较 | 净增量（元） | 收益差（bps） | MDD降低（百分点） | 校正区块区间（bps/日） |
|---|---:|---:|---:|---:|
| P2−W15（主） | 98,099.42 | 98.0994 | -0.5267 | [-0.9906, 2.0875] |
| P2−C2 | -22,134.67 | -22.1347 | -1.0173 | [-2.3733, 2.1281] |
| P2−B0 | 562,287.72 | 562.2877 | 1.8552 | [0.3160, 5.8473] |

区间估计对象严格为每日配对净PnL差/共同初始资本，不是全期收益或MDD区间。P2−W15仅4/9个月净差为正；8月贡献110,697.13元，其余8个月合计-12,597.71元。季度净差Q1为20,148.73元、Q2为-30,760.42元、Q3为108,711.11元。P2−C2也是4/9个月为正；P2−B0为7/9个月、3/3季度为正。完整四户月季收益/费用/MDD和三项逐期比较全部保留于报告，不以月份或子集回选政策。

机制证据：P2共6,419父计划（BUY 3,355、SELL 3,064），6,416个有控制状态的修订中791次ADVERSE_PREFIX、5,596次BUDGET、29次未释放。791个预算前首次机会对应791父计划，783个曾模拟派单，684个在15交易分钟前派单、681个在该时限前首次模拟成交；C2对应1214/104/44/42，均按各自完整人口报告，不作数量匹配或因果提升倍数。固定8条工程流全部走预算的历史证据仍保留；全研究观察到791个提前机会来自不同的预冻结人口，不是回选工程样本。
P2有成交父计划5,998/6,419，W15为5,896/6,435，C2为5,115/6,586。P2较C2费用增加57,584.59元，净差反为-22,134.67元；加回费用差后为35,449.92元，只是账户算术分解，不是纯价格alpha。P2相对W15的同目标数量且双方全成子集，BUY/SELL条件价格改善为12,719.83/35,393.62元（958/957对）；该子集有选择偏差，不能替代完整账户比较。P2派单后的首次成交中位数50秒；派单排队和信号等待分别统计，P2就绪至派单中位数720秒，仅以已派单SUBMIT为分母。盘中时间加权暴露仍UNAVAILABLE；日终现金比例均值P2为8.7927%，不作其替代。完整失效转换原因保留UNKNOWN，不把OPPORTUNITY_WAIT推断为TTL原因。

结论：P2在模拟中确实实现提前执行，但本轮没有证明其优于W15或C2；相对C2收益和MDD点估计均较差，相对W15净差集中且校正区间跨零。不晋升、不替换线上规则；保留C2研究候选、W15简单对照、B0执行基准及冻结P2证据。下一步优先解释既有四户的月份集中、费用与排队交互，不扫5根/15分钟阈值、不立即增加模型。若扩期，必须沿四户各自真实期末承接，不能直接拼接此前从共同B0 opening起跑的10月三户研究；后续历史扩展仍不得冒称未见样本。

### 冻结证据与复现入口

根目录`X:/AIstock_temp/position-timing-p2-analysis-20261109/`，WSL对应`/mnt/x/AIstock_temp/position-timing-p2-analysis-20261109/`。同一文件映射不改写任何既有JSON或FileRef。

- `analysis_spec_wsl.json`：完整SHA `48b9f3ba1f691986eab560bbeaeda4796b43199b2f08a263823dad9a10370273`。
- 正式报告：`artifacts/intraday_v1/adverse_wait_analyses/f2d1a39d152cfec515975b5d195c79eae7c2589994abce80a2408cf8c30ea956.json`，完整SHA `5a610f5e33bc7dd4975d7d8b54b0fda8da5e53205f26b8efe0d8fab09390887e`。
- `verify_receipt.json`：完整SHA `f07204839b7e27880af22404e867cf26ce2ce519fd0f129339962582c5a3f4c7`。
- `interval_verification.json`：完整SHA `545724bac4d9e13c06762cae35322c82a7e32d0c499b43da2d28e467087ae4a3`。
- `mechanism_analysis.json`：完整SHA `d4b8dd248676b29e46323bb7779a7e9132583f0c3630550fd28a69beb1b18f00`。
- `retry_receipt.json`：完整SHA `6d47d5f11294ceed8fe0140699287408fee6fc19aa3e535de33e116bc10e63ef`。

正式CLI：`python -m backend.services.position_timing.intraday.adverse_wait_results analyze --spec <analysis_spec_wsl绝对路径> --spec-sha256 48b9f3ba1f691986eab560bbeaeda4796b43199b2f08a263823dad9a10370273 --timing-root <独立结果根>/artifacts`；同入口`inspect`只做来源检查、不读P2 NAV。报告以NumPy 2.2.5在WSL生成，版本写入报告，不修改原统计函数。
初次报告已原子发布，临时operator封装环境信息时`platform.platform()`拟调用uname子进程，被离线guard拒绝；仅改为不启动子进程的系统信息读取，按唯一已发布报告恢复回执。随后独立复算和exact retry均通过，未重算账户、覆盖报告或改变策略。回执保留首轮operator SHA及恢复原因。
当前Agent完成正确性、来源/前缀与完整性多轮自审，未声称独立agent审核。10项直接测试、45项相关矩阵、Ruff及两项changed-file L0均通过；classifier保持targeted_ci_required、workflow_gate=passed、unexecuted_test_files=[]。F1共13项与F2共12项验证通过；源码PR #7158的最终CI/合入及root同步以`source_delivery_receipt_r1.json`记录为准，源代码合入、线上激活及经济确认分别报告。

## 2026-11-09：月份/方向补充诊断与10月自然承接设计

本次消费者源码commit为`cc191252e69c3d9854d071727f488eb3b5b5e61d`；新增诊断模块和直接测试，不改变原P2、B0/W15/C2或旧分析implementation closure。来源先验完整核查8810项引用，才消费724个账户日投影；发布前再次核验。报告完整覆盖40个全期/月度账户面板、80个方向面板及30个比较面板，不按结果剔除股票、月份或未成交。

新增事实及结论边界：

1. P2相对W15的非8月净差为-12,597.71元；非8月费用加回差仍为-5,614.77元。因此集中性不能只用手续费差解释；这不是证明其他月份无信号，也不据此筛掉月份。8月净差110,697.13元、费用反而多1,149.42元；双方全成且同目标数量的条件价格改善BUY为17,065元、SELL为-6,904元，覆盖仅42/65对，不能用两者相减的“残差”归因为持仓收益或纯alpha。
2. P2相对C2的费用多57,584.59元，其中BUY 7,866.23元、SELL 49,718.36元；相对W15多8,132.36元，其中BUY 1,117.21元、SELL 7,015.15元。方向费用是实际账户算术分解，不表示SELL信号本身贡献了同额负收益，也不证明额外交易无必要。
3. 就绪到派单的墙钟等待有明显方向差异：P2 BUY中位数1200秒（3027个已派单SUBMIT），SELL 360秒（2975个）；C2分别1260秒（2586个）和360秒（2536个）。两者派单到首笔成交中位数均50秒，按已成交父计划另计分母。60秒人工响应不等于60秒总执行延迟，分钟信号与操作排队的交互应优先考虑，不能据此推算取消排队后的收益。

| 规则/方向 | 预算前机会父计划 | 15分钟前派单 | 15分钟前首笔成交 | 全部父计划/有成交父计划 |
|---|---:|---:|---:|---:|
| C2 BUY | 603 | 21 | 19 | 3428 / 2579 |
| C2 SELL | 611 | 23 | 23 | 3158 / 2536 |
| P2 BUY | 372 | 311 | 308 | 3355 / 3023 |
| P2 SELL | 419 | 373 | 373 | 3064 / 2975 |

上表前两类执行只对可归属的单修订提前机会计数，各组人口和数量不同，不报告因果提升倍数。P2释放后锁定与C2短暂机会、人工排队的交互是与事实相容的解释，不是已识别的单独因果效果。

4. 跨月账户路径分化很明显。P2/W15在8月的BUY双方全成同目标配对42（两侧全部BUY父计划440/449）、SELL配对65（398/395）；9月仅BUY 10（308/310）、SELL 10（291/289）。不能把少数条件配对的价格优势推广到全部账户，也不能宣称只要改成交分钟就能复制整个NAV差。下一轮自然承接回答的是完整执行政策的账户结果，不是固定数量的纯成交价效应。
5. 因此不在本轮回选BUY/SELL阈值、调5根/15分钟或训练模型；下一轮先按Contracts-8保持规则不变、各户自然承接。只有后续确需分离持续机会/排队与价格选择的贡献时，另冻结单一机制对照，不把缺失的完整生命周期或暴露重建变成研发门槛。

本次权威产物位于`X:/AIstock_temp/position-timing-mechanism-october-20261109/`（WSL同文件为`/mnt/x/…`）：

- `diagnostic_spec.json`，完整SHA `bab70b7ae3a4c95b167a94fc587c08cd06852cc92cffb1efcd12223b2081b34e`。
- `inspect_receipt.json`，完整SHA `0a698350c88af5a7f968aa747b2534479a99154e6671cb5f8c4fe75378340a1f`；源引用8810、账户日724、诊断收益尚未消费。
- 诊断报告 `artifacts/intraday_v1/adverse_wait_diagnostics/c67c9fcd335cc1c97d778f772066d3cffb23693f42d53da7807d16999a7e101b.json`，完整SHA `4603b5f66667b358fe67305f3656ebd3fbd2077d9bf4c2977e2b824b94c0d0a5`。
- `analyze_receipt.json`，完整SHA `2a06e6ffe74e4208a93a9b886c1e735f2f901f77c529ffc87705396765fc4262`。
- `independent_readback_r2.json`，完整SHA `354ba6f9d430a69f156cf9b43640c1d3d33fc833bd5b1b507053cd9793d516ac`；独立逐标量复算724日、80个方向面板和30个比较面板，父计划/费用/排队分母与中位数、原收益端点和条件配对均通过。
- `retry_receipt.json`，完整SHA `11fe38b570416896af5b3cff34215bb008f25fb5ed9e3b2862f153b138faa659`；ALREADY_MATERIALIZED、同report FileRef、账户重算0、新经济提交0。

公共入口：`python -m backend.services.position_timing.intraday.adverse_wait_diagnostics analyze --spec <上述diagnostic_spec绝对路径> --spec-sha256 bab70b7ae3a4c95b167a94fc587c08cd06852cc92cffb1efcd12223b2081b34e --timing-root <独立结果根>/artifacts`；同入口`inspect`只验证来源/父报告身份，不消费诊断经济值。原operator未设置PYTHONPATH时导入失败，后使用显式本任务worktree路径；独立复算R1未兼容Python 3.10的UTC `Z`后缀，另存R2验证器修正并完成全批复算，保留原operator SHA和首轮失败原因；正式消费者早已正确处理Z，策略、数据和报告均未改变。

该次Agent完成正确性、来源/数值和完整性多轮自审，不冒称第三方/独立agent审核。8项新直接测试、22项最终相关小矩阵通过，Ruff及两项changed-file L0均0 findings；classifier为targeted_ci_required、workflow_gate=passed、unexecuted_test_files=[]，无需DEV数据库。独立统计复算与产物重试均完成；当时F1 17项/F2 12项设计核查及源码/CI/合入状态由该根目录`source_delivery_receipt_r1.json`记录。无BUG登记：这是新增研究能力，验证operator修订不改产品语义。当时10月仅Contracts-8设计冻结；其后准备/适配见8.1及下文。线上激活未执行且本次不需要后端重启。

## 2026-11-09：10月自然承接价集与四户分析软件（非账户结果）

新增源码基线 `d96cd2c6a65e2fc7aff06067e3b6086c1ecd7c33`。WSL从#7174正式回执核验准备提交与完整来源，生成新研究身份的共享价集；10962项来源引用闭合。7项candidate相对描述符按绑定candidate根解析并校验内容SHA，不跳过、不相对cwd猜路径。独立复核17日、850条BUY、2141条SELL，全部2991条可用、UNKNOWN=0，175份真实raw前收盘价bin的Decimal区间逐条一致；53只历史SELL覆盖完整，不为历史持仓补BUY、不提前使用未来目标。Windows另回读21份价格产物完整SHA通过。

权威产物根 `X:/AIstock_temp/position-timing-natural-october-consumer-20261109/`，WSL为同文件 `/mnt/x/AIstock_temp/position-timing-natural-october-consumer-20261109/`：

- `price_spec.json` 完整SHA `c3a05f853389e21420485d423a4fdf771931d4c3d0d23f161cda09177747c115`。
- 价集目录 `artifacts/intraday_v1/shared_price_plans/13f4d83944286cd43e67332301a76a1124cb454bc9a146f1f39d1c71d537b65c/`；其中 `manifest.json` 完整SHA `842cf72d760d3ca1512b2bf6f749a58e76ed4087bec0d097e1b773c3c85d6639`、`templates.json` 完整SHA `5a080214d341996031bd8d1d9636ef3abf314acf48f0b9894d070577db4cda1a`、`policy_document.json` 完整SHA `d50e19ebf66688272f5c0663910e6e46c4058aa782af0fd78741892e9d2d7336`。
- `inspect_receipt.json` 完整SHA `d022e528f9892a3a0b9b39bf6e0973295c39b3592383b4655872ab9eabc73328`。
- `independent_readback.json` 完整SHA `0fad99e719c4785a51df32932651886180149020c3d277207b8d10a13df21915`。
- `retry_receipt.json` 完整SHA `dd27111cc15da9f8d17e964f4785900e343b2579b15f7989686be2b3abf14381`，ALREADY_MATERIALIZED、同manifest FileRef，账户重算及新增经济提交均0。
- `consumer_contract.json` 完整SHA `1642a731d690440dbc407e0bc9366c919e6da7d5c14b72c6250dc12f8f9070fa`，提供facts/spec JSON schema、冻结producer字段及inspection/retry合同；明确不是可执行账户请求。

WSL真实build→inspect→exact retry全部通过，总耗时1217.30秒，主要为完整冻结来源内容校验，不是账户回放。9项直接测试与31项最终相关矩阵、Ruff、两项L0和归属检查通过；不登记新BUG，这是新增合同适配。自审修正了相对描述符解析、preflight纳入完整复核及跨日新峰值下的测试回撤oracle，并补充发布前漂移与旧命名空间拒绝、原始资本归一的区块统计测试。最终CI/合入状态由本轮根目录`source_delivery_receipt_r1.json`记录，不冒称独立agent审核。
首轮CI在570项中569项通过，唯一失败为#7174已批准离线准备入口未登记SDK隔离例外。按8.1精确文件及只读成员补齐本模块隔离测试，5项定向验证及新增作用域L0通过；不删除/跳过失败测试，不扩大生产runtime分类，不修改LocalSIM或旧实现。价格spec/manifest/consumer合同的不可变身份保持不变；最终CI以修订后的精确HEAD为准，不复用首轮失败状态冒称通过。

公共入口 `natural_period_prices.build/inspect` 及 `python -m backend.services.position_timing.intraday.natural_period_results inspect|analyze --spec <owner事实交回后冻结的spec路径> --spec-sha256 <完整SHA> --timing-root <独立分析根>`。分析spec/facts模型分别为 `NaturalAnalysisSpec`、`NaturalFacts`；最小producer字段绑定随 `consumer_contract.json` 交接，该文件不是可执行账户请求。旧B0/C2不复用；由LocalSIM owner最小对接并仅新跑10月四户68账户日，交回完整来源检查和artifact exact retry，再由本窗口做正式期间/累计比较。

本轮新10月账户运行0、收益读取0；database/network/runtime/service_process_control均false，仅在WSL运行离线验证脚本。旧1—9月、原13户、288案例及价格模板未改，无需后端重启。quantity_matched=false、pure_timing_attribution=false、economic_confirmation=false；本轮软件和价集通过不等于四户LocalSIM接入/账户验收、线上激活或经济有效。

## 2026-11-11：四户10月自然承接结果消费

本节是上述2026-11-09准备/软件交付之后的实际进度。LocalSIM #7192（merge `c8f270b90ea847359ef7673f0a4039aa8a3c0683`）已完成68/68账户日；正式交付回执 `X:/AIstock_temp/localsim-natural-october-replay-20261110/source_delivery_receipt_r1.json` 完整SHA `465e32e8ae5cbcd8606d181e3d2da8e06a344bbd222b18eac2eb1194ebb9f4a5`。来源元数据采用显式只读补充清单修复，旧经济产物没有覆盖。择时只消费其事实，不重跑1—9月或10月、不复用旧十月B0/C2、不修改规则、成本、数量或日期。

11,144项完整来源预检已通过，确认四户/17日且outcomes_read=false；正式报告已生成，68日/16,184个分钟NAV观察点的独立数值复核及报告exact retry通过。本节结果统计已验收，不等于策略经济有效或可晋升线上。

### 对象与口径

期间为2026-10-08—10-30，17交易日、850个共同原生目标。各户承接本户9月末现金、持仓、成本和高水位，无充值。期间PnL为终值NAV减本户opening；累计收益以1月原始1000万元为原点。1—10月合计198交易日/792账户日，其中本轮新增读取68日，旧724日仅只读承接。全部分钟NAV的观察范围保留原14:57终点，不扩展为15:00或Tick全行情。

| 规则 | 10月净PnL（元） | 本户10月收益 | 10月分钟MDD | 1—10月累计收益 | 累计分钟MDD | 10月费用（元） |
|---|---:|---:|---:|---:|---:|---:|
| B0 | -726,722.78 | -7.4617% | 8.4998% | -9.8733% | 25.9525% | 35,181.16 |
| W15 | -705,424.49 | -6.9135% | 8.0671% | -5.0184% | 23.5706% | 35,916.09 |
| C2 | -699,522.36 | -6.7758% | 7.9559% | -3.7571% | 23.0800% | 32,875.20 |
| P2 | -753,143.51 | -7.3109% | 8.2447% | -4.5146% | 24.0973% | 36,176.34 |

期间和累计MDD分别按本户opening、历史高水位及既有MDD归约；累计MDD并非月MDD相加或平均。10月没有刷新各户历史最大回撤，故累计MDD与9月值相同。10月月面板与`2026-Q4_TO_DATE`为同一期间，不能称完整Q4；1—9月月/季面板继续引用原父报告，不改历史账户命名空间或改写旧报告。

### 固定三比较与不确定性

| 比较 | 10月净增量（元） | 原始资本归一增量（bps） | 1—10月累计差（bps） | 10月MDD降低（百分点） | 校正区块区间（bps/日） |
|---|---:|---:|---:|---:|---:|
| P2−W15（主） | -47,719.02 | -47.7190 | +50.3804 | -0.1775 | [-6.6362, +0.8072] |
| P2−C2 | -53,621.15 | -53.6212 | -75.7558 | -0.2888 | [-7.5879, +2.3936] |
| P2−B0 | -26,420.73 | -26.4207 | +535.8670 | +0.2551 | [-9.0428, +4.9300] |

区间仍采用冻结的5交易日循环区块、2000次、seed=20261107和三比较校正，估计对象仅为17日的每日配对净PnL差/原始1000万元，不是累计收益或MDD区间，也不是新增1—10月区间。三项校正区间均跨零；点估计较差不等于已经统计确证P2有害。P2相对W15/C2/B0分别仅6/17、5/17、8/17日净差为正，不回选日期。

尤其不能用期间收益率差替代主比较：P2的-7.3109%好于B0的-7.4617%，但P2本期实际多亏26,420.73元，因为其opening较高；累计P2优于B0的535,866.99元主要已在1—9月形成，不能记成10月新增优势。

### 机制边界与下一步

- 完整父计划分母BUY/SELL分别为B0 299/290、W15 295/287、C2 295/292、P2 295/287；全成/部分/未成/不可用分别保存在正式报告。C2有成交507/587，P2和W15均560/582，B0 576/589；不同父计划人口不作数量匹配。
- P2相对C2多付3,301.14元费用，净差却为-53,621.15元；加回费用后的账户差仍为-50,320.01元，不能把落后仅解释为费用。P2/W15成交状态计数相同也不代表股票、数量、持仓或成交价相同，不能据此识别纯时点效应。
- 日终现金比例均值B0/W15/C2/P2为8.9683%/9.1759%/9.6558%/9.0580%。四户盘中时间加权暴露均为UNAVAILABLE（0/17日），不能用日终均值替代。派单至首笔成交投影17/17日可用，中位数均50秒，分母分别为576/560/507/560个已成交父计划；就绪至派单中位数分别960/960/840/780秒，以已派单SUBMIT为分母，不混作预算/信号等待。更早呈现或派单不等于更高收益。

2026-11-11时确定的方向保持精简：暂停P2的阈值搜索、模型化及线上晋升，保留其冻结证据；C2继续作为研究候选，W15作为简单对照，B0作为执行基准。当时的下一步是优先解释C2与W15的成交覆盖、等待/排队、费用及自然持仓路径差异，不新增显著性比较、不回选政策。该优先项现按Contracts-9执行，结果及后续方向见下节；未知归因仍不得用推测补齐。

上述结果属于固定选股包下完整执行政策的历史账户比较，不是对长期持有的择时alpha检验；不宣称样本外有效，不替换线上规则。quantity_matched=false、pure_timing_attribution=false、economic_confirmation=false保持。

### 本轮证据位置

分析源代码基线 `c7929bf2d15e0a3e8e20debe9580f5fce407577a`，沿用现有`natural_period_results`，没有业务代码修改。独立工作树 `F:/Dev/AIstock_worktrees/position-timing-natural-october-analysis-20261111`；冻结报告及验证材料写入独立根 `X:/AIstock_temp/position-timing-natural-october-analysis-20261111/`，WSL对应`/mnt/x/…`，不复制大型数据。

- 输入spec：`X:/AIstock_temp/localsim-natural-october-replay-20261110/natural_analysis_spec.json`，完整SHA `9b623ffcf15a8635c9741b0f6b0c98187b90b4e1cc145c91685ae80121db2a66`。
- `inspect_receipt.json`：本窗口完整来源预检结果，11,144项来源、四户/17日，未读新收益。
- 正式报告：`artifacts/intraday_v1/natural_period_analyses/ca336dc5d6ef611f43a1999bca11d61618724b3f9759b1d797a02e4d3deea88c.json`，完整SHA `1bb7279cf0f028dde52bdc79ad80b065a3f50faa67cc461d50534a47cef62e4f`，Windows回读一致。
- `independent_readback.json`完整SHA `570ca19c92c824778061c46fa38a7dde0e6d31bae120b5fdbd722b2715da9a72`；68日、16,184个分钟NAV观察点的期间/累计收益、费用、MDD、高水位及完整父计划/成交/排队/日终现金分母一致，三项区块区间另用逐次采样与线性分位插值复算至1e-12。
- `run_receipt.json`绑定正式报告，`independent_readback.json`绑定独立Decimal/区块统计复算，`retry_receipt.json`绑定报告exact retry；完整文件SHA及本轮源码/CI/合入状态以同根`source_delivery_receipt_r1.json`为准。

正式analyze为MATERIALIZED、耗时811.553秒；完全同输入重试为ALREADY_MATERIALIZED、耗时808.260秒，报告路径/完整SHA相同。主要耗时为完整来源内容校验；重试仍重新校验/归约统计，但不重跑账户，账户重算与新增经济提交均为0。spec中的source_delivery绑定#7174准备回执，不是#7192结果交付回执；后者通过facts/request/inspection/retry身份对齐，二者不可互换，不改写冻结spec。

## 2026-11-13：C2/W15十月自然承接机制诊断

本轮遵循Contracts-9，只补齐10月机制诊断；1—9月已有结果和机制证据保持原身份，不把本轮称为新198日检验。独立工作树基线`c0b45e307494581c00e7e3ae1d623872c551e2ee`，26个分析源码文件与上轮seal逐项一致。没有业务代码、测试、规则、费用、撮合、数据或其他模块修改，也没有新生产分析器；有界operator复用现有来源检查、方向面板与条件配对函数。

完整预检先验证原11,144项来源，再绑定本轮spec、父报告、上轮交付、operator及源码seal，共11,150项；通过来源及父报告身份核验后才读68个新期间day投影，发布前重验全部来源。首次本轮day收益读取时间`2026-11-13T02:43:45.474511+00:00`；旧父报告已含已知收益，不能把该时间误称首次见到研究收益。正式分析耗时1097.20秒，主要是完整来源内容核验，不是账户回放。产物覆盖四户×（全期＋17日）=72个账户面板、144个BUY/SELL方向面板、18个C2/W15描述性面板和68条真实持仓路径；原三个P2比较逐项保留，不新增检验或置信区间。

### 收益、费用与完整分母

C2−W15的10月净差为 **+5,902.13元**，费用差 **−3,040.89元**，净差加回费用差为 **+2,861.24元**。后者仅为账户算术恒等式，不是识别出的纯价格alpha或BUY/SELL收益贡献。全部17日中11日净差为正；日差范围−21,825.43至+18,316.61元，单日波动明显大于期间净差，不能依据正点估计晋升。全部日期面板保存在报告，不回选好月份或日期。

| 规则/方向 | 完整父计划 | 全成/部分/未成/不可用 | 费用（元） | 15分钟前首次机会 | 其中15分钟前派单/首成 |
|---|---:|---|---:|---:|---:|
| W15 BUY | 295 | 277/0/18/0 | 4,860.47 | 0 | 0/0 |
| W15 SELL | 287 | 283/0/4/0 | 31,055.62 | 0 | 0/0 |
| C2 BUY | 295 | 250/0/45/0 | 4,435.74 | 34 | 0/0 |
| C2 SELL | 292 | 256/1/35/0 | 28,439.46 | 35 | 0/0 |

费用方向投影覆盖均为17/17日。C2较W15少买约447.18万元、少卖约439.84万元的实际原始价成交名义额，但不同人口/数量不能转换成“少交易导致改善”的因果结论。全父计划及未成交分母不能被双边成交子集替换。

### 等待、排队与首笔成交是不同的时钟

下表每格为“中位数（样本数）”；首次机会覆盖所有单修订父计划，派单列只含已派单SUBMIT。交易分钟按既有交易时段整数分钟合同，排队/成交延迟按秒；不同分母的中位数不能相加减作逐笔分解。上表机会/派单/首成三项都是计数而非比率。

| 规则/方向 | 批次t0→首次机会（交易分） | t0→首次派单（交易分） | ready→派单（秒） | 派单→首笔成交（秒） |
|---|---:|---:|---:|---:|
| W15 BUY | 15（295） | 40（277） | 1,440（277） | 50（277） |
| W15 SELL | 15（287） | 24（283） | 480（283） | 50（283） |
| C2 BUY | 15（295） | 38（250） | 1,320（250） | 50（250） |
| C2 SELL | 15（292） | 23（257） | 420（257） | 50（257） |

本轮四户所有父计划均为单修订；C2有69个预算前首次机会，但没有一笔在t0后15交易分钟前派单或首成。信号提前并未兑现为该时限前的执行，需关注与人工队列、机会持续性/失效的交互，而不是先微调价格阈值。以上不能证明每个未成交都由排队或TTL造成：原投影只给first/last审计与动作结果，完整失效转换仍UNKNOWN。可见→派单包含反应/排队等等待，也不能替代t0→首次机会。盘中时间加权暴露仍为四户UNAVAILABLE（0/17日），日终现金仅作状态统计。

适用范围仍是冻结选股包每天50个原生目标、60秒人工响应与原操作员容量/排序。不能将该队列下的等待外推为用户仅处理1—2笔手工计划时的固有延迟，也不是个股日频择时相对长期持有的alpha验证。

### 持仓路径与条件成交价不构成纯归因

C2期初NAV10,323,814.57元、现金146,517.28元、53只持仓；W15期初NAV10,203,580.48元、现金746,721.71元、50只持仓。50只共同持仓的数量全部不同，C2另有3只；既有财富差120,234.09元不能算作10月新增优势。期末C2/W15持仓为47/44只，共同44只中41只数量仍不同；期末财富差126,136.22元正好等于期初差加本期净差。全部68条本户现金/数量/成本/原始价mark状态路径留证，不重置为同仓位。

| 方向 | C2/W15全父计划 | 同日同股同方向 | 其中目标数量相同 | 双边完整成交 | 条件成交价改善、未扣费（元） |
|---|---|---:|---:|---:|---:|
| BUY | 295/295 | 284 | 11 | 10 | +501.00 |
| SELL | 292/287 | 270 | 8 | 7 | −337.00 |

绝大多数共同股票的目标数量也不同。上述10对BUY及7对SELL具有成交选择偏差，不代表完整账户，不能将价差合计约164元与5,902.13元账户净差对接为因果分解。未成交反事实、同路径纯价格贡献、BUY/SELL收益归因均UNAVAILABLE；`quantity_matched=false`、`pure_timing_attribution=false`、`economic_confirmation=false`保持。

### 证据、复核与后续方向

证据根为`X:/AIstock_temp/position-timing-c2-w15-mechanism-20261113/`，WSL路径对应`/mnt/x/…`，没有复制大型数据。冻结`diagnostic_spec.json`完整SHA `b16123c7325df2d2ff4025469ffe5234aa2b0a10cc30e4d19424d439914c1056`；`source_inspection.json`完整SHA `570d7e4dec9c49c9cf0cac169d278e9b5ae4aff10737ba9ff85d1c61540103b0`。正式报告相对路径`artifacts/c2_w15_natural_diagnostics/85d1944df85c31728d7d92c3f180361401d94d978ec3f426c34741ada0343f39.json`，完整文件SHA `2fc7d34200a9dcd98da6b59ed0e4f102dbf36c35f52463aaf702bb30e6cb3e65`；文件SHA与canonical artifact identity不是同一个值。

`independent_readback.json`完整SHA `4017887a493b29d4c2ae83856aec1d34797483cd6fa09f98ae115b91d640abf7`：独立标准库在重新核验11,150项来源后读取全部68日，72/144/18面板、费用、数量/持仓承接、各等待分母及条件价格复算一致，没有调用业务统计函数或账户引擎。`early_opportunity_readback.json`完整SHA `a2cca18710848e4b77d9051b9246a2a2fcd39f16f85c1575f857b731ac47b81b`另行复核144个方向面板的提前机会/派单/首成分母，确认C2的34/35个提前机会及0/0预算前派单/首成。

`retry_receipt.json`完整SHA `0b2c3d8da91eb4a16b0c2f0704897bb408860d85d73eb952924c9553a035bd27`：完整来源preflight重新通过，报告返回ALREADY_MATERIALIZED、路径/完整SHA不变；本次重试只校验既有统计产物身份，统计重新归约0、账户重算0、新增经济提交0，不启动producer经济重算。原producer_economic_retry=NOT_SUPPLIED仍为合法状态。最终文档表格复核、PR/CI和root同步状态由同根`source_delivery_receipt_r1.json`绑定，不回写旧报告或旧回执。当前Agent执行多轮正确性/完整性自审，独立的是标准库数值复算，不冒称独立agent审核。

方向不变但优先级更具体：保留C2研究候选、W15简单对照、B0基准和P2冻结证据，暂停阈值搜索、模型堆叠及线上晋升。当时提出的下一轮最小机制对照现由[容量机制对照F1](position_timing_dispatch_coupling_f1_20261113.md)单独冻结：仅新增C2L父计划独立容量臂，复用旧C2为唯一主对照，尚未实施或运行。本设计的P2/四户结果与验收不因此改写；仍不把历史描述性结果当同路径因果效果或样本外确认。

### 2026-11-11结果消费者的历史复现说明（不属于新容量对照）

原四户结果复现使用现有CLI：`python -m backend.services.position_timing.intraday.natural_period_results analyze --spec /mnt/x/AIstock_temp/localsim-natural-october-replay-20261110/natural_analysis_spec.json --spec-sha256 9b623ffcf15a8635c9741b0f6b0c98187b90b4e1cc145c91685ae80121db2a66 --timing-root /mnt/x/AIstock_temp/position-timing-natural-october-analysis-20261111/artifacts`；同入口`inspect`只做来源校验。WSL执行前从该次封印源码工作树加载，不依赖Windows的Git worktree元数据。该CLI不生成本节C2/W15机制诊断，也不支持新增C2L臂。

2026-11-11结果消费的首次新收益解析是完整来源预检及68个day内容检查之后的独立初算，UTC为`2026-11-10T18:02:22.805774+00:00`，不是2026-11-13机制诊断的首次day读取时刻。该正式消费者的首次新day解析阶段仍为`AFTER_COMPLETE_NATURAL_SOURCE_PREFLIGHT_BEFORE_DAY_HEADER_VALIDATION`；源码先在Windows封印，再于WSL验证26份源码身份。源码/软件、来源检查、账户回放、统计读回和经济有效性分别报告；该次数据库/行情网络/服务/账户操作均未执行，无需重启。

独立复核针对文件身份和所交付投影的统计归约，不冒称独立重演逐笔账本或撮合；原始账户/经济inspect仍委托LocalSIM，`account_day_journals_independently_reverified=false`保持。`producer_economic_retry=NOT_SUPPLIED`符合合同，不误判为失败或发起经济重算。
