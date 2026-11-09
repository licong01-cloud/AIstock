# P2：不利方向持续移动时提前结束等待

日期：2026-11-09。Owner：position_timing。Feature tier：F1。状态：RESULTS_VERIFIED_EXPLORATORY_NOT_PROMOTED。

## Background

终极目标是改善明确BUY/SELL计划的成本后结果和回撤。既有181日三户研究中，C2相对B0净增584,422.39元、MDD降低2.8724个百分点；相对W15净增120,234.09元，但区块校正区间跨零且8月贡献超过全期。保留三者，不以收益回选月份或阈值。
本轮只研究一个假设：价格已经连续朝本次交易的不利方向移动时，继续等固定15交易分钟可能增加机会成本。BUY的不利方向是上涨，SELL是下跌。该假设不承诺盈利，也不预测当日最高最低点。前轮权威结果与完整SHA见[同等待预算设计](position_timing_wait_budget_control_f1_20261106.md)末尾。

## Scope

本设计初次交付研究专用纯函数、可恢复状态、冻结研究plan/只读inspect、定向测试和真实来源验收；LocalSIM owner随后在#7150完成新P2账户181日。当前择时只消费交回事实继续经济分析，不重新回放账户。
纯函数交付允许写入以下7个文件；该次operator和不可变验收产物仅在`X:/AIstock_temp/position-timing-p2-adverse-wait-20261109/`：

- `backend/services/position_timing/intraday/adverse_wait_control.py`
- `backend/services/position_timing/intraday/adverse_wait_research.py`
- `backend/tests/position_timing/test_intraday_adverse_wait.py`
- `backend/tests/position_timing/test_isolation.py`
- `docs/architecture/position_timing_adverse_wait_p2_f1_20261109.md`
- `docs/architecture/position_timing_wait_budget_control_f1_20261106.md`
- `docs/architecture/position_timing_intraday_advice_localsim_blueprint_f2_20261023.md`

LocalSIM #7150交付后，追加结果消费范围为`backend/services/position_timing/intraday/adverse_wait_results.py`、`backend/tests/position_timing/test_intraday_adverse_results.py`及上述两份设计文档；本次operator和不可变分析证据仅在`X:/AIstock_temp/position-timing-p2-analysis-20261109/`。不修改原7个文件中的算法、来源冻结或统计实现，以免改变既有plan的implementation closure。
本次机制诊断及下一轮设计只写`backend/services/position_timing/intraday/adverse_wait_diagnostics.py`、`backend/tests/position_timing/test_intraday_adverse_diagnostics.py`、本设计和日内上位蓝图；operator及不可变证据仅写`X:/AIstock_temp/position-timing-mechanism-october-20261109/`。不修改旧结果消费者、冻结来源、LocalSIM或任何其他业务模块。
复用core.evaluate、分钟/日级合同、CandidateReader、交易分钟时钟、freeze、原history来源预检；不修改旧core、W15、原统计函数或其冻结implementation closure，不另建Reader、撮合、账本或研究平台。

## Non-goals

不改线上PolicyId/API/UI或已有B0/W15/C2，不接QE/Advisory/HMM/MiniQMT，不训练/扫参，不用Oracle标签。新策略经济回放属于LocalSIM，本窗口不读生产数据库、不抓行情、不改数据集/active/profile、不控服务，不把软件或前缀机制检查称为P2盈利证据。

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
实际消费接缝已核到`dual_mode_research.py`的research_control分派、`human_portfolio.py::run_human_portfolio_day`及`wait_budget_execution.py::research_facade`：当前仅接受W15。由LocalSIM owner通过显式factory/spec注入或最小分派扩展接入P2 facade，保留默认入口与既有三规则；不修改W15常量冒充P2，不猴补丁替换核心，不新建账户引擎。P2外层spec与底层B0 policy须分别传递并绑定。
后续择时在完整来源检查后将新增181日与冻结543日合并评价；总证据724账户日，不产生旧三户的新提交。缺失时间加权暴露/完整生命周期保留UNAVAILABLE/UNKNOWN，不阻止新研究设计或运行，不建监控或ACK平台。

### 5. 评价冻结

主比较P2−W15，次比较P2−C2、P2−B0，共3项新增比较。沿用同初始资本成本后PnL/收益、全分钟NAV正向MDD、费用和BUY/SELL全部父计划分母；月/季按各自上期期末NAV承接。5交易日循环区块bootstrap、2000次、seed=20261107，报告每日配对净PnL差/共同初始资本的nominal 95%和三比较Bonferroni区间；不把它称为累计收益/MDD区间。
不挑月份、指数或成交子集做晋升，未成交保留；预算前机会、实际派单、成交分别报告。数据已被用于前轮机制研究，所有新结果属于探索性历史研究。quantity_matched=false、pure_timing_attribution=false、economic_confirmation=false；软件通过不晋升规则，不设置MDE、自然日等待或盈利准入。

### 6. P2事实消费与不可变分析

LocalSIM交付回执`X:/AIstock_temp/localsim-p2-adverse-wait-20261109/source_delivery_receipt_r1.json`，完整SHA256为`f2af7d04c9bb4c655cbc926552f67fb794dd1b3dd9a21465a05e8d3fcccc9206`；#7150已完成单P2的181日及inspect/artifact exact retry，旧543日只读复用。此事实不代表经济有效。
`AdverseAnalysisSpec`仅绑定该交付FileRef；新入口核验plan、candidate/价集/费用/包/selection身份、原三户及新一户的opening/有序days、producer request、来源检查、共享经济兼容证据、release FileRef和artifact retry。全部来源内容SHA通过后才解析P2 NAV；分析结束前再次核验来源，漂移不发布报告。不导入LocalSIM内部实现、不运行账户或重建模板。
统计复用现有`period_account_rows`、`period_panels`和单户诊断；新模块固定三项P2比较及同规格区块区间，不修改原三户分析器或其closure。全期、月、季报告全部四户和三个比较；各期承接本户上期期末NAV，MDD使用完整分钟观察而非仅日终。BUY/SELL完整父计划分母保留，释放证据、提交/派单/成交区分，缺失暴露/生命周期明确UNAVAILABLE/UNKNOWN。producer经济重算未提供不是失败，不发起重算。
报告绑定spec、全部来源FileRef及分析实现身份；同输入exact retry返回相同报告FileRef，已有产物不覆盖。独立Decimal复算收益、费用、分钟MDD和月季承接；软件及来源验收与探索性经济结论分开记录。

### 7. 四户机制诊断的有界补充（本次实施）

本次只消费已冻结四户报告及其原始事实，不重放账户。新增 `adverse_wait_diagnostics` 的 `inspect/analyze` 接缝，输入仅为原 analysis spec 与报告 FileRef；先复用 Contracts-6 的完整 source preflight，再核验父报告实现/人口/日期身份，最后才读取账户收益及明细。发布前重验全部来源，输出自有不可变结果；exact retry 必须复用同一 FileRef。旧结果消费者及其实现 closure 不变。

固定输出全部四户、全期和所有月份，不按收益筛选日期。对 BUY/SELL 分别报告全部父计划、完整/部分/未成交/不可用、费用覆盖及金额、SUBMIT/派单/失效、已派单样本的等待中位数、已成交父计划的首笔成交延迟；分母和缺失覆盖必须显式。预算前机会及15分钟前派单/成交只在可归属的单修订父计划上比较，不能把机会当成交或把不同人口的计数当因果提升。费用差按方向拆分；双方同日同股同方向同目标数量且完整成交的价格比较仅作有选择偏差的条件诊断。

逐月净差必须与原报告逐分一致，费用加回只作算术恒等式；不建立 BUY/SELL 收益归因或假设“不成交的订单本来可以成交”。盘中时间加权暴露仍由原投影覆盖决定，完整失效转换原因仍为 UNKNOWN，不以日终现金均值、first/last 状态或推断 TTL 填补。本轮诊断不新增假设检验、参数搜索、规则、训练、推理或实时输入。

### 8. 10月自然承接的下一轮冻结设计（仅设计，不运行）

范围固定 B0/W15/C2/P2、60秒人工响应、90秒TTL、既有人工排队/费用/数量与交易限制、14:57终点、P2的5根/15交易分钟及0.5%研究价集。沿用已交付10月原生850目标、17个交易日（2026-10-08至10-30），不重新推理、重训或重选股票。新研究绑定四户各自2026-09-30的真实期末 day/receipt/state、现金、持仓、成本和累计NAV锚点；无充值、不清仓、不资本均衡，不把旧共同B0 opening三户研究拼接为自然承接。

允许复用原13户10月中真正同源且本户期初/研究合同完全一致的 B0/C2 结果，但必须逐项核验来源、opening、规则、数量/费用/排队及有序日期；不依据收益决定复用。W15/P2 未有兼容延续产物时由 LocalSIM owner 新建其独立连续账户。本窗口不构造账本或调用账户runner。价格计划覆盖当日BUY/SELL与四户历史可能持仓SELL的并集；既有模板不足时仅追加新研究身份，不覆盖原模板，缺真实来源保留UNKNOWN。

10月来源使用其冻结candidate/inventory并披露相对9月来源的差异，不切换active/profile。期间PnL以各户真实opening为起点；主要比较仍 P2−W15，次比较 P2−C2/P2−B0，报告NAV变化之差，不能把9月末财富差算成10月优势。累计收益保留1月1000万元原点；累计MDD继承本户全分钟高水位和既有MDD，期间MDD从各户opening另算。不得重置锚点后拼接MDD，或把差异期初的期间收益率之差当资本匹配因果效果。全期/月/季、完整BUY/SELL分母及UNKNOWN边界保持原合同；现有历史已被观察，仍是探索性而非sealed holdout。

本次交付下一轮可复制给LocalSIM owner的设计和精确来源引用，不声称已生成可执行账户request或已通过10月新账户验收；尚未交付的兼容opening/完整历史SELL模板由各owner在下一轮消费预检中核实，不以等待未来行情或经济显著性作为研发门槛。
接口差异须明示：现有`adverse_wait_results`只消费1—9月四户、`wait_control_results`只消费共同opening三户；原13户期间来源校验也不能直接当四户自然承接消费者。下一轮由LocalSIM owner沿既有`period_continuation`及W15/P2 facade准备本户opening、有序days、来源检查和retry FileRef；择时仅补最小的四户期间消费适配，复用`continuous_results.period_account_rows`和现有期间/累计统计。不能将本节声明性设计或历史spec传给不兼容的旧CLI冒充接入完成。

## Implementation Plan

初次一个交付块完成纯函数/plan实现、定向验证、真实冻结来源prepare/inspect/exact retry与LocalSIM交接。#7150账户交付后，一个结果消费块完成来源预检→全期/月/季分析→独立复算及报告exact retry。本次再以单一交付块补齐Contracts-7机制诊断，并冻结Contracts-8的下一轮自然承接设计；不在本次执行10月账户，不新增细碎阶段或基础设施。

## Verification Plan

定向测试覆盖BUY/SELL对称、只用t0以后5个连续已闭合close、无趋势预算释放、锁定后安全约束仍有效、午休/缺bar、未来/重述、状态恢复与时钟/spec篡改、机会身份隔离、源缺失fail-closed及不可变重试。复用已有core测试夹具，不复制API/账本fixture。
真实验收只读冻结candidate/原价集和历史来源，先验证所有引用，再对固定预声明的小型真实分钟样本做前缀一致性和尾部扰动检查。工程人口预声明为000001.SZ/600000.SH、2026-01-05/01-06、BUY/SELL，共8条无量机会流；使用明确NONE工程约束，不改原共享价集或新经济研究的0.5%价集。缺数据不换股/日期，合法停牌保留。样本不挑收益，不宣称自然账户目标或新P2账户回测。运行于WSL的离线环境；旧账户/报告实现closure保持可核验。

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
| F-514 | 本文Contracts-7；intraday/adverse_wait_diagnostics.py | 定向测试及真实冻结来源验收待执行 | PENDING | implementation_pending |
| F-515 | 本文Contracts-7；intraday/adverse_wait_diagnostics.py | 定向测试及全期/月/方向诊断待执行 | PENDING | implementation_pending |
| F-516 | 本文Contracts-7；intraday/adverse_wait_diagnostics.py | 原报告逐分一致性与独立复算待执行 | PENDING | implementation_pending |
| F-517 | 本文Contracts-8 | 本次仅设计，下一轮由LocalSIM owner消费验收 | DESIGN_VERIFIED | none |

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
