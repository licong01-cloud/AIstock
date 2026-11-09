# P2：不利方向持续移动时提前结束等待

日期：2026-11-09。Owner：position_timing。Feature tier：F1。状态：LOCALSIM_ACCOUNT_VERIFIED_ANALYSIS_IN_PROGRESS。

## Background

终极目标是改善明确BUY/SELL计划的成本后结果和回撤。既有181日三户研究中，C2相对B0净增584,422.39元、MDD降低2.8724个百分点；相对W15净增120,234.09元，但区块校正区间跨零且8月贡献超过全期。保留三者，不以收益回选月份或阈值。
本轮只研究一个假设：价格已经连续朝本次交易的不利方向移动时，继续等固定15交易分钟可能增加机会成本。BUY的不利方向是上涨，SELL是下跌。该假设不承诺盈利，也不预测当日最高最低点。前轮权威结果与完整SHA见[同等待预算设计](position_timing_wait_budget_control_f1_20261106.md)末尾。

## Scope

本窗口交付研究专用纯函数、可恢复状态、冻结研究plan/只读inspect、定向测试和真实来源验收；LocalSIM owner另行执行新P2账户181日，并交回事实后由择时继续经济分析。
纯函数交付允许写入以下7个文件；该次operator和不可变验收产物仅在`X:/AIstock_temp/position-timing-p2-adverse-wait-20261109/`：

- `backend/services/position_timing/intraday/adverse_wait_control.py`
- `backend/services/position_timing/intraday/adverse_wait_research.py`
- `backend/tests/position_timing/test_intraday_adverse_wait.py`
- `backend/tests/position_timing/test_isolation.py`
- `docs/architecture/position_timing_adverse_wait_p2_f1_20261109.md`
- `docs/architecture/position_timing_wait_budget_control_f1_20261106.md`
- `docs/architecture/position_timing_intraday_advice_localsim_blueprint_f2_20261023.md`

LocalSIM #7150交付后，追加结果消费范围为`backend/services/position_timing/intraday/adverse_wait_results.py`、`backend/tests/position_timing/test_intraday_adverse_results.py`及上述两份设计文档；本次operator和不可变分析证据仅在`X:/AIstock_temp/position-timing-p2-analysis-20261109/`。不修改原7个文件中的算法、来源冻结或统计实现，以免改变既有plan的implementation closure。
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

## Implementation Plan

一个交付块完成本窗口：冻结本设计→纯函数/plan实现和定向验证→真实冻结来源prepare/inspect/exact retry与LocalSIM交接。账户经济验证由LocalSIM接入后继续，不新增细碎阶段或基础设施。

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

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-501 | 本文Contracts-1/5 | artifact:docs/architecture/position_timing_adverse_wait_p2_f1_20261109.md | DESIGN_VERIFIED | none |
| F-502 | backend/services/position_timing/intraday/adverse_wait_control.py | backend/tests/position_timing/test_intraday_adverse_wait.py | VERIFIED | none |
| F-503 | backend/services/position_timing/intraday/adverse_wait_control.py | backend/tests/position_timing/test_intraday_adverse_wait.py | VERIFIED | none |
| F-504 | backend/services/position_timing/intraday/adverse_wait_control.py | backend/tests/position_timing/test_intraday_adverse_wait.py | VERIFIED | none |
| F-505 | backend/services/position_timing/intraday/adverse_wait_control.py | backend/tests/position_timing/test_intraday_adverse_wait.py | VERIFIED | none |
| F-506 | backend/services/position_timing/intraday/adverse_wait_research.py | backend/tests/position_timing/test_intraday_adverse_wait.py；artifact:X:/AIstock_temp/position-timing-p2-adverse-wait-20261109/validation_receipt.json | VERIFIED | none |
| F-507 | 本文Contracts-3/4 | artifact:docs/architecture/position_timing_adverse_wait_p2_f1_20261109.md | DESIGN_VERIFIED | none |
| F-508 | backend/services/position_timing/intraday/adverse_wait_research.py | artifact:X:/AIstock_temp/position-timing-p2-adverse-wait-20261109/validation_receipt.json | VERIFIED | none |
| F-509 | backend/services/position_timing/intraday/adverse_wait_control.py；test_isolation.py | backend/tests/position_timing/test_isolation.py | VERIFIED | none |
| F-510 | 本文Contracts-6 | artifact:docs/architecture/position_timing_adverse_wait_p2_f1_20261109.md | DESIGN_VERIFIED | none |
| F-511 | 本文Contracts-5/6 | artifact:docs/architecture/position_timing_adverse_wait_p2_f1_20261109.md | DESIGN_VERIFIED | none |
| F-512 | 本文Contracts-6 | artifact:docs/architecture/position_timing_adverse_wait_p2_f1_20261109.md | DESIGN_VERIFIED | none |
| F-513 | 本文Contracts-6 | artifact:docs/architecture/position_timing_adverse_wait_p2_f1_20261109.md | DESIGN_VERIFIED | none |

## 本窗口实施与交接证据

实现源码commit `f02cf9358a463cebd3043bf4813886864360ea51`。旧core/W15/history/统计函数没有修改，其原plan与报告closure仍通过。新增19项定向测试、最终相关小矩阵40项通过；Ruff和等价changed-file L0两项扫描均0 findings。L0直接调用与nox相同的两项scanner，输出到X盘；nox没有RTK wrapper，且默认输出路径在F盘。classifier为targeted_ci_required、workflow_gate=passed、unexecuted_test_files=[]。当前Agent完成正确性及完整性两轮自审，不声称独立review。

- 准备spec：`X:/AIstock_temp/position-timing-p2-adverse-wait-20261109/preparation_spec.json`，完整身份由验收回执绑定。
- 正式plan：`X:/AIstock_temp/position-timing-p2-adverse-wait-20261109/artifacts/intraday_v1/adverse_wait_plans/e93bec5f010854d6342d8cff09fe2f1d2045279cba219b09591c6c42ef6a4c0a.json`，完整SHA `89cc858463058cb7e2445285a7373f4ac13096019766058ef160f57e236a3fa9`。
- 验收回执：`X:/AIstock_temp/position-timing-p2-adverse-wait-20261109/validation_receipt.json`，完整SHA `862f30da976f25bb790b8d259b240d075295ebd3e8aa59f786aeb0dc4ad7a000`。

首次prepare=MATERIALIZED，inspect=VERIFIED（1732个引用），同输入exact retry=ALREADY_MATERIALIZED、plan FileRef完全相同。工程operator初次plan_id含股票代码的点，被已有schema拒绝；只修正工程ID后从原不可变plan恢复，再次inspect和exact retry通过，P2算法/参数没有改变，原错误未用数据替换掩盖。

WSL离线验收覆盖预声明8条机会流、1896次前缀评估、8次未过滤未来输入拒绝，24份实际消费市场文件pin复核通过。所有合法前缀在未来尾部价格扰动下的decision/state完全一致。8条工程流的首次释放均为BUDGET；真实小样本未出现提前信号，不换股或调5根窗口。ADVERSE_PREFIX正向及锁定分支由定向测试覆盖，不宣称已有真实提前成交或收益证据。

本窗口没有创建/重跑任何账户；新P2完成0/181日、旧三户重算0/543。LocalSIM接入和经济评价仍待其owner完成，不影响已交付的纯函数、真实来源和只读plan验收；不把F-507 DESIGN_VERIFIED称为消费者已接入。

消费命令：`python -m backend.services.position_timing.intraday.adverse_wait_research inspect --spec <上述plan绝对路径> --spec-sha256 89cc858463058cb7e2445285a7373f4ac13096019766058ef160f57e236a3fa9 --timing-root <独立X盘根>`。WSL将X:/改为/mnt/x/，不改JSON内的冻结FileRef。公共函数为`adverse_wait_control.evaluate_adverse_wait`及模型、`adverse_wait_research.inspect_adverse_plan`；后者只读返回完整输入和来源身份，不生成请求/账户。LocalSIM按Contracts-4接入后返回单P2账户181日及兼容/来源/重试回执；随后择时按Contracts-5完成三比较，不等待后端重启。
