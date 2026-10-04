# Advisory 日频资金流条件买入价格价值 M6 F2详细设计 v1

2026-10-05。EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY；只研究价格价值建议，不研究QE因子或分钟执行。设计交付不等于源码、研究、收益确认或运行启用。

## 1. Background / 事实与假设

原R2 M2/M3/M4已完成11物理fit和1索引，M1四fit开发导航为正但两净增量置信区间跨零，M5四fit负向停止自身候选；累计19fit+1index，不重跑。M1日频/API/UI PR #5445当前HEAD a8ea11ee5adcd8b3ae11c8c3b6be838d986003a0必需CI成功，六项浏览器收据仍待交付；BUG-1726本地三层空链修复71tests通过，尚因公共新端点业务smoke语义缺口未创建源码PR。两者不阻止新的独立Advisory详细设计。

H-DAILY-MONEYFLOW-PRICE-1：当D已可见的大/超大订单金额失衡及参与度，是否在既存12D行情/包分数与买价坐标g之外，增加冻结复评政策下成本后净价值的辨别信息？仅三项日频新信息；不称订单金额桶为机构账户、聪明资金或已证明alpha，不更换loss/seed救活旧候选。

事前工程spike只读原7720键/386D（2024-07-04～2026-02-02），market.moneyflow_ts八项side amount在D及前五交易日均完整7720/7720，共17013股票日，无重复或缺日。首尝试30秒预算取消；同键/字段/窗口增加精确calendar常量日期范围裁剪后，2 SELECT耗时5.891秒成功（两尝试累计4 SELECT），没有返回金额值、收益或标签，fit=0。此事实只证明当前历史源可读取，不证明历史发布时钟、可学习性或盈利。

## 2. Scope / 精确写入登记

本设计文档在最新origin/main e58cea30bd07c5b2c82d165998694e8bc9e787e8创建的独立树`advisory-moneyflow-price-value-m6-design-20261005`交付；设计PR仅本文件。

设计审核合入后，最新main独立源码树只允许下列精确文件；开始源码前再次登记，不借接收主线变更扩张本窗口ownership：

- backend/services/advisory_model_first/economic_moneyflow_price_source_v1.py（只读批量来源与单位边界）
- backend/services/advisory_model_first/economic_moneyflow_price_v1.py（三字段纯计算、M6 plan与同核包装）
- backend/services/advisory_model_first/economic_moneyflow_price_pipeline_v1.py（原子研究阶段/来源/预算薄编排）
- backend/services/advisory_model_first/economic_sector_price_value_v1.py（仅加入显式M6固定块/身份/status路由，M1/M5数学与SHA公式不变）
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py（仅加入M6/23累计fit路由，既存M1/15与M5/19行为不变）
- backend/tests/advisory_model_first/test_economic_moneyflow_price_source_v1.py
- backend/tests/advisory_model_first/test_economic_moneyflow_price_v1.py
- backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py（仅旧M1/M5数学和M6路由的直接回归）
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py（仅预算路由直接回归）
- 本设计及docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md（只同步实际M6阶段/结果与最新主线）

只复用已存在的公开moneyflow单位normalizer、只读会话、原标签/完整四臂模拟器、JSON GBDT核、stage/registry/fit journal；不复制label/OPE/simulator平台。不得修改公共依赖源码。

## 3. Non-goals / 授权边界

不修改QE、Selection、HMM、StrategyPackage、行业、公共数据、Execution、Paper、CI、工作流或AGENTS；不提交QE实验、不重建候选/池、不追加父资格门。禁止DB写入、DDL/DML、profile/模型/运行配置激活、依赖安装、用户服务或其它进程控制。临时产物仅X，正式新实验F独立hash目录。后端重启user-owned；纯研究交付无需重启。

现行蓝图禁止新增Advisory分钟特征研究/训练，本M6不访问分钟数据。不读sealed/new holdout，不用已消费窗口冒充确认；旧负结果不追加收集证据、不重训，不运行其它模块任务。本阶段无API/UI/binding/生产bundle交付，不隐称整个价格功能完成。

## 4. Architecture / 真实接口与信息时钟

`normalize_tushare_moneyflow_units(frame, copy=True, require_all=False)`来自backend/data_service/moneyflow_contract.py；本适配器先严格验证八字段schema再调用一次，source=10k CNY→consumer=CNY，receipt版本`tushare_moneyflow_shares_yuan_v1`。require_all=False只是选择本研究需要的八项金额而非18项全部字段；不是允许八项缺失或幅度猜单位。不重构或替代Tushare官方net_mf_amount，不调用QE共享loader/cache或修改因子库。

只读源使用既存`BoundedEntryReadSession(EntryWorkBudget(30))`，参数化SQL、REPEATABLE READ/READ ONLY、超时、rollback/close。按全部原候选键联查最近五个market.trading_calendar正式交易日，常量范围从calendar推导，非手写业务日期；金额查询≤2个SELECT/一次来源批，候选≤7720、唯一股票日≤38600、RSS≤2GiB。只查询匹配source证券及stock-day，不全表扫描、每股循环或建DB索引；预算取消保留原keys、报告SOURCE_BUDGET而不称数据缺失。CPU阶段不延续DB的30秒会话预算。

来源只包含trade_date≤对应D；完整交易calendar含D，最近五日是五个交易日而非五条非空记录。五日源边界可早于父候选第一D，仅向过去扩展正式calendar，不缩短原候选窗口；每个原D必须真实位于calendar且下一T吻合父冻结calendar。交易日期不是发布/修订时间。当前DB历史没有原生as-of发布证明，源证据保留CURRENT_DB_HISTORICAL_NON_VINTAGE / native_identity=UNPROVEN；query_at记真实当前读取时间，不倒填D捕获时间。历史导航可用当前重建，不能据此声称无修订偏差的独立确认或启用。未来日频消费需显式当D收盘后源可用且实际捕获时钟合法，本设计不自行实现生产激活。

原instrument/D/T/policy/hash由父冻结输入绑定；不以当前股票池过滤，证券source映射仅公开确定身份，当前原7720键已全部精确匹配。重复或一对多别名/交叉市场身份矛盾硬失败，不能拆后缀、取首条或当前symbol回填。新增证券/包适配不在本实例研究中，不设新的策略包alpha准入。

## 5. Contracts / 来源、名单与缺失

先冻结清洁源码HEAD、plan/source/query/单位/父manifest/hash与完整名单，然后预登记，再读取金额和旧开发标签。spike spec SHA256=4e61c8b8411204b74fd82c7a53df3c30419bffd78774af2c1f61238228dd9013；它是ENGINEERING_SOURCE_FEASIBILITY，不算模型trial。原feature key文件SHA256=cbe54ede6f5de28dc35484295cf4372772616ed3d2f10192738631ff527e1bf7，386D/7720键与全部原排序必须保留。

显式公开来源profile由原消费合同解析并绑定，不修改profile截止或默认QE窗口。父plan、价格prepared、12Dprepared、VALUE_REVIEW_5_V1 value-label prepared、股票池/包/policy/profile均按现有冻结引用和hash只读核定，不重建/更新旧工件。原D/T下一交易日合同和source可识别性不一致、重复键、外来证券、未来trade_date或byte/hash漂移fail closed；历史native恢复限制不升级。

单日正常停牌、无金额行、任何必需金额缺失、五交易日不足、合法零分母等只令该原股票新块UNKNOWN，原键和日期不删、金额不补零/前填。负side amount、bool/非法非数/Infinity/重复行或source证券矛盾属合同错误；数据库NULL是正常未知。合法真实零金额不是缺失，但分母零不能定义为中性失衡。

原始快照保存请求键/实际calendar/source金额单位/实际query时间/源字节hash/状态与原因，原子发布并冻结后不再从DB重拉替换；阶段续跑只读已发布快照。每个source证券日最多一行，只有原键所需五日并集可进入快照；某D的计算只取自己的五日，不可因全批最大D截断失效而读到后续数据。SOURCE_AVAILABLE不能写成NATIVE_COMPLETE。抓取预算失败不给拟合输入，不缩短窗口或筛更便宜股票。

## 6. Frozen information block / 唯一三个字段

对任一原证券与正式交易日u，令B(u)=buy_lg_amount+buy_elg_amount、S(u)=sell_lg_amount+sell_elg_amount、A(u)=八项buy/sell sm/md/lg/elg金额之和，单位均CNY。

| 字段 | 精确定义 | 合法范围/未知 |
|---|---|---|
| large_order_imbalance_D | (B(D)-S(D))/(B(D)+S(D)) | [-1,1]；大单分母=0或D金额未知→UNKNOWN |
| large_order_turnover_share_D | (B(D)+S(D))/A(D) | [0,1]；总额=0或D金额未知→UNKNOWN |
| large_order_imbalance_5D | Σ最近五交易日(B-S)/Σ最近五交易日(B+S)，含D | [-1,1]；缺任一交易日金额/不足5日/分母=0→UNKNOWN |

五日比率是金额加权，不是逐日比率平均；不另加net_mf、成交量、分钟、行业或状态字段。字段之间可能冗余，当前不拆组/择优/调整窗口。三字段都有限且状态AVAILABLE才有共同mask；否则candidate与matched共同UNKNOWN，保留原UNKNOWN研究控制贡献，不能只让candidate弃权或只用available日评价。

features由D及以前来源独立生成，不能读取split收益/maturity/T HLC来确定可估性。模型查询的g是给定合法价格坐标；真实T open仅代入冻结D函数。未来资金流/行情/标签毒化不能改变D字段。观察价格条件净值不是因果限价成交、最佳分钟买卖点或真正无偏uplift，完整shadow两臂只提供冻结日频政策下的历史导航。

## 7. Model / 匹配监督、支持与价格集合

matched=12D+g（13维），candidate=12D+三资金流+g（16维）。均GradientBoostingRegressor，200 trees、learning_rate=.05、depth3、min_leaf30、subsample1、max_features=None、seed20261004、无early-stop；gross Y均值与日级path L q10各两头，共4物理fit/一candidate。沿现有JSON公开树、float32分裂与sklearn1.8.0/scipy1.16.3 parity；不pickle/joblib或安装依赖。

同VALUE_REVIEW_5_V1、同成熟净值/path标签和同共同train keys：12D完整、三字段AVAILABLE、真实label_information_end≤train_end且T≤train_end、合法gap在原无标签global支持，最低100行/20D。不把不同M1/M5监督人口的旧结果当matched。公共支持仍从无标签train完整12D与原观察价生成，100bps桶/≥30行/≥5D、总体2.5～97.5%交集、真实支持洞；不按金额可取或收益重筛支持。

父实例train2024-07-04～2025-05-30、validation2025-06-03～2025-09-30、已消费test2025-10-09～2026-02-02、label cutoff2026-03-10；日期只从冻结父plan读，不硬编码公共默认。validation只诊断，无反调/选点；test不拟合、校准或选阈值。成本buy .95/sell 5.95bps扣一次，价格节点expected net>0且下行参考≤800bps；法律tick完整估值，多段/空集/UNKNOWN分开，支持洞不连桥。

## 8. Trial / 新lineage与原累计预算

新campaign=`advisory_price_moneyflow_v1_20261005`，model_id=M6；计划固定上述来源/特征/query/window/政策/成本/GBDT/支持/评价预算及implementation SHA。parent SOURCE字段与原研究相同，`budget_anchor_ref`只读指向原R2 M2 preregistered manifest；`predecessor_manifest_ref`指向实际M5 evaluated manifest。只核定19既发fit与1index的真实阶段/登记/身份，不重新分析负向收益；M1/M5正负终态均不形成新包或本候选准入门。

持久根仍F:/Dev/AIstock_model_artifacts/advisory_price_research_campaign_r2_20261004，独立experiment_id目录；trial_registry.jsonl与campaign_fit_journal.jsonl原位置只追加，不换根/清账。M2=4、M3=5、M4=2、M1=4、M5=4既发物理fit，加M6最多4，累计cap23+原1index；新lineage不等于统计独立。foreign study/root、journal缺失/重复/矛盾fail closed，partial不隐式重试；unit-test拟合单独计数，不混为研究。

每次fit前后只读核定QE experiment、custom_evo、multi-alpha三条running路径；任何running或unknown只暂停本候选fit，不停止QE或其它Advisory研发。长实验30分钟检查；本4fit≤30min、2线程、RSS/新增工件各≤2GiB，candidate≤7720、价格行≤500000，原18h/48h剩余预算不重置。每个physical-fit启动前原子记journal；全部四fit成功才发布trained。sealed_accessed=false/deployable=false。

## 9. Evaluation / 完整四臂与导航

冻结完整原81D/1620候选及共同100估值日，baseline原Top5、固定±300bps规则、matched、candidate四臂；同Top40复评政策、五槽、现金0、A股T+1/停牌/限价延迟及持仓完整mark，不补Top6、不因缺资金流删除日期。UNKNOWN只有市场证明确实可执行时才能作为原动作研究控制，分列贡献而非模型TAKE；行情不证明四臂一致不进入。任一endpoint/held-mark/unsettled不可证则经济BLOCKED，不输出虚假净指标。

沿原导航合同：candidate相对baseline及matched两日均net增量各≥5bps；两种实际进入差异各≥12D且≥15%原决策日；模型真实TAKE≥30episode；MDD恶化≤200bps，最差5%日均恶化≤20bps。block5/reps2000/seed20261004配对区间，NAVIGATION_ONLY。预先保留实际干预支持、现金/coverage/UNKNOWN控制贡献，不用胜率或开盘价coverage代替净收益。正仅CONSIDER_CONFIRMATION_DESIGN_ONLY，负仅STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION，执行未证单独BLOCKED。

数据/研究族窗口已多次消费，即使两区间为正也不是独立确认，不支持activation或收益承诺。探索结果可导航但不能关闭整个资金流方向；当前候选负向后结束它，继续实质不同已设计路线/当前业务交付，不用同信息换loss/seed/门槛循环。

## 10. Implementation Plan / 分层推进与终止

1. 本F2三视角审核修订/validator/diff，文档PR必需检查绿后合入及自己官方清理。
2. 最新main独立源码树登记§2精确范围，实现moneyflow source/pure block/thin stage；仅两个Advisory共核加入M6路由，旧M1/M5数学和研究不重跑。
3. 多轮源码审核及失败节点优先修复，最终一次小直接矩阵、F2/L0；清洁提交绑定真实implementation/source HEAD。
4. 先预登记M6新lineage/4fit/累计23预算，再一次只读prepare并原子保存原金额/状态/hash；真不足保留UNKNOWN并报告，不找数据窗口补业务样本。
5. QE三路径空闲才一次四fit/完整四臂，报告SOURCE/研究/经济/证据等级；无需用户重启。只在真执行错误时修复，不复跑已终态模型求漂亮结果。
6. 当前HEAD必需CI及最终审核通过后合入源码/真实进度，自己官方清理；M1原交付依赖单独接续，不由M6顺便改API/UI/公共流程。

工程预算≤4h，超出列真实缺口，不省去合同；负向、数据真缺口只结束/暂停M6，不耗空余时间作旧失败证据固化。用户停止、总预算到期或预定有价值路线全部完成/真实阻断且无剩余价值设计才交还整轮，不强行跑满18/48h。

## 11. Verification Plan / 三视角审核与最小矩阵

时钟/来源轮：严格八schema/明确一次单位转换/负值bool非法数重复拒绝，NULL/停牌/零分母/不足五日保留UNKNOWN；calendar最近五交易日/不同D、精确query裁剪、参数化只读rollback和30秒退出；未来资金流/标签毒化、外来证券/alias矛盾、源hash漂移不通过。当前历史重建不冒充原生发布。

数学/学习轮：手算三比率及金额加权五日，空输入/全缺失数量与dtype，13/16共同监督/全局支持无新块筛选、未来maturity/test毒化、四fit/JSON parity、费用与价格洞；M1/M5旧身份公式及显式预算路由的精确直接回归，不重跑旧研究。

编排/业务轮：预登记先于金额/labels、源快照原子且续跑不重拉、原19+4/foreign/partial/refusal、QE非idle禁止fit；完整四臂的原键/控制/held-mark/真干预保留。最终只跑直接矩阵，广回归交当前HEAD必需CI；不改公共CI、借旧UI收据或把docs PASS当收益。

## 12. Design Acceptance Index

| ID | 验收 |
|---|---|
| F-880 | 真正日频新信息三字段；不是机构身份/分钟或旧loss延伸 |
| F-881 | 原7720键/时钟/八项单位边界与只读有界批量来源 |
| F-882 | 完整交易日5D/零分母与正常缺失UNKNOWN，不删除填补或伪造native |
| F-883 | 13/16同核四fit、共同监督/global支持、JSON价格集合及旧核不变 |
| F-884 | 原四臂、成本/风险/干预净增量与导航证据等级 |
| F-885 | 先登记、新lineage/4和整批23fit+1index、原阶段/hash/journal不重置 |
| F-886 | QE互斥、X/F与资源边界，金额源重建不污染sealed或共享模块 |
| F-887 | 精确设计/源码范围与多轮审核，源码/经济/运行分报 |

## 13. Design Acceptance Matrix

设计PR #5450已合入1f18c3e3bdd23007ae77f5db5a2380d63a6e98e8且自己官方cleanup_done。最新main独立源码树已实现五叶源码及三测试；48项新M6/旧M1/M5共核直接测试、Ruff、两项L0同静态入口（显式输出X，未改nox/公共脚本）PASS，blocking=0。以清洁源码1550c5f3d73aa647ebc3a523c8a215e1717aec82先登记后一次prepare/四fit/完整四臂已完成负向导航，见§17；尚未源码合入，收益确认/运行启用0，不把研究完成冒称盈利。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-880 | backend/services/advisory_model_first/economic_moneyflow_price_v1.py | backend/tests/advisory_model_first/test_economic_moneyflow_price_v1.py | SOURCE_VERIFIED | none |
| F-881 | backend/services/advisory_model_first/economic_moneyflow_price_source_v1.py | backend/tests/advisory_model_first/test_economic_moneyflow_price_source_v1.py | SOURCE_VERIFIED | none |
| F-882 | backend/services/advisory_model_first/economic_moneyflow_price_v1.py | backend/tests/advisory_model_first/test_economic_moneyflow_price_v1.py | SOURCE_VERIFIED | none |
| F-883 | backend/services/advisory_model_first/economic_moneyflow_price_v1.py; backend/services/advisory_model_first/economic_sector_price_value_v1.py | backend/tests/advisory_model_first/test_economic_moneyflow_price_v1.py; backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py | SOURCE_VERIFIED | none |
| F-884 | backend/services/advisory_model_first/economic_moneyflow_price_pipeline_v1.py | backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py; artifact: §17实际一次完整81D/100估值日四臂 | VERIFIED_RESEARCH_COMPLETED_NEGATIVE | none |
| F-885 | backend/services/advisory_model_first/economic_moneyflow_price_pipeline_v1.py; backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py | backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py; backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py | SOURCE_VERIFIED | none |
| F-886 | backend/services/advisory_model_first/economic_moneyflow_price_source_v1.py; backend/services/advisory_model_first/economic_moneyflow_price_pipeline_v1.py | backend/tests/advisory_model_first/test_economic_moneyflow_price_source_v1.py; backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py | SOURCE_VERIFIED | none |
| F-887 | §2/10/11/15/16 | artifact: 精确八源码/测试加两进度文档、三轮源码自审及48直接测试 | SOURCE_VERIFIED | none |

## 14. Risks / 剩余风险

资金流信息可能由既有成交量/动量完全解释，也可能存在修订/发布时间偏差、订单金额分类漂移和多实验选择偏差；当前完备性不消除它们。不通过额外门禁阻止策略包消费，但诚实保留UNKNOWN和NAV证据边界。加入M6路由改变当前源码hash，不改变旧M1/M5冻结模型身份/结果；旧研究源收据仍指向实际当时提交，不能冒称新代码重验过旧研究。

## 15. Rollout / Rollback / Production Gates

本设计及源码/研究均离线：API/UI/model binding/profile activation/database/dependency/process=noop，backend_restart_required=false。若以后要接入日频family，另立精确业务交付设计并复用同数学，不借本合同启用。研究失败保留真实终态/独立目录，不能覆盖旧输入、删除失败计数或换合同。

DESIGN-COMPLIANCE-001逐项：文档/源码/研究/收益与运行状态分开，不把子集功能称整体完成；未知与矛盾区分、无silent fallback；事前冻结完整支持/成本/政策/同对照，不结果后放宽；禁止无授权跨模块、新资格/平台/分钟执行/自然等待或旧失败固化。

## 16. 三轮设计自审 / 交付记录

源码第一轮输入/时钟审核：完整原键、每D自己的五交易日、query固定原边界、八金额schema与一次CNY转换；正常NULL/缺日/零分母保留UNKNOWN，金额坏值不静默coerce。第二轮共核/身份审核：M6 recipe固定moneyflow_features/状态/独立model SHA；仅新增共核M6/23路由，M1/M5计算和身份公式不变，成熟监督/test毒化/价格洞直接回归。第三轮编排/交付审核：原19fit+1index的预算锚点与实际M5终态metadata核定、不重新读旧负收益；预登记前不取金额，prepare后精确复用快照、不重查；四fit/partial/QE未知拒绝、source只读依赖hash与X输出分别验证。均为本窗口自审，非独立外审。

第一轮方法/时钟自审：确定三项订单金额分桶而非机构账户；五日金额加权而非逐日比率平均，支持/共同监督/冻结成本及导航门不变，当前DB非vintage不作因果或无修订承诺。补明确每D五日局部边界/早期向过去扩calendar，不以全批最大D容许未来金额。

第二轮来源/复用自审：实际公开normalizer require_all=False仍在本叶先验证八schema，一次金额×10000；仅复用BoundedEntryReadSession及原四臂，不调用QE缓存。真实_common _fit_event尚只支持M1/15和M5/19，已在精确范围登记新增M6/23路由，禁止照旧预算而偷偷清账；旧模型SHA数学不变但源码receipt新旧分开。

第三轮交付/边界自审：初稿矩阵使用未知DESIGN_ACCEPTED状态及把实施pending写入gap字段导致F2失败，修订为已经审核的DESIGN_VERIFIED且明确仅设计交付；不把源码/研究PENDING写成已通过，也不改校验器。验收ID试用四位数字又未被现有校验器识别，最终采用本合同三位F-880～887；编号在本设计内稳定，不自建全仓编号平台。三视角为本窗口自审，不冒称独立外审；本设计无未授权例外。

## 17. 一次真实研究结果 / 2026-10-05

run=`advmoneyflowvalue_a4e4e4d38d461616edbad7be`，正式根为原R2根下独立同名目录，plan SHA=a4e4e4d38d461616edbad7bec568e1cae45f5e252ce00b2a7d07992fd92dec3b，实际fit源码1550c5f3d73aa647ebc3a523c8a215e1717aec82、implementation SHA=0e9f2e02c74c858ac027fd9b9cc162eed39425c46709b363d084129f15ef85c8。先登记再取金额与labels；一次prepare10.328秒/2SELECT/17013股票日，7720键全部保留、7700AVAILABLE/20合法零分母UNKNOWN，没有金额补零或删除。成熟train4031行/214D、validation1591行仅诊断；4physical-fit8.062秒，fit加完整评价21.937秒。拟合前后QE experiment/custom_evo/multi-alpha三公开running路径均0，未提交QE或重训旧研究。

| 指标 | 真实结果 | 边界 |
|---|---|---|
| 完整评价 | 原81D/1620候选/100共同估值日，baseline/rule/matched/candidate四臂 | 同冻结VALUE_REVIEW_5_V1与成本，非指数超额/实盘成交或独立OOS |
| 名义净收益 | candidate17.4101%、baseline21.3220%、matched18.9958%、rule20.5747% | 原基线优于本次两个模型，不回选matched/规则作为winner |
| 两配对增量 | candidate减baseline -3.1318bps/日，95%[-20.8496,13.5986]；减matched -1.2873bps/日，[-11.7031,9.8324] | 两区间跨零，净增量条件失败，NAVIGATION_ONLY |
| 风险 | candidate/baseline/matched MDD -11.6673%/-10.3314%/-10.8796%；最差5%日均candidate -293.6101bps vs baseline -254.3254bps | MDD容忍通过、尾部非劣失败，风险或胜率不能替代净值 |
| 实际干预 | 相对baseline/matched进入不同60/46日；candidate84真实TAKE＋2UNKNOWN控制，matched82＋3 | 干预和真实TAKE条件通过，控制不算模型TAKE |
| 完整性 | 四臂endpoint限制/held-mark问题/未结算均0；新4fit令原累计23fit+1index | 日级端点不是成交证明；SOURCE NON_VINTAGE、native UNPROVEN/原RECOVERED_LIMITED不升级 |
| 结论 | STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION，net/tail失败，一candidate、selected0 | 结束本精确假设，不调参/补旧证据/重跑，不关闭整个日频资金流方向或包消费 |

SOURCE实现/研究完成、负向经济导航与源码合入分别报告；当前无确认/binding/激活/DB写/分钟数据/sealed/服务操作。后续只作本次源码真实进度交付，不以此为旧失败归档项目。
