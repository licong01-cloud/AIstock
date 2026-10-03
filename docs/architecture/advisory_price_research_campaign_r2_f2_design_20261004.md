# Advisory 条件买入价值多模型任务 R2 F2详细设计 v1

日期2026-10-04；48小时预算`2026-10-04 02:36～2026-10-06 02:36 Asia/Shanghai`。用户明确要求单个候选不通过后继续下一个不同模型的设计/实现/验证。本轮是有界多假设开发研究，不是旧候选调参重试、收益确认或生产接入。

## 1. Background / 当前事实与目标

前轮设计#5383、源码#5388已合入，main=`d18dfb14e996ea7100f843bd7a03d1694e9619ef`；H-CONTEXT-VALUE-1四头/四臂负向停止。原386D/7720候选、938股票不变；行业严格知晓3729、未证明3977、当前池外14，非普遍行情缺失。经济确认和ENTRY_VALUE正式启用仍0，不为旧失败补证/重跑/归档。

本轮目标是连续验证多个实质不同的条件买入价值估计器，给出可为空/多段的建议买价集合，允许不推荐；不预测开盘价覆盖、不研发分钟执行。单模型STOP只结束该lineage，继续剩余预定路线；整个任务不因第一次失败而结束。全部结果只NAVIGATION_ONLY，正结果仅触发另立独立确认设计。

## 2. Scope / 写入登记

设计树`advisory-price-research-campaign-r2-20261004`经PR #5395合入并官方清理后，源码在最新main独立树`F:/Dev/AIstock_worktrees/advisory-price-models-r2-20261004` / `feat/advisory-price-models-r2-20261004`操作以下精确文件；只清理本任务自身树，不操作其它树：

- docs/architecture/advisory_price_research_campaign_r2_f2_design_20261004.md
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md
- backend/services/advisory_model_first/economic_price_campaign_contracts_v2.py
- backend/services/advisory_model_first/economic_price_campaign_models_v2.py
- backend/services/advisory_model_first/economic_price_campaign_inference_v2.py
- backend/services/advisory_model_first/economic_price_campaign_pipeline_v2.py
- backend/services/advisory_model_first/economic_price_campaign_evaluation_v2.py
- backend/tests/advisory_model_first/test_economic_price_campaign_contracts_v2.py
- backend/tests/advisory_model_first/test_economic_price_campaign_models_v2.py
- backend/tests/advisory_model_first/test_economic_price_campaign_inference_v2.py
- backend/tests/advisory_model_first/test_economic_price_campaign_pipeline_v2.py
- backend/tests/advisory_model_first/test_economic_price_campaign_evaluation_v2.py

新增模型走同一最小纯接口/不可变研究链，不复制旧五叶平台，不修改任何旧模型/产物。临时规划/runner/cache/log在任务专属X目录，正式新研究在`F:/Dev/AIstock_model_artifacts/advisory_price_research_campaign_r2_20261004`。本轮设计/源码通过多轮审核可按已有授权提交PR、合入及仅自身官方清理。

## 3. Non-goals / 许可边界

不修改QE/Selection/HMM/StrategyPackage/公共数据/Execution/Paper/CI；不提交QE训练，不重选候选、不改原排序。只读消费活动profile/冻结数据，禁止数据库写入、DDL/DML、数据激活、依赖安装、服务或他人进程控制。后端重启仍user-owned，本轮纯离线合入不需重启。sealed/新holdout/自然确认不读，研究不转成生产binding。

本轮已消费窗口上比较多个算法不会产生独立OOS；不把正点估计当收益承诺，不为外部文章/SOTA或更多算力制造alpha。旧P0排名同族保持冻结，本轮仅条件价格价值角色，不启动上游因子/包研究。

## 4. Architecture / Allowed APIs / 复用与来源

复用`value_anchor_sources_v1`验证源plan/窗口授权/价格及12D快照，`build_value_anchor_labels_v1`和VALUE_REVIEW_5_V1标签政策，`replay_shadow_portfolio`完整组合，`publish_stage/read_stage`原子阶段及`AdvisoryResearchTrialRegistryV1`登记。只读消费前轮已发布prepared中的同政策labels/rows时，必须绑定manifest/plan/code/profile hash并验证全部父键；不读取旧模型或旧evaluation，不能仅信目录名。

活动profile显式传入path/sha；当前generation=`20260928-v15-unified-moneyflow1` / release=`qe_hmm_full_v2_20260831`。普通指数股票池/并集合同由现有Advisory消费者处理，当前旧冻结人口不能被当前池筛掉。

本地预构建sklearn1.8.0/scipy1.16.3/numpy2.4.0。官方[GradientBoostingRegressor](https://scikit-learn.org/1.8/modules/generated/sklearn.ensemble.GradientBoostingRegressor.html)、[GammaRegressor](https://scikit-learn.org/1.8/modules/generated/sklearn.linear_model.GammaRegressor.html)及[tree结构](https://scikit-learn.org/1.8/auto_examples/tree/plot_unveil_tree_structure.html)与本地签名核定一致。API数学能力不证明金融收益。使用模型公开coef_/intercept_/estimators_/tree_导出非执行JSON；不读任意pickle/joblib、不采用私有HGB内部序列化。导出推理须与原估计器预测定向parity验证，tree输入按sklearn的float32分裂语义，不擅自改成float64阈值。

## 5. Sequential hypotheses / 预定不同路线

| 研究ID | 业务假设与候选 | matched及唯一对比变量 | 物理fit上限 | 当前放行 |
|---|---|---|---:|---|
| M1 / H-SECTOR-DYNAMIC-2 | D可知板块强弱、个股相对板块偏离的动态信息增量 | 同算法/共同监督/支持，matched去掉该新信息块；数值须独立子设计先冻结 | 4 | 源映射未证明，暂不拟合；不阻断M2～M4 |
| M2 / H-NONLINEAR-PRICE-2 | 12D×真实价格条件的非线性交互能改善进入价值 | 候选GBDT均值/日级下行q10；matched受限加性Ridge/Quantile；同13D信息、标签、支持及监督。不是新信息实验 | 4 | 当前可设计/实现 |
| M3 / H-HURDLE-PRICE-2 | 正/零/负概率与条件盈亏幅度拆开，避免均值掩盖非对称盈亏 | 一个Logistic类别模型×两Gamma幅度；matched直接GBDT净均值；共同GBDT path q10。同信息/人口，唯一变量是价值分解 | 5 | 当前可设计/实现 |
| M4 / H-LOCAL-DISTRIBUTION-2 | 局部可比状态的经验收益分布比全局参数化更稳健 | 候选固定邻域经验Y均值/L q10；matched GBDT Y均值/L q10。同信息/监督，局部支持另报 | 2 + 1 index构建 | 当前可设计/实现 |

按M1只读映射核对→M2→M3→M4串行拟合。M1现有行业分类为220800等六位taxonomy、行情code_map为801xxx.SI；分类知晓不等于指数成分。`sector_data.h5`有3339006表行/OHLC/l2_code_id，不能以成员/名称/字符串推导权威crosswalk。无合格crosswalk只暂停M1源，记录具体缺口，继续其它模型；如果后来具备源，先完成独立子设计及范围登记再实现，不直接增加参数/读取收益选特征。

M2/M3/M4三种可独立路线在首个结果前一起冻结；最多三经济候选、11次物理fit及一次索引构建。预定M1具备独立设计后总上限四候选/15fit；未放行M1不算已生成trial。禁止失败后临时追加seed、期限、risk gate、特征组合或同族变体；真正新增路线需新的明确设计/lineage且占总预算，不能偷偷无限搜索。

## 6. Contracts / Common data / 标签、PIT及支持

原完整候选/日期/排序保留，12D来自真实同核快照，query gap是价格条件，不是预测的未来开盘；D冻结函数只读D及过去，T既定日频观察点只用当时可见raw open/法规状态。T HLC、未来Y/L/成熟终点不能进预测或UNKNOWN mask。身份/hash/未来时钟矛盾硬失败；正常行情/可选分类未知保留逐行，不删证券/日期，不填0或前填报价。

沿父plan精确窗口：train2024-07-04～2025-05-30、validation2025-06-03～2025-09-30、已消费test2025-10-09～2026-02-02、label截止2026-03-10。这些是本登记实例，不是公共模块默认值。split按D，监督按真实label_information_end purge，test不拟合/校准/选点。validation仅诊断，不early-stop/反调模型或阈值。

政策统一VALUE_REVIEW_5_V1、原Top5入/Top40复评退出、五次有效复评、5固定槽、2次退出rank确认、5替换预算、A股T+1/停牌/涨跌停延期；止损/止盈/移动止盈0。Y/L为冻结退出值/日级path mark相对D元参考×T坐标的gross ratio；不是盘中最低价、任意价格限价成交或组合uplift。费用buy0.95/sell5.95bps扣一次，现金0。行情/endpoint或持仓mark未证明则全组合经济BLOCKED，不能删未完成episode凑收益。

M2～M4不依赖分类，其共同**全局价格支持**来自无标签train完整12D且T≤train_end的合法观察：100bps桶，每桶≥30观察/≥5个D，桶真实min/max交训练总体gap2.5%～97.5%，支持洞保留。这是新三模型在本次设计前冻结的共同合同，不是放宽前轮按行业支持继续跑；原H-CONTEXT-VALUE-1结果/参数不改，不跨人口宣称改进。各比较只在同一新run中做matched。

模型监督为完整12D、合法价格及Y/L AVAILABLE且label-end未越split、共同全局支持。共同train最低100行/20个D，不足仅暂停该模型，不改样本。M3概率/幅度的子监督由预定标签定义在train内固定，两臂评价人口一致；M4只从该train存训练库，不能把validation/test结果加库。scaler均值/std只由共同train监督拟合，常数scale1明确；不是未知填充。13原变量一起存scaler；M2加性basis标准化12D但保留原g/hinges，M3 GLM和M4局部使用标准化13D，GBDT用原13D。共同keys/hash、支持/recipe/order/模型身份都登记。

## 7. Fixed estimator specs / 不作网格搜索

共同原始13变量=12D+g(query gap bps/100)。GBDT采用`GradientBoostingRegressor(n_estimators=200, learning_rate=.05, max_depth=3, min_samples_leaf=30, subsample=1, max_features=None, random_state=20261004, n_iter_no_change=None)`；均值loss=squared_error，path loss=quantile/alpha=.1。固定树数，无early-stop、seed平均或超参搜索；预测有限正gross值，非法硬失败，不clip救模型。train-only标准化用于线性/局部模型；树使用同原始信息，模型族处理差异不是信息差异。

M2 matched basis为标准化12D、g和固定hinges k=-3,-1,0,1,3；Ridge(alpha10, solver=svd)预测Y，QuantileRegressor(q.1, alpha.0001, highs/time_limit300)预测L。candidate GBDT使用13原变量，不含类别或新增结果。两个配置各两头、同样监督4fit。

M3在真实已知训练进入价计算net_bps=`10000*(Y*(1-s)/(x*(1+b))-1)`，x=1+gap/10000。类别按精确符号固定为负=-1、零=0、正=1；不引入事后零容差。一个LogisticRegression(C=1, solver=lbfgs, max_iter=500, l1_ratio=0)拟合类别概率；训练未出现零类时其概率为0，并保留固定三类输出合同，不另拟合/改模型。GammaRegressor(alpha=1, max_iter=500, tol=.0001)分别拟合严格正净收益及严格负净收益绝对幅度；各幅度至少30观察/5D，否则停止本候选不换默认常数。零收益行仍用于类别/直接均值/path，但不纳入任何幅度Gamma。EV=p_positive*positive_mean-p_negative*negative_mean，零类贡献0，不能把零类按负类均值扣减。不是只选胜率，概率未独立校准不称已校准置信度。matched直接GBDT net mean；双方共享同一GBDT L q10（一fit）。2 Gamma+1 Logistic+1 mean+1 path共5fit。每个query x的EV换回同价格gross均值，只为公共价格/风险接口；非法EV≤-10000bps或overflow硬失败。

M4存共同train的标准化12D/g及Y/L，固定k=100、Euclidean，按distance及原canonical KEY确定tie，至少20不同训练D且第100邻居距离≤4标准单位，否则局部UNKNOWN。由100邻居取Y算术均值/L经验q.1（linear quantile）；不按收益选择邻居、不用未来目标查库。matched同共同train的两个固定GBDT头；2物理fit+1训练索引构建分别记账。局部支持是模型支持，不扩大全局价格支持、不把无法估计算负收益。

所有模型在每个合法tick重新计算价格条件值，净期望>0且下行参考≤800bps得到多段/空集；价格超支持/局部或源未知返回UNKNOWN不授权买入。不能假设跌得越多越安全。开盘不在建议集合是弃权/不可估计，不是开盘预测失败。生产family/API尚未接入。

## 8. Four-arm evaluation / 一次完整研究及晋级条件

每个run一次原Top5 baseline、固定±300bps规则、matched、candidate；同完整旧人口/日历/成本/政策，Top6～20不补位，空槽现金0。市场可执行性已证明但模型UNKNOWN按事前研究控制保留原Top5动作、贡献单列，不算模型TAKE，不是生产fallback；市场/停牌/法规未知所有臂不买。

冻结开发导航条件沿业务最小效应：candidate减baseline和matched平均日净增量均≥5bps；相对两者实际进入变化均≥12原决策日且≥15%原决策日；真实模型TAKE≥30episode；MDD相对两者恶化≤200bps，最差5%日均值恶化≤20bps。任何条件未过停止该candidate但继续下一预定模型；没有真实模型支持时，UNKNOWN控制的盈利不能让其晋级。所有完整tail估值日配对，不把tail当干预分母。

moving-block bootstrap固定block5/reps2000/seed20261004；只是开发区间，不是独立显著性。上述数值门是开发导航筛选，不是确认功效或激活门；不把非显著误称不可学，不因3条路线中最佳而免除独立确认。跨模型试验登记共享campaign_id/lineage和累计数量；当前开发结果不得报告经DSR/PBO确认（累计模型/研究选择尚不充分）。三路线可比较导航结果但不能按最大回测净值宣布赢家；正开发候选仅供独立确认设计。最优风险不能替代预登记收益，不放宽旧合同、不重复在同frontier选点。

## 9. Registry / 原子链、预算与重试

每模型拟合前原子预登记完整plan/数据refs/recipe/support/code/scope/物理fit预算，stage=preregistered→prepared→trained→evaluated；旧父快照只作为数据，不改旧实验。reuse准备也要新plan身份并核hash/source policy；prepare不可偷读旧evaluation或模型。

每次物理fit前fsync记STARTED，fit_attempt持久防隐式重试；失败不改identity再跑同fit，必要正确性修复改变源码/模型后必须新attempt/lineage，保留失败计数。完整已发布stage exact retry只读，不重复拟合/评价；hash/文件额外/跨run漂移拒绝。三种模型recipe可同一最小模块，但plan强制model_id/参数枚举，不能任意kwargs。

单run≤7720候选/500000价行/2GiB RSS/2GiB新工件/30分钟fit；四个累计fit预算≤15且M1未放行时≤11。训练2线程，拟合前/后公开QE experiment/custom_evo/multi-backtest状态，运行/未知只暂停fit并继续设计/测试；不停止QE。本窗口回放可在资源允许时并行。长实验半小时检查，不持续busy-poll；短实验完成立即接续，不人为睡够工时。

## 10. Implementation Plan / Timeline / 整轮停止与推进

0～4h核定/统一详细设计和三轮自审；4～14h最小公共合同及M2实现/审核/研究；14～24h M3设计细化/实现/审核/研究；24～34h M4实现/审核/研究及M1源问题再核定；34～42h正候选仅确认设计、全部负则分析真正下一个不同经济假设的可识别性；42～48h剩余必要修复、CI/交付及准确蓝图更新。时间是最大工作预算，训练秒级不代表完整设计/源码已完成；按优先级串行fit，CI等待可做下个不同模型设计/实现，不把负结果变整轮终止。

整轮终止仅：用户停止、48小时到期，或所有预定不同路线均已完成/存在无法绕过的真实阻断且无剩余有价值设计工作。预算不授权无限模型网格，不以耗时为由补证旧失败。提前完成全部路线须说明逐项终态，不能只做第一个便宣称完成。到点不强杀安全进程，报告运行/剩余，后端任何重启必须由用户。

## 11. Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-689 | 单候选STOP与整轮STOP分开，M2/M3/M4预定不同模型连续推进 |
| F-690 | 固定profile/源快照/原键/窗口授权，M1编码/知晓不猜测 |
| F-691 | 同政策Y/L/净盈亏、真实maturity purge、test不拟合/校准 |
| F-692 | 固定参数及共同支持/监督，各模型真实fit/index全部记账 |
| F-693 | 非执行JSON模型parity、价格tick/费用、多段/空集/支持洞 |
| F-694 | 完整四臂、UNKNOWN贡献、固定收益/风险/真实干预门及跨模型开发证据限制 |
| F-695 | 不可变stage/registry、无隐式retry、资源/QE互斥/X临时 |
| F-696 | 精确Advisory范围、重复审核修复、源码/经济/运行分报及用户重启 |

## 12. Verification Plan / 直接合同与复核

test毒化不改变fit/basis/support，训练标签变化允许改变拟合但不得改变无标签价格支持；未来时钟/身份/hash/越界键硬失败；UNKNOWN/池外/缺行情仍保留；全局支持train-only及洞；Gamma两类不足拒绝默认、正/零/负原子不混淆；邻居不含validation/test或按收益择优、tie/日期支持；tree float32及linear/log-link JSON与原公开预测parity；非法数值不clip；同输入单行/批量一致、纯D价格函数不读未来；完整四臂held-mark/endpoint fail-closed、未知控制不算TAKE；partial fit/exact retry无重复，预算超限拒绝。12D训练范围仅诊断，不据测试分布或结果新增训练box门；固定支持/模型条件之外未知缺失必须保留。

三轮设计自审：方法轮修复M3零原子被误算为亏损；时钟/公平性轮澄清支持不能依赖标签，并区分共同全局支持与M4局部可估性；工程/证据轮核对非执行JSON、实际fit计数与开发筛选/确认/启用隔离。以上为本窗口自审，不冒充独立外审；无收益结果用于修订。

各阶段方法、时钟/统计、工程三轮本窗口自审/修订；失败只重跑fix-point，稳定后一次最小相关矩阵。广Advisory回归交CI，不重跑整个model_first、不建新平台或UI来证明未达标模型。F2结构validator只验文档矩阵，不等于模型效果/独立审核。

## 13. Design Acceptance Matrix

源码初稿/三轮本窗口自审及15项定向测试/Ruff已通过；只读来源预检PASS，真实研究尚未预登记/拟合/评价。以下源码态不等于收益/业务完成；端到端四臂真实研究及runtime/经济仍分别报告。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-689 | campaign evaluation/contracts | `backend/tests/advisory_model_first/test_economic_price_campaign_evaluation_v2.py` | SOURCE_VERIFIED | approved_by_user: 整轮研究未完成，单模型停止不结束整轮 |
| F-690 | campaign pipeline/contracts | `backend/tests/advisory_model_first/test_economic_price_campaign_contracts_v2.py`；只读预检 | SOURCE_VERIFIED | approved_by_user: M1映射未证暂停，旧native限制保留 |
| F-691 | campaign models/pipeline | `backend/tests/advisory_model_first/test_economic_price_campaign_models_v2.py` | SOURCE_VERIFIED | approved_by_user: 仅开发消费，未读sealed/确认 |
| F-692 | campaign models/pipeline | `backend/tests/advisory_model_first/test_economic_price_campaign_models_v2.py` | SOURCE_VERIFIED | approved_by_user: 真实fit尚0，11预算及1索引不变 |
| F-693 | campaign models/inference | `backend/tests/advisory_model_first/test_economic_price_campaign_inference_v2.py`；JSON parity tests | SOURCE_VERIFIED | approved_by_user: 纯价格函数非任意限价成交/生产接口 |
| F-694 | campaign evaluation | `backend/tests/advisory_model_first/test_economic_price_campaign_evaluation_v2.py` | SOURCE_VERIFIED | approved_by_user: 完整真实四臂研究尚未执行 |
| F-695 | campaign pipeline | `backend/tests/advisory_model_first/test_economic_price_campaign_pipeline_v2.py` | SOURCE_VERIFIED | approved_by_user: 研究需公开QE空闲检查，DB/进程未操作 |
| F-696 | §2/3/10/12/14 | artifact: 本设计范围与三轮自审 | SOURCE_VERIFIED | approved_by_user: 经济确认/启用0，服务重启user-owned |

## 14. Rollout / Rollback / Production Gates / 设计符合性

仅离线研究，backend_restart_required=false，DDL/DML、依赖、数据/运行激活、binding均noop；失败只停止新模型，不覆盖旧权重/策略包/产物。未来生产接入另立独立合同并等待必要用户重启。

DESIGN-COMPLIANCE-001：①完整研究与未完成生产/经济分报，不把partial当目标完成；②UNKNOWN/身份/未证明端点及求解失败不伪成功；③用户本次已明确扩展连续多模型范围，旧负实验及门不改判，新增全局支持须在本轮首个结果前冻结；④无虚构行业窗口/新平台审批/等待自然20日门，QE训练互斥仅保留用户已有边界。

## 15. Risks / 限制

同一开发窗口的多模型选择仍有研究者偏差；不能用三候选最大收益或开发区间作为独立确认。全局价格支持不保证12D任意状态可学；局部模型有稀疏支持，概率头未证明校准，退出标签仍是政策条件结果而非盘中最优价。新数据映射未证只暂停M1；源/求解/序列化不合格暂停对应run，不用默认值救结果。研究控制与真实TAKE分报，不能把未知基线持仓的收益归给模型。本轮仅开发导航，不承诺收益或生产激活。

## 16. 源码审核接续（拟合前）

方法轮：概率/零原子/幅度和gross-net代数、共监督及无标签支持；修正Gamma数值下溢须失败不填值。时钟/身份轮：test毒化、实际maturity、JSON浮点32边界、局部日期/距离支持、D输入与T开盘隔离；新标签仅复用原prepared数据，不读旧模型或评价。工程轮：核定部分fit禁止隐式重试、原子阶段hash、profile projection绑定、真实fit/索引分别记录、累计预算与X目录。一次测试失败来自错误预期异常类型，修正为已有typed Advisory错误后定向通过；随后15项完整最小矩阵通过（2.88秒），Ruff/F2通过。测试中的合成拟合不是商业研究trial，本轮真实研究fit仍0。

设计PR #5395已合入`1b87c68fe9776b7ed7a0be2a43fca7b58e42cac2`，CI37146034669 SUCCESS，设计树官方cleanup_done。源码树从之后最新main新建，main同步带入其它窗口代码不是本窗口修改。只读来源预检profile `20260928-v15-unified-moneyflow1`、原父/12D/同政策标签均PASS；M1仍缺crosswalk。正式拟合须清洁提交源码、三plan在首结果前登记、公开QE状态确认，无新生产接入/重启需求。
