# Advisory 自由流通换手状态 M13 F2详细设计

2026-10-05；RESEARCH_COMPLETED_NEGATIVE_SOURCE_DELIVERY_PENDING / EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY。

## 1. Background / Goal

M12一次prepare/4fit/完整四臂已完成，candidate/baseline净收益4.8425%/21.3220%，配对日−14.8813bps，只有收益增量条件失败；仅停止本candidate，不调阈值/重跑或接family。源码#5465 HEAD0f0e8c65a、CI37251660467 SUCCESS后已合入1f85b44ff658d1a93ea00940151deef38bfafca7并自己official cleanup_done。当前实际47 physical fit+1原index，无经济确认或激活；M1六UI及BUG-1726公共smoke仍是独立工程辅线。

H-FREE-FLOAT-TURNOVER-1：相同OHLC及按本股历史总量归一的量价轨迹，可以对应不同自由流通股本和绝对换手强度。新增自由流通换手20session均值/波动和D自由流通股本规模，是否改善原价格条件化净价值？M9/M11按调整成交量的相对权重或价格分布计算，不含自由流通分母；旧12D量比也不是绝对自由换手。不是上游alpha挖掘、任意换loss/seed、实际持有人筹码或跨包通用权重。不叠加旧负块。

事前0trial工程spec de29a8d10822e3c90b67cdf7933562cbec10b0a9343087f528e104fde9db1825，只读一次聚合非空键检查1.438秒：386D/7720原候选、38168请求对/38158存在键，两个字段非空各38158；完整依赖7330、预热380、正常缺行10，D股本非空7720。首次连接未加载既有.env而失败于认证前，0SELECT；按原Advisory脚本显式读取F:/Dev/AIstock/.env后上述一条SELECT成功，不打印凭据、不改配置。没有源值/标签/收益/fit读取，非空不等于数值合法或有效信号。

## 2. Scope / Non-goals

设计独立树从最新origin/main创建，仅本文和advisory_strategy_conditioned_model_blueprint_v1_20260710.md；多轮自审修订/F2/当前HEAD CI绿后交付及自己官方清理。实现随后最新main独立树、事前登记精确12文件：

- backend/services/advisory_model_first/economic_free_float_turnover_v1.py：纯特征/plan/模型薄入口。
- backend/services/advisory_model_first/economic_free_float_turnover_pipeline_v1.py：精确有界只读daily_basic、冻结prepare与薄阶段委托。
- backend/services/advisory_model_first/economic_moneyflow_price_pipeline_v1.py：仅显式free_float_extension/真实M12前驱/累计51预算；旧默认不改。
- backend/services/advisory_model_first/economic_sector_price_value_v1.py：仅M13字段/identity/status路由；旧模型数学/hash公式不改。
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py：仅M13/51固定fit路由。
- backend/tests/advisory_model_first/test_economic_free_float_turnover_v1.py
- backend/tests/advisory_model_first/test_economic_free_float_turnover_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py
- 本文、上述蓝图。

不修改M1 daily/API/UI、generic/core、旧label/支持/政策/成本；不修改QE/Selection/HMM/StrategyPackage/公共数据/行业/Execution/Paper/CI/工作流/AGENTS。不提交QE任务，不重建候选/池，不补数、写数据库/DDL/DML、激活profile/模型/配置、安装依赖、控制服务或他人进程。后端重启user-owned。X临时、F独立持久；最多本价格主线及必要工程辅线。

## 3. Architecture / Contracts / Data source / Clock

唯一新增来源market.daily_basic，字段trade_date、ts_code→instrument、turnover_rate_f、free_share。只消费D及以前按原calendar请求的20原session/instrument精确键集，最多154400请求对/返回行，单SELECT/30秒，参数化unnest JOIN+BETWEEN+LIMIT(requests+1)，BoundedEntryReadSession真实REPEATABLE READ/readonly/autocommit=false，退出rollback/close。缺行NULL保留原候选；重复/外来键/schema/已知非法数为真实计算错误，不伪UNKNOWN。空原名单不查询；失败不自动重试或用别源替换。

字段及单位已按仓库daily_basic映射与[Tushare官方每日指标](https://tushare.pro/document/2?doc_id=32)核对：turnover_rate_f是自由流通换手百分数，free_share为万股。只读现有数据库，不调用Tushare接口或补齐；不调用公共fetch_fundamental的moneyflow/bak或全池读取。D日信息为盘后合同，不能解释为D盘前已知；本来源当前库历史NON_VINTAGE，不证明原capture/known_from，native仍UNPROVEN、不可部署。不因旧vintage未知新增策略包资格门。

沿原frozen Top20/386D/7720 KEY、D-T邻接、rank/order、manifest/池/package/policy/cost/profile身份。纯free_float_turnover_rows_v1(*, candidates, basics, calendar)，候选仅KEY，输出原KEY/顺序+三字段/free_float_feature_status/free_float_feature_visible_through/free_float_unknown_fields规范JSON。宽源先按实际消费对投影，未来/无关毒化不影响早D；不压缩停牌session、不删日期/股票、不填0/前填。完整候选与D价格/价值标签仍来自原冻结来源，M12 evaluated只作预算/阶段前驱，不重跑它或把旧implementation时钟作资格门。

## 4. Fixed new information / Numerical semantics

设20原session各自由换手百分数r_i，f_i=r_i/100，D自由流通万股s_D。三个固定字段：

| 字段 | 唯一公式 | 必要依赖 |
|---|---|---|
| free_float_turnover_mean20 | mean(f_i)，无量纲日换手fraction | 全20原session的turnover_rate_f |
| free_float_turnover_std20 | population std(f_i)，ddof=0，同单位 | 同上，不是年化波动 |
| log_free_float_shares_D | log(s_D)+log(10000)，自然对数股数 | 仅D free_share；历史free_share不消费 |

自由换手可超过100%，不凭经验clip/winsorize或设上限；NULL/正常NaN是UNKNOWN，已知必须真实numeric非bool/字符串/inf。turnover>=0，free_share>0；超大整数/Decimal转换溢出typed ValueError。已知零换手均值/std为合法0，股本0不是NULL。均值先按20分摊、std按最大值缩放，再还原，避免合法大数中间溢出；log不先乘10000。派生非有限仍计算错误。

前19session不足时两历史量UNKNOWN_20D_HISTORY，D股本仍可独立已知；历史换手缺失只使前两量UNKNOWN_FREE_FLOAT_SOURCE，D股本缺失只使第三UNKNOWN。不验证未消费历史free_share，避免无关毒化阻断。块AVAILABLE仅三字段已知，否则保留逐字段原因与真实known值；模型自身不支持该块则该股UNAVAILABLE，研究UNKNOWN控制单独计账，不删股或全名单冻结。

## 5. Model / Policy / Train-test boundary

matched原12D+g=13，candidate原12D+三个新字段+g=16；两臂共同成熟监督，同GBDT200/lr.05/depth3/min_leaf30/subsample1/maxfeaturesNone/seed20261004，无earlystop。各mean及path-min q10共4 physical fit。只原training_eligible、values_available、label_information_end<=train_end、原12D/新块/合法g有限共同集合，最低100行/20D。validation仅诊断；test禁止fit/校准/选点，缺失状态不包含Y成熟度。

沿VALUE_REVIEW_5_V1：五有效复评而非五交易日、Top5/Top40退出、同原label、费用buy.95/sell5.95bps一次、T+1/停牌/涨跌停延期。支持仍原train12D已观测g，不被新块/label/test缩样；100bps桶>=30观察/>=5D、2.5～97.5%与实际支持洞。原完整合法tick、多价格段、净期望>0/path downside<=800bps、float32 JSON树parity不变。父实例train2024-07-04～2025-05-30、val06-03～09-30、已消费test10-09～2026-02-02、label截止2026-03-10仅本study，不是QE全局硬编码。本新块不是直接规则/买点或订单，非通用价值label的替代。

## 6. Registry / Budget / Atomic stages

campaign=advisory_free_float_turnover_v1_20261005，model_id=M13，schema=economic_free_float_turnover_v1，experiment_id=advfreefloatvalue_+plan_sha前24位。budget_anchor为原M2同campaign_root，predecessor_manifest_ref role=free_float_predecessor为真实M12 evaluated。相同source/policy/cost/root与原47实际+新4 cap51；显式free_float_extension要求session_path_extension/M12四heads完整及原子阶段链，旧23/27/31/35/39/43/47默认不改并拒绝M13事件。不是包准入或全局训练冻结。

先typed plan dump-revalidate预登记（model_copy绕过literal必须拒绝），绑定完整实施闭包SHA/参数/profile，后prepare一次只读取数。新F hash目录basic_daily.parquet/source receipt/rows/preparation原子publish；已有prepared exact retry仅校验manifest、不重查库。trained/evaluated同核原子委托，partial_attempt不隐式再fit，工程fit前后QE三公开running各0；没有实际训练则不报已启动。fit1800秒/RSS2GiB/artifact2GiB上限，旧index不新增。仅真正新信息一次候选；跨轮已消费开发证据，不按独立OOS或DSR选择校正完成解释。

## 7. Full evaluation / Evidence boundary

原81D/1620全部候选、100共同估值日四臂baseline/rule/matched/candidate，五等槽空置现金0、无Top6补位/动态资金仓位。T实际open仅观察已有D价格集合，不把T日涨跌/收盘输入模型。原政策模拟与成本不变；所有episode结算/held-mark/endpoint检查，未知控制与真模型TAKE/收益分账。实际干预T映射原D、报告81分母，不当独立样本。

原NAV分类：对baseline及matched配对日增量均>=5bps、真实模型TAKE>=30、干预>=12D且>=15%、MDD恶化<=200bps、最差5%日均值恶化<=20bps；固定block5/2000resamples/seed20261004描述区间。缺一则STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION，继续下一真正不同路线；不回选控制或放宽旧合同。正NAV也仅制定独立确认设计，不读sealed/新holdout、不自动激活；多轮共享开发数据的正点估计不能当收益证明。股本信息可能只是size效应，若正须后续冻结归因/残差/独立确认，不能冒称新alpha。

## 8. API / UI / Production gates

本切片仅离线研究与价格数学，不新增daily family/API/UI、调度/binding、资金仓位或数据库迁移，无后端重启需求。部署与模型/数据/运行配置激活不在授权内；M1新接口和BUG辅线独立保留，不借本源码/CI作为其UI或公共smoke通过。通用输入已交付，但独立通用label口径待产品选择，本M13不默选新目标。

## 9. Verification plan / Design Acceptance Index / Multi-round review

定向新两叶+三个明确改动路由/预算测试，覆盖手算百分数/股单位与std，OHLC相同/自由股本或换手不同的新信息，零换手合法、NULL依赖局部/预热/停牌、历史share无关毒化、未来投影、重复/错T/空schema、已知负/零share/bool/string/inf/巨大值、稳定大数；真实BoundedEntryReadSession的参数化单SELECT/readonly/rollback/close/schema/dup/foreign/timeout、冻结prepare无重查/partial不再fit/QE未知、共同成熟13/16及test poison、identity/节点/Dclock、47默认拒绝51显式只认可M12真实前驱。

至少三视角本窗口自审：合同/时钟，数据/数值/实现，预算/业务/旧兼容；不冒充独立审核。失败节点修复后定向重跑，稳定后一次最小矩阵；Ruff、diff/scope、F2验收及L0精确扫描，广回归交当前HEAD必需CI。原M1冻结JSON bundle兼容最小只读核定模型hash/非部署等级，不读market/labels或重拟合。所有设计项逐条DESIGN-COMPLIANCE-001；设计PASS不冒充实现、利润或上线完成。

## 10. Implementation plan / Rollout / Rollback / Risks

设计PR→当前CI绿/自己清理→独立源码树精确12文件→多轮审核/单次最小验证→预登记/prepare→QE idle一次4fit/完整四臂→事实更新蓝图→当前HEAD CI绿后授权交付/自己清理。无需修改原产物或数据库；不自动激活即无runtime rollback，撤回只移除未启用调用不破坏旧family。正常缺失保留UNKNOWN，来源差异/超预算/毒化/钟错则停止本计算，不能用当前全池/别date回填。本段原18h终点2026-10-05 18:08、原48h终点10-06 02:36均不重计，不凑时长/重复旧研究。

## 11. Design Acceptance Matrix / Index

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-940 | §3/4自由换手与局部依赖；economic_free_float_turnover_v1.py | backend/tests/advisory_model_first/test_economic_free_float_turnover_v1.py 手算/零/依赖/单位/大数/毒化 | SOURCE_VERIFIED | none |
| F-941 | §3原候选时钟/只读；economic_free_float_turnover_pipeline_v1.py | backend/tests/advisory_model_first/test_economic_free_float_turnover_pipeline_v1.py 参数化单SELECT/事务/原KEY | SOURCE_VERIFIED | none |
| F-942 | §5共同13/16；economic_sector_price_value_v1.py | backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py 成熟/test poison/JSON identity | SOURCE_VERIFIED | none |
| F-943 | §6实际47→51；economic_moneyflow_price_pipeline_v1.py | backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py M12四head/旧47/新51/partial | SOURCE_VERIFIED | none |
| F-944 | §7四臂NAV；economic_sector_price_pipeline_v1.py | backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py TAKE/UNKNOWN/干预/结算 | SOURCE_VERIFIED | none |
| F-945 | §9/10多轮审核精确交付旧兼容 | test: backend/tests/advisory_model_first/test_economic_free_float_turnover_pipeline_v1.py；artifact: 精确scope、F2/Ruff/L0/原bundle兼容收据 | SOURCE_VERIFIED | none |
| F-946 | §2/6/8/10授权/资源/QE互斥 | backend/tests/advisory_model_first/test_economic_free_float_turnover_pipeline_v1.py；fit前后X:/AIstock_temp/advisory/daily-price-delivery-18h-20261005/m13-qe-idle-pre.json及post.json收据 | SOURCE_VERIFIED | none |

## 12. Current execution state

本段立项检查点只完成设计与0trial非空键聚合，源码/正式预登记/prepare/fit/收益当时均0。设计#5467 HEAD0294872f3/CI37252593479 SUCCESS后合入7c024171f7527ed9079f0c569e38b4da0bf4917c并自身清理，之后独立源码树事前登记12文件。从93c600814最新main同步仅其他模块已交付差异，本窗口没有修改那些文件。正式研究前新五叶实现/五直接测试已通过稳定52项、Ruff与两L0，当时无正式prepare或研究fit；实际当时47fit+1index、sealed/经济确认/激活0，M12源已合入/清理。7330非空几何不是数值/收益/原生验收，实际源值将仅在正式prepare校验。

## 13. 三轮设计审核与修订

信息/经济轮核对真实M9/M11字段只是相对量权重，不含自由流通股本分母；按官方来源把percent除100、万股log加log10000写成固定公式，避免把自由流通与无限售流通混为一谈，不承诺新alpha。时钟/依赖轮明确仅D股本消费、前19预热仍可保留D规模、历史股本毒化与未来宽源先投影；盘后D声明与非vintage/native UNPROVEN分开，不补捕获时间或重验QE包资格。预算/业务轮按真实原评价核实尾部是最差5%而非10%，已修正；固定47实fit→51显式扩展/真实M12阶段及零partial隐式重跑；空名单0SQL、源值校验与非空几何、工程/研究/激活分开。

初次F2发现章节/矩阵标识及可验证测试引用不足，已补充正式Contracts/Implementation/Verification/Production gates/Index/Matrix标题、精确F-940～946与直接测试/收据路径；修订后7/7、warnings0 PASS，未把设计检查当源码完成。本窗口分视角重复自审而非独立外审；最终diff/精确两文件scope与currentHEAD CI分别通过后才交付，不接旧负候选补证项目。

## 14. 三轮源码审核及预研究核实

合同/时钟轮逐行核对唯二新基本字段、原20session/D-T及D股本局部消费；零换手合法、NULL只UNKNOWN依赖字段，future/unconsumed先投影，研究T观察不进入D。数值/实现轮核对大数均值/std与log单位、原KEY顺序；初50通过/2失败均为新测试fixture的名称和JSONL换行错误，按失败节点修复后2/2通过，再稳定52直接项/Ruff PASS，未修改业务数学救活试验。预算/业务轮确认实际M12前驱、旧所有默认预算保持、partial不再fit、typed plan篡改拒绝和readonly事务rollback/close；原M1真实JSON bundle模型hash872acff3894c7a64b1b87c51ebd440d739a82069d68be0e30ea27dee9c81931e不变、native UNPROVEN/nondeployable，0fit/market或label数组/DB/激活。

L0 feature0finding，standard3个P2复杂度告警/0阻断；两项是未改旧M6/M1 join，一项新KEY一对一merge：原7720行、source最多154400且实际请求38168对、每候选至多20session，O(source+7720×20)有界，无Cartesian放大/逐日SQL。不因该warning跨范围重构旧代码。上述为研究前检查点；随后干净producer/实施闭包绑定的一次研究已完成见§15，未重跑旧结果，不把单元fit记为research fit。

## 15. 一次真实研究结果与当前断点

实际producer4f37f85d8bac9bb5f0d12ee30f00c741f546c542，plan ab0773ce59d14dff25a2c80f1d97cf325e125f3313bc057f87cdb63340937c14，实施SHA0eb56222d3250f646372add7ff8aa39b8f09e03e9e75ebdc12ee7864881d22ef，run advfreefloatvalue_ab0773ce59d14dff25a2c80f。提交前diff检查发现三新文件EOF空行，首次commit未成功、源码非干净导致预登记在发布/registry前拒绝，0fit；已仅修格式后干净提交，不补造source receipt、不称两个研究候选。随后一次真实预登记及prepare7.453秒：单只读SELECT，38168精确请求/38158冻结basic行，原7720/386D保留7330AVAILABLE/380预热/10正常UNKNOWN，nonvintage/native UNPROVEN不升级，0DB写。

研究前2026-10-05T01:59:08UTC、完成后02:00:41UTC三公开QE running均0；四fit及完整四臂21.782秒，3693成熟train/195D，1591validation只诊断、test未训练/校准；candidate16/matched13，累计真实51 physical fit+1原index。原81D/1620候选与100共同估值日全保留。

| 指标 | candidate | baseline | matched | rule |
|---|---:|---:|---:|---:|
| 成本后100共同日名义净收益 | 12.0020% | 21.3220% | 4.5719% | 20.5747% |
| MDD | −8.8667% | −10.3314% | −11.8944% | −9.4780% |
| 最差5%共同日均值bps | −248.9084 | −254.3254 | −282.2307 | −254.3254 |
| 完成episode | 85 | 91 | 83 | 91 |

candidate减baseline配对日−8.1577177bps、95%描述性block区间[−31.2631519,12.8105619]；减matched+6.7015410bps、区间[−6.4012411,21.0360766]。只net_increment=false，干预/MDD/真实TAKE/tail四项true；风险改善/对matched正点估计不能替代基线收益条件。80真实model TAKE/5UNKNOWN研究控制，85episode全结算，无endpoint/holding/unsettled限制；baseline原进入跳过26笔中盈利/亏损各13，仅诊断不事后改阈值。58/54干预T逐日映射回原D、分母81，不当独立58/54样本。

结论STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION，entire_campaign_stopped=false/continue_next=true；仅结束这个固定新信息候选，不换seed/损失/阈值、回选控制、扩大窗口补证、再fit/确认或接新family。不证明策略包无alpha或所有价格模型无效；原开发窗口共享、自适应研究和非vintage限制仍在，未读sealed/新holdout。源码交付当前HEAD CI后合入与自身清理；source、研究、经济确认、运行态分报，经济确认/activation0。原goal持续下一有价值路线及M1工程辅线，原截止不重计。
