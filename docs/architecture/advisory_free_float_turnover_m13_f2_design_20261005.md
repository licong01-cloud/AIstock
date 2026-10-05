# Advisory 自由流通换手状态 M13 F2详细设计

2026-10-05；DESIGN_VERIFIED_IMPLEMENTATION_PENDING / EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY。

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
| F-940 | §3/4自由换手与局部依赖；economic_free_float_turnover_v1.py | backend/tests/advisory_model_first/test_economic_free_float_turnover_v1.py 手算/零/依赖/单位/大数/毒化 | DESIGN_VERIFIED | none |
| F-941 | §3原候选时钟/只读；economic_free_float_turnover_pipeline_v1.py | backend/tests/advisory_model_first/test_economic_free_float_turnover_pipeline_v1.py 参数化单SELECT/事务/原KEY | DESIGN_VERIFIED | none |
| F-942 | §5共同13/16；economic_sector_price_value_v1.py | backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py 成熟/test poison/JSON identity | DESIGN_VERIFIED | none |
| F-943 | §6实际47→51；economic_moneyflow_price_pipeline_v1.py | backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py M12四head/旧47/新51/partial | DESIGN_VERIFIED | none |
| F-944 | §7四臂NAV；economic_sector_price_pipeline_v1.py | backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py TAKE/UNKNOWN/干预/结算 | DESIGN_VERIFIED | none |
| F-945 | §9/10多轮审核精确交付旧兼容 | test: backend/tests/advisory_model_first/test_economic_free_float_turnover_pipeline_v1.py；artifact: 精确scope、F2/Ruff/L0/原bundle兼容收据 | DESIGN_VERIFIED | none |
| F-946 | §2/6/8/10授权/资源/QE互斥 | backend/tests/advisory_model_first/test_economic_free_float_turnover_pipeline_v1.py；fit前后X:/AIstock_temp/advisory/daily-price-delivery-18h-20261005/m13-qe-idle-pre.json及post.json收据 | DESIGN_VERIFIED | none |

## 12. Current execution state

仅设计、数据源字段语义与0trial只读聚合工程检查完成；M13 source/正式预登记/prepare/fit/收益均0。实际仍47fit+1index、sealed/经济确认/激活0，M12源已合入/清理。不把7330非空几何当数值验收、原生身份或研究有效；正式prepare再进行实际值校验，普通缺失保留。源码实现必须等本设计交付后进行。

## 13. 三轮设计审核与修订

信息/经济轮核对真实M9/M11字段只是相对量权重，不含自由流通股本分母；按官方来源把percent除100、万股log加log10000写成固定公式，避免把自由流通与无限售流通混为一谈，不承诺新alpha。时钟/依赖轮明确仅D股本消费、前19预热仍可保留D规模、历史股本毒化与未来宽源先投影；盘后D声明与非vintage/native UNPROVEN分开，不补捕获时间或重验QE包资格。预算/业务轮按真实原评价核实尾部是最差5%而非10%，已修正；固定47实fit→51显式扩展/真实M12阶段及零partial隐式重跑；空名单0SQL、源值校验与非空几何、工程/研究/激活分开。

初次F2发现章节/矩阵标识及可验证测试引用不足，已补充正式Contracts/Implementation/Verification/Production gates/Index/Matrix标题、精确F-940～946与直接测试/收据路径；修订后7/7、warnings0 PASS，未把设计检查当源码完成。本窗口分视角重复自审而非独立外审；最终diff/精确两文件scope与currentHEAD CI分别通过后才交付，不接旧负候选补证项目。
