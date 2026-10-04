# Advisory 市场宽度历史状态条件买入价值 M10 F2详细设计 v1

2026-10-05；EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY。设计与研究各自报告；不作QE因子、HMM模型、资金仓位或分钟执行。

## 1. Background / 唯一假设与真实断点

R2的M2/M3/M4/M1/M5/M6/M7/M8/M9实际完成35 physical-fit+1历史index。M8 #5454/#5455与M9 #5456/#5457均合入及自身官方清理，最新main=4f7793a0f。M9成本后25.9681%高于原基线21.3220%，但配对日+3.9776bps低于事前5且区间跨零，仅停止该candidate，不调参救活。M1 #5445已同步main后push d1ccaae19，43本叶测试、两份F2及L0通过，新CI37240066029待终态；六UI和BUG-1726公共端点smoke仍独立待交付，不重复原20日业务批量或旧研究。

H-DAILY-BREADTH-STATE-1：当日market_up_ratio相同的两个D，过去20个原session中上涨股票占比的平均水平、修复趋势和弱宽度持续性，是否提供不同的条件买入净价值？原12D只有当日宽度，M8只有指数价格时序风险/个股beta；本块增加宽度历史，不是替换loss/seed或将旧隔夜字段换名。candidate仅原12D+本次三字段+g，不叠加负向M5～M9块。

可行性只读原已消费冻结features.parquet的D/instrument/market_up_ratio和原calendar；实际7720键/386D、同D宽度冲突0、386D值已知且[0,1]、原406session日历中决策日连续。完整20session历史367D，首19D无原历史保留UNKNOWN，不补预热。未读收益/label、新窗口或sealed，无数据库访问/fit。临时分析最初列名及numpy整数JSON错误已修正，仅runner错误，不修改数据或成为正式研究次数。

## 2. Scope / 精确文件与顺序

本设计从最新origin/main 4f7793a0f的独立advisory-breadth-state-price-value-m10-design-20261005树建立，仅写本文及advisory_strategy_conditioned_model_blueprint_v1_20260710.md。多轮自审/F2/当前HEAD CI后按已有授权独立合入及自己官方清理；源码之后从最新main独立树事前登记12文件：

- backend/services/advisory_model_first/economic_breadth_state_price_v1.py：纯历史宽度计算、冻结plan及已有同核薄包装。
- backend/services/advisory_model_first/economic_breadth_state_price_pipeline_v1.py：原冻结输入投影/原子prepare、薄阶段编排，不新增SQL。
- backend/services/advisory_model_first/economic_moneyflow_price_pipeline_v1.py：仅显式breadth_state_extension、旧23/27/31/35默认不变。
- backend/services/advisory_model_first/economic_sector_price_value_v1.py：仅M10字段/身份/status路由，不改旧数学/身份公式。
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py：仅显式M10/39cap路由。
- backend/tests/advisory_model_first/test_economic_breadth_state_price_v1.py
- backend/tests/advisory_model_first/test_economic_breadth_state_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py：仅新extension及旧默认回归。
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py：仅M10及旧身份直接回归。
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py：仅39/旧route。
- 本设计和上述蓝图。

复用campaign_sources_v2、原stage/JSON核、共同监督、train/load/evaluate_information_study_v1完整四臂及registry/journal；不复制标签/模拟器或建设新平台。不修改QE/Selection/HMM/StrategyPackage/行业/公共数据/Execution/Paper/CI/公共工作流/AGENTS，不提交QE、重建候选/股票池、写DB/DDL/DML、激活profile/模型/运行配置、安装依赖或控制用户服务/他人进程。X临时、F新独立正式工件；后端重启user-owned。

## 3. Architecture / Contracts / 原输入与宽度来源合同

沿原父plan/12D prepared/value-label prepared/profile/包/股票池/policy/cost/原7720 KEY、rank与顺序。原冻结features文件SHA=cbe54ede6f5de28dc35484295cf4372772616ed3d2f10192738631ff527e1bf7；仅投影decision_as_of_trade_date、target_trade_date、instrument、market_up_ratio、feature_visible_through，不读取额外列或新的市场时点。原calendar.json SHA=3cab62e37b39326eb18591fa9dddf2d5b60a25aec3572bb5eefe89ce8c4da776，不改日历或以有数据的日期压缩窗口。

同D的原20个重复市场值仅去重为一条真实D记录，先验证原候选唯一、D/T相邻、实际原键和日期声明一致，同D数值/可见时钟必须一致；不能任取first掩盖冲突。market_up_ratio沿原core来源及分母，不称重新复现全市场股票池、与任意指数池同分母或原生capture。已消费开发数据、非vintage及native UNPROVEN限制保留，不新增包资格门。

已知值须为有限数值[0,1]，bool、坏数/已知非有限、负值或>1、重复键、冲突/外来D/T、未来可见时钟均正常计算错误。普通NULL、缺原session市场值或不可得可见时钟是UNKNOWN，保留原候选；不前填/补零/删日、不重新查询数据库。源输入可存较晚D，纯函数先投影本批最大请求D及各自原20session的实际消费键，再检查这些键的市场值和时钟；不能先校验/聚合未被本批消费的未来值而改变早D结果。原文件hash/结构校验不是行情数值读取。每个候选自身只访问u≤D且visible_through≤u；真正被当前窗口消费的未来声明必须拒绝，而非吞成UNKNOWN。

## 4. Information block / 固定三个字段

每个原D取原calendar最近20session（含D），u_i为原market_up_ratio，i=0..19。完整窗口、全部原市场值及时钟已知才AVAILABLE；首19个原决策日缺此前市场值时UNKNOWN_20D_HISTORY，不补预热。之后任何内缺值/时钟UNKNOWN_BREADTH_HISTORY。三字段同可用，visible_through=D，原KEY/order全保留。

| 字段 | 冻结公式及单位 | 解释边界 |
|---|---|---|
| market_up_ratio_mean20 | mean(u_i)，[0,1] | 平均上涨占比，不是上涨概率预测 |
| market_up_ratio_slope20 | sum((i−9.5)×u_i)/sum((i−9.5)^2)，占比/原交易session | 线性修复/恶化趋势，不是HMM regime预测 |
| market_weak_breadth_share20 | mean(u_i<0.5)，[0,1] | 上涨占比不足一半的持续性；=0.5不计弱，不是对当日股票的强制弃权 |

固定20窗口/0.5中性锚事前冻结，不观察收益择窗口/阈值，不做quantile寻找、clip/winsorize或利润过滤。恒定宽度允许零slope；同日20候选共享该块是合理市场状态，不把重复行当独立样本。不是HMM训练、择时执行或自动空仓策略；是否建议某价仍由原条件净价值/风险函数处理。

## 5. Model / 公平共同监督与价格集合

matched13D与candidate16D；原GBDT200/lr.05/depth3/min_leaf30/subsample1/max_featuresNone/seed20261004/noearly-stop，gross-Y mean及path-min q10各两头，共4 physical-fit/单candidate。只增三历史字段；原JSON/float32树parity、无标签global支持、支持洞、多段法律tick、期望net>0/downside≤800bps不改。

共同监督为原training_eligible/values_available、原12D及新块完整有限、合法g、T及真实label_information_end≤train_end；最低100行/20D。不让未来label成熟度决定D状态；不因为新历史UNKNOWN而删除原候选或测试日期。global支持仍仅原无标签train12D/合法g，100bps桶≥30观察/≥5D、总体2.5～97.5%及真实洞，不用新块mask或Y/L回选支持。

split/cutoff是本父实例：train2024-07-04～2025-05-30、validation2025-06-03～2025-09-30、已消费test2025-10-09～2026-02-02、label截至2026-03-10；不硬编码到公共QE。validation仅诊断，test不fit/校准/early-stop/选点。VALUE_REVIEW_5_V1五有效复评退出不改为五自然交易日；通用独立价值标签另立设计，当前问题不借机改目标。

## 6. Trial / 原35加4与一次研究

campaign=advisory_breadth_state_price_v1_20261005，model_id=M10；固定本信息块/来源/算法/政策/费用/支持/评价/资源和implementation SHA。predecessor为真实M9 evaluated manifest；原budget_anchor、同一F:/Dev/AIstock_model_artifacts/advisory_price_research_campaign_r2_20261004不变，独立新run，不改旧plan/receipt/terminal。

显式price_path_extension+market_risk_extension+volume_context_extension+breadth_state_extension，仅同源/同根/固定类型/policy/cost相同，验证M9真实四fit及原子terminal/hash后新增M10≤4，总cap39。旧M6=23、M7-only27、M8-only31、M9-only35保持，不接受M10 journal；原累计4/5/2/4/4/4/4/4/4+1index不清零。旧terminal仅核工程身份/次数，不读取旧收益择参数或再加载权重。

preregister→prepared→trained→evaluated复用现有原子链。prepare一次只读冻结输入，无DB SELECT，保存新宽度块/真实source投影说明/状态计数；发布后的exact retry只读原prepared。fit前STARTED/fit_attempt，partial禁止隐式再fit；完整stage exact retry不追加fit。study_type=MODEL_TRIAL、objective_contract=RISK_MANAGED_ADVISORY、decision_use=NAVIGATION_ONLY沿现有registry，不新建审批或UI。

fit前后核定QE experiment/custom_evo/multi-alpha三running空闲；未知/不空闲仅暂停fit，不中断QE。线程2，4fit≤1800秒、RSS/新工件各≤2GiB、原≤7720候选/原输入行预算不扩大。长实验每30分钟检查；原18h 2026-10-05 00:08～18:08、原48h 2026-10-04 02:36～10-06 02:36均不重计。禁止为凑时长再试同信息参数。

## 7. Evaluation / 完整四臂与停止边界

原81决策日/1620候选/100共同估值日，baseline Top5/固定±300bps rule/matched/candidate完整同人口四臂。VALUE_REVIEW_5_V1五槽/空槽现金0/Top40复评/五有效复评/T+1/停牌及涨跌停延期；buy.95/sell5.95bps一次，Top6～20不补位。UNKNOWN研究控制单列非模型TAKE、不成为生产fallback；实际市场不可执行/未知共同不买，完整endpoint/held-mark/未结算阻断时不删episode。

导航条件沿旧预注册：配对mean日净增量分别相对baseline及matched≥5bps；两者实际进入差异各≥12原D且≥15%；模型真TAKE≥30；MDD相对两者恶化≤200bps、最差5%日均值恶化≤20bps。block5/reps2000/seed20261004仅描述性探索区间，不独立OOS、DSR/PBO或确认。NAV门是本candidate研究分类，不是QE包准入、用户使用或全局研发门禁。

任何未过STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION，不回选matched/规则、不调seed/阈值/期限/窗口或追加旧失败固化；正导航只CONSIDER_CONFIRMATION_DESIGN_ONLY。sealed/新holdout不读，无自动绑定/启用/收益承诺；新信息相邻窗口高度重叠、只有单一已消费开发期，限制诚实报告。

## 8. Implementation Plan

方法/信息新增审查→PIT/正常缺失与原日历审查→预算/精确范围及产品边界审查；F2/当前HEAD CI合入设计/自身清理；最新main独立树登记上述12文件→纯函数和薄编排/路由→重复审核修复/最小定向矩阵→干净源码及新plan登记→一次prepare→QE idle一次四fit/完整四臂→真实进展和蓝图更新/当前HEAD CI合入及自己清理。M1六UI/公共BUG交接是工程辅线，不开第二训练线。

## 9. Verification Plan / 直接高价值测试

20值手算mean/slope/strict弱占比；恒定0/0.5/1、反序slope符号、同D共享块；缺原session不能压缩、首19D预热/NULL/未知clock保留原KEY；重复/外来日期和同D冲突、bool/范围外/坏数、被消费未来clock拒绝；较晚D毒化不影响早D。纯函数无DB/fit/收益访问；prepare hash/原序/计数、原子exact retry不重prepare。共同mask/maturity/test不进fit、无标签支持、旧JSON数学/身份公式不变。预算默认23/27/31/35、固定39、缺失/partial/foreign计数、QE未知均直接测，复用旧矩阵不扩重复快照/fixture/ownership转移。广回归交当前HEAD CI。

## 10. Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-909 | 新宽度历史假设与当日宽度/M8价格风险的区别，无旧负研究复跑 |
| F-910 | 原冻结投影/日历/时钟/同D一致、UNKNOWN原人口及0新SQL |
| F-911 | 三固定公式、13/16同监督/支持/冻结价格价值政策 |
| F-912 | 完整四臂/实际干预/原净增量与风险边界、证据分层 |
| F-913 | 原35+新4cap39、显式固定extension/旧默认不变 |
| F-914 | QE互斥、原时间资源预算/X-F及零DB写入/控制/激活 |
| F-915 | 独立精确范围、多轮审核/源码与研究/经济/运行分报 |

## 11. Design Acceptance Matrix

此表只验收设计，源码/新run均未开始，不能冒称业务或研究完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-909 | §1/4/7 | artifact: 旧信息与新宽度历史对照及开发导航边界 | DESIGN_VERIFIED | none |
| F-910 | §3/4/9 | artifact: 7720键/386已知D/367历史完整D、原SHA和时钟方案 | DESIGN_VERIFIED | none |
| F-911 | §4/5/9 | artifact: 固定公式/同监督/旧支持与五有效复评政策 | DESIGN_VERIFIED | none |
| F-912 | §7/9 | artifact: 原四臂/实际干预/导航数值和描述性区间合同 | DESIGN_VERIFIED | none |
| F-913 | §6/9 | artifact: 显式M10/39及旧23/27/31/35不变 | DESIGN_VERIFIED | none |
| F-914 | §2/6/12/13 | artifact: QE互斥/原截止/X-F与禁止生产操作 | DESIGN_VERIFIED | none |
| F-915 | §2/8/12 | artifact: 12精确文件/三视角审核/设计与实现分层 | DESIGN_VERIFIED | none |

## 12. Risks / 多轮审核与符合性

宽度三字段可能冗余、滞后，单开发窗口重复研究有适应性选择偏差；不会因特征新就恢复独立证据。来源分母/非vintage与原包股票池不同含义保持，不称跨池泛化。常数市场块不是每只股票独立信号，弱占比不是概率或可成交价格保证，gross price条件化仍非因果限价执行。

第一轮方法自审：明确新增的是20session宽度历史而非已有当日比值/M8指数波动，三字段固定，不叠加旧负块；收益条件不后放宽。第二轮PIT/缺失自审：沿原calendar且先同D去重一致性检查，不任取first、压缩缺日或补预热；补充先投影实际消费历史键再验证值/时钟，防止先全源扫描未来坏值影响早D。第三轮预算/边界自审：新显式M10/39仅同源扩展，旧35等不变；保持五有效复评标签，通用模型解耦问题不借机改现有政策，不跨QE/HMM或新建平台。初次F2校验仅缺Contracts标题组，现显式标注实际§3合同，不以校验失败声称通过。

DESIGN-COMPLIANCE四项：不把设计/prepare/mock-only冒称完整；矛盾拒绝而未知如实保留；原名单/价格价值/退出及费用不改；不新设包资格/确认审批，导航分类不阻断功能。三轮为本窗口分视角自审，不冒称外审。实现后实际问题回写本节，未审核实现不先标SOURCE_VERIFIED。

## 13. Rollout / Rollback / Production Gates

仅离线设计/研究交付backend_restart_required=false。无数据库migration/依赖/数据或模型activation，既有M1 consumer不绑定M10、不改变实时运行配置；用户后端无操作。负研究不复活，完整新工件保留但不另开归档项目；设计/源码合入、研究结果、经济确认和生产状态分别报告。

## 14. 源码预检断点（2026-10-05）

设计#5458 HEADa45673bb0/CI37240953708 SUCCESS后合入9e39c3c1c并自身官方cleanup_done，源码独立树从该main开始、修改前登记原12文件。仅两新Advisory叶模块及已有同核M10字段/身份/status、显式39 budget扩展；旧23/27/31/35和旧模型hash公式保持，0新SQL。

三视角实施自审：信息/时钟轮用20值手算、常数零slope、严格0.5与未来值/clock毒化检查历史计算；编排/预算轮核原source KEY、原子prepare retry、partial/QE未知不fit、旧默认及真实M9完整四fit/terminal要求，M10 extension重验证typed plan避免model_copy绕过literal；消费/兼容轮发现新增status及fit-identity路由遗漏，精确修复后仅复跑三个失败/新增节点，稳定后38项直接矩阵、Ruff、两L0静态入口PASS/阻断0。未新增重复快照或大fixture，不冒称外审。

真实原M1 published JSON bundle在本次新路由下读回/verify_unchanged通过：model SHA872acff3894c7a64b1b87c51ebd440d739a82069d68be0e30ea27dee9c81931e、bundle SHAc674a38822a9bf37a6afb90478a8200029636ccbf4d8ec005b47c8d97302bf14不变；未读行情/label arrays、查DB、fit或激活。旧bundle元数据/evaluation摘要为公开消费者既有读取，不冒称整个检查没读任何结果元数据。首轮python -m ruff在AIstock环境无模块，未作为PASS；改用既存C:/Users/lc999/miniconda3/Scripts/ruff.exe经RTK执行通过，无安装。

本断点只源码验证；正式新plan/prepare/研究拟合及四臂评价尚未执行，实际仍35物理研究fit+1index。单元合成小拟合与正式研究journal分账。F-912仍仅设计验收，矩阵在实际研究后更新，不先标经济确认/源码合入或运行完成。四项DESIGN-COMPLIANCE再次核对：不交付partial为完整，不吞输入错误，原名单/五有效复评/成本不改，不增策略包资格或确认审批。
