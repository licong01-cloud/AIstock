# Advisory 有序资金流路径条件买价 M17 F2详细设计

2026-10-05；DESIGN_ONLY_RESEARCH_NOT_STARTED / EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY。

## 1. Background / Goal

M14～M16已各一次完成，真实63研究fit+1旧index，均未满足原净增量条件，不挽救、回选或确认。M17检验H-ORDERED-FLOW-PERSISTENCE-PRICE-1：在相同D当前大单强度和五日总净流入下，连续流入与反复买卖是否改变给定买价的后续净价值。不是改M6窗口/seed/loss，也不把聚合大单等同真实机构身份；条件价格建议不重选股、不研发QE alpha或分钟执行。

工程metadata spec43551563...确认原M6冻结CNY Parquet17013行、六源字段与单位attrs存在，未读财务/行情/label值、0DB/fit。合成I路径A=(-.2,.2,-.2,.2,.2)、B=(-.2,-.2,.2,.2,.2)，每日gross相同、D和五日加权净流入相同，但末端run=.4/.6、平均相邻变化=.3/.1；旧M6三量不能识别该有序路径。此是可识别性而非经济有效证明；只能一次独立预登记评估，正NAV不能称确认。

## 2. Scope / Non-goals

仅Advisory模型研究叶文件、对应直接测试及本设计/蓝图。实现显式12文件：
- backend/services/advisory_model_first/economic_flow_path_v1.py
- backend/services/advisory_model_first/economic_flow_path_pipeline_v1.py
- backend/services/advisory_model_first/economic_moneyflow_price_pipeline_v1.py
- backend/services/advisory_model_first/economic_sector_price_value_v1.py
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_flow_path_v1.py
- backend/tests/advisory_model_first/test_economic_flow_path_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py
- docs/architecture/advisory_flow_path_m17_f2_design_20261005.md
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

不改QE/Selection/HMM/StrategyPackage/公共数据/Execution/Paper/CI或AGENTS；不查/写DB、DDL、激活、安装、控制用户/其他进程。新代码不接daily/API/UI或自动角色绑定；使用原名单/日期/政策/成本/标签，单股正常缺值仍UNKNOWN。临时X、正式独立F，原18h/48h时限不重置。

## 3. Architecture / Contracts / Clock

原KEY=(decision_as_of_trade_date,target_trade_date,instrument)，原D→下一calendar T；原候选≤7720、信息来源≤38600股票日、每候选同原五session≤D，不更换窗口。复用moneyflow_calendar_v1的D/T与窗口合同，只读原M6 prepared/source_calendar.json和amounts_cny.parquet；没有SQL或当前配置回填。

正式plan新增flow_source_manifest_ref(role=flow_path_source,stage=prepared)：原M6 run advmoneyflowvalue_a4e4e4d38d461616edbad7be/prepared/manifest.json。必须核对manifest/原M6plan/source lineage、同parent/reusable/feature/profile/universe/budget/policy、amounts及calendar文件hash和CNY attrs=tushare_moneyflow_shares_yuan_v1；只读一次，不重复10000换算。原来源是CURRENT_DB_HISTORICAL_NON_VINTAGE、native UNPROVEN，依旧RECOVERED_LIMITED，不补capture或重验QE资格。

纯helper只消费instrument/trade_date及buy_lg_amount/sell_lg_amount/buy_elg_amount/sell_elg_amount；先投影原候选真实五日请求，再做数值校验。其他尺寸字段/外股/未来宽源不消费；重复实际源键、坏数、错误D/T与真实消费未来clock拒绝该计算。原候选、顺序、日期不删填；缺历史session、原源行/四字段局部UNKNOWN，完整五点中任一gross=0则UNKNOWN_ZERO_FLOW_DENOMINATOR。

## 4. 固定数学与正常缺值

对五点i，L_i=buy_lg+buy_elg、S_i=sell_lg+sell_elg，I_i=(L_i−S_i)/(L_i+S_i)。真实四侧金额为有限非负CNY；0金额合法，quiet Decimal NaN/float NaN/NULL正常缺失，bool/string/Inf/signaling NaN/负数/非零转换下溢拒绝。先按每点最大侧金额scale，再算I，避免1e308合计溢出；不裁剪实质坏数。
- large_flow_positive_share5=mean(I_i>0)，[0,1]；恰0不算净流入。
- large_flow_trailing_positive_run_share5=从D向前连续I>0的点数/5，[0,1]；D≤0为已知0。
- large_flow_mean_abs_imbalance_step5=四相邻abs(I_i−I_(i−1))均值，[0,2]；完整恒定路径合法0。

路径中恰0可中断run但不造UNKNOWN；全五点正流入run=1合法。三量不使用股价/收益或监督Y来定义流入和人口，不因model action/label maturity改变输入。源clock是D可见盘后声明，不伪装当日历史捕获时间。

## 5. Model / label / support不变

共同成熟样本的matched=原12D+g共13，candidate=同13+固定3路径共16。GBDT200/.05/depth3/minleaf30/seed20261004，净价值mean与path q10各两臂共4物理fit；train only，validation仅诊断，test不fit/校准/选点，old VALUE_REVIEW_5_V1、支持洞、D终值锚/买价转换与成本保持，不调loss/seed/阈值。同源或新单位验证不能充当可学或盈利证据。

## 6. Study / identity / budget

campaign advisory_flow_path_v1_20261005/model M17/schema economic_flow_path_v1/experiment advflowpath_+planSHA前24；额外原source ref与implementation SHA进入plan/fit/stage身份。predecessor_manifest_ref role=flow_path_predecessor必须真实M16完整evaluated、四heads、stage/ledger，原同campaign_root/budget_anchor实际63，再新增4至cap67，原23～63与旧index不变、不清零。

新flow_path_extension须candidate_cohort_extension和原全部链；model_copy typed plan重新验证，拒绝假63/伪evaluated。0SQL prepare原D表与路径一对一KEY合并保序、immutable stage+atomic发布/hash读回，exact retry复核已发布身份，不读取收益后换ref或覆盖旧输入。prepare缺数据只原股UNKNOWN，不人为强制新版策略包准入。

## 7. Inference / business oracle

同13/16核计算给定假设g下mean/path，原训练支持与合法tick求多段/空/UNKNOWN价格集合；TAKE、SKIP、UNKNOWN保持不同语义，不把UNKNOWN静默SKIP/删原股。完整原81D/1620候选/100共同NAV日四臂，5槽现金0/不Top6补买、T+1/停牌/限价递延及原费用只记一次。所有episode须结算，限制说明保留；名义endpoint检查不是真实分钟fill证明。

## 8. Evaluation / API / UI / Production gates

只已消费development NAVIGATION_ONLY。报告共同NAV净收益/paired日增量与5D block2000 bootstrap、原成本、MDD/最差5%tail、干预D比例/真模型TAKE和UNKNOWN控制分开。原5bps相对baseline和matched、12D/15%干预、30真TAKE、MDD200bps/tail20bps用于本candidate导航分类，不是包消费或项目资格门，不事后放宽；失败只STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION，继续下一真正新假设，sealed/独立确认/启用0。

## 9. Implementation plan / Verification plan / Risks / Rollout / Rollback

设计三轮事实/数学-PIT/范围预算自审修订、F2/精确scope/diff/currentHEAD CI后合入与自身官方清理；latestmain独立12文件source→多轮审核修复/最小直接矩阵/Ruff/F2/L0/原bundle兼容→干净producer/预登记→0SQL prepare→fresh QE三running0时一次四fit/完整四臂及post核对→真实结果更新蓝图、currentHEAD CI后源码交付/精确自身清理。允许研究工具工程交付先行但必须保留冻结实施闭包至研究实际消费；无重启需求或自动绑定，不把源码完成称价格模型有效。

风险与发布边界：有序流入可能只是既有动量/换手效应的重复，五点统计噪声和NON_VINTAGE历史修改仍存在；一次开发窗口正结果仅导航，不触发确认或部署。Rollout只有独立研究工具SOURCE交付，不增API/UI或运行绑定；Rollback停止本candidate后保留原study，不覆盖输入或回选其它arm，没有配置指针/DDL/服务回滚操作。Production gates为数据库/依赖/模型与profile激活/进程控制NOOP，后端重启仍用户执行。

## 10. Design Acceptance Index

- F-980 固定有序路径三量及可识别性。
- F-981 原五点PIT、quiet缺值/0、坏数、empty、窗口唯一性。
- F-982 原M6 frozen/单位hash/同lineage、0SQL prepare/atomic。
- F-983 共同13/16、train-only/支持、旧公式与bundle。
- F-984 真实M16 predecessor、63→67/旧cap及身份。
- F-985 完整四臂、真TAKE/UNKNOWN分账、证据边界。
- F-986 精确所有权、审核与交付、运行态和正式产物保留。

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-980 | §1/4；economic_flow_path_v1.py | test: test_economic_flow_path_v1.py；artifact: 同M6摘要/不同有序路径手算 | DESIGN_REVIEW_PASS | none |
| F-981 | §3/4；economic_flow_path_v1.py | test: test_economic_flow_path_v1.py；artifact: 缺值/0/1/坏数/未来/empty | DESIGN_REVIEW_PASS | none |
| F-982 | §3/6；economic_flow_path_pipeline_v1.py | test: test_economic_flow_path_pipeline_v1.py；artifact: 原M6 hash/单位/0SQL/保序/atomic | DESIGN_REVIEW_PASS | none |
| F-983 | §5/7；economic_sector_price_value_v1.py | test: test_economic_sector_price_value_v1.py；artifact: 共同成熟/test未fit/旧bundle | DESIGN_REVIEW_PASS | none |
| F-984 | §6；economic_moneyflow_price_pipeline_v1.py | test: test_economic_moneyflow_price_pipeline_v1.py；artifact: 真M16 stage/四heads/63→67/旧caps | DESIGN_REVIEW_PASS | none |
| F-985 | §7/8；economic_sector_price_pipeline_v1.py | test: test_economic_sector_price_pipeline_v1.py；artifact: 同四臂/证据层/UNKNOWN控制 | DESIGN_REVIEW_PASS | none |
| F-986 | §2/9；范围及交付 | test: test_economic_flow_path_pipeline_v1.py；artifact: scope/F2/L0/QE互斥/无公共改动 | DESIGN_REVIEW_PASS | none |

## 12. Current state

事前spec43551563...仅17013行schema/单位attrs及合成识别对照，0行情/财务数组/Y/fit/DB。当前真实63fit+1，M14～M16已完成/仅STOP各candidate、源码#5478/#5480/#5482已合入且自身官方清理；进度文档#5483已合入bbbc181bc并清理。M17尚无source、研究预登记/prepare/fit/收益。M1 UI/公共BUG smoke仍独立待交付，不制造合格角色或停止整体。

## 13. 三轮设计审核与修订

第一轮核对原M6确实已有五点资金流而非只D：保持五session和全部旧摘要不变，合成等旧特征/不同有序路径反例证明新信息可识别，复用原17013源行/显式CNY单位、不需要新SQL或行业窗口。首次F2发现章节标题分组与设计阶段矩阵状态不规范，已按canonical标题及DESIGN_REVIEW_PASS修订；该状态仅指设计审核，不称源码或模型已完成。

第二轮逐项核对每点gross=0、quiet NaN/0/恒定/连续run、无关尺寸不消费、future/外股先请求投影与每点scale。明确没有原known_from或捕获clock时只保留D盘后声明和NON_VINTAGE限制，不能将不存在的clock当正常零或伪补原生receipt，也不由此阻断包消费；路径只依赖五点四侧，不用Y/成熟/动作筛人口。

第三轮按F-980～986映射完整一次13/16核、原M6 source manifest/单位及原lineage、M16真实63/完整前驱才67、旧caps/成本/标签/支持不变、所有权12文件与研究/源码/运行状态分离。F2七项/七行0warning、精确两文档scope/diff后currentHEAD CI才合入，再开始实现。本窗口分视角自审而非独立外审，不占据QE训练或读取新sealed。
