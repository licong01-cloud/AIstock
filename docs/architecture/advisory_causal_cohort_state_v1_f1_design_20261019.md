# Advisory 成熟候选组合状态单一研究 F1 设计

日期2026-10-19；tier F1，单模块离线研究，不是完整荐股价格功能或生产日频接入。依据[已完成R1诊断](../analysis/advisory_prediction_action_diagnostic_20261019.md)。用户本轮授权按蓝图规划并执行；只允许本模块源码/测试、普通文档和自身研究文件。

## Background / 目标与非目标

假设`CAUSAL_MATURE_COHORT_STATE_1`：既有包原Top5的固定五交易日组合收益，其已成熟历史的近期状态能预测新的日级机会，改善原包基线。R1中预测接近固定训练先验而月度实现差异较大，只是立项动机，绝非有效性证明。

不重新运行Selection/QE，不改股票池/排名/原生定价/权重/执行，不构造任意买价反事实，不研发分钟择时、动态资金仓位、UI或服务。研究输出仅日级条件收益/风险和原槽TAKE/SKIP/NO_ADVICE，不能称完整盈利买入区间已交付。

## 与既有研究的关系

旧因果Admission v2是score/raw-market Ridge静态与20日expanding重训，已停止；它已经覆盖“因果更新”概念，不能重新包装。本研究只比较固定训练期**组合收益经验分布**与同目标近期成熟20D分布，无旧模型权重、个股回归或阈值/窗口搜索。新的被检验信息是D可知的历史原候选流结算状态；若无增量则停止，不派生窗口家族。信息、目标和动作同时改动的收益归因只限这一整体假设，不归因给某一项。

## 数据与时钟

1. 原同业研究的preregistered/prepared/trained/evaluated阶段及forecast身份只读；只消费prepared/domain与evaluated/measured_rows、原days/calendar和原四臂汇总，不读取sealed，不访问SQL/API。
2. 每原D、每原package按原Top5完整槽，合法不可入场为已知现金0；真缺价为UNKNOWN，不填0、不删日期。保留少于5候选的原空槽，固定五槽分母。各包等权构成**研究计量组合**，不是新的生产组合；相同股票跨包重复属于不同业务槽，不作为独立统计样本。
3. 当日组合target为各包五槽净值的均值，尾损定义`max(-cohort_net_bps-800,0)`，不是单股尾损或路径回撤。任一原包/槽金融未知时该cohort target UNKNOWN，原记录保留。
4. 预测信息时钟为D_CLOSE：状态只能用`label_information_end <= D`的cohort，且原cohort D必须小于当前D；当前入场为T，标签结束T+4。既有冻结文件可能物理包含未来标签，但提供给预测函数的视图先按H投影，只读取合法行的值。未来标签毒化不能改变更早预测；生产captured-at/vintage不升级。
5. 静态control使用train_start..train_end内、H<=train_end的已知cohort；记录所有未知/未成熟训练行，不修改原finance。rolling用D时钟已成熟的最后20个**原决策日**，不跳过未知、缺日或以20个有值日替换；少于20日或任一UNKNOWN时NO_ADVICE，原基线继续。该状态只限制本研究建议，不增加包准入门或阻断原荐股。

## 模型、动作与预算

- 两个统计估计器：static empirical control、rolling20 candidate。闭式均值/盈利比例/尾损期望，无优化器或`fit`，明确报告0 physical fit、2估计器、1候选和逐D状态更新数；不是没有学习数据，也不把每次更新计成独立试验。
- 预测`mu=mean(cohort_net)`、`tail=mean(max(-cohort_net-800,0))`、`utility=mu-tail`、`p_profit=mean(cohort_net>0)`。TAKE当mu>0且utility>0，否则SKIP；NO_ADVICE继承原槽基线，不能冒充模型TAKE或现金。
- 日级建议作用于本研究中该日全部原槽，名义买入/退出价与原成本不变；没有递补/重排。合法不可执行保持原状态0，真实未知保持UNKNOWN。
- 四臂为原baseline、原固定规则、static control、rolling candidate。预测函数每D只接可见历史状态；预测和状态来源摘要先保存，之后独立评价当前cohort的未来Y。允许先前评价期已成熟标签进入后续状态，但不得宣称后续输出是完全静态的独立OOS。
- 一次预登记、一次研究；window固定20，风险800与零utility界限固定，不在结果后试别的窗口/阈值/包。CPU最多2线程、无需GPU/QE空闲、研究输入约数MB、无大型复制。失败/未知/技术重试原身份留存，不覆盖。

## 评价和结论

同原86D/860槽，81成熟D/810槽，原未知与未成熟完整保留。复用已有`paired_statistics`的原日期5日moving blocks/2000次与两个比较端点：candidate−baseline、candidate−static。差值为重叠固定5TD cohort bps，非daily return/NAV/实际成交。

研究继续条件：两端点净增量point>5bps、同时95%下界>0、组终值尾损不劣于两对照，真实干预≥30唯一stockD、≥20D且≥25%成熟日；两包分别报告且不得择包。此为新研究合同，不改旧阴性候选门槛。未满足为`EXACT_CANDIDATE_NOT_SUPPORTED`，不进行R4/角色接入；满足也只为`WORTH_CONFIRMING`，不自动启用。所有预测技能/支持/风险/覆盖分报；MDE只判可解释范围，不把欠功效当证明全局无效。

研究窗口早已消费、R1再次消费，全部仅导航；登记沿已有`ADVISORY_GP5_ENTRY_VALUE` lineage。无调参获胜声明、无独立OOS、无实盘盈利声明。

## Implementation Plan / 实施方案与精确范围

精确源码：`backend/services/advisory_model_first/causal_cohort_state_study_v1.py`，复用原stage身份读取、原始评分/finance产物、registry与配对统计；不复制选股/过滤/融合/TopK算法。直接测试：`backend/tests/advisory_model_first/test_causal_cohort_state_study_v1.py`。研究driver在X，正式输出在独立Advisory artifact根；生产模块无调用入口，不需要重启。

## Verification Plan / 验证方案

测试覆盖H截止/当前D与未来毒化、原calendar缺日、未知不跳过、合法空槽/不可执行、原包/键/排序不变、状态和政策摘要、NO_ADVICE基线恒等、固定分母和归因、输入错配fail closed、完整真实文件研究。行为稳定后执行一次直接小矩阵、changed-file Ruff、F1校验、DESIGN-COMPLIANCE-001四项及CI；不得以合成测试替代真实研究。

## Risks / 风险

历史重叠收益的自相关不代表未来预测力；必须H<=D且新T>D才能消除机械共享未来。研究只有81成熟D且窗口已消费，不能从正点升级实盘。日级状态可能恒等或错误拒买；未知窗口不跳过，不为了产生干预改window或阈值。基础行情修复、训练、生产启用均不属于此研究。

## Production Gates / 生产边界

无API/调度/绑定接入、无DB/DDL、无QE任务、无后端控制；仅离线脚本及正式研究产物。runtime activation=not_requested，backend restart=noop。源码交付不等于价格产品/经济目标完成。

## Design Acceptance Index

| ID | Requirement |
|---|---|
| F-301 | 新日级问题与旧Admission/个股价格研究明确区分，0physical fit/2估计器/1候选，研究不冒充价格功能交付 |
| F-302 | 原包/槽/排名/日期/UNKNOWN及固定五会话和原成本保持，源stage链只读且验证 |
| F-303 | D_CLOSE仅消费H<=D已成熟历史，最后20原决策日不跳UNKNOWN；未来毒化不影响早期预测 |
| F-304 | 静态及rolling同cohort目标；NO_ADVICE基线恒等，不新增包门禁或以未知填现金 |
| F-305 | 预测先保存，后读当前未来Y；原86D/860槽及81成熟日全量评价，双端点、支持与风险分报 |
| F-306 | 一次研究和原registry lineage，不优化窗口/阈值、不覆盖产物或混淆OOS/角色激活 |
| F-307 | 多轮自审、精确测试及真实文件读回；只改Advisory，无QE/其他模块/DB/服务操作 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-301 | causal_cohort_state_study_v1.py::run_study / 本文Research Result | artifact: docs/analysis/advisory_prediction_action_diagnostic_20261019.md；新run prepared receipt | IMPLEMENTED_RESEARCH_COMPLETE | none |
| F-302 | causal_cohort_state_study_v1.py::build_cohorts/run_study | test: backend/tests/advisory_model_first/test_causal_cohort_state_study_v1.py::test_cohort_fixed_slots_cash_unknown_and_empty_original_day；test_original_identity_rejects_mutations；真实305D基线对账 | IMPLEMENTED_VERIFIED | none |
| F-303 | causal_cohort_state_study_v1.py::predict_state | test: backend/tests/advisory_model_first/test_causal_cohort_state_study_v1.py::test_projection_before_future_finance_and_exact_horizon；test_unknown_not_skipped_static_freezes_and_source_untouched | IMPLEMENTED_VERIFIED | none |
| F-304 | causal_cohort_state_study_v1.py::freeze_forecasts/evaluate_states | test: backend/tests/advisory_model_first/test_causal_cohort_state_study_v1.py::test_native_statistics_attribution_cluster_denominator_and_no_advice；真实24D NO_ADVICE | IMPLEMENTED_VERIFIED | none |
| F-305 | causal_cohort_state_study_v1.py::run_study/evaluate_states | test: backend/tests/advisory_model_first/test_causal_cohort_state_study_v1.py::test_unknown_finance_prevents_confirmation_and_remains_unknown；新run trained/evaluated身份及本文Research Result | IMPLEMENTED_RESEARCH_COMPLETE | none |
| F-306 | causal_cohort_state_study_v1.py::run_study | test: backend/tests/advisory_model_first/test_causal_cohort_state_study_v1.py::test_real_stage_chain_exact_retry_and_identity；新registry四记录/既存原run未改 | IMPLEMENTED_VERIFIED | none |
| F-307 | 精确一源码/一直测及三文档；本节四项符合性 | test: backend/tests/advisory_model_first/test_causal_cohort_state_study_v1.py；artifact: 本文Research Result | LOCAL_IMPLEMENTATION_VERIFIED | none |

设计审核：第一轮澄清原五槽、组终值风险和单股风险不同；第二轮禁止跳过未知凑20日，明确先过滤H再向预测函数暴露数值；第三轮明确旧expanding研究已做过、不宣称全新算法、不变旧政策且无生产接入。随后完成源码、测试及一次真实研究，结果如下；本窗口多轮自审不冒充独立外审。

## Research Result / 2026-10-19一次真实研究

干净producer `bc045e7b086f5b86e93202125daab1d4ad4f4697`，run `advcohortstate_f10e3bf268322af7b04ed5bc`，正式根`F:/Dev/AIstock_model_artifacts/advisory_causal_cohort_state_v1_20261019/`。新plan SHA `f10e3bf268322af7b04ed5bc10ef3763c572ffd0169e81aef9e2181ca24030aa`；trained `176392b502a36014b67d0bcdba2195e2d3cb2310fdd8aee399fa5cd099b824b9`；evaluated `6eb989c4ec54dfd0c98f5a169c95bf7fb0e5f854441f31bed118d0b2bebaa86b`。原生registry四阶段登记，源产物只读，网络连接被driver禁止。

305原D/3050研究Top5槽完整，286个已知cohort；评价86D/860槽、81成熟D/810槽。0 physical fit、2统计估计器、1候选、86日状态更新；累计仍158研究fit＋1旧index，不能加成160fit。研究计算1.478秒，不是设计/审核总耗时；没有为了长任务时数增加训练或循环。

| 原81成熟D四臂 | 固定五槽5TD均值/bps |
|---|---:|
| 原包baseline | 128.389817 |
| 原固定规则 | 121.630581 |
| static empirical | 128.389817 |
| rolling20 | 130.588447 |

candidate相对baseline和static均+2.198630bps，同时95%区间均[−8.787359,18.248385]，MDE80 18.451300bps。避亏14.368816−漏盈12.170186=+2.198630bps。唯一stockD干预69、7D/81=8.642%；81成熟日中50 TAKE/7 SKIP/24 NO_ADVICE。全86日为50/12/24，额外5个SKIP尚未成熟，不能算已验证干预。四臂该组终值800bps超额尾损均0，仅说明本指标无观察事件，不代表无路径风险。

按包全量报告：LSTM增量+5.907386bps、区间[−13.651974,26.599747]；TCN−1.510125bps、区间[−5.325591,11.956404]，均7干预D；不选LSTM救结果。统计输出全部导航，不属于独立确认、日收益、NAV或真实成交。

**NO_ADVICE来源及解释边界**：24D为2025-06-03..07-04，历史窗口中包含原训练快照在2025-05-26..05-30标为IMMATURE的50槽；随D前进，本研究没有将原截断标签重新结算。不是24个当前评价日行情缺失，也不是判定这24日不该荐股；原包基线全量保留。不把缺状态归为真实模型预测失败，也不为此旧候选补数/扩窗重跑。它限制了该精确文件协议的覆盖，不能据此否定所有实时滚动方法。

共同有预测的57日，static/rolling RMSE=375.594523/407.750346bps，Brier=0.238203/0.243158。此共同口径来自只读冻结forecasts与原baseline的核算，0新模型/阈值搜索。evaluated中各自有效口径为static81日、rolling57日，不能直接把两个不同分母当配对比较。已观测57日也没有出现值得确认的预测优势，不将其推广为全局不可学。

结果`EXACT_CANDIDATE_NOT_SUPPORTED / NAVIGATION_ONLY / NOT_ACTIVATED`。既定点增量、区间及干预条件未满足；R4/R5不执行。不调整20/800/阈值，不重跑此候选，不等待自然市场数据。只完成该离线研究，完整收益型价格产品仍未交付。

## 源码审核与DESIGN-COMPLIANCE-001

1. 禁止简化交付：F1范围完整为离线状态研究，不声称买卖价产品或生产接入完成；真实文件研究与合成测试分别报告。
2. 禁止静默错误：源stage/包run/manifest/原日/排名/目标核对；未知保留，NO_ADVICE仅让原包恒等继续，不伪装现金或模型预测。
3. 禁止改变业务逻辑：复用原固定五槽目标/成本与原双端点统计；只在新研究注册日级动作，旧生产、源码政策和产物不变。
4. 禁止私增门禁：源身份/时钟属于本研究复现验证，不增加QE策略包使用审批；无DB、服务或数据owner任务。

源码第一轮检查修正paired_statistics的原端点键适配；第二轮分离准备3050槽与评价860槽，避免元数据覆盖；第三轮检查零候选现金、未来标签毒化、未知不跳过、原生stage exact retry及各包完整性。最终17项直接测试、Ruff、F1 7/7、diff检查通过；公共CI待本PR。没有运行无关模块套件、修改所有权或发起QE实验。数据库/依赖/运行态=noop，后端不需要重启。
