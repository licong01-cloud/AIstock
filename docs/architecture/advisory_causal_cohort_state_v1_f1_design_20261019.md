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
| F-301 | 本设计Background/模型与预算 | artifact: docs/analysis/advisory_prediction_action_diagnostic_20261019.md | DESIGN_REVIEW_PASS | none |
| F-302 | 本设计数据与时钟 | artifact: docs/architecture/advisory_causal_cohort_state_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |
| F-303 | 本设计数据与时钟/Verification Plan | artifact: docs/architecture/advisory_causal_cohort_state_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |
| F-304 | 本设计模型与预算 | artifact: docs/architecture/advisory_causal_cohort_state_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |
| F-305 | 本设计评价和结论 | artifact: docs/architecture/advisory_causal_cohort_state_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |
| F-306 | 本设计模型与预算/Production Gates | artifact: docs/architecture/advisory_causal_cohort_state_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |
| F-307 | 本设计Implementation/Verification Plan | artifact: docs/architecture/advisory_causal_cohort_state_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |

设计审核：第一轮澄清原五槽、组终值风险和单股风险不同；第二轮禁止跳过未知凑20日，明确先过滤H再向预测函数暴露数值；第三轮明确旧expanding研究已做过、不宣称全新算法、不变旧政策且无生产接入。上述矩阵只代表设计自审，不代表源码或研究完成。
