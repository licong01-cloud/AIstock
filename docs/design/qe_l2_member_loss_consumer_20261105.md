# QE成员损失C独立正式消费入口（F1）

## 背景

用户要求执行主线后的B0/C两臂三seed验证，并解决QE侧接线阻断。原125日C已完整交付，但尚无匹配772数据身份及Q4的后继。旧consumer明确拒绝新schema；复用旧准入或改写载体会破坏冻结身份。本设计现覆盖完整独立消费源码入口，真实准入、匹配资产完整Composer和六账户须另行验收；源码和fixture通过不代表实验就绪。

本次沿用户2026-11-05“开始执行规划的实验和任务”推进已返回的精确兼容方案，保持原秩预算公式，未新增收益寻优参数或审批门槛。

2026-11-06范围更新：用户进一步要求执行全部QE侧阻断，扩展到正式消费入口、默认关闭的Composer接线、执行器模式检查及实际订单测试，仍在同一PR内完成。真实C/Q4、AF及映射SHA未闭合前不登记准入、不宣称运行就绪；以下新增设计条目记录源码接线与真实资产验收的区别。

## 范围与非目标

新增独立QE脚本、后端消费适配及对应测试，并在现有Composer/BacktestExecutor中增加显式可选接线；不改调度框架、旧consumer/ADMISSIONS、HMM、旧preset_A或默认策略。消费不训练或推理，策略包装保留原订单生成所有权，只改自然新买预算。输入预检binding不是正式asset admission；独立源码注册表在真实资产未闭合时为空，拒绝所有未知登记。未交付SHA不能填占位生产记录。

## Design Acceptance Index

- F-001：实文件哈希、唯一JSON键及发布者receipt验证；binding严格固定目标772、信号142日/经济141日，旧590不能接收为新资产。
- F-002：新C目标/schema、完整131行业×142日、prev_open时序、季度模型/参数/receipt pins校验；保持尾部标签NA，不因缺成熟outcome删除预测。
- F-003：验证生产者发布值min(1,max(0,raw))，不自行裁剪或解释为概率；availability/reason有缺口时明确失败，不补安全值、不缩分母。
- F-004：内部适配复用原自然新买预算核；保留原S分组、J及基准金额、κ=0.10、0.90..1.10、组预算和上限；B0不读取风险，保留原载体/审计字段，不新增交易。
- F-005：CLI成功/拒收均有明确状态及非就绪声明，完整真实旧C应因数据身份不匹配被拒；两节点fresh-process验证代码与不依赖资产的数学Oracle，不冒充完整Composer。
- F-006：独立不可变正式准入注册接口；请求不能自批allowlist，真实资产未到时所有未知版本拒绝，旧ADMISSIONS不变。逐seed的AF推理回执和投影必须同目标身份并匹配登记。
- F-007：正式MemberEntryInputs校验物理dataset、完整AF/精确PIT映射/分类authority、142信号与141执行窗口及原子执行源码身份；仅新买需要resolved映射，B0不读风险。
- F-008：Composer显式选项默认关闭且与旧overlay互斥；复用原源码SHA预算钩子，保持1亿元/Top50/C1原交易参数与费用，末日9/30和10/29不缩窗，打包全部worker并在运行前实读验证。
- F-009：BacktestExecutor只允许同SHA的prediction replay消费；禁止训练、隐式重新预测、替换AF或多层风险同时生效。
- F-010：独立worker策略保留原自然买卖/已有持仓和强制退出，提供完整131行业原字段审计；用真实Qlib订单Oracle核对B0恒等、风险组预算及已有持仓无额外订单，实际新资产full Composer另列待验收。

## 实施方案

新增scripts/qe_l2_member_loss_consumer.py。纯解析函数接收预测对象、预检binding和日历；期望binding精确冻结预测SHA、发布合同SHA、release_binding、131行业和逐季度模型/receipt/vintage。CLI先校验实文件字节，禁止由预检binding自动产生正式准入记录。

发布值用于完整行业平均秩；内部短暂转换为既有预算核接受的expected_loss参数，返回审计恢复原expected_member_loss/raw及availability/reason，明确字段来源。此为函数参数适配，不改写或重标预测文件、不把C冒充旧资产。纯预算入口不生成订单；正式MemberEntryMixin核对已有持仓与J、经济日历及前一交易日，原策略继续生成订单、正常和强制退出。B0不打开风险文件。

独立admission脚本由源码登记真实发布合同、物理dataset、release、authority及文件SHA，逐seed钉住AF/推理回执和投影；请求不能自批allowlist。正式MemberEntryInputs校验物理dataset、AF、完整PIT映射/authority、执行源码及142信号/141执行。原生Series只规范内存视图，不重写资产文件。AF仍须严格冻结回执，历史修订不等价时拒收，不能改true或增加容差。

后端member-loss适配默认关闭、与旧overlay互斥，复用原源码SHA及AST恢复检查的自然预算钩子，固定原交易参数及费用，打包全部worker并在qrun前实读验证。执行器只接受同SHA的PREDICTION_REPLAY，不执行因子准备、训练或隐式推理。运行前同时验证test信号段，防止经济段不变而预测段被静默缩短。真实新C/Q4及AF/mapping身份闭合后才能登记、CAS和完整Composer；当前零真实登记。

信号4月1日..10月30日共142日×131行业，经济4月1日..10月29日共141日（原125＋10月16），保留9月30日及10月29日执行估值，不重复滞后或再减一日。AF完整源参考1月5日..10月30日，显式投影3月31日..10月30日。10月30日保留信号、不执行。原资金1亿元/Top50/h20与seed42/2024/2026不变。按完整131行业的expected_member_loss平均秩r=(rank-1)/130，在原AF分组的真实自然新买预算内用b'=b*(1+0.10*(mu_g-r))，mu_g为原预算加权平均秩；保留0.90..1.10、组预算及原上限，不裁剪、不新增整仓调仓。

## 验证方案

精确测试完整142×131输入与142/141/125/16窗口、不成熟尾部、跨季度和as_of，错manifest/哈希/schema/目标/缺行/重复/缺失/NaN/raw投影/模型漂移明确失败。预算测试用事前金额Oracle验证分组和空/单股/同风险恒等、原上限、输入不变；默认B0不读风险。真实旧C和冻结日历执行CLI拒收，保持其原SHA；节点进程分别验证纯预算Oracle，不运行回测。新匹配C的完整消费验收仍未完成。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | scripts/qe_l2_member_loss_consumer.py | backend/tests/quantevolver/test_qe_l2_member_loss_consumer.py::test_cli_hashes_real_bytes_and_does_not_grant_admission | PASS | 无 |
| F-002 | scripts/qe_l2_member_loss_consumer.py | backend/tests/quantevolver/test_qe_l2_member_loss_consumer.py::test_complete_signal_window_and_economic_terminal_are_separate | PASS | 无 |
| F-003 | scripts/qe_l2_member_loss_consumer.py | backend/tests/quantevolver/test_qe_l2_member_loss_consumer.py::test_publisher_projection_is_checked_without_qe_transform | PASS | 无 |
| F-004 | scripts/qe_l2_member_loss_consumer.py | backend/tests/quantevolver/test_qe_l2_member_loss_consumer.py::test_member_budget_preserves_groups_and_original_audit_field | PASS | 无 |
| F-005 | scripts/qe_l2_member_loss_consumer.py | backend/tests/quantevolver/test_qe_l2_member_loss_consumer.py；validation-receipt: tmp/handoff/qe-member-loss-consumer-20261105/node_checks_final.json | PASS | 无 |
| F-006 | scripts/qe_l2_member_loss_admission.py | backend/tests/quantevolver/test_qe_l2_member_loss_composer.py | PASS | 无 |
| F-007 | scripts/qe_l2_member_loss_consumer.py | backend/tests/quantevolver/test_qe_l2_member_loss_composer.py | PASS | 无 |
| F-008 | backend/services/quantevolver/l2_member_loss_consumer.py；backend/services/quantevolver/config_composer.py | backend/tests/quantevolver/test_qe_l2_member_loss_composer.py | PASS | 无 |
| F-009 | backend/services/quantevolver/executors/backtest.py | backend/tests/quantevolver/test_qe_l2_member_loss_composer.py | PASS | 无 |
| F-010 | scripts/qe_l2_member_loss_strategy.py | backend/tests/unified_engine/test_qe_l2_member_loss_strategy.py；validation-receipt: tmp/handoff/qe-member-loss-consumer-20261105/run_v2/native_order_receipts.json | PASS | 无 |

2026-11-05本阶段验证：26个针对性测试及审核后2个类型错误回归、Ruff、git diff --check通过；真实旧C完整16,375行CLI按预期返回QE_L2_MEMBER_DATASET_IDENTITY，原SHA未变。两节点fresh-process预算Oracle通过，结果3074999.9999999995/925000/2000000元，相对事前金额Oracle误差小于1e-8元，未改业务容差；B0恒等、131行原字段保持。该验证是纯预算和拒收，不是完整Composer或新资产成功消费。

作者第一轮检查数值/原公式/字段、原订单路径和窗口；第二轮检查准入、实际字节、失败路径、默认行为及状态措辞，修复原生Series消费及运行前test窗口篡改缺口。针对性测试另覆盖AF非等价、处理器/权重漂移、资金费用不变和回放模式；双节点真实Qlib四种场景各核对B0/C订单，Windows无Qlib的测试明确skip，不冒充原生订单通过。

原三个C1源AF双节点解析已核对；本轮三seed各一次冻结推理已保存候选，但新数据历史输入有修订，原参考面板少934行且共同样本有预测变化，严格回执未通过。不得把候选声明为已验收、重复推理或放宽旧参考校验。QE仍需闭合新旧数据差异的来源与版本桥接，才可完成映射和真实准入；匹配C/Q4由HMM交付。旧九账户及旧HMM不重跑。

DESIGN-COMPLIANCE-001：源码逐项覆盖F-001—010，未用预检替代正式入口；真实资产、准入、CAS、完整Composer和经济实验仍明确未验收，没有空白可通行登记、默认安全值或假成功。原预算公式及默认策略不变，未降低历史等价条件、缩窗或新增未经批准公式。新C/Q4交原owner，不由QE代训或重标；源码、CI、合入、运行加载和经济结论分别报告。

## 风险与失败模式

新HMM发布协议如变动，先返回精确差异而非放宽校验。预检binding没有授予准入的权限；成功仅说明给定载体符合其固定期望，不证明AF、执行或经济有效。原资产当前不匹配目标，不以单元fixture冒充真实新资产验收。回溯分类和已消费历史保留，收益改善待六账户验证。

## 生产门禁

以上矩阵只验收源码设计，不验收真实资产或运行。以下条件必须独立闭合，全部完成前实验仍为BLOCKED：

| 运行交付条件 | 当前状态 | 责任及证据 |
|---|---|---|
| 匹配772、142日完整C及Q4模型 | BLOCKED | HMM交付；旧590不改绑 |
| 三seed新AF冻结回执和版本差异闭合 | BLOCKED | QE；现有候选不等价，不伪造verified |
| 全AF映射、真实版本化准入、DEV验证 | BLOCKED | QE；当前注册表为空，未知版本全部拒绝 |
| 双节点CAS及真实资产fresh-process完整Composer | BLOCKED | QE；fixture及订单Oracle不替代此项 |
| 后端实际加载及节点可用 | BLOCKED | 用户控制重启；源码不代表运行已加载 |
| 六账户及经济结论 | NOT_EVALUATED | 仅在前置齐备后执行，不重复旧实验 |

DDL/DML、生产目录/准入登记、部署/激活、数据集、服务控制均noop。离线脚本不需服务重启，正式后端接线合入后的运行加载须另行核验；后端重启仍由用户执行。WSL不可用时只暂停依赖动作，其他开发/远端核验继续，不绕过节点规则或重复提交。
