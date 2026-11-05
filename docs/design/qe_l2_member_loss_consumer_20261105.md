# QE新成员损失C消费预检与预算适配（F1）

## 背景

用户要求执行主线后的B0/C两臂验证。原125日C已完整交付，但尚无匹配772数据身份及Q4的后继。旧consumer明确拒绝新schema；复用旧准入或改写载体会破坏冻结身份。本设计完成可独立实施的QE预检和预算适配阶段，整个B0/C实验、真实准入与完整Composer仍须在新资产到位后验收；不能将本阶段通过当成实验就绪。

本次沿用户2026-11-05“开始执行规划的实验和任务”推进已返回的精确兼容方案，保持原秩预算公式，未新增收益寻优参数或审批门槛。

## 范围与非目标

仅新增独立QE脚本、对应测试和本设计，并将精确文件路径纳入既有QE验证ownership映射；不改QE调度框架、旧consumer/ADMISSIONS、HMM、旧preset_A或默认策略。脚本不训练、不推理、不生成订单，不登记、部署或启动服务。输入binding是本地预检期望值，**不是正式asset admission**；预检结果始终明确admission、AF/mapping、Composer及实际交易尚未验收。未交付的模型SHA不能填写成占位生产记录。

## Design Acceptance Index

- F-001：实文件哈希、唯一JSON键及发布者receipt验证；binding严格固定目标772、信号142日/经济141日，旧590不能接收为新资产。
- F-002：新C目标/schema、完整131行业×142日、prev_open时序、季度模型/参数/receipt pins校验；保持尾部标签NA，不因缺成熟outcome删除预测。
- F-003：验证生产者发布值min(1,max(0,raw))，不自行裁剪或解释为概率；availability/reason有缺口时明确失败，不补安全值、不缩分母。
- F-004：内部适配复用原自然新买预算核；保留原S分组、J及基准金额、κ=0.10、0.90..1.10、组预算和上限；B0不读取风险，保留原载体/审计字段，不新增交易。
- F-005：CLI成功/拒收均有明确状态及非就绪声明，完整真实旧C应因数据身份不匹配被拒；两节点fresh-process验证代码与不依赖资产的数学Oracle，不冒充完整Composer。

## 实施方案

新增scripts/qe_l2_member_loss_consumer.py。纯解析函数接收预测对象、预检binding和日历；期望binding精确冻结预测SHA、发布合同SHA、release_binding、131行业和逐季度模型/receipt/vintage。CLI先校验实文件字节，禁止由预检binding自动产生正式准入记录。

发布值用于完整行业平均秩；内部短暂转换为既有预算核接受的expected_loss参数，返回审计恢复原expected_member_loss/raw及availability/reason，明确字段来源。此为函数参数适配，不改写或重标预测文件、不把C冒充旧资产。该纯预算入口无法生成订单/持仓，已有持仓与J的身份由后续正式策略钩子继续执行原检查。

正式接线待新C/Q4及AF/mapping真实身份可冻结后，另沿本目录接续记录推进独立版本准入、CAS和完整Composer；本阶段不提供空白可通行登记、不替换旧入口。

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

2026-11-05本阶段验证：26个针对性测试及审核后2个类型错误回归、Ruff、git diff --check通过；真实旧C完整16,375行CLI按预期返回QE_L2_MEMBER_DATASET_IDENTITY，原SHA未变。两节点fresh-process预算Oracle通过，结果3074999.9999999995/925000/2000000元，相对事前金额Oracle误差小于1e-8元，未改业务容差；B0恒等、131行原字段保持。该验证是纯预算和拒收，不是完整Composer或新资产成功消费。

作者第一轮检查数值/原公式/字段和窗口；第二轮检查反证、完整性及状态声明。原HMM三个C1源AF已在双节点只读解析核对，预测仍到9月30日，续推理参考必须覆盖原2026-01-05起的181日，不能只截取4月后冒充完整参考等价。新资产、AF补推理/映射、正式准入及完整Composer属于H-C接续工作，尚未完成。

DESIGN-COMPLIANCE-001：本阶段按F-001—005实现，未把它叫作完整H-C接入或实验完成；没有默认值/假成功，真实不匹配资产明确拒收；原预算公式和默认策略未变；没有新增外部门禁或审批。新C/Q4的具体外部缺项已返回原owner，不由QE代训或重标旧资产。

## 风险与失败模式

新HMM发布协议如变动，先返回精确差异而非放宽校验。预检binding没有授予准入的权限；成功仅说明给定载体符合其固定期望，不证明AF、执行或经济有效。原资产当前不匹配目标，不以单元fixture冒充真实新资产验收。回溯分类和已消费历史保留，收益改善待六账户验证。

## 生产门禁

DDL/DML、生产目录/准入登记、部署/激活、数据集、服务控制均noop。离线脚本交付无需重启；后续正式Composer如果涉及运行加载，另报源码、重启和验收状态。当前user backend重启仍由用户执行。
