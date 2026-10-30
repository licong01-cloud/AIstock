# QE L2损失相对调权：F1单模块设计

## 背景与批准

用户已明确批准 `QE-L2-LOSS-RELATIVE-ALLOCATION-V1` 的D1～D10及六账户方案：先B*三个seed，再向同任务追加R三个seed；固定κ=0.10、同名单与组预算、零训练。批准原文为“批准完整合同及6账户方案”。完整业务合同位于 `tmp/handoff/qe-hmm-relative-holding-longtask-20261029/consumption_contract_v1.md`。

旧B不对继续持有的股票每日再平衡，因此新B*单列为目标再平衡研究控制，旧B只作额外诊断。R消费B*下单前冻结的名单、AF排序和基准目标，不根据R自身成交路径重新选股。

## 范围与非目标

只增加QE可选消费、正式准入、Composer接线、冻结目标日程、共同订单适配及对应测试。不改HMM模型、旧概率consumer、preset_A、默认策略、数据库或数据集。不重训、不重新推断、不自动晋升。后端重启仍由用户执行。

精确写入范围：本设计；`backend/services/quantevolver/l2_relative_consumer.py`、`config_composer.py`、`executors/backtest.py`；`scripts/qe_l2_relative_admission.py`、`qe_l2_relative_consumer.py`、`qe_l2_relative_strategy.py`；`backend/tests/quantevolver/test_qe_l2_relative.py`、`backend/tests/unified_engine/test_qe_l2_relative_strategy.py`；`tests/aistock_validation/catalog/file_ownership.yaml`。临时预检、审核、实验请求和手动接续程序仅存独立ignored交接目录，不修改旧产物。

## Design Acceptance Index

- F-001 (D1)：冻结loss、三seedAF及590b3b20…数据/PIT/C-013身份；零训练、零推断。
- F-002 (D2)：B*捕获原基准下单前目标及原AF排名；R严格消费同一日程，不依当日成交或未来持仓选股。
- F-003 (D3)：AF前ceil(n/2)与其余分组，原组预算冻结；空组/单股票明确处理。
- F-004 (D4)：完整131行业升序平均秩r；组内mu=sum(w*r)/W；w'=w*(1+0.1*(mu-r))。预算守恒、0.9..1.1倍、单股≤5%；越界失败，不clip或另行归一化。
- F-005 (D5)：两臂同一个绝对目标订单生成器；保留交易约束和费用；已有持仓不连续乘历史系数。
- F-006 (D6)：decision=t，AF/mapping/loss as_of=prev_open(t)；目标缺项失败，不补默认安全值；完整availability/reason。
- F-007 (D7)：125日及9月30日执行/真实分钟终值；保留原资金、股票池、TWAP/TAIL_BOOST。
- F-008 (D8)：先3个B*，手动接续后同任务追加3个R；提交一次，无后台观察器或自动重试。
- F-009 (D9)：R−B*、B*−旧B、R−旧B逐seed及分段收益、现金/成本/回撤/换手；研究边界披露。
- F-010 (D10)：正式可选接线、真实准入、双节点CAS与fresh-process；源码/运行/实验状态独立。

## 实施方案

新参数 `qe_l2_relative_manifest` 与所有其他overlay互斥。manifest独立schema，正式ADMISSIONS钉住loss、模型、AF、映射和物理数据绑定；不将loss冒充旧probability。共享现有只读PIT/映射和终值校验函数，不改变旧consumer。

对选定目录源码只增加两处可审计目标钩子及原ranked参数，原评分/排名/替换代码保留。B*先取得原目标，再由共同绝对目标订单生成器生成实际交易意图。R不重新运行选股，读取B*的125日下单前日程。两臂禁止从目标集外的备用候选增加股票；无法成交允许实际现金变化。原PIT/ST强制退出继续执行，非目标持仓无需补行业风险。

共同订单计算使用持仓系统的下单前账户估值；用正式交易报价计算目标数量，卖单优先，买单在扣费的计划现金预算内分配。计划卖出收入仅用于订单意图预算，实际成交、费用和现金仍由同一Qlib执行器核算，不冒称卖出必然成交。交易单位、报价缺失、限价、T+1、持有期和PIT检查保留明确原因。满额退出保留原数量，部分调整按合法交易单位向下取整。无额外止损或持有期搜索。

参考日程包含源task/Loop、arm=BSTAR、seed、预测/数据/基础合同/执行源码哈希及动作文件哈希。每行只保存下单前日期、as_of、权重、原排名及组预算，不含当日成交或当日收益作为决策输入。R严格验证125日、键集合、分组顺序、预算、时序和身份。

## 验证方案

直接测试组预算/幅度、并列/空/单股票、目标未知映射、输入SHA/身份/日期/模型/重复行、跨seed日程和中性恒等。真实Qlib订单测试覆盖已有持仓、新买、退出、现金/费用/整手/报价/交易限制，及B*目标日程中性回放的订单一致性。完整Composer与双节点fresh-process读取正式资产，保留末日执行器。未启用新参数时恒等，不启动旧HMM验证实验。

源码验收运行F1设计校验、changed-file检查及一次相关最终小矩阵；跨模块回归交CI。缺失资产或业务不变量失败不能以mock通过替代。

## 风险与失败模式

R即使目标等敞口，成交后仓位也可不同；全额目标再平衡可能增加费用。旧B与B*差异是研究结果的一部分。三个seed共享市场历史，历史分类回溯且窗口已消费；不把三seed或125×131行当独立样本。9月未成熟h20为NA。新控制的真实目标路径只在其完成后冻结，不用原B事后目标冒充B*日程。

## Production gates

生产DDL/DML、因子目录、数据集、HMM训练、服务控制均为noop。代码合入不代表运行加载或经济验收；用户重启后只读验证正式入口，再单次提交B*。R需真实B*日程，不能预先填充。用户手动触发进度和追加阶段。

## Design Acceptance Matrix

源码验收与六账户研究验收分开。用户批准的D8/D9顺序本来就是源码合入、用户重启、B*运行、手动触发R及最终分析；下面保留其运行状态，不能把正式资产预检称为已完成收益实验。2026-10-30直接矩阵50 passed；Windows缺少Qlib的4项明确skipped，同4项在既有远端与WSL原生Qlib中通过。三seed真实Composer均在任何reservation/POST之前捕获，均完成正式资产完整读取、617223行预测/PIT对齐和125日终值边界核验。最终收据目录为 `tmp/handoff/qe-hmm-relative-holding-longtask-20261029/preflight_release/`。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | scripts/qe_l2_relative_admission.py；executors/backtest.py | backend/tests/quantevolver/test_qe_l2_relative.py；artifact: tmp/handoff/qe-hmm-relative-holding-longtask-20261029/preflight_release/Loop1.native.json | verified | 无 |
| F-002 | scripts/qe_l2_relative_strategy.py；target_reference/validate_reference | backend/tests/quantevolver/test_qe_l2_relative.py::test_original_af_rank_not_target_dictionary_order；同文件日程身份测试 | verified | 无 |
| F-003 | scripts/qe_l2_relative_consumer.py::relative_targets | backend/tests/quantevolver/test_qe_l2_relative.py::test_exact_budget_oracle_and_neutral | verified | 无 |
| F-004 | scripts/qe_l2_relative_consumer.py::relative_targets | backend/tests/quantevolver/test_qe_l2_relative.py::test_unequal_group_property_and_cap_fail_closed | verified | 无 |
| F-005 | scripts/qe_l2_relative_strategy.py::_relative_orders | backend/tests/unified_engine/test_qe_l2_relative_strategy.py；artifact: tmp/handoff/qe-hmm-relative-holding-longtask-20261029/preflight_release/Loop3.native.json | verified | 无 |
| F-006 | scripts/qe_l2_relative_consumer.py::RelativeInputs | backend/tests/quantevolver/test_qe_l2_relative.py；artifact: tmp/handoff/qe-hmm-relative-holding-longtask-20261029/preflight_release/Loop2.native.json | verified | 无 |
| F-007 | backend/services/quantevolver/l2_relative_consumer.py | artifact: tmp/handoff/qe-hmm-relative-holding-longtask-20261029/preflight_release/Loop1.compose.json；同目录三份native收据 | verified | 无 |
| F-008 | tmp/handoff/qe-hmm-relative-holding-longtask-20261029/task_plan.md | artifact: tmp/handoff/qe-hmm-relative-holding-longtask-20261029/preflight_release/bstar_loops.json | pending_experiment | 用户批准分阶段六账户合同：源码合入后由用户重启，仅单次启动B*；真实日程形成后手动触发同任务追加R，不声称已运行 |
| F-009 | scripts/qe_l2_relative_strategy.py动作账本；冻结分析口径 | artifact: tmp/handoff/qe-hmm-relative-holding-longtask-20261029/consumption_contract_v1.md | pending_experiment | 用户批准的D9须等六账户完成后执行；当前没有新增收益结论，无自动晋升 |
| F-010 | backend/services/quantevolver/config_composer.py；独立正式ADMISSIONS | backend/tests/quantevolver/test_qe_l2_relative.py::test_fresh_backend_package_import_without_worker_path；artifact: tmp/handoff/qe-hmm-relative-holding-longtask-20261029/preflight_release/Loop3.native.json | verified_source_pending_runtime | 用户批准并明确后端重启由用户执行；双节点资产与fresh-process已验证，8001运行加载仍需重启后核对 |

## DESIGN-COMPLIANCE-001逐项审核

1. 无简化交付：实现完整组预算、共同目标订单及因果日程；六账户与最终经济评价明确保留为分阶段运行状态。
2. 无静默错误：NA、身份、资产、模型、源码、费用/策略合同、目标上限和日程不一致均失败；订单不可成交写明原因。完整资产检查不使用默认安全值。
3. 无业务漂移：旧目录源码按哈希验证，原AST恢复证明保留；使用原AF排名。只实施获准的B*共同再平衡与R组内调权，旧consumer、默认策略和HMM不改。
4. 无额外审批：合同及六账户已批准，不重复请求；不新增晋升或后台观察步骤。重启及手动接续沿用用户要求。
