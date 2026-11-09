# QE 中断训练产物的零推理回放恢复（F1）

## 背景与批准范围

用户2026-11-10要求按 `tmp/handoff/qe-seed-package-next-stage-20261110/plan.md` 执行。两项LSTM已保存模型、预测及标签，账户中断；原失败Loop保持不变，现有backtest_only会超出未批准的推理预算。新增显式、版本化的保存预测恢复合同，继续使用正式prediction replay执行器，训练／推理增量均为0。此次授权覆盖模块内开发和既定账户恢复，不授权服务重启。

## 范围与非目标

仅QE接口、配置、来源准入、消费元数据和对应测试；默认completed来源路径保持不变。恢复只允许同任务向后追加Loop，来源必须为合同中准确声明的failed或cancelled，并以节点权威提交收据证明终态。禁止改写原状态、补造recorder、删除原产物、复制seed、重新训练／推理、使用HMM专用桥接或隐式回落。未知原处理器运行指纹仍记UNKNOWN；这里消费已保存预测，不声称重建或复现处理器。

## Design Acceptance Index

- F-001：公开 `prediction_recovery` 类型合同，精确锁定原recorder、seed、数据manifest/profile及七项源文件SHA；只在纯prediction replay启用，pred SHA与既有来源字段必须一致。
- F-002：普通来源仍要求completed；显式恢复来源必须DB状态和节点提交终态同时匹配，且同任务新增Loop索引大于源Loop。原失败Loop和工作区不变。
- F-003：七项冻结文件及唯一预测必须目录可见、普通文件、大小限制及逐项实际字节SHA通过；原config/model/params/pred/label/direct-v2绑定均存在，数据身份、因子/model/horizon/seed和真实节点绑定一致。节点把failed目录标partial，只允许明确的loop_not_terminal:failed或cancelled、以及冻结集合外symlink_not_followed警告；其它partial或缺项仍拒绝，不把partial冒称完整目录。传输错误不变成默认成功。
- F-004：沿原正式预测回放执行器消费冻结pred，恢复回执贯穿源引用、持久配置和消费工作区；不调用fit/predict，不把backtest_only算纯回放。
- F-005：保留真实原失败→新增恢复Loop及源recorder映射、UNKNOWN指纹；恢复批次在预算和报告中计为失败账户恢复，不增加研究臂，不伪称原Loop完成。
- F-006：缺失/改动资产、非终态、目录不完整、跨任务/身份、pin冲突和非纯回放均失败；普通completed回放兼容。

## 实施方案

`SavedPredictionRecovery` 为不可变、禁止额外字段的版本合同。公开Loop→ExperimentConfig→统一服务显式传递；不注入模型或策略kwargs。来源DB状态检查仅对该显式合同接受准确的失败终态，随后独立读取节点收据和全部冻结文件，实际验证后才构造标准预测引用。中断资产使用独立的有界字节准入路径，不调用或放松普通completed目录解析器；只准入精确冻结集合，记录原partial语义。唯一预测、已有root recorder的身份一致性、预测SHA和零fit/predict结果检查仍保留；无root recorder时不能补造。恢复回执加入标准来源引用并随执行快照持久化。没有缺省安全值或自动推理后备。

任务恢复脚本仍需查重、冻结原策略/费用/窗口/共同面板，单次正式append，未知POST只读核对。实现不自行增加训练、账户或服务动作。恢复只消费原预测，不能据此宣称模型可重新推理、严格历史as-known或收益增益已证实。

## 验证方案

最小本地检查包括合同/API/配置传递、来源终态与实际文件验证正反例、正式纯回放执行器、已有预测回放回归和变更文件lint。fresh-process使用实际两份来源只读核验七项资产和来源payload，不提交训练。运行态通过用户重启加载后再单次追加，真实账户经济验收另列，不以源码测试代替。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/quantevolver/saved_prediction_recovery_contract.py | backend/tests/unified_engine/test_qe_saved_prediction_recovery.py | verified | 无 |
| F-002 | backend/services/quantevolver/qe_evolution_service.py; saved_prediction_recovery.py | backend/tests/unified_engine/test_qe_saved_prediction_recovery.py | verified | 无 |
| F-003 | backend/services/quantevolver/saved_prediction_recovery.py | backend/tests/unified_engine/test_qe_saved_prediction_recovery.py | verified | 无 |
| F-004 | backend/services/quantevolver/executors/base.py; experiment_config_builders.py | backend/tests/unified_engine/test_qe_prediction_replay.py | verified | 无 |
| F-005 | backend/services/quantevolver/qe_evolution_service.py | backend/tests/unified_engine/test_qe_saved_prediction_recovery.py | verified | 无 |
| F-006 | backend/tests/unified_engine/test_qe_saved_prediction_recovery.py | backend/tests/unified_engine/test_qe_prediction_replay.py | verified | 无 |

## 风险与生产门禁

失败来源不是成功账户：恢复合同不授予completed身份，不晋升原模型。来源在读取中改变、任务重试或身份不明时拒绝提交；两个失败来源不共享recorder。读取pickle只做有界字节校验，不反序列化模型。服务内存与节点并发规则不变。

无DDL/DML、依赖安装、数据集或旧策略包变更。源码交付与本机8001加载、fresh-process来源核验、同任务两账户恢复、20核心／26首轮账户经济验收分别报告。8001重启仍由用户执行；WSL/远端无需因本次Python控制面修改而重启。

## 本轮验证与设计符合性

两轮作者审核（非独立审核）已覆盖正确性和完整性：普通completed路径不变；失败目录仍如实partial，仅锁定集合可准入；终态/recorder读取中变化停止；原资产不写入、无训练和推理回落。实际两份来源分别为原任务Loop1/2，预测SHA为`a9f52e6034d4baab37bf368c9beea38e6d8b751606e31c459536eebe6b811b3c`／`61e35e5275fdc39144aff08e57523ea4fdda865eba399e60115d16603722da20`；fresh-process逐项真实字节校验与完整Composer捕获均通过，PortAna策略/执行/费用与原conf完全一致，命令仅为pred-backtest，未提交worker。原处理器运行指纹仍UNKNOWN。协议和默认回放相关矩阵91项通过，新增partial/recorder/对齐负例另做最终定点复核；精确命令为`pytest -q backend/tests/unified_engine/test_qe_saved_prediction_recovery.py backend/tests/unified_engine/test_qe_prediction_replay.py`。

DESIGN-COMPLIANCE-001：不把源码通过说成账户完成；不吞传输/资产异常；不改变公式、源预测或账户经济合同；不增加设计之外的审批。source-ready与pending_user_restart、账户恢复和经济验收保持独立。真实资产/完整Composer收据位于本轮既有handoff的`recovery_fresh_process_v1.json`，后续实际账户由正式入仓收据闭合，不以此临时路径替代终态归档。
