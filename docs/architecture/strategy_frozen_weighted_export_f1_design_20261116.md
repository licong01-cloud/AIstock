# StrategyPackage 固定权重新版本导出 F1 设计

## Background / 背景

用户批准20h双线任务，主线实验在运行。父包线复用原窗口5+5及更新窗口3+3共16
真实模型，不恢复旧33+5身份。现有晋升只接收ic_weighted；简单放开名称仍会漏传rank、
重归一化权重、按NA裁样，并丢失float32/float64原seed均值精度，因此需完整接线。

## Scope / 范围

仅编辑本设计、`backend/services/strategy_package/frozen_multi_alpha_contract.py`、
`backend/services/strategy_package/multi_alpha_promotion.py`、
`backend/services/strategy_package/multi_alpha_live.py`及
`backend/tests/strategy_package/test_frozen_multi_alpha_export.py`。

## Non-goals / 边界

无新训练/全期预测/账户，原38权重与6完整/32缺件原资产身份不变。不替换旧包，
不绕过V25退役，不写生产目录、DB或激活荐股/模拟盘。重启由用户执行。
原窗口固定融合经济门槛失败结论不变；技术恢复不等于建议发布。

## Design Acceptance Index / 设计验收索引

- F-001：新显式frozen_weighted仅消费真实成功run的冻结合同及原权重，不重估或改称IC。
- F-002：新manifest保留原rank、逐seed聚合、完整闭包、精度与来源，形成独立不可变身份。
- F-003：实际消费严格同键/有限/唯一，按源dtype均值后rank及固定权重，不静默裁样或回落。
- F-004：零权重仅作来源证据，不推理或冻结虚拟腿；不改变原名单支持及正权重预算。
- F-005：旧IC/默认行为及退役检查保持；真实资产与独立算术、失败反证有验收证据。

## Implementation Plan / 实施方案

仅新模式读取run持久化请求快照，核对唯一frozen_weighted、rank、关闭walk_forward、
完整roster/seed、声明权重与实际结果/per-window来源完全一致。声明source_ref必须
是可读、大小有界且SHA匹配的原文件，其原始字节嵌入冻结来源证据，避免仅留易失路径。
权重值不重新归一化。源合同无证据、失败/partial run或未完成资产直接拒绝。

源run含AF零权重对照腿，导出明确记录完整source_roster及zero_weight_legs；只有正权重
LSTM/FUND进入运行腿，并保持源共同参考面板与完整PIT键。正权重和必须已为1，不能删腿
后重分配预算。此项只消除数学系数为0且不参与选股的参考腿，不改变已有两个真实运行腿。

每seed冻结其实际原预测SHA及float32/float64精度，仍逐一绑定真实模型、拟合处理器、
schema与因子代码。源预测读取只用于核对已存在资产，不产生新预测。消费端将模型返回
的Python浮点还原为该seed原dtype，拒绝非有限、重复或缺键，再做均值、每日期average
rank[-1,1]和原权重相加。各腿键必须一致，禁止outer/inner裁样掩盖缺失。

新模式的归一化必须显式rank；缺少或冲突即失败，不采用旧zscore默认。已有IC包及其他
消费方法保留原路径。本模块的已有退役检查、包hash、runtime self check与资产保护不放宽。

## Verification Plan / 验证方案

测试覆盖真实promotion服务、两腿/多seed、严格源合同/权重/NA/键/精度、rank并列与独立
oracle、零权重来源腿、源文件缺失/变化及V25反证；旧IC直接回归。
fresh-process读取实际16来源、真实冻结合同及三日分数，对比原QE冻结公式和新消费端；
复用可证明相同的既有模型消费证据，任何新model.predict逐项计入63总预算。
本地与DEV资产写入只在隔离目录/既有DEV，不创建数据库。生产写入和正式激活另列且不执行。

## Risks / 风险

不同dtype均值会改变近似并列rank，不能统一转float64后冒称原语义。缺原seed闭包不能
用代表模型重复。UNKNOWN环境锁继续保留，不伪造依赖。历史已接触，不声称未消费holdout。
3日技术检查不替代全周期收益验证，现有负经济结果不被技术修复覆盖。

## Production Gates / 生产门禁

无DDL/依赖/数据集/服务变更。所有新版本留在研究/DEV，禁止覆盖旧manifest或生产资产。
源码合入、DEV资产验证、API加载和经济准入分别报告；原旧包仍是32原闭包缺件状态。

## Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/strategy_package/frozen_multi_alpha_contract.py; backend/services/strategy_package/multi_alpha_promotion.py | backend/tests/strategy_package/test_frozen_multi_alpha_export.py; execution/reviews/parent_frozen_formula_dev_v2.json | pass | - |
| F-002 | backend/services/strategy_package/multi_alpha_promotion.py | backend/tests/strategy_package/test_frozen_multi_alpha_export.py | pass | - |
| F-003 | backend/services/strategy_package/multi_alpha_live.py | execution/reviews/parent_frozen_formula_dev_v2.json; backend/tests/strategy_package/test_frozen_multi_alpha_export.py | pass | - |
| F-004 | backend/services/strategy_package/frozen_multi_alpha_contract.py; backend/services/strategy_package/multi_alpha_promotion.py | backend/tests/strategy_package/test_frozen_multi_alpha_export.py | pass | - |
| F-005 | backend/services/strategy_package/multi_alpha_promotion.py; backend/services/strategy_package/multi_alpha_live.py | backend/tests/strategy_package/test_frozen_multi_alpha_export.py; execution/reviews/parent_frozen_formula_dev_v2.json | pass | - |

实证文件位于忽略目录 `tmp/handoff/qe-parallel-next-plan-20261116/`。
验证使用真实16个正权重来源模型的已保存预测、两个来源运行的原权重文件及已发布分数，
对2026-04-01、06-30、09-30逐键验证新消费公式、原QE公式及实际发布分数和排序完全一致。
该步骤没有调用模型推理。源码、公式消费、完整资产冷推理、API加载、生产登记与经济验收分别报告。
