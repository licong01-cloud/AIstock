# Advisory H-VALUE-ANCHOR-1 独立标签与纯价格内核（F1）

> 日期：2026-10-03；v0.1；交付范围仅为离线标签与计算内核。父级[F2研究设计](advisory_economic_value_anchor_v1_f2_design_20261003.md)。未训练、未执行真实新收益比较、未交付每日API/UI或资格。

## Background / 当前进度

设计PR #5344已合入3ee16cac00c69038844de6cbef355981084e6d75。当前切片落实价格无关复评场景、未来gross价值/路径下沿标签与支持域内价格集合；不把工程计算正确性当作经济有效性。

## Scope / 精确文件

仅新增`backend/services/advisory_model_first/economic_value_anchor_contracts_v1.py`、`economic_value_anchor_labels_v1.py`、`economic_value_anchor_inference_v1.py`，以及`backend/tests/advisory_model_first/`下三个对应同名叶测试和本文。既有episode labeler、shadow simulator、core和cost只导入不修改。

## Non-goals / 边界

不实现训练、研究编排、经济评价或正式每天推荐；不修改QE/Selection/HMM/Paper/执行/公共数据，不重选候选、不读库、不改历史产物。不给原模型补新schema，不改变生产退出。无新合格模型、binding或服务操作。

## Implementation Plan / 实施合同

1. `contracts`：独立VALUE_REVIEW_5_V1固定场景与不可变估计/支持/价格输出；0关闭价格触发、5次有效复评、原rank40/confirm2、原费用。
2. `labels`：严格父成本/名单hash、原Top20/Top40、D/T、源/参考/坐标。复用共享labeler生成新场景episode；用D参考与T换算生成Y/L，T close至退出open保留日级mark，退出后close不读。正常缺失/停牌/执行未证明输出UNKNOWN而保留20；身份矛盾fail closed。仅纯DataFrame输入，真实manifest授权/字节核验由后续研究切片负责。
3. `inference`：train-only无收益支持，100bps/30观察/5D及固定中央95%范围；只有价格域内的纯估值，不把观察支持当因果重叠。费用一次，Decimal明确严格收益根和边界，按合法tick向内；不跨支持洞。输出NAV、nondeploy，不声称价格等于开盘预测、真实成交、条件于T价的收益期望或用户资金预算。

所有原候选/日期保留；因信息不明停止实际经济比较的要求归属后续评价切片，不在此伪造完成。单点估值和批网格调用同一纯predicate；无需运行后端。

## Verification Plan / 最小合同测试

只运行三个直接叶测试，不加相邻模块回归扫描：新场景不改变原政策；time/rank退出时买价变化不改变端点；停牌和跌停延期labeler/portfolio一致；企业行动同坐标价值不变；退出后close毒化无效；正常未知保留、名单/源/未来参考矛盾拒绝；成本一次、支持洞/tick严格边界、train-only及收益列毒化不改支持。没有实现快照或重复大fixture。

## Design Acceptance Index

| ID | 要求 |
|---|---|
| F-641 | 价格无关独立场景，不变更共享/生产退出 |
| F-642 | Y/L时钟、坐标与执行未证明/正常缺失保留 |
| F-643 | 精确名单和来源矛盾fail closed |
| F-644 | D训练支持无收益筛选、价格内核成本一次与洞/tick边界 |
| F-645 | 单点/网格一致，范围及工程/经济/启用分报 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-641 | backend/services/advisory_model_first/economic_value_anchor_contracts_v1.py | pytest backend/tests/advisory_model_first/test_economic_value_anchor_contracts_v1.py backend/tests/advisory_model_first/test_economic_value_anchor_labels_v1.py | VERIFIED | none |
| F-642 | backend/services/advisory_model_first/economic_value_anchor_labels_v1.py | pytest backend/tests/advisory_model_first/test_economic_value_anchor_labels_v1.py | VERIFIED | none |
| F-643 | backend/services/advisory_model_first/economic_value_anchor_labels_v1.py | pytest backend/tests/advisory_model_first/test_economic_value_anchor_labels_v1.py | VERIFIED | none |
| F-644 | backend/services/advisory_model_first/economic_value_anchor_inference_v1.py | pytest backend/tests/advisory_model_first/test_economic_value_anchor_inference_v1.py | VERIFIED | none |
| F-645 | 本文；backend/services/advisory_model_first/economic_value_anchor_inference_v1.py | pytest backend/tests/advisory_model_first/test_economic_value_anchor_inference_v1.py | VERIFIED | none |

## Risks / 审核修复记录

业务轮：明确rank/time退出和最早T+1、有效计数延期；最小真实引擎例子证明两类退出不随买价变化，停牌及跌停延期同组合。源范围轮：补齐父名单hash、完整rank/target唯一性与原evidence limitations，不造原生证据。数值/PIT轮：修复浮点gap造成边界tick丢失和收益根的数值误收；改用Decimal，严格D时钟，不隐藏非法估计为UNKNOWN控制；补充同坐标缩放与退出后close毒化。三轮是本窗口不同视角自审。

模型可能根本无增量，估值单调不表示低价安全；本内核没有任何现金/订单接口。接口接受已证明合法价格域，但其法规来源真实性仍由业务消费者负责；不能单凭这三叶代码宣称完整每天荐股已实现。

## Production Gates / 发布与回滚

纯离线新增模块，旧调用不路由到本family；合入无需启动/重启服务才能用离线代码，不宣称后端已加载。本轮数据库/profile/binding/依赖/进程均noop，临时X，持久F。后续训练切片只有本切片审核通过后进入，未生成任何新模型/经济证据。

DESIGN-COMPLIANCE-001四项：范围内功能实际实现而非placeholder；未知不伪成功；原政策不改且费用/场景不事后放宽；无新平台/日期/历史固化门禁。整个父F2仍按阶段报告，不把本F1完工冒充训练或每日交付。
