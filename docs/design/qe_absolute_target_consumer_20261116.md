# QE绝对目标权重消费（F1）

## 背景

用户批准共享协作文档revision317的T/B精确合同及最小独立消费入口开发。目标是检验跨行业趋势目标的实际扣费收益、回撤、成本和可执行性，服务后续人工Top20及荐股Top5/10研究。源码交付、运行加载、账户完成、入仓与经济验收分别报告，不能将准备通过写成实验完成。

## 范围与非目标

增加可选absolute_target_replay模式，复用QE custom evolution、active绑定、节点容量、提交、Qlib分钟执行和入仓。没有AF、模型、预测、标签、seed、训练或推理；不创建虚假目录条目。现有训练、预测回放、HMM、TopK默认行为不变。

首次预算仅T/B两个确定性账户，1000万元空仓。信号2024-07-01～2026-10-28，账户2024-07-02～2026-10-29共564交易日。原targets.parquet SHA256为696a7e4b3230d7ecb4e62cfc525aab86b64c2c5415d8c9bcc302dcdb892bfce8；完整566日原文件不裁剪、不改写、不推演未来日期。131行业及UNKNOWN保留。行业目标和股票绝对NAV权重直接读表，不重新计算，不加TopK、softmax、最小权重或0.95缩放；允许原执行约束下的残余现金、费用和未成交，不在目标集合外补位。

release=qe_hmm_full_v2_20261030，manifest=772112c58f959748787b939e33ec874fb0766ca921d88eddbd49c131c3b3c43d，profile=ca3fc50e75d64010d2cc91e5da5ef012cc3d6bfe8061f514ffe9d798ce2317e8。新任务正常解析active，核对持久化执行摘要而非仅显示用摘要；不一致则停止。历史回溯分类known_at=UNVERIFIED，全部历史为research_feedback。禁止数据集、生产策略包激活及服务控制；后端重启由用户执行。

## Design Acceptance Index

- F-001：正式模型自由模式、冲突拒绝和纯回测容量；禁止训练回落。
- F-002：原SHA、冻结绑定、完整日历及全量目标，严格一次T到T+1，保留UNKNOWN与131行业。
- F-003：原NAV绝对调仓及费用、现金、整手、风险、停牌和原TAIL_BOOST执行语义。
- F-004：完整Composer及独立worker/runner接线，不生成虚假预测、模型或IC。
- F-005：真实任务幂等提交、完整账户收集和可重试入仓的合同实现。
- F-006：默认兼容、直接回归、逐项审核和运行验收分离。

## 实施方案

新增QE所属的portable reader、strategy、record和后端consumer。显式目标模式在API、ExperimentConfig、模式选择、种子与容量、execution manifest和结果收集边界传递。Composer只为该模式生成无model/dataset的Qlib配置，以原执行参考SHA核对NestedExecutor/day、SimulatorExecutor/1min、TailTWAPWithLimitStrategy和原费用/整手。原冻结风险与停牌构建器及C-013/PIT校验继续使用，不新建撮合引擎。现有data_split中的训练/验证日期仅为active入口兼容元数据，实际目标运行没有训练/验证或标签任务。现有task表的label_horizon列为NOT NULL，物理行沿用默认1作为存储兼容值，strategy_evo_config明确标记label_status=NOT_APPLICABLE；目标Loop、运行清单、任务API展示和经济报告的期限均为NA，不生成或评价h1标签，无需DDL。

reader逐日读取Parquet行组，不构造N乘N矩阵。日目标缺席股票为零；损坏、缺日期、负值、NA、无可判定行业不得当安全值。策略使用原exchange报价和预决策NAV，卖出优先，现金和费用约束产生确定性订单意图；实际订单、成交、持仓和现金分别记录。独立record复用qlib.backtest及标准账户产物命名，不依赖pred.pkl。

完成收集核对实际564日账户和逐日消费收据；IC保持NA。入仓以版本化目标资产作为真实身份，不伪造因子/模型。results_only只重新收集原账户，不重新训练、预测或回测；另一账户尝试必须走原custom-evo意图核对与rerun入口，不能由通用retry猜测训练。

## 验证方案

Windows直接测试覆盖坏SHA、缺日、重复滞后、UNKNOWN、无效权重、整手费用现金、卖出退出与买入阻断、公开请求、容量及零新增账户的收集恢复。实际原目标覆盖564日、131行业、2756086个选中行；两个节点实际active/PIT绑定、正式请求准备和完整Composer见validation-receipt: tmp/handoff/qe-trend-parent-longtask-20261116/preflight_v3.json。原训练、预测回放、配置、容量、execution manifest及archive直接回归212 passed、6 skipped。正式任务及registry的真实DEV事务创建、读取和最终回滚通过，见validation-receipt: tmp/handoff/qe-trend-parent-longtask-20261116/target_dev_create_validation_v2.json；未留下DEV任务，也未提交节点账户。广泛回归委托CI；真正账户仅在运行加载验证后正式单次提交。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/routers/quantevolver_evolution.py；backend/services/quantevolver/experiment_config.py；qe_batch_execution_policy.py；seed_contract.py | backend/tests/quantevolver/test_qe_absolute_target_consumer.py::test_model_free_experiment_configuration_and_capacity | PASS | 无 |
| F-002 | backend/services/quantevolver/qe_absolute_targets.py；absolute_target_consumer.py | backend/tests/quantevolver/test_qe_absolute_target_consumer.py；validation-receipt: tmp/handoff/qe-trend-parent-longtask-20261116/preflight_v3.json | PASS | 无 |
| F-003 | backend/services/quantevolver/qe_absolute_target_strategy.py | backend/tests/quantevolver/test_qe_absolute_target_consumer.py::test_sells_first_exit_oddlots_trim_add_cash_fee_and_buy_block | PASS | 无 |
| F-004 | backend/services/quantevolver/absolute_target_consumer.py；config_composer.py；qe_absolute_target_record.py；scripts/qrun_limit_minute.py | validation-receipt: tmp/handoff/qe-trend-parent-longtask-20261116/preflight_v3.json | PASS | 无 |
| F-005 | backend/services/quantevolver/executors/backtest.py；qe_evolution_service.py；backend/services/qe_archive/asset_lifecycle.py | backend/tests/quantevolver/test_qe_absolute_target_consumer.py::test_results_only_recovery_never_submits_another_account | PASS | 无 |
| F-006 | 默认关闭的可选模式及原链路 | backend/tests/unified_engine/test_backtest_executor.py；backend/tests/unified_engine/test_qe_prediction_replay.py；backend/tests/qe_archive/test_qe_asset_lifecycle.py | PASS | 无 |

DESIGN-COMPLIANCE-001：F-001～006逐项审核源码合同与直接验证；无未批准的公式变更、简化、假成功或默认安全值。矩阵仅表示上述源码与准备验收，不表示账户已运行。

## 风险与失败模式

数千目标股票可能因100股整手、最小费用或交易约束留下大量现金；必须使用真实订单成交分析，不能用目标权重收益冒充账户。两臂目标敞口相同不保证实际敞口相同，实际敞口差异必须披露。回溯行业分类和历史反馈不能被标为严格as-known或未消费holdout。身份、资产或执行参数不符均失败关闭，不自动改绑或搜索参数。

## 生产门禁

正式请求已冻结于tmp/handoff/qe-trend-parent-longtask-20261116/formal_request_v1.json，尚未提交。源码合入后仅本机8001需要用户重启以加载新入口；执行节点通过原API接收本次worker，无须因此重启。验证加载、身份与业务准备后单次提交两账户。完成真实564日账户、2/2入仓、目标/实际行业敞口及全期分段经济分析，才可报告本轮实验完成。无生产策略包写入、激活、数据集更改或服务控制。
