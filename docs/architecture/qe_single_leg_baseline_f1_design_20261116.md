# QE 单腿原始预测均值基准 F1 设计

## Background / 背景

用户已批准2026-11-16的20h研究计划并要求启动。两个必需AF集成和最多三个条件
集成需要一个真实账户；原入口要求两腿并总是生成scheme账户，不能精确表达。
在途五次训练和八个单预测回放独立接续。本功能只扩展显式消费能力，不增加预算。

## Scope / 范围

仅编辑本设计、`backend/services/multi_alpha/combine_backtest.py`、
`backend/services/multi_alpha/durable_orchestrator.py`和
`backend/tests/multi_alpha/test_baseline_only_contract.py`。
复用现有REST模型、durable planner/materializer、归档和全局准入，无DDL或依赖变更。

## Non-goals / 边界

不改变任何旧请求默认值、IC/rank公式、策略、调度器或数据集；不创建虚拟腿、
复制seed、修改旧预测、训练或推理。不会自动启动额外账户或晋升策略包。
生产资产和API重启仍由用户控制；source/DEV/runtime/账户经济验收分别记录。

## Design Acceptance Index / 设计验收索引

- F-001：显式单腿baseline_only只生成一账户，非法混合模式和含糊默认拒绝。
- F-002：全部seed原始分数严格同键、有限、唯一；同序等权均值，不归一化、裁样或补NA。
- F-003：REST→快照→身份→planner→实际materializer及恢复保留合同，不读取IC。
- F-004：legacy与durable在唯一基准真实成功时完成；失败及缺结果不冒充成功。
- F-005：旧请求和默认身份、既有组合及失败检查不变，真实资产fresh-process有证据。

## Implementation Plan / 实施方案

请求明确传入：单个roster腿及唯一seed_run_ids，`weighting_schemes=[]`，
`prediction_task_selection={"include_baseline":true,"include_loo":false,"baseline_only":true}`，
`walk_forward={"enabled":false}`。baseline_leg_id省略时为唯一腿；指定时必须相符。
拒绝scheme、LOO、frozen_weights、rank_fusion或第二腿混入，不能静默忽略配置。
normalize_method字段沿既有存储兼容值；baseline子任务无腿间标准化，其得分始终是
原始seed均值，不能把该字段当成执行了rank/zscore。

复用严格预测规范化器仅规范日期/证券键，拒绝非有限及重复键，保留数值dtype。
所有seed必须具有完整一致键；均值为现有行方向算术均值，不按标签或交集裁剪。
只在严格检查后沿既有账户窗口选择日期，不改变股票样本或seed成员。

baseline_only只在调用方明确传入时加入归一后的selection快照，旧请求结构不变。
既有planner自然产生一个baseline子任务；其输入保存selection及全部seed来源。
durable finalizer必须读取并校验冻结请求且核对唯一baseline；没有新显式合同仍沿
旧“至少一个scheme成功”检查。legacy结果保存同样显式判定，失败继续失败。
归档沿现有成功attempt的真实result_manifest读取，不创造scheme结果行。

## Verification Plan / 验证方案

直接合同测试覆盖REST/快照回读/身份变化、唯一child、真实materializer原始均值
算术及dtype、并列/多日、缺键/NA/重复seed和非法模式；拒绝读取IC面板。
覆盖legacy及durable终态的成功、失败、错误child和旧缺scheme反例。
fresh-process用已有真实冻结AF预测限定三日核对独立均值oracle和原文件SHA；
不调用model.predict、不训练、不回测、不写生产。实际323日实验另作运行验收。

## Risks / 风险

旧API未加载新代码会拒绝模式，不允许退回旧入口或增加账户。单腿原始分数的尺度
不能冒充rank融合；strict seed键不一致即失败，不能靠交集缩小样本。沿既有归档、
取消和全局容量机制，无新控制或绕过。多个seed不当作独立市场样本。

## Production Gates / 生产门禁

无DDL/DML、依赖或数据集改动；既有DEV仅验证读/compose和真实保存预测消费。
不写生产策略目录，不替换冻结身份，不启停服务。合入后仍待用户重启8001与加载核验。

## Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/multi_alpha/combine_backtest.py:parse_request | backend/tests/multi_alpha/test_baseline_only_contract.py | pass | 无 |
| F-002 | backend/services/multi_alpha/combine_backtest.py:build_prediction_only_legs | backend/tests/multi_alpha/test_baseline_only_contract.py | pass | 无 |
| F-003 | backend/services/multi_alpha/combine_backtest.py:request_snapshot_for; durable_plan.py; durable_execution_adapter.py | backend/tests/multi_alpha/test_baseline_only_contract.py | pass | 无 |
| F-004 | backend/services/multi_alpha/combine_backtest.py:_persist_task_outcomes; durable_orchestrator.py:_parent_status | backend/tests/multi_alpha/test_baseline_only_contract.py | pass | 无 |
| F-005 | existing request paths; real AF frozen assets | backend/tests/multi_alpha/test_baseline_only_contract.py; F:/Dev/AIstock/tmp/handoff/qe-parallel-next-plan-20261116/execution/reviews/baseline_only_dev.json | pass | 无 |

fresh-process沿现有正式materialize_child_input读取真实冻结AF42/2024/2026，三日
2026-04-01..04-03、14,751行，原float64均值逐值等于独立pandas oracle，源文件
计算前后SHA一致。直接测试另覆盖float32。0fit/0model.predict/0回测/0传输/0DB写入。
收据`F:/Dev/AIstock/tmp/handoff/qe-parallel-next-plan-20261116/execution/reviews/baseline_only_dev.json`。
不据此声称323日账户、收益或运行API加载已经完成。

DESIGN-COMPLIANCE-001作者两轮逐项检查覆盖F-001～F-005：没有虚拟腿、额外账户、
默认回落或样本裁剪；baseline-only的durable成功严格绑定其冻结请求和唯一真实子任务，
旧缺scheme成功检查不变。legacy路径仅为仓库已有测试兼容路径，生产正式入口仍为
durable，未重新开放旧执行引擎。无新增控制门槛或审批，不触及在途任务及旧资产。
