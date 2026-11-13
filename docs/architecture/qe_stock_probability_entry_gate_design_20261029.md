# QE 股票概率买入消费：F1 单模块设计

## 背景与接收合同

用户已授权在主线结束后执行资格退出风险 B/G/F 三臂配对验证，并要求新增共享能力前报告精确缺口。QE 已报告缺口并接收交接中的唯一规则。本设计实现 `QE-STOCK-PROBABILITY-ENTRY-GATE-V1`；仅消费已冻结的股票风险概率，不执行研究训练评价脚本，不要求生产因子登记。

基准为 C1 `qe_20261028_104720_a38a` 的 LGB Loop1/3/5，seed 42/2024/2026。当前消融任务的负结果不改变基准。G/F 共用原 AF、PIT、样本、资金、执行和费用，至多九账户，新增训练和 AF 推断均为零。旧 B 只有完整可比性成立才复用。

## 范围与非目标

只修改 QE Composer、`executors/backtest.py` 的纯回放/预测 SHA 校验、可选 consumer、ScoreWeightedV2 默认恒等买入候选钩子及对应测试。新文件为 `stock_probability_consumer.py`、`qe_stock_probability_gate.py`、`qe_stock_probability_gate_strategy.py`。不改 HMM、preset_A、数据集、生产目录或官方指标。新 HMM 组内调权需求排在本实验分析之后，本设计不批准其新公式。

## Design Acceptance Index

- F-001：显式可选接线。未启用时保留原配置及交易行为；拒绝与其他 overlay 混用。只支持正式 prediction replay 的 ScoreWeightedV2 / 原停牌与 PIT 包装器。
- F-002：冻结资产。核验 manifest、概率文件、预测 SHA、同一 direct-v2 数据/PIT绑定、日历与信号日期；重复键、缺少日期或股票、无效概率、身份漂移均失败。
- F-003：因果时序与 NA。执行日 D 读取前一实际交易日 T 的风险；值的 input_as_of 是 T 的前一交易日，不再 shift。显式 unknown 拒绝买入/加仓，已有持仓按原规则处理。结构缺项不降级成 unknown。
- F-004：唯一阈值与订单合同。2026 F=0.004557943883386495、G=0.004670301317604409，严格 p>阈值阻止买入，等于允许。阈值来自截止 2025-12-31 的冻结拟合，不重新估计。
- F-005：保留原评分、TopK 出售候选、原持仓评分阈值、权重函数和执行条件。仅在买入候选中顺原排名递补至原买入候选数量；正常换仓仍通过原显著性规则，不额外强制卖出。所有最终 BUY/加仓和 inner 备选股票再检查风险。无合格递补时允许现金增加。
- F-006：真实意图与成交可区分。记录原买入候选、扫描候选、拒绝原因、递补候选、最终订单及备选股；意图不是成交。原生 orders/positions 为成交与现金核对依据。只写明确的工作区日志。
- F-007：完整 Composer 与 fresh-process 读取测试；源实现、运行激活、实验和经济分析分别报告。零 fit/零 AF 推断，不以单元测试代替实际收益结论。

## 实施方案

控制面使用独立 `qe_stock_probability_study` manifest；Composer 首先提取，完成原始组合后验证同绑定和基础策略，再注入可选类、不可变输入文件及执行前读取检查。只在显式参数存在时打包新脚本。概率文件通过现有 experiment_files 的 `.b64` 路径传输；manifest 引用固定工作区文件名，节点运行时重新验证 SHA。

manifest 记录风险臂、固定 policy、资产 SHA/行数、源训练截止、完整 direct-v2 binding、原预测 SHA、原账户起止日以及日历引用。资产列为 signal_date、input_as_of、instrument、probability、availability、reason。available 要求有限 [0,1] 且无 reason；unknown 要求概率 NA 和非空 reason，禁止默认安全值。

G 只按交接的四输入系数和季节校准做纯推断，采用与 F 相同的十输入完整性 mask；保留原始跨截面 rank、float32 转 float64 语义。以交付的 2,865 条买入诊断作数值 oracle，不能将这些子集行用作回放资产。F 从已复现的负安全分数还原 p=-s；不是新公式或翻号试验。

ScoreWeightedV2 增加 `_prepare_buy_candidates` 默认恒等方法，不修改 TopK 集合或 scores。实际 C1 选择的是旧目录源码；正式 Composer 仅在其原买入候选排序完成处添加该恒等钩子，并证明移除钩子后 AST 与原源码相同，不能替换成较新的默认策略。B/G/F 使用同一份钩子源码。可选派生类先保留原候选（含原并列顺序），再按原 ranked 顺序递补，保留原买入候选数量上限。既有持仓从不因风险而删除评分。备选股及最终正向订单检查避免绕过；保留原卖出订单、T+1、停牌、涨跌停、费用、整手和现金约束。买卖配对是否实际发生仍由原换仓显著性、持有期限及成交条件决定。

首日没有前日 AF 时沿用原空仓结果，仍验证日历与风险资产结构；不制造上一日 AF。原 C1 账户窗口为 2026-01-05..09-29（180 日），风险信号到 09-30 不表示账户含末日。任何改变账户边界须另行证明并共享重放 B；本开发不增加日期。

## 验证方案

在 `tests/aistock_validation/catalog/file_ownership.yaml` 登记两个新增 worker 脚本及对应 consumer/tests 的 QE core 归属，复用既有 QE 直接测试计划；不新增 CI 工作流或门禁。

直接测试阈值等号、unknown、重复/缺日/缺股票、错 SHA/绑定/预测、重复滞后、替补排序、原持仓不因风险强卖、最终加仓过滤、备选股过滤、默认恒等与真实 Qlib 订单。以完整 F/G 资产运行 fresh-process 校验及完整 Composer；依赖已有 Qlib 环境，不安装依赖或重启服务。

F1 验收前运行设计校验、changed-file lint/compile、direct tests、diff check。外围回归交 CI；不增加与本修改无关的测试矩阵。

## 风险与失败模式

G/F 可能降低参与和错过上涨，收益分析同时报告成本后 F−B、G−B、F−G、现金、退出损失、机会成本和 H1/Q3。三个 seed 共享市场历史，不能当作独立市场样本。9 月未成熟 h20 保持 NA。缺失也可能影响递补数量，必须单列 unknown 拦截。

## Production gates

DDL/DML、生产目录写入、数据集更新、依赖安装均为 noop。源码合入不等于 API 激活；后端重启由用户执行，加载核验通过后才单次提交实验。正式实验账户最多九个，不重跑旧主线，不调参、不晋升。此设计批准的是因子风险消费实现；HMM 的新损失调权公式仍待后续精确合同。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/quantevolver/stock_probability_consumer.py; backend/services/quantevolver/executors/backtest.py | backend/tests/quantevolver/test_stock_probability_consumer.py; backend/tests/unified_engine/test_qe_prediction_replay.py::test_stock_probability_study_cannot_accidentally_fit_or_change_prediction | verified | - |
| F-002 | scripts/qe_stock_probability_gate.py | backend/tests/quantevolver/test_stock_probability_consumer.py; artifact:tmp/handoff/qe-trend-capture-plan-20261029/next_tasks_v3/run_v1/factor_node_validation_v2/completion.json | verified | - |
| F-003 | scripts/qe_stock_probability_gate.py | backend/tests/unified_engine/test_qe_stock_probability_strategy.py | verified | - |
| F-004 | scripts/qe_stock_probability_gate.py | backend/tests/quantevolver/test_stock_probability_consumer.py | verified | - |
| F-005 | scripts/score_weighted_strategy_v2.py; scripts/qe_stock_probability_gate_strategy.py; backend/services/quantevolver/stock_probability_consumer.py | backend/tests/unified_engine/test_qe_stock_probability_strategy.py; backend/tests/unified_engine/test_score_weighted_strategy_determinism.py | verified | - |
| F-006 | scripts/qe_stock_probability_gate_strategy.py | backend/tests/unified_engine/test_qe_stock_probability_strategy.py | verified | - |
| F-007 | backend/services/quantevolver/config_composer.py | artifact:tmp/handoff/qe-trend-capture-plan-20261029/next_tasks_v3/run_v1/factor_composer_preview_v3/completion.json; artifact:tmp/handoff/qe-trend-capture-plan-20261029/next_tasks_v3/run_v1/factor_node_validation_v2/completion.json | verified | - |

## 实现审核与直接证据

两轮均为作者审核，不宣称独立第三方审核。第一轮核对交易语义，发现实际 C1 目录源码不同于新版默认策略，改为只给已选源码加恒等钩子并验证 AST；修复原候选并列顺序保留和 inner 备选路径。第二轮在真实节点验证 Python 3.10 兼容性，并修正测试对旧策略备选股集合的错误假设；风险消费仍只过滤不安全/unknown 备选，不改变原策略的已下单股票处理约定。

Windows 最终相关矩阵 18 项通过。远端 Python 3.10.20、WSL Python 3.10.19，分别对 G/F 完成全量 967,083 行读取、181 信号日/180 账户日、真实冻结预测 SHA、物理数据/PIT 元数据验证，并各通过四项真实 Qlib 订单测试。新增 G 与原交付 2,865 行 oracle 最大误差 6.94e-17；F 原值与同源缓存算术对账误差 5.00e-16。两臂共同保留 89,516 行 unknown，无额外滞后。完整六 Loop Composer/执行器预览没有实际调度、训练或 AF 推断。

DESIGN-COMPLIANCE-001 四项检查：本次源码能力与索引逐项对应；结构缺失明确失败，NA 明确拒绝新增买入；AF 模型/评分、原卖出规则、费用和执行未替换；未新增生产目录、数据集控制或人工审批流程。源码验收不宣称实验经济价值或 API 已加载。合入后仍需用户重启本机 API 并完成加载身份/正式入口读回，才在现有授权内单次提交。
