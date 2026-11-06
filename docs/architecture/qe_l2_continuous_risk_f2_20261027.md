# QE L2季度风险连续消费与五臂评价

日期：2026-10-27；tier=F2。用户授权：开发QE侧、补齐缺口、多轮审核修复后提交合入；不包含服务重启、数据集写入或替HMM训练。

## Background / 背景

HMM季度交接要求B/M0/M1/X0/X1同源配对。既有ENTRY-GATE-D1..D7仅过滤买单，不代表连续权重合同。复用ScoreWeighted V2目标权重/补充调仓钩子、C-013/PIT映射、完整131行业以及125日终端适配。数据窗口已修复缺数，本包不重新排查或修改数据。

## Scope / 范围与 Non-goals / 边界

独立可选合同`QE-L2-CONTINUOUS-RISK-V1`，默认无配置不进入新路径。仅QE消费、compose、研究计划与评价、测试；不改HMM、旧preset_A、旧门控/行业overlay、默认策略、数据集、服务、数据库。M1资产及生产激活是外部交付状态；源码验收不伪称真实模型经济接受或实验完成。

允许文件：本设计；backend/services/quantevolver/config_composer.py；新增backend/services/quantevolver/l2_continuous_consumer.py、l2_continuous_evaluation.py；新增scripts/qe_l2_continuous_admission.py、qe_l2_continuous_consumer.py、qe_l2_continuous_plan.py；新增backend/tests/quantevolver/test_qe_l2_continuous.py及独立真实Qlib验证脚本。原始资产不写入Git。

## Architecture / 架构

CI归属补充范围：`tests/aistock_validation/catalog/file_ownership.yaml`为新脚本登记`qe.core`，复用既有`qe_read_backend`直接测试计划；不改变CI策略或增加新门禁。

正式HMM M0/M1_predictions.json及模型bundle → QE源码审核的资产准入记录 → 完整概率/时钟/映射检查 → 现有AF策略目标权重钩子 → 连续目标与持仓差额订单 → 原分钟TWAP/成交约束 → 目标/订单/实际成交及净值分离审计。B/M0/M1先运行；X0/X1读取各自M臂预成交目标敞口带独立运行。基础Alpha、seed、池、费用和初始账户一致。

## Contracts / 契约

1. 基础AF评分、TopK选择及原基础目标权重算法不变。M0/M1对每个基础目标`w_base`使用`w=w_base*(1-p)`；p=0不增加额外调仓，曾发生风险缩减时允许恢复到基础目标。warning>=0.20仅审计，不硬禁买。原min/max权重约束先作用于AF，缩减后不再次归一化或用min_weight抬回；剩余预算留现金。基础目标来自当天AF计算，不乘昨日实际缩减仓位，避免复利式去仓。
2. 基础买卖订单保持；新买单按调整目标产生。已持仓通过补充调仓钩子按绝对目标差额买卖，不叠加已有买卖订单。保留hold_thresh、T+1、交易单位、资金、单单金额、报价和停牌/涨跌停限制；不可成交留原持仓并记录目标/实际偏离，不虚构成交。连续消费包含持仓调仓，其与旧B的总效果不等于纯概率排序效应；M-X用于区分总敞口影响。
3. 输入严格完整125×131，按decision日使用当日概率，其as_of必须是前一交易日，不再shift。合法unavailable和权威分类不可用：目标0、留现金/按交易约束尝试退出，保留reason；预测缺行、重复、未知映射状态、未知日期和哈希漂移失败。不补p=0、不沿用旧概率、不因NA或标签未成熟删净值日。
4. M0固定原模型；M1按Q2/Q3模型bundle、训练末日及生效区间逐行核验。读取正式`hmm_risk_l2_quarterly_rolling_v1_predictions`，验证producer canonical receipt、外部文件SHA及模型参数SHA。准入是QE源码审核的不可变记录，不接受请求自带allowlist；真实资产未交付则明确ASSET_ADMISSION_UNKNOWN，不编造记录。模型身份与推理release身份分开，旧准入不重绑。
5. `E_m(t)=sum(w_m(t))`在下单前记录。X_m按自身同一AF基础目标等比例缩放至E_m，比例在[0,1]；不可实现的目标明确失败，不借未来收益改比例。参照带只接受日期/as_of/目标敞口/来源模型及共享预测身份，不读取未来收益、实际成交敞口或期末净值。各臂持仓路径可能不同，因此同目标敞口不宣称逐股或实际成交敞口完全相同。
6. B保持原AF行为，另外仅记录基础目标；不是新建日更再平衡基准。历史B仅在预测、数据/PIT、初始状态、策略执行及费用完全一致时复用。五臂共享每seed一份冻结基础预测，默认42/2024/2026，最多15账户、无HMM或Alpha训练；资产身份变化则报告基础预测重建需求，不自动重训。当前运行批次不扩展预算。
7. 2026-04-01..09-30完整125账本，保留原终端区间适配和真实收盘估值，期末持仓不强平；基础信号含03-31。9月未成熟h20为NA。报告全期、Q2/Q3及月度净收益、回撤、费用、换手、现金、指数超额、目标/实际敞口、M1-M0、M-X、X-B和机会成本；不把seed视为独立市场样本。历史回溯分类和已消费窗口均明确披露。

## Design Acceptance Index

- F-001：资产准入、季度身份与完整目录。
- F-002：连续目标、合法NA、时序与无中性回落。
- F-003：已有持仓差额调仓和订单/资金约束。
- F-004：五臂共享预测、因果同敞口参照和执行计划。
- F-005：独立compose、125日末日适配、旧路径不变。
- F-006：分段配对评价与实际消费审计。
- F-007：多轮审核、真实Qlib fresh-process验证和交付边界。

## Implementation Plan / 实施方案

冻结本合同 → 准入/完整输入reader → 纯目标函数及策略钩子 → compose/预检CLI与五臂计划 → 评价 → 作者多轮逐项审核及修复 → 本地最小矩阵及真实Qlib进程 → PR/CI/合入。接线和运行资产就绪状态分别交接。

## Verification Plan / 验证方案

检查真实持仓减仓/恢复、新买单、合法NA现金、非交易日/错as_of、重复和缺行、未知季度、文件及模型身份篡改、无未来参照、p=0和无配置路径等价。Windows独立单测不冒充Qlib验收；用WSL已安装Qlib独立进程对实际策略钩子和真实Order/Position类验证订单与金额，不重训、不运行未授权研究批次。广域回归交CI。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | scripts/qe_l2_continuous_admission.py; scripts/qe_l2_continuous_consumer.py | backend/tests/quantevolver/test_qe_l2_continuous.py | verified | none |
| F-002 | scripts/qe_l2_continuous_consumer.py | backend/tests/quantevolver/test_qe_l2_continuous.py | verified | none |
| F-003 | scripts/qe_l2_continuous_consumer.py | backend/tests/quantevolver/test_qe_l2_continuous.py | verified | none |
| F-004 | scripts/qe_l2_continuous_plan.py | backend/tests/quantevolver/test_qe_l2_continuous.py | verified | none |
| F-005 | backend/services/quantevolver/l2_continuous_consumer.py; config_composer.py | backend/tests/quantevolver/test_qe_l2_continuous.py | verified | none |
| F-006 | backend/services/quantevolver/l2_continuous_evaluation.py | backend/tests/quantevolver/test_qe_l2_continuous.py | verified | none |
| F-007 | 本设计、交付回执 | validation-receipt: tmp/handoff/qe-l2-continuous-dev-20261027/validation.json | verified | none |

## Risks / 风险

持仓差额调仓会改变费用及路径；必须分别归因，不把纯预测质量与组合收益混同。真实M1准入依赖生产者完整交付，未交付时保持明确不可运行；这不等于补入默认资产。假设目标敞口匹配不代表受成交约束后的实际敞口匹配，单独披露残差。

## Rollout / Rollback / 发布与回滚

新配置默认不启用，工作区随compose携带独立脚本，不覆盖旧资产或默认策略。取消本研究配置即回旧路径；不删除旧结果或修改运行中批次。源码合入不等于常驻API激活或真实实验通过。

## Production Gates / 生产门禁

production_ddl_gate=noop；无DML、数据集、官方指标或评级写入；无依赖安装；backend_restart_owner=user，本包不重启；无自动晋升。新资产尚未交付时单列asset_ready=false，真实十五账户实验另按既有授权及就绪情况执行，不将测试产物登记为研究资产。

## 审核修复与验收证据

第一轮核对连续目标、合法NA、持仓差额、现金与费用，修复交易数量重复除权及资金预留缺少费用的问题。第二轮检查compose、资产传输与因果参照，将冻结绑定及参照身份校验前移到文件打包之前；不允许先返回未经准入核验的资产。第三轮补充producer交易日历、C-013行业权威映射以及M1模型训练输入和训练release的一致性核验，并增加篡改拒绝用例。

新功能测试首轮最终矩阵为25项通过、1项Windows无Qlib跳过，随后新增传输身份用例及两项训练身份用例均通过，受影响节点也已复验；旧L2相关回归50项通过。WSL使用已有Qlib独立进程执行真实AF父类、Order、Position和交易费率路径，五臂均通过。合成持仓用例卖出250份、剩余750份、费用5、现金12495、账户19995；次日不重复缩减。以上是功能及账务证据，不代表真实M1收益验证。

真实M0/M1生产准入表保持为空，等待正式生产者资产及共享AF预测身份交付，不将测试fixture登记为生产研究资产。未获准或缺少身份时明确失败，禁止编造默认概率或临时换绑。多轮审核没有训练HMM、创建新的经济实验、修改数据集或重启服务。

DESIGN-COMPLIANCE-001逐项检查：完整实现本设计七项范围，无未批准的简化或POC；失败显式抛出；未改变旧合同业务语义；未新增人工审批门禁。F2验证7/7通过、warnings=0。详细回执位于tmp/handoff/qe-l2-continuous-dev-20261027/reviews.md及validation.json；广域回归交PR CI验证。
