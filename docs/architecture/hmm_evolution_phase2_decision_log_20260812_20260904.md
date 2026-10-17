# HMM Evolution Phase 2 决策与实验终态日志

> **用途**：保存父蓝图不再承载的历史审核、实验终态和方向变更；本文件不是active模型合同，也不授权重跑、阈值变化、源码、DDL、依赖或runtime动作。
> **Active authority**：`hmm_evolution_and_risk_management_system_design_20260716.md`与其当前引用的G2-A详细设计。
> **不可变原则**：历史candidate、holdout、artifact、失败reason和未执行动作不得因新合同而反写成功。

## 1. 2026-08-12～2026-08-14 产品验收方向调整

- 蓝图审核确认旧合同把逐sector结构、level和family串成全局合取，偏离日期×横截面的板块轮动产品目标；诊断、fit和artifact不得计入产品进度。
- 产品验收改为数值安全、逐sector语义、横截面样本外有效性、coverage/代表性四层；`FULL_READY|CAPABILITY_AVAILABLE|NOT_AVAILABLE`分离。
- 旧B3继续blocked，11个D6 failure、model/READY=0及artifact identity不变；不得以新验收语义追认旧结果。
- 审核结论：`PASS_BLUEPRINT_PRODUCT_ACCEPTANCE_REALIGNED_EXACT_SPIKE_CONTRACT_PENDING_NO_IMPLEMENTATION_AUTHORIZED`。

## 2. 2026-08-16 P2-3A终态

- 296个fit正常完成后在L1 lambda selection按合同停止，L2、holdout、model、READY、DB和runtime均未执行。
- 正式min-5 spread在18/18个lambda×fold为负，Rank IC仅3/18为正；即使诊断性缩小extreme组，也未形成稳定正向结果。
- 终态：`NOT_AVAILABLE_FOR_PROMOTION`；不得放宽阈值、反转label或重跑。

## 3. 2026-08-17 P2-3B与P2-3C

### P2-3B

- producer=`24e4ae79780e5bacdf34a3affb63d1db46f6d8a4`；request canonical=`f3d9014ba6c1aa59eceda41b148ab97e37bed5f0c05a471128b8dc0f26c471b1`。
- market 152/152 fits完成；L1 15个fold fit完成；总计167/184后按合同停止。
- alpha100三fold Rank IC=`-0.0078072859/+0.0867491657/-0.0570807842`，spread=`+0.0009441909/+0.0054583522/-0.0064346381`。
- 终态：`NOT_AVAILABLE_FOR_PROMOTION`；未读取holdout，未写model/READY。

### P2-3C

- 用户批准market-conditioned Ridge单一候选；36/36 fits完成，L1/L2均在development选择alpha100且Rank IC/spread为正。
- producer=`8ca1b98d…fbd0`；request canonical=`4807125d…6336`；report canonical=`792d4f6a…17e3`。
- development candidate冻结，`holdout_accessed=false`，不是FULL_READY或CAPABILITY_AVAILABLE。

## 4. 2026-08-23 P2-4终态与能力拆分

- acceptance canonical=`16004b24…7c87`；两个fresh-process payload bitwise一致；0 fit、无reselection、正式读取holdout。
- 最终`NOT_AVAILABLE`，未写model/READY，DB/runtime无变化；`2025-04-01..2026-03-31`永久成为已消费证据。
- L1 directional局部通过、risk失败，L2虽有正向Rank IC但spread/季度coverage/risk identity未共同闭合；该事实推动四能力独立验收，不追认旧candidate成功。

## 5. 2026-08-23～2026-08-31 C-012-RL1/HR1/RW1

- C-012-RL1曾批准五fold development、24-fit双fresh-process、独立holdout和最小writer；设计批准不等于能力。
- HR1将既有fold作为历史因果回放；正式执行在fresh process 1完成5个market和5个L1 Ridge fit后停止。median spread和两项OOF Newey-West t-stat未通过。
- RW1固定`rolling_window_open_days=252`执行；最终正式输入bundle canonical=`9d9658bff4c7074f962903fb0e64e8de10e041b24c96d458d2b59c8b24ac57aa`。
- 五fold `(Rank IC, spread)`为：`(0.037615,0.003153)`、`(0.050261,0.004616)`、`(-0.049110,-0.005888)`、`(-0.029526,-0.002161)`、`(0.034689,0.003874)`。
- OOF Rank IC：mean=`0.00833121109224675`、sample_count=`570`、NW lag=`9`、t=`0.5025403124977336`、variance_mean=`0.0002748365360182871`；由此精确LRV=`0.156656825530424`。
- OOF spread NW t=`0.4200001659947658`。fresh process 1完成10/24 fits后停止；第二process、final、holdout、model/product/READY、DB和runtime均未执行。
- RW1 fold符号变化不足以区分真实非平稳、低效应与抽样噪声；旧`TIME_NON_STATIONARITY_SUPPORTED`表述由2026-09-04审核降级，不再作为新模型类别的已证明理由。
- 终态：`ROTATION_L1_NOT_AVAILABLE`，parent failure canonical=`eee9e2e14ba319d47ca730393ea0df1c15acebdfa1f81e844d4befb091f605ba`。

## 6. 2026-08-31 输入执行架构修订

- 首次RW1启动曾在zero-fit request preparation约77分钟后由用户中断；0/24 fits，无request/model/bundle/READY。
- 根因是request preparation和fresh process重复执行多年数据库读取、PIT投影及面板构建；fresh-process复现不要求重复构造同一输入。
- 后续批准并实现最小immutable HMM input bundle；fresh process只读同一bundle并独立完成fold/preprocess/fit/hash。该修订不改变模型、窗口、阈值或旧终态。

## 7. 2026-09-03 G2-A新方向

- 用户确认HMM/jump只作因果market context，`rotation_L1`由单一浅层监督式非线性L1横截面scorer预测。
- development-only battery、5D/10D选择、唯一GBDT、全新tail和真实prediction/repository/API/UI属于同一G2-A，不并行L2、risk、第二模型或平台工程。
- MBE与MDE分离；旧P2-3A～P2-4、HR1、RW1终态不变。

## 8. 2026-09-04 功效与交付解耦决策

- RW1精确LRV下，若tail成熟日期约94，`MBE_IC=0.02`对应MDE约`0.1015`，说明短尾部对最低业务效应功效不足；这不是所有模型失败证明，也不禁止唯一GBDT。
- 用户批准Rank IC 0.02为唯一binding MBE，来源标记`CONVENTIONAL_PRIOR_MAGNITUDE_NOT_VALUE_DERIVED`；spread降为经济解释与非阻断方向分歧诊断。
- 用户批准`research_product_gate`与`tail_access_gate`解耦：真实OOF可闭合experimental surface，但不得推导rotation capability；tail access要求development OOF Rank IC达到0.02。
- 用户批准MDE只决定forward power状态；effect failure使用实际tail one-sided 95% HAC上置信界`<=0`，不硬编码样例阈值。
- 用户批准`min_child_samples=310`及训练后每叶至少20个distinct decision dates；低于阈值typed失败且不得调参重训。
- 用户批准fold-local K=2 market context不读取target并由5D/10D共享；battery上限保持15 fits，完整G2-A上限保持39 fits。
- performance-based date abstention删除；只保留输入、market context、score或identity合同导致的typed unavailable。
- 审核结论：`USER_APPROVED_POWER_AND_DELIVERY_SPLIT_REMAINING_EXACT_CONTRACT_PENDING_NOT_IMPLEMENTATION_READY`。

## 9. 不可变状态边界

- 本日志中的设计与审核不授权源码、fit、tail读取、model/product bundle、DDL/DML、依赖安装、runtime activation或进程控制。
- 历史artifact保持原路径只读；不复制、不迁移、不重新聚合为新产品成功。
- active合同只以父蓝图和当前G2-A详细设计为准；本日志不能覆盖其最新批准状态。
