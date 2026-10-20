# 多时期固定5交易日 Top10 有限模型比较（F1）

版本：v1.0；日期：2026-10-21；业务归属：Advisory / model_first。
依据：荐股蓝图 v5.21 §6.3.4、§6.10、§16.0 的 B0–B3，不是旧 serial candidate 重试。
状态：详细设计待交付；源码、研究结果、经济确认、角色激活分别验收。

## Background / Goal / Feature Card

研究问题：在已批准策略包原始冻结 Top50 内，使用 D 收盘可见输入，有限比较正则线性与受限非线性模型，
预测从紧邻交易日 T 开盘至 T+4 收盘的五交易日净收益相对值，研究派生 Top10 是否优于原始 Top10。
不是预测开盘价，不输出买卖价格区间、最优分钟、绝对收益承诺或资金仓位。
本轮改变量是更长的多时期开发样本及预注册模型表达，不继续单信息块、单 Ridge、最多两次换股的旧试法。

本任务为单模块离线 F1，不改生产荐股、LocalSIM 或共享信号内核。已有 QE 包直接消费，不增加包准入门禁，
不重做 QE 训练或对其未来数据合同二次审批。父评分跨日期不假设可比。

## B0 已核实数据与证据等级

2026-10-21 元数据/评分只读检查（0 SQL、0 新 fit、0 收益读取）：

- 两包各 2,581,509 条冻结评分、528 个 D、每日 4,784–4,933 只；2024-07-01..2026-08-31。
- LSTM：`pkg_cc7eccb6202e4816b9d7c7d6b748bd37`，run `qe_20261007_141342_0221_L4`；
  prediction SHA `e19afb9ebf230256f59c51aeaa16c643c9fa9d2bd639a7e747c8a4627fd348c5`。
- TCN：`pkg_8dbd396d27184de6aa5fe288011220f6`，run `qe_20261006_013449_0c29_L4`；
  prediction SHA `50ebdc4c558bf091d869cba2502605034a9f725083113e1b2e679b8d14860129`。
- 既存文件 binding `589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`，
  release `qe_hmm_full_v2_20260831`，generation `20261015-v18-unified-quote1`，覆盖至 2026-08-31，
  canonical PIT `aistock_equity_pit_canonical_v2 / shsz_a_252td_st_delist_asof_v2`。
  此绑定作为显式研究输入，不因 active profile 更新改变边界，不回滚或修改全局 active profile。
- 旧 N0 元数据已登记开发至 2026-03-10、2026-05-15..07-16 已消费回放；sealed 为 2026-08-31..11-30。
  旧父评分研究只准备至 2025-09-30；扩展数据不等于未消费数据。

因此整轮 `EXPLORATORY_SCREEN / ALPHA_RANKING / NAVIGATION_ONLY`，没有独立 OOS、自然前向或激活证据。
新 release、预测文件较新、未在某个旧模型使用，都不恢复独立性。本轮不打开 sealed 收益，不自动执行 B4。
B0 原始元数据与评分文件内容 hash 见任务 X 盘 `b0_inventory.json`；不得把其称为原生历史运行 receipt。

## Scope / Non-goals / 精确写入范围

- 本详细设计及现有蓝图进度。
- `backend/services/advisory_model_first/multiperiod_top10_5td_inputs_v1.py`
- `backend/services/advisory_model_first/multiperiod_top10_5td_study_v1.py`
- `backend/services/advisory_model_first/multiperiod_top10_5td_pipeline_v1.py`
- `backend/tests/advisory_model_first/test_multiperiod_top10_5td_v1.py`

除此之外不写源码。复用 FrozenDataset、公用纯特征/五日计量构造器、stage publisher 和 JSONL registry；
不修改 StrategyPackage、Selection、QE、HMM、数据发布或 LocalSIM，不加平台、UI、调度器或依赖。
Windows 临时和研究产物全 X，WSL 经验证映射目录；数据/模型大资产原地只读，不复制。
数据库、服务控制、生产回放均不在范围。详细设计独立 PR 先入 main，之后实现。

## Input / 时钟、原始人口与标签

1. 显式传入 frozen binding、两个批准包 manifest/run/预测描述及内容路径，使用既有 SHA 验证与排序定义。
   不生成新 Selection，不用当前股票池修补历史名单。原始完整评分来源统计与 Top50 投影分别记录；
   派生排序不是原生资格过滤或原生 TopK 替代实现。模型未知不删除股票或日期。
2. D 范围固定 2024-07-04..2026-08-28；保留所有原 D、候选、UNKNOWN 及末五日未成熟槽。
   T 为文件日历紧邻交易日，H=T+4；不读取 2026-08-31 及之后任何价格/收益。
3. 输入使用现有六个个股日频量：ret_1/5/10、atr14_close、close_location_in_day、volume_ratio_5_to_20，
   加各包 D 内原始 Top50 的 parent_score Z 与 rank/50。父分数 Z 方差为0时明确 UNKNOWN。
   同股相同 D 的日频量计算一次。OHLC 沿发布的 factor 恢复原单位，再按 D factor 构造历史复权口径；
   不用未来 factor，成交量保持原 RAW_SHARES。复用 `build_generic_daily_price_input_v1`，
   基准/宽度字段无本轮来源时明确 UNKNOWN 且不进入模型，不临时补数或用别名冒充指数。
4. 模型输入不含 T 开盘、未来涨跌停、收益、label_status、未来缺失状态、instrument ID 或包 ID。
   每折用自身训练数据中位数/IQR及逐列 missing bit；全训练未知列固定停用，不因外层出现值启用。
   允许编码填训练中位数，但不更改原数据 UNKNOWN，也不伪称补出了行情。
5. 计量复用 `GENERIC_FIXED5_VALUATION_EXECUTION_V2` 及其 hash：T 原始开盘、H 收盘，
   buy=0.95/sell=5.95bps 各一次；涨停不可入场/明确停牌返回原有 CASH 或 UNKNOWN 语义。
   停牌仅权威确认后沿原计量合同持有估值，真实缺价不推导补值，H 不向后延长。
   `hypothetical_liquidation_net_bps` 是假设清算 cohort，不是实现成交、NAV、现金收益或可执行超额。
6. D 特征与收益监督为分离文件/通道。训练/验证标签先按日期和实际 label_information_end 投影，
   只读取各折训练可见已成熟标签；外层预测冻结后才生成外层收益计量。不得按未来 label 可用性筛选推理名单。
   所有读取记录文件内容 hash 或已绑定 bin stat lease + 实际消费切片 hash；不把 stat/table 名称冒称内容 hash。

## Models / 一次有限比较与确定性选择

时间拆分以真实文件交易日投影，边界日可为非交易日，不补造交易日：

| fold | expanding train D（均从2024-07-04开始） | inner validation D | label cutoff |
| --- | --- | --- | --- |
| 1 | 至2025-03-31 | 2025-04-01..05-30 | 2025-05-30 |
| 2 | 至2025-05-30 | 2025-06-01..07-31 | 2025-07-31 |
| 3 | 至2025-07-31 | 2025-08-01..09-30 | 2025-09-30 |

训练每行 H≤本折 train_end，内层验证 H≤本折 label cutoff；边界未成熟保持在原人口但不作已知标签。
同一 stock/D 跨包不分入不同时间折，不作独立时间观察；训练权重每 D 等重、D 内包等重、包内监督行等重。
Y 为各包/D 可观测训练行五日净值减该组均值，学习同候选流的横截面增量，而非负市场均值。

共六组配置，固定 seed=20261021：

- Ridge：alpha=1、10、100，lsqr，tol=1e-6，max_iter=2000，fit_intercept=false。
- HistGradientBoostingRegressor：max_leaf_nodes=3、7、15；共用 max_iter=120、learning_rate=.05、
  min_samples_leaf=100、l2_regularization=10、early_stopping=false、max_depth=None。
  假设是 D 波动/动量与原父排序的低阶非线性交互；不是换模型名重复旧单 Ridge。

六配置×三折=18 次候选物理 fit，之后仅最佳配置在截至2025-09-30且H已成熟的训练人口重新拟合一次，
总上限19次。原生 Top10 和训练组均值无排名差异的常量基线为0 fit，明确区分闭式统计与物理 fit。
不按包另训、不搜索 seed、持有期、TopK、入场阈值、时间窗或特征子集，不做 early-stopping 搜索。
所有配置使用相同编码列、训练监督键和权重；训练数据校准只在各折，不使用未来 fold。

每配置对每包/D 原 Top50 输出独立派生 rank，按预测值降序、原 rank 稳定破同分，选择十只。
存在输入未知时保留 missing bit；全部训练可识别输入均未知的股票固定原槽，不挤掉其他股票。
全组无可预测量则 `NO_RANK_ADVICE`、保留原 Top10；这是显式不提供建议，不伪造模型成功。
本轮不沿用旧“最多换两只”政策；不修改原 scores/ranks/UNKNOWN/资格/原产物。

内层以各配置均可计量的共同原 D 及两个包的完整十槽作配对，每 D 两包等重；
按三个折全部配对日合并后的平均 candidate−native 净增量最大选择一个，精确相同按预登记配置顺序。
无共同验证日必须明确失败，不填0或从部分包/时段择优。保留所有配置、逐折和未知日结果。
即使内层最佳为负，也只评一次该最佳非原生模型；不得事后改成另一模型。选择文件与19次预算身份原子冻结。

## Outer / 一次外层、统计功效和结论

外层 D=2025-10-01..2026-08-28；报告整体及事前 Q4/2026Q1/Q2/Q3 四时间块和两个包，不能事后择段。
单一候选预测先冻结，然后才进行一次外层收益读取；outer 为曾消费开发窗口，只用于导航。
候选不复选。任何计量错误修复保留原 run 与旧产物，exact retry 记录错误及是否触及经济假设；不趁机换参。

主端点为相同包/D 十槽净均值的 candidate−native，先包后D等重。共同已成熟完整日进入配对推断，
所有未计量日和数量另报；缺值不当0、不删原日期。不把多包同股或十只股票当独立样本。
统计固定 circular moving-block bootstrap：block=10个交易日，10,000次，seed=20261021；
两个包同步按D抽样，报告95%区间、bootstrap SE、MDE80=(1.96+.841621)×SE、原日数和保守 floor(N/10)块数。
保守块数不是精确有效样本量；同时报告以自协方差估计的有效日期数及方法限制，不声称 IID。
分包/分段区间仅诊断，不做额外显著性择优；跨轮研究族仍存在自适应偏差，不冒称DSR已控制全历史搜索。

探索结论在读取收益前固定：

- **值得独立确认**：总配对增量≥10bps，95%下界>0；至少20个外层干预D、20%成熟D覆盖，
  至少两个声明时间块有干预；共同已知配对覆盖≥80%，candidate未知率不比baseline高超过2个百分点；
  candidate最差10% cohort均值不比baseline差超过100bps。这里只支持确认设计，不激活。
- **不值得继续此候选**：95%上界<0，或完整外层平均增量≤0，或触及上述100bps尾部恶化。
  只停止本候选，不推出整个信息集/模型族不可学，不放宽风险线抢救。
- **不确定**：其余情况（包括干预稀疏、区间跨零或功效不足）。固定报告10bps经济目标与MDE的差距；
  不无限补样、不再从同一frontier挑另一名，不因此要求额外封存旧失败证据。

报告候选/原生十槽净收益均值、胜率、5/50/95分位、尾均值、入场/退出执行状态、UNKNOWN/成熟度、
换股数量/覆盖、按共同日的避免亏损与错失盈利（相同冻结计量两臂）、模型/原父排序相关及折稳定性。
matched/归因是诊断，不额外要求每信息块独立跨越10bps；不能把cohort相乘称为策略累计收益或年化。
若有正探索证据，先核真正未消费历史与成本/资本合同再另立B4；本轮sealed物理不读、无确认自动放行。

## Operations / 幂等、资源和身份

复用 publish_stage/read_stage + 只追加 trial_registry.jsonl；登记 objective_contract、decision_use、
父lineage、数据/代码/政策hash、六配置/三fold/最多19fit、窗口和生成/评价次数，不建设新registry平台。
阶段 preregistered→prepared→selected→predicted→evaluated 原子发布并绑定父hash。
每次 fit 先独占claim再记 STARTED/COMPLETED；中断留 STARTED，不静默重fit清零计数。
完成阶段重试只校验原文件并返回；漂移拒绝，既存失败和旧产物不覆盖。
源码实现身份包括本轮三叶及复用的输入、五日标签/计量和选择/统计函数；fit环境版本与源码commit登记。

拟合在既存 WSL Python，无依赖安装，CPU≤2线程、单进程RSS≤2GiB、源消费/输出≤2GiB、单阶段≤30min。
prepare仅原地按日期/股票读H5和所需涨跌停bin，不按日复制数据；两个包共用同股/D计算。
profile/文件覆盖漂移、真缺数或预算超限明确报错，不改人口来满足预算；合法停牌沿原合同。
训练前只读资源和QE状态；若资源不够则保存断点，不控制QE/后端或生产回放。小CPU研究不抢GPU。
离线源码无运行服务入口变化，无需为本轮强制重启；生产角色启用尚未发生。

## Implementation Plan / 实施顺序

B0已完成元数据与评分覆盖检查；B1本设计独立提交，审核通过合入后依次实施 inputs、study、pipeline 和直接测试。
B2先红后绿验证关键合同，再做三视角自审修复；冻结源码/环境/plan后才执行至多19次fit。
B3将内层选择与外层预测、收益计量分阶段冻结，完成一次外层及结果报告。没有真未消费历史不自动做B4/B5。

## Verification Plan / Design Acceptance Index

| ID | 验收目标 |
| --- | --- |
| F-401 | 原人口、来源身份、sealed隔离 |
| F-402 | D输入与成熟5TD标签分离 |
| F-403 | 六配置三fold、权重及19fit上限 |
| F-404 | Top10原槽、唯一性与确定性 |
| F-405 | 单次outer、缺失和块推断 |
| F-406 | 原子阶段/缓存/恢复身份 |
| F-407 | 真实研究与交付分态 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-401 | Input / 时钟、原始人口与标签 | artifact: X:/AIstock_temp/ADVISORY/multiperiod-top10-5td-20261021/b0_inventory.json | DESIGN_REVIEW_PASS | none |
| F-402 | Input / 时钟、原始人口与标签 | backend/tests/advisory_model_first/test_multiperiod_top10_5td_v1.py（实施时新增） | DESIGN_REVIEW_PASS | none |
| F-403 | Models / 一次有限比较与确定性选择 | backend/tests/advisory_model_first/test_multiperiod_top10_5td_v1.py（实施时新增） | DESIGN_REVIEW_PASS | none |
| F-404 | Models / 一次有限比较与确定性选择 | backend/tests/advisory_model_first/test_multiperiod_top10_5td_v1.py（实施时新增） | DESIGN_REVIEW_PASS | none |
| F-405 | Outer / 一次外层、统计功效和结论 | backend/tests/advisory_model_first/test_multiperiod_top10_5td_v1.py（实施时新增） | DESIGN_REVIEW_PASS | none |
| F-406 | Operations / 幂等、资源和身份 | backend/tests/advisory_model_first/test_multiperiod_top10_5td_v1.py（实施时新增） | DESIGN_REVIEW_PASS | none |
| F-407 | Test Plan / 合入条件与自审 | artifact: X:/AIstock_temp/ADVISORY/multiperiod-top10-5td-20261021/task_plan.md | DESIGN_REVIEW_PASS | none |

设计阶段验收只证明规格完整，源码和研究尚未执行，不代表功能通过。源码阶段逐项替换为实际引用与结果。

## Risks / Production Gates / Rollout

主要风险为历史消费偏差、有限日期功效不足、内层模型选择噪声、名义可交易不等于真实成交、未知收益选择偏差。
以上通过证据等级、时间簇推断、一次外层、保留缺失人口及后续资本合同处理，不把理论护栏升级为QE包准入。
DDL/数据库写入/数据激活/依赖安装/进程控制均 noop；无生产开关变更，无需本离线切片后端重启。
若实现需要超出精确范围、生产角色激活或新数据发布，先停在对应边界另行登记，不以本设计扩权。

## Test Plan / 合入条件与自审

只运行直接合同及失效点测试、Ruff、diff --check、F1 validator、精确ownership和L0；不重复跑间接模块大套件。
必要跨模块回归交现有CI，不启动UI、后端或数据库。用真实两包文件数据证明结果与身份，mock仅验证负路径。
三轮自审：①业务/蓝图一致性；②时间隔离、重复样本、缺失及统计；③恢复、预算、路径和交付范围。
发现项先修复再定向回归；这些是不同视角的自审，不冒称独立专家审核。
DESIGN-COMPLIANCE-001逐项：不把设计/测试冒充真实收益；错误明确；不改变原生选股和已批合同；
不新增包准入或生产审批。本研究三分类是证据解释，不是对QE包另设门禁。

Research Result：尚未开始prepare、fit或outer收益读取；后续只追加本轮实际结果，不改事前假设和选择规则。

### 设计三轮自审记录

1. 业务/蓝图审：明确这是多时期六配置比较，不是旧候选改阈值；Top50完整保留，派生Top10不覆盖原生资格算法。
2. 时钟/统计审：明确实际H purge、两包同步按D抽样、最后未成熟日期保留、全部扩展窗口仅探索；不消费08-31 sealed收益。
3. 交付/资源审：补齐独立设计PR、四个精确源码/测试文件、claim中断不得默默重fit、所有生产动作noop及设计/源码/经济状态分离。
   三轮均为本窗口自审；F1结构检查和diff检查通过后交付设计，不冒称实现完成。
