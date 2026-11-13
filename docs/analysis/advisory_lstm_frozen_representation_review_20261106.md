# 冻结LSTM时序表示的5TD重排序结果

2026-11-06。P0～P4适用任务已完成，精确候选`FROZEN_LSTM_STATE_FIXED5_V1`为`CLOSED_NEGATIVE / STOP_EXACT_REPRESENTATION_CANDIDATE`。相对原生Top10，开发平均+25.740525bps，压力−70.472897bps；未达到用户要求的扣费后5TD批次增量100bps，没有可上线的新模型。停止本候选，不追加第四次拟合、维度/参数搜索或独立确认。该结论不外推为所有LSTM表示或全部机器学习重排序均无效。

## 1. 已执行内容及身份

[研究计划](advisory_lstm_frozen_representation_research_plan_20261106.md)经用户“开始按规划执行”授权；[F1设计](../architecture/advisory_lstm_frozen_representation_v1_f1_design_20261106.md)先通过[PR #7062](https://github.com/licong01-cloud/AIstock/pull/7062)合入，之后在独立源码树开发。生产、旧V2、原生Top20、Top50→Top10及5TD不变。

精确包`pkg_cc7eccb6202e4816b9d7c7d6b748bd37`，manifest=`93a7fb7c5c0745c96d2d97376db3c669e527256666ed1e014a710c38799a7dc6`。原模型实际46列、20步、64维，监督期20TD；不能从包名“10D”推断监督或窗口。权重及处理器来自Loop1，实际训练截止2022-12-30，验证/处理器拟合截止2024-06-28；2026年选择该包、历史输入重构的限制保留。

原305D开发/118D压力、21,150输入行完整保留。原模型eval/no-grad、冻结处理器和D以前序列提取h64；复用原54字段Ridge对照，新增173坐标的固定Ridge读取头。三折完整成熟训练KEY与旧控制一致，分别8,300/9,750/11,850行。仅候选3fit，0控制/父包/final-fit；累计266实际fit＋1旧index，追加预算0。

新核心为`backend/services/advisory_model_first/lstm_frozen_representation_v1.py`；入口为`scripts/advisory_lstm_frozen_representation.py freeze|run`。源码producer=`e08b781`，协议先冻结，三折两臂全部开发及压力预测发布后才读取评价收益。源码固定后真实研究完成，未安装依赖、操作QE、查写DB、启动服务或等待新交易日。

## 2. 输入验收与限制

文件消费和探索输入支持通过，不等于历史PIT通过。所需上下文77,661个KEY中237源整行缺失；原`neg_momentum_price_volume`及`neg_volatility_breakout_momentum_v2`分别41/2个负无穷，保留原值/来源和受影响KEY。后者2个极值的逐物理公式成因没有重新计算，不能把源含义称为已完全证明。原RobustZScoreNorm裁剪、Fillna及TSDataSampler的D以前ffill+bfill是继承变换，未另添补值规则。

变换后21,150行h均有限、各训练/评价段支持100%。首训练段数值秩64仅表示可测试，不证明64个独立Alpha。七个事前探针验证同输入forward、原输出头linear(h)、公共推理函数AST、分批及未来数据追加不变性；不是完整API运行验收。独立NumPy门和cell递推复算全部21,150×64，最大状态误差4.4456e−7，原分数误差1.5224e−7。

原历史张量/分数parity、首次可得时间及历史PIT均未证明，仍为`NON_VINTAGE`。已反复消费的历史只能探索；没有挪用旧V2结果或打开新窗口来补证。

## 3. 原生主终点与冻结控制

固定十槽，T开盘到T+4收盘，买0.95bps、卖5.95bps；原CASH、UNKNOWN、IMMATURE完整保留。区间为原全D轴10D循环块、10,000次、seed20261021的描述性95%区间，非独立确认。

| 指标 | 开发窗 | 压力窗 |
| --- | ---: | ---: |
| 原D / 成熟D / 主完整配对D | 126 / 111 / 101 | 118 / 113 / 108 |
| 主完整覆盖率（成熟D分母） | 90.99% | 95.58% |
| 候选−原生均值bps | +25.740525 | −70.472897 |
| 描述性95%区间bps | [−39.745706, 91.338180] | [−187.137864, 49.463746] |
| 候选 / 原生净批次均值bps（各自主配对） | 114.885305 / 89.144781 | 23.619305 / 94.092202 |
| 下尾10%较原生变化bps | +21.840911 | −81.322322 |
| 候选 / 原生未知槽（含未成熟） | 166 / 168 | 52 / 54 |
| 成熟未知槽差 | −0.180180pp | −0.176991pp |
| 成熟干预D / 平均换入只数 | 111 / 7.567568 | 113 / 8.141593 |
| 候选−冻结Ridge辅助均值bps | −25.236428 | +19.103408 |
| 辅助完整D及描述性95%区间bps | 101；[−55.004837, 5.200747] | 107；[−45.332593, 82.862827] |

冻结Ridge原生主开发101D为+50.976952bps，压力108D为−81.637570bps。压力候选和控制虽各有108个主完整D，日期集合不同，辅助只有107D；不得直接相减两个主均值得出辅助增量。新表示相对旧控制两窗未同时为正，当前没有稳定增量机制证据。输入增维同时改变容量和正则几何，该比较也不是纯信息因果实验。

开发三折主增量分别−98.369609、+35.989524、+97.142929bps。压力2026Q1/Q2/Q3为−33.239546、+49.213557、−258.400665bps，各季度完整D为15/55/38。保留所有季度，不据此筛季节、改包或调整参数。

| 十槽损益分解（完整配对D平均bps） | 开发 | 压力 |
| --- | ---: | ---: |
| 避免亏损 | 150.905084 | 230.988725 |
| 新增亏损 | 116.532454 | 235.814633 |
| 新增盈利 | 208.533490 | 256.872561 |
| 错失盈利 | 217.165595 | 322.519549 |

压力净避亏−4.825909bps、净盈利机会−65.646988bps，两者合计−70.472897bps。候选确实改变排序，主要未能保留原生的盈利机会。完整日增量为正的比例开发55.45%、压力43.52%；最大的5个正增量D占全部正贡献24.03%/27.75%。这些是事后描述，批次5TD互相重叠，贡献之和不是账户累计收益。

## 4. 验证、审核与交付

- 38项直接合同测试通过：支持/缺失块、身份/时钟、训练KEY/编码、173坐标和官方predict、极值与同分排序、UNKNOWN原槽、三次预算、半完成禁止重训、完整重入及所有决策分支。
- changed-file Ruff、compile、diff/scope、5/5 ownership和catalog通过；L0无P0/P1阻断。7个P2复杂度提示中，新join已说明13,100行上界和one-to-one KEY，其余6处为classifier未改的既有行；没有为清告警改无关模块。
- 三次金融STARTED/FINISHED完整，正常方程相对残差7.81e−16、5.29e−16、5.17e−16；2线程/0GPU，评价资源快照21.27秒、峰值449.89MiB。每次开始时实际资源充足，QE状态未查询且不作否决。
- 独立第二实现未导入项目编码/模型/排序/经济函数：整数二进制有理点积核验24,400行分数/名次；原gross、entry-gap及费用复算两臂共24,400个股票槽、488日表、主辅及控制统计区间。PIT和经济独立性仍未成立。
- 新进程`run`逐阶段/hash/registry/claim核验后返回`STUDY_REUSED`，0新fit。三轮同窗口自审完成：合同/数值、执行链/预算、经济分母/设计逐项；没有冒称第三方评审。

F-981～F-987逐项验收见F1。设计合入、源码CI/合入、研究经济、生产状态分别记录：设计#7062已合入；本报告提交时源码PR待CI/合入，最终来源以证据根`source_delivery.json`为准。研究终态已负面关闭；生产激活=false，DDL/DML/dependency/service/cleanup=noop，无后端重启要求。

## 5. 证据及当前队列

证据根`X:/AIstock_temp/ADVISORY/lstm-frozen-representation-20261106`：

| 证据 | 身份/用途 |
| --- | --- |
| `asset_clock_final.json`、`representation_input_review.json`、`p1_independent_receipt.json` | P0/P1、缺失源及独立原模型算术 |
| `representation_inputs.parquet` | SHA256 `fb4c5ba066c794324eacf3bf1d642dc535e13766ac592043b1fd53f4647ce0ee` |
| `study/preregistered/protocol.json` | 协议digest `51b6bc9fe3fb654c9ca784bd817c184c7966d3374d7e511a423c7a3d14dee3e0` |
| `study/fold{1,2,3}/trained`、`fit_claims` | 3个模型、两臂冻结预测与预算身份 |
| `study/evaluated/report.json`及两臂日表 | 原主/辅助分母和所有季度负面证据 |
| `study/terminal/evaluated/manifest.json` | terminal `9bb9ba15d5288e7e93e7d8794a8ee89cb8f5640a8fad7b6c557f48cd32326f3b` |
| `independent_research_audit.py/json`、`validation/descriptive_breakdown.json` | 独立全量复算及只描述的集中度 |
| `implementation_review.json`、`validation_receipt.json`、`source_delivery.json` | 审核/测试及源码PR、CI、合入分态 |

本长任务适用阶段完成。当前没有自动排队的新候选或fit预算，不把本负候选恢复为“等待下个交易日”。原生推荐和旧V2按已有职责自然运行；研报/现金流/ETF/新增数据/价格/Exit等暂停范围保持。

若另开下一轮，建议先明确可检验的不同机制与最小证据，或处理已复现的源特征正确性问题，再决定有限研究。当前结果不足以支持继续在同一历史上追加隐藏维度、换头或调正则；这不构成新增任务授权。
