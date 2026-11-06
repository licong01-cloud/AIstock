# Advisory下一轮计划：跨期尺度失配审查与条件性横截面编码研究

2026-11-06，`PROPOSED_NOT_EXECUTED`。本轮只规划；已完成文档/源码查重、最近交付回执和输入文件元数据核对，未解码特征或收益Parquet、生成新排序、拟合或修改生产。累计266实际金融fit＋1旧index不变。本轮新增fit授权为0；下述最多3fit为下一次完整执行本计划的预算提案，不能重复使用上一轮额度。

建议只推进一条线：**先检验固定训练尺度在跨期输入上是否存在持续且有评分影响的失配；证据成立才用同日原Top50内的因子相对位置，替换一个固定读取头的因子幅度编码，做一次三折研究。** 本计划不把分布不同等同于数据错误或经济失效；不是已证明有效的Alpha，也不是继续为负结果换模型。

## 1. 最新状态与为什么调整

规划基线`origin/main=b189a5ee1e6d0f40afa8cd94835d4d82d91353b3`。设计#7062、源码#7063已合入；最新[source_delivery.json](X:/AIstock_temp/ADVISORY/lstm-frozen-representation-20261106/source_delivery.json)确认CI成功、main同步及合入后0fit重入。冻结LSTM表示候选主开发+25.740525bps、压力−70.472897bps，按事前规则关闭；独立算术已通过，不重复其提取/训练/收益复算。[最近结果](advisory_lstm_frozen_representation_review_20261106.md)

目前单纯增加输入、换损失、浅树、同伴上下文和h64均没有达到原生Top10成本后5TD增量100bps。这个序列不证明所有重排序无效，但不支持继续同一历史上的无条件模型轮换。

现行54字段读取头使用本折训练median/IQR及缺失位，然后对D中心化。**中心化已经消除了组内共同平移，因此仅发现跨期均值漂移不能支持本计划。** 尚可检验的是各因子的横截面尺度/尾形在训练和应用期之间是否改变，使固定线性系数的相对作用发生持续变化。已知VSTD5单点约6.05e18及数值舍入问题已被BUG-2178追溯/修复；不能再把该单日当作新发现或整个压力失败的解释。[数值修复记录](advisory_lstm_linear_rank_stability_20261106.md)

同日秩编码只保留相对位置，丢失幅度信息，可能降低收益；它也改变线性模型的函数表达和正则几何。因此即使改善，也只能支持这个明确编码合同，不能据此证明新增信息或纯尺度因果效应。

## 2. 查重：已有结果直接复用

| 路线 | 已有依据 | 本轮处理 |
| --- | --- | --- |
| 成熟标签顺序更新U1 | [蓝图§6.3.16](../architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md)：每20D扩展训练，真实8fit，原生−0.065670bps、静态−15.572048bps，INNER_PROTOCOL_NOT_SUPPORTED | 不以“旧模型过期”为由再做滚动窗/更新频率搜索；本计划没有更新训练截止 |
| 净替换、父锚定、残差及OOF组合 | [P8查重](advisory_replacement_decision_review_20261023.md)、[三模型路线评审](advisory_lstm_three_model_route_review_20261105.md)、P0-J和C1 | 不把少换股、边界配对或同分数融合包装成首次研究 |
| 54字段SPO+/Ridge/HGB | [因子信息](advisory_lstm_factor_information_review_20261102.md)、[非线性结果](advisory_lstm_factor_nonlinear_review_20261102.md) | 固定现有Ridge作为对照；不再试树深、loss、seed或字段子集 |
| 局部同伴关系 | `lstm_pool_relation_v1.contexts(inputs, encoding)`按训练median/IQR距离选5同伴，生成相对上下文；已关闭 | 不增加同伴、图或上下文；不是同一候选改参数 |
| h64冻结表示 | #7063，3fit已耗尽 | 不追加维度/层/头/正则或恢复fit |
| 历史横截面rank方法 | N3 minute已有原分钟字段rank_pct，QE generator已有分数相关性rank；旧P0-I/M1/M5已有排序目标和输出percentile | 横截面rank不是新算法；这些不是当前精确LSTM的46因子输入编码实验，不直接继承其经济结果 |

限定检索Advisory设计/分析及研究源码，并读取逐因子、非线性、冻结表示三份registry，未找到当前精确“原八字段保持＋46因子同日秩编码＋固定Ridge”的已完成记录。不是全仓全球新颖性证明；执行P0须补核相关registry及源归属，若发现相同合同已结束则直接复用结论。

Microsoft Qlib官方实现提供按datetime分组的`CSRankNorm`；它证明这种变换有明确实现，不证明本项目收益。[官方processor源码](https://github.com/microsoft/qlib/blob/main/qlib/data/dataset/processor.py#L299-L330)。本计划按原Top50边界实现有版本的变换，不直接调用可能原地改帧的Qlib处理器，不扩大到全市场。

## 3. 固定范围与可用文件

精确package=`pkg_cc7eccb6202e4816b9d7c7d6b748bd37`，manifest=`93a7fb7c5c0745c96d2d97376db3c669e527256666ed1e014a710c38799a7dc6`。原Top50→Top10、开发305D/15,250行、压力118D/5,900行保持；评价仍开发126D/6,300行、压力2026-03-11～08-28的118D/5,900行。固定T开盘到T+4收盘，买0.95bps/卖5.95bps，原生Top10为主对照，目标100bps。

只读复用`X:/AIstock_temp/ADVISORY/lstm-factor-information-20261102/study/prepared/inputs.parquet`；本轮物理SHA256=`6685741751b73a554923826190809bba50c296d2e49e25fb0a5af551ae036f67`，Parquet元数据21,150行/64列，包含身份和原54数值字段。本轮未解码其特征数组。无需再扫描46份大因子文件、采集数据或提取h64。

原对照为`X:/AIstock_temp/ADVISORY/lstm-factor-nonlinear-20261102/study/fold{f}/ridge/trained/model.json`，规划时文件hash及训练身份已读回：

| fold / cutoff | 训练行 | 控制JSON物理SHA256 |
| --- | ---: | --- |
| 1 / 2025-03-31 | 8,300 | `9910ded89cc636a64782170b7fe7f255103152e385ad212adf81c989e70790c5` |
| 2 / 2025-05-30 | 9,750 | `48239063c1ded10ba445cea79562044d3b2c991dae087afe53f3dc53aae74cd4` |
| 3 / 2025-07-31 | 11,850 | `f54b39d2fa3513cf4727ea20c52ebf28943af0321227fb79619d7b33c406023a` |

训练/评价金融投影继续使用P19 recovery-v2原train_finance、evaluated/finance和P18 target_finance，路径可从已合入`advisory_lstm_factor_nonlinear.paths()`取得。P0/P1不解码它们；后续P3才按冻结顺序消费。真实文件身份与阶段链在执行时复核，不把当前元数据核对冒充已完成新输入验收。

原SOURCE NaN、已审允许的Inf、未知与不可执行CASH合同保持，不能按收益删日期/股票。NON_VINTAGE、父包后选及已消费历史限制不变；没有新增历史PIT证明或独立确认。

## 4. 一个长任务的阶段和停止条件

### P0：恢复与查重，零拟合

读本计划、三份近期结果、原控制阶段链及相关registry。固定唯一新证据根、所有文件hash、源码和写范围；核对266累计fit及原候选关闭状态。复用BUG-2178证据，不重做VSTD5追溯。

产物：身份/查重收据、输入来源表和事前诊断协议。重复合同、真实来源漂移或当前无可测试差异则形成精确终态；不自动切到其它方向。

### P1：无收益的尺度与支持审查，零拟合

仅打开上述原54字段面板及三个旧模型JSON。使用旧训练编码，先按原可建议槽D中心化，逐字段记录横截面尺度、尾部、缺失/有效数、与训练参考的比值，以及旧冻结系数下的绝对评分贡献。参考仅取各fold截止日以前的原输入D；这是输入参考人口，不冒称8,300/9,750/11,850个成熟监督行。压力使用fold3参考。

为避免单点故事推动再训练，事前固定以下研究优先级门槛，不作统计显著性或因果判断：

1. 对每个原因子、每个原D，用有效且原可建议的值计算旧编码中心化后的`q90(abs(x))`；只在该字段当日至少40个原候选有效时计算此诊断，仍保留全部50行。参考阈值是对应训练输入D的该统计量99%分位。零/不存在参考列标为NOT_COMPARABLE，不改用另一个阈值。
2. 某因子在应用D超过参考阈值，且旧冻结系数乘该字段的平均绝对贡献占全部原108编码坐标绝对贡献之和至少10%，才记一次“有影响的尺度偏离”；分母为0保留无可比状态。所有54字段、全部D都报告，不按结果挑列。
3. 至少两个**不同的原因子**各在同一个评价窗口达到20个原D、覆盖至少两个预定自然月，才支持进入一个候选设计。开发按各自fold参考，压力按fold3参考；两窗口分别报告。该门槛只是事前有限预算分配规则，不能据此称尺度解释了负收益。只有已知单日VSTD5异常则不通过。
4. 另独立验证46列同日秩变换的原KEY、同值/常数/缺失、置换和未来追加不变性；有建议槽在每个训练/评价段可得到至少80%原因子秩坐标的行比例与完整D比例均至少80%。分母含原全部行/D，不删缺行提高覆盖。

产物：`scale_support_review.json`及全原D统计。不生成新的推荐名单、不读Y、不训练域分类器/密度比模型，不以分布漂移名义多算fit。任何准入条件不满足，终态`NO_SCALE_CANDIDATE_EVIDENCE`或精确输入失败，结束本长任务的研究分支，不追加样本或调整门槛。

### P2：通过才冻结一个F1，零拟合

拟议唯一假设`LSTM_FACTOR_CROSSSECTION_RANK_FIXED5_V1`：在原Top50内，46原因子的同日相对位置比固定历史幅度坐标更能跨期迁移至5TD选择。原八字段保留幅度和父分数/排名信息，避免一次替换所有经济尺度；这是事前选择，不能根据P1改换字段。

对一个D的每个原因子，在**原50行中具有合法有限值的n行**计算average midrank，`u=(rank-0.5)/n-0.5`。同值取同rank，n=1及全常数有效值为0；缺值仍NaN/显式缺失位，n=0无有效坐标。排名可引用同日其它原候选的D已知输入，不引用T/H或收益可用性。因子原值永久保留；这不是补零或把缺失当真实0。

保持原八字段＋46变换字段、相同108维编码上限及train-only median/IQR/std/1退化顺序；原监督资格仍由原八字段和成熟完整50行决定，KEY必须与旧控制逐项相同。同日秩先在原50行构造，之后才投影训练资格；禁止先按完整收益筛行再算秩。原八字段编码逐项匹配旧控制；46新编码另存，不要求假装与旧编码相同。所有合法列固定，不按Y选列。

学习器仍`Ridge(alpha=100, fit_intercept=False, solver=cholesky)`及原其余参数，按D中心化X/y；三fold截止和评价窗口保持。控制只复用现有54字段Ridge；原生仍主对照。无h64、额外图/树/深度网络、窗口/损失/阈值/seed搜索。先三轮审查和F1合入，再开发代码；P1通过也不声称收益成立。

### P3：实现与一次有界实验，最多3新金融fit

按已核实的研究叶、不可变阶段和FitClaims模式建立新版本，不能把上一轮冻结表示`CONTRACT`或旧模型就地改造成新候选。直接测试输入身份、秩规则、原训练KEY/时钟、缺失、边界、不变性、精确排序、预算和重入。提交干净producer后冻结唯一协议。

三折各一次候选fit，0控制fit、0父包fit、0final-fit；STARTED即扣预算，失败不赠送恢复fit，不换根绕过。全部候选/对照开发预测及fold3压力预测先冻结，再评价收益；后续fold训练可消费已成熟的先前日期，但必须先冻结先前fold预测。完成时最多269累计fit＋1旧index。旧对照采用已修正的精确线性排序语义，在新根另存投影，不改旧模型/报告。

两CPU线程、0GPU、单owner顺序fit；沿已验2GiB/1800秒/200MiB研究上限检查真实本机余量。远端QE不阻断，本机资源够也不阻断；不查询QE作为前置、不控制其任务，不等待新交易日。预计主要成本是审查/开发，机器fit时长在实际执行后报告。

### P4：独立核算、价值判断和交付

独立第二实现核验average-rank/缺失/编码、全部分数和排名、十槽费用、日表、10D循环块10000次seed20261021区间。合成测试fit单列；新进程完整重入须0fit。每个原D和UNKNOWN/IMMATURE/CASH保留，各自主端点与辅助同分母分别输出，不能异分母均值相减。

- 任一窗口主增量≤0、下尾10%较原生恶化超过100bps或成熟未知槽差>2pp：关闭本候选。
- 两窗正但任一<100bps：BELOW_TARGET_STOP。
- 两窗均≥100bps但完整成熟覆盖<80%、干预<20D或<20%成熟D、开发不足2/3折正：INCONCLUSIVE_NO_EXPANSION。
- 全部满足仅支持独立确认设计；候选相对旧Ridge两窗未同时正时，另记未支持编码相对现有头的增量。零样本/无法完整估值为NOT_EVALUABLE。

各fold/既定季度、换入数量、避亏/漏盈/新增盈亏、集中度和缺失分母完整报告。均是已消费历史探索，不成为生产证明；现阶段不承诺找到可上线模型。正分支若无兼容未消费历史，记录CONFIRMATION_SOURCE_NOT_READY并完成计划，不把等待未来交易日变成开发阻断，不借旧V2正式观察。

适用分支全部结束后更新结果报告与蓝图，三轮同窗口审查、直接测试/最小门禁和F1逐项验收后，源码PR/CI/合入与经济、生产分态记录。负分支不自动排下一模型，不以完成长任务为由扩大研究。

## 5. 已发现的真实接口及复用限制

| 真实入口 | 允许用途/验证 |
| --- | --- |
| `lstm_factor_information_v1.check_inputs(frame)`、`numeric(frame, fields=FEATURES)`、`ordered(frame)` | 原人口、字段、允许Inf与排序合同；不修改旧实现或源面板 |
| 同模块`encoding(raw)`、`transform(raw, median, scale)` | train-only编码范式；新输入需独立schema，不能载入旧系数直接用于秩坐标 |
| `spo_top10_5td_v1.training(rows, cutoff)` | 成熟训练资格和完整50组；只在授权金融阶段调用 |
| `lstm_factor_nonlinear_v1.training(rows, fold, expected_keys)`、`validate_model(packet)` | 旧控制资格/编码核对，不能调用旧fit重训控制 |
| `lstm_frozen_representation_v1.exact_scores(x, coef)` | 已修正线性排序语义参考；该模块predict强制h64，不能直接套作本候选预测 |
| `economic_entry_pipeline.publish_stage(*, study_root, stage, plan_sha256, parent_sha256, artifacts)`、`read_stage(path, *, stage, plan_sha256, parent_sha256)` | 原子阶段与完整链复用；新study root，旧目录只读 |
| `advisory_lstm_factor_nonlinear.paths()`；`advisory_lstm_frozen_representation.verify_control_chain()` | 精确源路径/控制链参考，不能调用旧run或把新预算写入旧claim |
| Pandas 2.3.2 `DataFrame.rank(axis=0, method='average', na_option='keep', ascending=True, pct=False)` | 本轮已核对真实本地签名；按D/原Top50局部调用，保留原索引，不按全时段混排 |

本计划没有声称已经存在`crosssection_rank`生产服务。未来拟新增叶/CLI/直接测试`lstm_crosssection_rank_v1.py`等名称只是建议，P2需登记实际文件及所有权后开发。新代码应从已验模式复用，不建立通用特征平台、DB或新调度器。

## 6. 本轮交付、边界和工作量

本轮唯一允许写入文件为本文：`docs/analysis/advisory_lstm_crosssection_scale_research_plan_20261106.md`。工作树`F:/Dev/AIstock_worktrees/advisory-lstm-crosssection-rank-plan-20261106`、分支`research/advisory-lstm-crosssection-rank-plan-20261106`，基于上述最新main。根目录和当前蓝图不变，不把规划提案登记成正在运行或已授权fit。

P0/P1预计半天内完成；不通过即交付诊断终态。若通过，P2～P4预计约1个工作日的设计/实现/研究/审核工作量；不是等待1.5个自然日，也不保证收益。下一次执行可以在一个有界长任务中按顺序完成全部适用分支，开始时须先复核状态避免重复。

无新金融数据集、采集、数据准备窗口任务、ETF、研报/现金流、公告、价格/Exit、QE父包重训、封存窗口消费、生产推荐替换。生产DDL/DML、依赖安装、服务/启动脚本/流水线、清理均noop；无需后端重启。现有原生/V2职责保持，本轮未重新核验其运行健康。

规划自审已检查三类问题：①不能把U1、rank方法或BUG-2178说成新研究；②均值漂移不足、全50构造先于资格投影、输入与收益缺失不混用；③预算是提案、诊断无Y、条件分支有限、100bps和生产边界不变。执行前仍需用实际P1结果决定是否进入F1，不将本计划当作已完成的实验验收。
