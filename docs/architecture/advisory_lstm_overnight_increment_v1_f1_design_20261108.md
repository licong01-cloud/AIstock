# LSTM单一隔夜价格坐标增量重排序（F1）

2026-11-08；Advisory/model_first；`LSTM_OVERNIGHT_RATIO_INCREMENT_FIXED5_V1`；`EXPLORATORY_SCREEN`。

2026-11-08最新研究终态：用户授权下一阶段后，P2/P3真实3个STARTED/FINISHED、两窗评价和独立算术审核均完成，`STOP_EXACT_OVERNIGHT_INCREMENT_CANDIDATE`/`CLOSED_NEGATIVE_EXACT_CANDIDATE`，追加预算0，累计269实际Advisory fit＋1旧index。相对原生Top10开发+46.714083bps、压力−75.666671bps；相对原54字段Ridge为−4.262869/+5.970900bps。未达100bps且触发事前停止，无新可上线模型；不是所有重排序或隔夜信息的普遍否定。完整分母、区间和证据见§12。

当前权威任务为`X:/AIstock_temp/ADVISORY/lstm-overnight-increment-20261108-v1/task.json`，同根完整阶段链、`independent_audit.json`、`validation_receipt.json`及`delivery_receipt.json`分别承载实验、审核和源码交付。此前P0/P1及其44项直接合同、只读预检与源码#7141保持原回执；其“0fit、未读标签、P2未授权”只代表前阶段，不覆盖本次用户明确的三次实验授权。原输入接收见[接续计划§9](../analysis/advisory_lstm_next_research_plan_20261107.md)，源面板未重算。设计、程序、实验、经济及生产状态分开。

## 1. Background / Goal

只回答一个问题：**原54字段的同一Ridge学习规则，增加已交付ID1272 `m_overnight_return_5d`这一坐标后，能否改善精确LSTM Top50内的固定5TD Top10推荐，并达到相对原生Top10的100bps扣费后批次增量？**

原54字段SPO+/Ridge/HGB、同伴关系和冻结h64表示已有明确终态，不恢复其预算。ID1272由原DEV研究任务`0e1f0b97-e911-456e-ad1c-55b4c9179efa`的既有优先级提名，原任务revision7已保留头部消费者用途，revision12延续；不是按本次Advisory收益挑出的信号。上游h10/h20的用途储备并未证明本业务的5TD效果。

隔夜机制本身并非新发现：旧[H-TIMING-1](advisory_economic_entry_timing_hypothesis_v1_f2_design_20261003.md)及[M12](advisory_session_path_m12_f2_design_20261005.md)消费过19步log隔夜/日内及波动坐标，面向价格条件政策，原负面结果保留。本次差异仅在既有原式、原LSTM候选人口、54字段增量对照及固定5TD业务对象。不能以窗口变化宣称全局新机制，也不把旧价格研究当成本实验结果。

可证伪假设是该价格坐标对固定表示具有消费者增量；不预先断言正向动量或负向反转一定成立。方向仅由一次固定监督拟合的系数学得，不按评价结果翻转信号、筛选方向或另立候选。B与B+F的维度不同，比较是固定学习规则下的特征增量实验，不是无容量差异、无修订混杂的纯因果信息证明。

## 2. Scope / Non-goals / 当前交付范围

目标package=`pkg_cc7eccb6202e4816b9d7c7d6b748bd37`，manifest=`93a7fb7c5c0745c96d2d97376db3c669e527256666ed1e014a710c38799a7dc6`。保留原21,150行/423D、Top50→Top10、固定5TD、T开盘至T+4收盘、买0.95bps/卖5.95bps。开发2024-07-04～2025-09-30为305D/15,250行，其中评价126D/6,300行；压力2026-03-11～08-28为118D/5,900行。开发评价是开发子集，不重复相加。原生生产Top20不变。

原KEY为`package_id/decision_as_of_trade_date/target_trade_date/instrument`，保留原manifest、rank及所有未知槽。原八字段资格继续决定可监督组和推理支持；新增信号不能删股票/日期、改变Top50、填补收益或把不完整原槽改成现金。正常缺失、源身份错误和未知历史可得时间分别处理。

P0/P1的8文件实现范围保留原记录。本次编辑前只登记本文、接续计划、蓝图3个源码文档；实验仅使用原已合入新叶/CLI，不改学习器、参数、源输入或评价合同。机器写入仅在上述新实验根内的任务、既有CLI阶段/claim/checkpoint/registry、一次独立审核脚本与必要回执；原设计、实现及输入接收根只读。已按授权解码本候选三折训练及指定评价金融值；没有数据库查询/写入、因子重算、QE控制或服务操作。

三个新文件及必要的CI classifier/ownership登记已在#7141合入；本次从干净producer `405fb26f2315d319153e9d8f917feada554d0210`冻结并使用唯一实验根，恰好用尽三次预算。旧学习器、公共因子、父包、Selection、API/UI、研报、现金流、ETF、价格/Exit、数据集发布及V2自然观察均保持边界，不使用其受保护结果补确认。

## 3. Architecture / Documentation Discovery / Allowed APIs

以下为已读当前源码的真实接口，仅作未来实现的复用依据，本轮没有执行拟合或经济函数：

| 已存在接口 | 允许的复用及限制 |
| --- | --- |
| `lstm_factor_information_v1.check_inputs/ordered/numeric/encoding/transform` | 验证原54字段身份/顺序；train-only median/IQR及missing bits。`numeric`只允许旧已审`neg_momentum_price_volume`的Inf转模型缺失，不能扩展为所有Inf通行 |
| `spo_top10_5td_v1.training(rows, cutoff)`及`group_center` | 沿原八字段、成熟H及完整50行组选择监督人口；不能由新信号资格筛选。标签用bps，不照搬SPO内部除100的损失尺度 |
| `lstm_factor_nonlinear_v1.training/factory/validate_model/feature_matrix/predict` | 原54字段Ridge的训练、模型校验及稳定排名参照。其字段和合同固定，不能直接拿来接受55字段或冒充新模型校验 |
| `economic_entry_pipeline.publish_stage/read_stage` | 原子不可变阶段与plan/parent/artifact哈希链；`read_stage`核对文件hash，不等于经济验收 |
| `lstm_factor_information_v1.summarize/decision` | 原十槽、成熟、费用、主辅分母、风险和区间算术参照。旧`versus_eight_fields`名称必须在新叶明确改为`versus_matched_54_ridge`；新辅助判定见§7，不能直接继承旧结论 |
| `scripts/advisory_lstm_factor_nonlinear.py`中的`resource_admission`、stage及claim流程 | 复制资源观测、一次STARTED预算及恢复模式；不能调用旧`run`、借用其6fit或修改旧协议 |

新叶已显式提供`assemble/training/fit/validate_model/feature_matrix/predict/summarize/decision/FitClaims`，CLI提供`preflight/freeze/run`。55字段合同独立校验，不全局改写旧`FEATURES`；没有自动加载latest缓存、执行源`values.py`或重算Top50。`preflight`仅读固定原输入与信号投影、原协议/模型/阶段及文件hash，不调用资源/QE观测或读取收益数组。真实预检曾识别Python环境差异，切回旧协议登记的`C:/Users/lc999/miniconda3/python.exe`既存环境后通过，未安装依赖或放宽版本一致性。

无标签入口为`python -B scripts/advisory_lstm_overnight_increment.py preflight`，可用`--receipt <new-path>`显式create-only保存回执。`freeze/run`都须由调用者提供`--authorization-ref <覆盖本实验的实际授权引用> --max-new-fits 3`；授权引用、合同和精确预算进入协议并在run核对，不存在默认run或默认授权。该参数记录现有动作授权，不新增审批服务；本轮未调用这两个真实实验入口。

### 3.1 唯一信号与两个时钟

只消费已验收`X:/AIstock_temp/ADVISORY/single-signal-intake-20261108-v1/signal_projection.parquet`，SHA256=`1662ab1cabc092907dfcc60d52d31e5070ab4acf444210c94d1f4315b2eda9ec`。原投影含`signal_value/signal_status/catalog_id/signal_name`及原KEY；模型坐标命名`extra__m_overnight_return_5d`，只从同一KEY的`signal_value`映射，要求ID1272和名称一致。连接须一对一，全部21,150个原KEY/rank及行序不变；重复、外来或丢失KEY、漂移hash、Inf/bool/非法字符串拒绝，不能当普通NA。

原定义SHA256=`de0856a4ff1dfd5e7aacd31276eba566284074a809a1b046f33ab2e7ecd1e2b6`：按股票`open/close.shift(1)-1`，再`rolling(5,min_periods=3).mean()`。只说明原公式，不在新叶计算。它是未用`adj_factor`的原生价格比值，不称复权隔夜经济收益；公司行动、修订、观察行间隔等可能影响语义，不静默修正或裁剪。

已验收21,124有限值、26NA；开发15,224/15,250，评价6,282/6,300，压力5,900/5,900；全部423D内有横截面变化。原源索引9,110,582行按股票严格递增及官方D→T下一交易日检查已通过，不重做。5指原观察行窗口，至少3个有限值，不保证五个连续完整市场交易日。

计算截断D与历史首次公开可得时间分开：现有截断证据允许在研究中按D完成后、最早下一交易日T消费，未证明历史`available_at`或D收盘立即可用。继续标`NON_VINTAGE`，不宣称历史PIT。源绑定为已存在`20261030-monthly-v2-unified-correction-4388f6c6d166`/`qe_hmm_full_v2_20261030`，cutoff2026-10-30；它与旧54字段的冻结源版本分别保留，不跨版本拼值、不切换数据集、不声称修订混杂已消除。调整价格口径需另行候选，本实验不会暗改ID1272。

### 3.2 B与B+F的公平对照

B严格为旧`lstm_factor_information_v1.FEATURES`有序54字段，B+F只在末尾附加上述一个坐标。原输入仍为`lstm-factor-information-20261102/study/prepared/inputs.parquet`，SHA256=`6685741751b73a554923826190809bba50c296d2e49e25fb0a5af551ae036f67`。不能恢复八字段SPO作信息增量主对照，不能引入F-only、其他算法、交互、秩编码、去极值或额外窗口。

拟复用`lstm-factor-nonlinear-20261102/study/fold{1,2,3}/ridge/trained/model.json`三份原B模型，不训练新B。原训练元数据：

| 折 | 截止D | 完整成熟训练行/D | 原训练KEY SHA256 |
| --- | --- | --- | --- |
| 1 | 2025-03-31 | 8,300 / 166 | `2fa90b6ec3a92a62991487a0afb15208c659b63eeba3e40439e3f5aa5871b53b` |
| 2 | 2025-05-30 | 9,750 / 195 | `9ce62adcdd790d0d4ae3b3b9b2cbf3f04d7107b44e47d6a8979587cec59f80b4` |
| 3 | 2025-07-31 | 11,850 / 237 | `d43c61e7ff7e09f57fbc83ca57dd59822ffdc240eaddaa376baf60f327c520fe` |

原设计阶段只读验证过三模型物理hash/stage manifest、canonical payload、54字段顺序/参数、54组有限median/正scale及108系数维度，保留在原设计根`control_metadata.json`，当时仅`PASS_METADATA_ONLY`，不包含完整训练KEY/目标、共同编码或上游链证明。本次P2/P3才完成下述真实匹配及独立复核，不把早期metadata验收追认为当时已经完成训练验证。

未来fit前须核对旧producer依赖闭包和完整阶段链、原train_finance物理hash/资格/成熟H/费用/目标单位、每折新旧完整训练KEY顺序与统计，以及B各列的active mask、median、scale和编码规则。新增F缺失不能改变监督KEY，B统计必须与旧模型匹配。对照按字段名映射共同坐标，不能因missing bits在55字段布局中位置后移而错配。共同编码及B手算分数须满足旧实现既有精度。

实施澄清了原“首次fit前”与标签时序的关系：无需解码收益的所有源/阶段/元数据检查在首次fit前完成；真实训练KEY、共同编码及目标的正规方程检验在**本折fit前**按顺序执行，不能提前打开较晚折标签。失配为`CONTROL_NOT_MATCHED`；较晚折失配保留已经使用的STARTED并停止，不恢复预算。不得重训B、替换模型、换solver或降低一致性门槛。新协议和发布模型绑定原训练金融工件，不仅绑定KEY字符串。本次三折真实训练和旧B控制均通过完整匹配，独立第二路径复核相同8,300/9,750/11,850训练行、166/195/237D及原KEY；目标残差、train-only编码和两臂分数通过。

## 4. Encoding / Learner / 推理合同

每折只在原完整成熟训练组学习编码：每列median和IQR，IQR退化用训练std，再退化1；训练全未知列按旧规则停用并明确记录。F若训练全未知或没有有限变化，记`NO_TRAIN_SIGNAL_SUPPORT`并停止，不执行无意义fit。该条件只验证所提坐标能否学习，不按评价收益筛选。

源输入26NA及旧已审非有限状态原样保留；仅模型变换用训练median代入并携带missing bit，不能写回、补零或前填源面板。标准化后的缺失数值为0是模型编码结果，不能被报告为观测值0。编码后按原package/D中心化；B最多108维，B+F最多110维，原八字段决定支持，未知原槽固定原rank。推理仅在当日支持子集内置换原支持名次，分数降序、原rank稳定破同分，仍输出全部原50行。

唯一学习器为与B相同的Ridge：`alpha=100`、`fit_intercept=false`、`solver=cholesky`、`copy_X=true`、`positive=false`、`max_iter=None`、`tol=0.0001`、`random_state=None`。冻结现有环境及所有构造参数，不搜索正则、seed、损失或模型；每个完整50行组等权且组内等权，`sample_weight`全1。目标为同日中心化的`hypothetical_liquidation_net_bps`，保留原5TD和费用，不使用上游h10/h20标签，也不把SPO的100bps数值缩放误用到Ridge。

新模型为严格JSON：独立schema/contract、包/manifest、折/cutoff、全部源和旧B引用、训练KEY/目标绑定、field order、active/inactive、median/scale、coef、训练证书及payload hash。新叶独立校验55字段白名单和维度；拒绝外来字段、坏hash、非法时钟及非有限模型参数。输出是未校准的组内相对决策分数，不是收益承诺、买卖价格或交易指令。

验算与原Ridge相同：正规方程`(XᵀX+100I)w=Xᵀy`相对无穷范数残差≤1e-8，分母`max(1,|Xᵀy|∞)`；JSON推理与官方预测满足`rtol=1e-11/atol=1e-8`。另以第二路径按字段名重建编码、矩阵分数与全部排名，异常保留为工程未完成，不能偷换经济失败或重训。

## 5. Experiment / Budget / 不可变时序

| 折 | 训练信息截止 | 评价D | 新fit提案 | 对照 |
| --- | --- | --- | ---: | --- |
| 1 | 2025-03-31 | 2025-04-01～05-30 | 1 | 原B第1折，0fit |
| 2 | 2025-05-30 | 2025-06-01～07-31 | 1 | 原B第2折，0fit |
| 3 | 2025-07-31 | 2025-08-01～09-30，另固定用于原压力118D | 1 | 原B第3折，0fit |

事前批准的新fit上限为3个STARTED，仅B+F每折一次；失败仍消耗，未用条件预算和266次历史fit不可借用。无第四fit、最终重训、恢复赠送、超参/模型/方向搜索或择折。本次3/3全部完成，累计269实际Advisory fit＋1旧index，残余及追加预算0；三份旧B只读复用，未重训。

原各折训练金融工件仅在实现验证通过且该实验执行获准后读取；H=`label_information_end`必须≤本折cutoff，CASH合法零与UNKNOWN严格按原合同。先完成本折训练并不可变保存模型及该折两臂预测，再允许读取较晚折的合法训练标签。第3折同时冻结开发末折及原压力预测。全部预测冻结后，才解码评价收益并一次执行§7判断。旧对照训练曾消费的历史窗口不可恢复成盲测，这个顺序只约束本次新实现，不声称抹去既有知识。

实际独立研究根为`X:/AIstock_temp/ADVISORY/lstm-overnight-increment-20261108-v1`。从干净已提交producer冻结代码闭包、环境、完整来源与协议，按既有`preregistered→prepared→fold{1,2,3}/trained→evaluated→terminal/evaluated`发布不可变阶段。old B作为只读绑定工件；单owner锁及create-only claim防重复fit，partial STARTED不能隐式重训。每折完成模型后、预测前create-only保存`fit_checkpoints/foldN.json`并绑定trained回执。本次完整链、checkpoint、三STARTED/FINISHED及唯一registry记录均通过；新进程再次调用为`STUDY_REUSED`、0fit，claim hash前后不变。不覆盖旧实验或失败回执。

资源沿已有实际容量规则：2CPU线程、0GPU、自身RSS≤2GiB、金融运行≤1800秒、输出≤200MiB；读取训练标签前及每次STARTED前检查本机可用CPU估计≥2、available内存≥2GiB、输出空间≥200MiB，资源不足形成有界终态，不建后台等待器。远端QE不得阻断，本机资源充足也不得被QE running/pending阻断；QE观测仅best-effort且有超时，不可用保留UNKNOWN，不控制任何任务或服务。训练能力和程序开发均不等待新交易日。

## 6. Implementation Plan / 下一长任务的执行顺序

| 阶段/估算上限 | 复用依据与实现内容 | 验证及退出 |
| --- | --- | --- |
| P0，约1小时 | 读本设计/当前回执；从最新main建新树，登记新叶、CLI、直接测试及必要CI登记的精确写范围；只读核对所有源和B模型依赖/阶段链 | 身份可闭合则继续；漂移/缺件即精确停止，不采集或重算，不恢复已结束分支 |
| P1，约3小时 | 参照§3真实接口实现原KEY一对一装配、54/55字段独立合同、train-only编码、Ridge导出与稳定排名、一次claim/阶段/registry；本次已完成 | 44项直接合同通过，含随机原规模三折阶段链；0真实金融fit。代码在历史只读输入/合成数据完成，不等待新D |
| P2，约1小时＋运行≤1800秒 | 本次获准后核对完整训练控制，完成3fit和全部不可变预测；实耗金融运行约10.37秒 | 3个STARTED/FINISHED，无加赠、final或搜索；完整复用再次调用0fit |
| P3，约2小时 | P2预测完整后按原金融投影一次评价，独立第二算术路径通过，按事前规则停止精确候选 | 原126/118D完整保留，主/辅各101/108个共同完整D；负面结果不挑季度或子集改判 |
| P4，约1小时 | 同一任务三轮自审/修复与最终版本复核，直接小矩阵、changed-file/ownership、F1/符合性，CI及已有授权的源码交付 | 实现、经济、源码、运行各自验收；失败或未执行阶段保留真实状态，无自动生产接入 |

约8小时是原实施估算，不是成果承诺。本次P0～P3已完成，P4结果/设计逐项审核与文档交付按最终回执独立报告。P1没有默认run；P2实际使用了本候选、3fit及冻结协议的用户授权，没有将源码合入授权泛化为其它训练。约10.37秒只指金融研究进程内运行，不是整个开发/研究任务耗时。

## 7. Economic Contract / 唯一事前判断

原生Top10是业务主比较；B+F减B是信息增量辅助比较，二者分别在各自共同完整成熟D配对。保留全部原126/118D及10槽；主/辅完整D不同必须单报，不能用两个异分母均值相减推导辅助增益。未完整可算的批次为NULL/UNKNOWN；可计算槽的局部贡献可另报，不补成完整批次或伪造交易。固定十槽5TD批次不冒充资金曲线或年化收益。

预先固定以下顺序，不依据结果改动：

1. 来源、训练、fit/数值证书或预测不完整：`ENGINEERING_NOT_COMPLETED`，经济`NOT_EVALUATED`，不作负收益结论；旧控制不匹配用`CONTROL_NOT_MATCHED`。
2. 任一窗主或辅助没有可算均值/所需风险分母：`INCONCLUSIVE_NO_EXPANSION`，保留缺件及全部分母。
3. 任一窗主均值≤0，或辅助均值≤0，或候选相对原生下尾10%均值恶化超过100bps，或成熟十槽UNKNOWN比例增加超过2个百分点：`STOP_EXACT_OVERNIGHT_INCREMENT_CANDIDATE`。辅助为负不能包装成单信号有效，辅助为正也不能挽救主比较失败。
4. 两窗主/辅均为正但任一主均值低于100bps：`BELOW_BUSINESS_TARGET_NO_EXPANSION`。
5. 只有两窗主点估计均≥100bps、辅助均为正、主/辅各自共同完整成熟D覆盖均≥80%、主干预各≥20D且≥20%、开发至少两折主增量为正且有干预，方为`DESIGN_INDEPENDENT_CONFIRMATION_ONLY`；其余为`INCONCLUSIVE_NO_EXPANSION`。这些支持/风险规则沿旧合同，辅助两窗正及其覆盖明确约束本次“增量”主张，不是生产或额外人工审批门。

所有统计使用原冻结的10D时间块、10,000次、seed=20261021的95%描述性区间，随完整协议复制，明确重复消费和5TD重叠；区间跨零必须说明不确定，点估计达标也不称确认成功。输出原D/成熟D/完整D、每折/季度描述、干预数量、UNKNOWN/CASH、费用、避免亏损/新增亏损/新增盈利/错失盈利四项贡献及原生/候选分布。辅助仅用于判断坐标增量，不事后替换主对照或100bps目标。

任何经济终态的追加fit预算均0，不自动转新算法/阈值/另一信号/多包或生产。历史开发和压力均为已消费探索样本；若通过，只能设计独立确认，另行核对兼容旧文件/前向来源，不借用V2受保护结果，不因当前缺少独立样本阻断P0/P1程序开发，也不宣称已经获得可上线模型。

## 8. Verification Plan / 设计符合性

未来最小直接测试必须区分：重复/外来/缺失KEY与正常26NA；原54源及新投影hash漂移；字段顺序和B missing-bit位置；原八字段支持及全部原槽不变；新增字段缺失不改变训练KEY；非法Inf/字符串/bool拒绝；训练全未知/恒定坐标停止；未来数据及评价标签毒化不影响编码、训练与预测；官方/JSON/第二路径分数和全部稳定排名；费用/CASH/UNKNOWN分母及5TD成熟时钟；三STARTED耗尽、partial恢复拒绝、完整复用0fit、无第四fit；远端QE/查询UNKNOWN且本机容量充分时可执行。

P0/P1的新叶44项直接合同与#7141 CI保留原证明，不因本次仅文档变化重跑模型测试。当前真实研究的第二路径不导入后端fit/predict/cohort/费用/区间函数，独立核验24,400个分数/排名、两臂各244日日表、各11,111条AVAILABLE费用及4组区间；最大分数差7.28e-12。三轮当前Agent审核覆盖冻结来源/三折时序/计数、算术与UNKNOWN/分母、终态/授权/逐项文档和最终版本；这不是第三方外审或独立经济样本。当前直接检查为审核脚本语法、链接、3文档范围、diff及F1校验，CI状态另见最终回执。

按`DESIGN-COMPLIANCE-001`逐项：完整研究已执行并报告负面，不以完成模型或审核冒充经济通过；UNKNOWN/NA和所有原D保留，审计脚本最初的metadata列序假设已记录并改为按同一列名核对全部值，没有修改研究工件；原人口/5TD/费用/100bps及事前停止规则不改；没有新增审批服务、数据库或等待新D。

## 9. Design Acceptance Index

本次索引验收对象扩至用户已授权的P2/P3完整实验；`PASS_EXECUTED_NEGATIVE`表示实现和真实研究按合同执行、算术复核通过，但经济结论为停止，不能解释为业务达标、独立确认或生产准入。

| ID | 本轮设计验收要求 |
| --- | --- |
| F-971 | 单一业务假设、原研究差异及非因果/探索边界明确 |
| F-972 | 原人口、信号身份/公式/单位、缺失、修订和两个时钟完整固定 |
| F-973 | B/B+F、三旧Ridge及完整匹配前置、train-only编码和同学习器明确 |
| F-974 | 三折、获准3次fit且实际恰好3次、预测时序/恢复/资源合同及残余0预算 |
| F-975 | 原生主与匹配B辅助、100bps/支持/风险、UNKNOWN和唯一终态预先确定 |
| F-976 | 真实接口、实施阶段/直接验证、允许范围、分态交付和三轮设计审核完整 |

## 10. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-971 | 新叶CONTRACT与本文§1；唯一坐标/旧研究终态保持 | artifact: X:/AIstock_temp/ADVISORY/lstm-overnight-increment-20261108-v1/study/preregistered/protocol.json | PASS_EXECUTED_NEGATIVE | none |
| F-972 | assemble/check_inputs及原人口/22源角色前后hash，26NA不改 | artifact: X:/AIstock_temp/ADVISORY/lstm-overnight-increment-20261108-v1/independent_audit.json | PASS_EXECUTED_NEGATIVE | none |
| F-973 | 三折真实KEY/目标/训练编码、同Ridge/JSON/独立分数排名及证书 | artifact: X:/AIstock_temp/ADVISORY/lstm-overnight-increment-20261108-v1/independent_audit.json | PASS_EXECUTED_NEGATIVE | none |
| F-974 | 3个STARTED/FINISHED、完整阶段/registry/checkpoint和新进程0fit复用 | artifact: X:/AIstock_temp/ADVISORY/lstm-overnight-increment-20261108-v1/study/terminal/evaluated/receipt.json; artifact: X:/AIstock_temp/ADVISORY/lstm-overnight-increment-20261108-v1/reuse_cli.json | PASS_EXECUTED_NEGATIVE | none |
| F-975 | 原生主/匹配B辅助、四项损益/区间和事前停止，经济不达标 | artifact: X:/AIstock_temp/ADVISORY/lstm-overnight-increment-20261108-v1/study/evaluated/report.json; artifact: X:/AIstock_temp/ADVISORY/lstm-overnight-increment-20261108-v1/independent_audit.json | PASS_EXECUTED_NEGATIVE | none |
| F-976 | 原44项及源码CI保持；本次三轮审核、F1/符合性、精确文档交付独立 | artifact: X:/AIstock_temp/ADVISORY/lstm-overnight-increment-20261108-v1/validation_receipt.json | PASS_EXECUTED_NEGATIVE | none |

## 11. Risks / Production Gates

源高覆盖、非恒定、旧模型可读和设计合入不证明Alpha。旧窗口反复消费、上游提名选择、额外坐标容量、价格未调整、不同源修订、正常缺失及未知历史公开时间都会限制解释；本实验即使正面，也不证明真实PIT、实盘可交易或已满足独立经济确认。缺失字段原因未解的旧行业源仍`UNRESOLVED_SOURCE`，不借本实验关闭它。

本轮`source_delivery`按最终回执独立报告；`implementation=P0_P3_COMPLETE`、`financial_fit_budget_authorized=3`、`new_financial_fits=3`、`remaining_fit_budget=0`、`economic_acceptance=STOP_EXACT_OVERNIGHT_INCREMENT_CANDIDATE`、`production_ready=false`。没有激活推荐，三份新模型及预测只在研究根保留。`production_ddl_gate=noop`、`production_dml_gate=noop`、`dependency_install=noop`、`backend_restart_required=false`、`runtime_activation=noop`、`QE_control=noop`、`cleanup=noop`。无需重启服务；旧树、模型、评价、前向与恢复材料均保留。

## 12. 真实研究结果与精确候选关闭

producer=`405fb26f2315d319153e9d8f917feada554d0210`，protocol SHA=`0f5627b21d19168e297ab81a0957173c0a25e14328d14c2062ebd76ef79efd80`，完整阶段链终点=`00a36233a627f8f7fa2fd79f662ee0c3cd1c2d7efce82c2b331701ffdd09f198`。三折在其各自fit前匹配真实训练人口/目标/编码，原三B模型只读；本次恰好3次拟合，无第四fit、final、参数/方向搜索或恢复拟合。

| 比较，净bps/5TD | 开发共同完整D | 开发均值及95%描述性区间 | 压力共同完整D | 压力均值及95%描述性区间 |
| --- | ---: | --- | ---: | --- |
| B+F－原生Top10，业务主比较 | 101 | +46.714083 [−13.033510, 106.474933] | 108 | −75.666671 [−196.213205, 49.696533] |
| B+F－原54字段Ridge，坐标增量辅助 | 101 | −4.262869 [−20.694770, 9.384455] | 108 | +5.970900 [−11.580200, 26.606927] |

所有原开发126D/6,300行与压力118D/5,900行均保留。成熟D分别111/113，完整主比较101/108，覆盖90.9910%/95.5752%；辅助本次恰好也为101/108D，仍独立配对计算，不由异分母均值相减。成熟日干预111/112D；UNKNOWN比例相对原生变化0/−0.176991个百分点，下尾10%均值变化+23.797052/+66.457599bps。覆盖和风险项通过不能挽救主端点/增量失败。开发三折主均值为−47.207039/+63.494656/+91.723721bps，不择折或季度。

独立新进程不调用生产学习/预测/cohort/费用/区间函数，复算24,400分数和完整排名、两臂各244日日表与四项损益、各11,111条AVAILABLE费用、4组10D/10,000次区间。最大分数差7.28e-12；全部原64字段按列名保持相等，唯一新坐标26NA保持，22个源角色hash前后相同。独立脚本首次把metadata物理列序当作合同，发现`parent_score/source_id`投影顺序后，在相同列集上逐列核对全部值；该脚本修正未改源、模型、预测、金融值或预算。最终`independent_audit.json=PASS`。

金融研究进程实耗10.3672秒、结束RSS约347.78MiB、2CPU线程/0GPU；7次本机资源快照最少估计25.6空闲CPU线程、约27.68GiB available内存。21条best-effort QE观测均成功且active=0，仅记录当次观测，不证明全平台任务不存在，也未用QE状态作否决。新进程完整复用为0fit，六份claim hash不变。

按事前§7第3条，压力主均值≤0、开发辅助增量≤0，且两窗主均值均未达到100bps，故关闭本次精确候选并保持追加预算0。区间均跨零，停止是预定价值规则的决定，不是对总体效应的统计证伪。没有可上线的新模型、独立经济确认或历史PIT成立；NON_VINTAGE、原生未调整价格比值和修订混杂限制保留。当前54字段模型轮换及已暂停方向保持；没有自动排下一模型/另一信号或等待新交易日的研究任务。
