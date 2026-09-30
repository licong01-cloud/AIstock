# Advisory ENTRY_PRICE 一次性历史确认 F2 详细设计 v1.3

> 日期：2026-09-28；Feature tier：F2；业务归属：Advisory。
> 状态：SOURCE_MERGED_COORDINATE_V2_PASS_CONFIRMATION_INPUT_AND_RESOURCE_BLOCKED（2026-09-30）。PR #5099已合入四阶段源码及v2坐标，用户重启后语义验证通过；仍缺合格连续窗口、QE已确认独占时段及正式运行结果。
> 配套：[独立价格角色](advisory_entry_price_independent_role_f2_design_20260928.md)、[绑定及每日运行](advisory_entry_price_delivery_f2_design_20260928.md)。

## 1. Background / 为什么需要新入口

旧 `historical_price_replay.py::prepare_historical_price_replay_request` 固定读取v4目录中的 `calibrated_test_predictions.parquet`，它不能通过改变日期参数给2026-03-10之后的新日期推理。旧回放是 HISTORICAL_REPLAY/NAVIGATION_ONLY，旧80日test已用于研究，保持不变。自然通道又只允许真实盘前capture，不能改系统时间回填。

本设计新增独立历史推理及一次性确认入口，使用冻结模型在当时可见的候选/特征上重新推理，完成后再揭示目标开盘结果。它可以在今天批量完成，不等待未来交易日；但新日期和“执行时先预测后读标签”本身都不证明独立样本外。必须先核查该日期是否被整个价格研究族及用于设计该候选的上游研究消费、是否拥有可信历史可见性。

模型目标限开盘价格分布。日内是否成交、区间内买入是否增厚收益、止盈止损或胜率属于其他合同，本次不据coverage作上述宣称。

## 2. Scope / 完整输出

提供离线 `prepare → predict → settle → evaluate` 四步及同请求恢复；正式候选只有冻结v4，control为开发/validation期冻结的简单经验gap分布，不产生新的调参或训练。输出独立价格合同的确认状态、逐日/逐股结果及完整支持度。运行时binding仍由delivery单独执行。

数据依赖核查可以读取schema、覆盖日期、源版本、冻结模型训练截止、窗口消费记录及可见性元数据，不运行目标窗口上的模型选择、分桶、coverage或收益统计。功能开发和PIT测试使用旧已消费窗口或合成fixture。

## 3. Non-goals / 所有权与证据

不改QE代码、profile、数据release、候选库或历史Program；不提交QE训练，不重建父策略候选，不写数据库。需要上游资产时只消费QE/Selection已有正式只读输出；缺失交由其所属窗口。本设计不升级旧historical/prospective receipt，不把重建历史冒充盘前发布，不启动进程或安装依赖。

## 4. Architecture / 窗口资格与分区

### 4.1 三种资格

| 分类 | 条件 | 允许用途 |
|---|---|---|
| ELIGIBLE_LOCKED_HISTORICAL_OOT | 模型/变换/校准/候选生成训练截止早于窗口；PIT输入可证明；研究族未消费结果；参数和窗口先冻结 | 一次性价格分布确认 |
| CONSUMED_OR_NON_VINTAGE | 结果曾用于研究选择、只能用事后修订特征、或消费历史不可证明 | 功能/探索回放，不能绑定 |
| INPUT_UNAVAILABLE | 没有exact候选/父Alpha特征、provider版本或必要原始行情 | typed阻塞，保留缺口 |

2026-03-10之后仅是搜索起点的历史背景，执行时从bundle split/训练来源解析边界，不能硬编码为“未消费”。同股票跨包重复按日期/股票标记，不能放大样本量。QE已在窗口调过父模型且影响本候选时视为相关消费；单纯入库行情不自动构成研究消费。依据不足归为第二类。

已存在sealed窗口只有对应研究合同允许且未消费时才可用；不抢占其他窗口的sealed资源。本研究的新window只用既有registry明确记录日期和身份，不建新数据平台。

### 4.2 日期选择与恢复

在不看目标结果的前提下冻结一个连续交易日区间、replay-as-of、candidate roster、package/policy/universe及模型身份。计划覆盖不少于60个目标交易日；来源只能支持更短时仍可预登记，最低支持为20日/300有效候选。不得根据运行中的coverage扩窗、缩窗或换日期；欠功效保持INCONCLUSIVE。窗口跨市场状态的描述使用D可见数据，不按事后表现挑子窗口。

揭示任何目标结果前将本请求的window消费阶段写为CONSUMING，防止失败后被当成全新holdout；完成变为CONSUMED（此二者为新confirmation请求的局部阶段，不冒充既有ResearchWindowState枚举）；在Advisory既有登记中保留SEALED_UNCONSUMED到FROZEN_TEST_CONSUMED的事实与消费引用，崩溃后只允许同request和预测hash恢复。exact retry不算新trial，不重新选候选。确认失败后若做新模型，必须新lineage及真正独立窗口。

## 5. Contracts / 请求、输入及阶段

已实现 `AdvisoryEntryPriceConfirmationRequestV1`：request hash、study_type=CONFIRMATION（复用Advisory的ResearchStudyType.CONFIRMATION，不修改QE registry）、objective_contract、decision_use、证据分类、研究族lineage、窗口资格依据、日期计划、候选引用/hash、model/package/manifest/style/policy/universe/schema、active profile及data release/node root、训练截止、transform/calibration identity、版本化算法及评价spec。

登记映射遵循现有Advisory控制面：合格OOT确认使用`study_type=CONFIRMATION / decision_use=DIRECTION_GATE`；开发回归使用`EXPLORATORY_SCREEN / NAVIGATION_ONLY`。确认通过的artifact可被后续`ACTIVATION / ACTIVATION_EVIDENCE`记录引用，但确认记录本身不得冒用该组合。激活仅指ENTRY_PRICE shadow，不允许发布收益策略。协议身份包含下面的统计标准；无法填完整即不能开始目标结果读取。路径从显式请求/profile解析；需要RD-Agent数据身份时显式传入节点data_root_uri并验证complete=true。long-trend snapshot不属于本任务。

### 5.1 prepare

枚举目标日期、合法候选及来源，验证候选生成在当时可见，不读取目标开盘结果。已有候选如不是精确Top20或策略模式不同，按其真实scope登记，禁止截断、补第21名或调用Selection重建来凑数。当前首个v4确认范围固定为原exact包Top20；指数/新包另做范围确认。

资格字段不得只由调用者填写一个“合格”字符串。复用请求中的三个只读证据引用：vintage元数据须匹配scope hash、profile/release/node root/dataset及四项fit截止，并明确PIT可见性已核查；candidate provenance须匹配完整days计划的hash；consumption review须覆盖请求的全部parent lineage，携带已消费日期区间并与目标窗口无交集。缺少完整性声明、内容不匹配或来源不可证明时只能降级开发回归。此最小JSON协议仅表达本次输入审计，不新建审批或归档平台。

control必须从`model_root/price_range_runs/<v4原训练request_id>/daily_price_envelope_labels.parquet`的原validation分区读取；日期集合与bundle内split精确匹配，不读取test分区、不任选子集。校准与early-stopping使用validation标签的实际T时钟，不仅比较D日期。未来结果不能因调用者自报更早的fit日期而通过验证。

### 5.2 predict

批量读取D及之前的已有日频文件/数据库PIT输入；同日期view裁剪business_date与available_at，as-of缺证不得用最新静态属性替代。加载v4三个头一次，逐日期共用ENTRY_PRICE内核，不加载M3推理、不依赖Ranking成功。先发布连续gap、raw/calibrated/tick范围、公司行动/法规投影、control范围、逐股状态及全部输入hash；prediction完成前禁止结果reader。

候选/特征自身必要的历史价格可以被读取；目标T结果只在预测完成后的settlement打开。为多日批量预取时，T日行情可成为下一日期的D特征，但绝不能进入前一D的推理view。未来数据poison测试必须证明这一点，不能只看SQL有end_date参数。

### 5.3 settle

所有预测及完整日期集合冻结后，单独只读查询历史T原始open与权威停牌。成熟性由冻结replay-as-of及对应历史数据完成状态判定，不依赖今天18:00或当天刷新。市场/模型状态正交，停牌=NOT_APPLICABLE，未知缺行=UNAVAILABLE，保留全部行；未解释缺行使结果INPUT_INCOMPLETE，而不是经济FAIL。

open_li转CNY一次。连续目标与独立角色设计§5.3一致，最终价格按冻结prediction直接比较，不从实际开盘反推新的边界。重复、冲突、hash漂移整次拒绝。批量持有模型及有界日频块，不逐日创建worktree或启动进程。

### 5.4 evaluate及artifacts

输出根 `<model_root>/entry_price_confirmations/<request_id>/`，包含request、prediction/、settlement/、evaluation及manifest；各阶段原子发布且互相绑定hash。状态：INPUT_INCOMPLETE / INCONCLUSIVE / NOT_CONFIRMED / CONFIRMED_PRICE_DISTRIBUTION。自然前向类型保持PROSPECTIVE_OOS，历史合格类型为LOCKED_HISTORICAL_OOT，旧回放仍HISTORICAL_REPLAY，三者不能混成样本数。

## 6. Evaluation / 本候选的固定判定合同

这是本次设计确定的首个v4 shadow确认标准，不是历史实验已使用或已通过的门槛。其用途为有限范围的价格预测展示，不要求证明交易收益，不新增审批角色。窗口资格失败时不运行统计选优。

1. 支持度：至少20个不同T、300个模型和市场均AVAILABLE的候选；至少80%的计划日期每日至少5个有效候选。全部原始候选计入可用性分母，模型可用率在市场可评价人口中≥95%。未知市场缺失、身份冲突为0；停牌单列，不削减冻结roster。
2. 连续模型和业务tick空间分别计算coverage、lower/upper miss、宽度、中点绝对误差、pinball loss、interval score、crossing及rounding rescue/harm；crossing=0。有效行等权统计与逐日等权统计均报告，判定采用逐日等权，防止候选数多的日期支配结论。
3. 延续前序规划的central-80允许5个百分点偏差：连续coverage点估计须在[0.75,0.85]；业务coverage须≥0.75，允许tick产生更高coverage但不可充当连续校准证据。该容差面向shadow展示，不外推“80%保证”。
4. 宽度control从原validation的可用标签一次性取q10/q50/q90（linear quantile），按同一CNY/公司行动/tick规则投影；取值与validation源hash写入request，不查看holdout拟合。模型平均业务宽度≤control的1.25倍；ratio与绝对bps都报告，control宽度为0时定义为INPUT_INCOMPLETE，不临时换基线。1.25是本合同的防过宽预算，不是收益经验结论。
5. 使用标准central-80 interval score：`IS=(u-l)+10*(l-y)*1[y<l]+10*(y-u)*1[y>u]`，单位为连续gap；对同股票同T计算模型减control，再逐日均值。冻结5交易日moving-block bootstrap、5000次、seed由request hash确定，给出双侧95% percentile区间；区间上界≤0才确认价格预测相对control具有正向或边界不劣证据；零差异容差是严格比较，不能把“未显著更差”当成非劣证明。不能把上界跨0写成PASS或失败的确定证据，而是INCONCLUSIVE；点估计已正且区间下界>0则NOT_CONFIRMED。
6. coverage点估计越界或宽度超预算归NOT_CONFIRMED；支持不足或比较区间跨0归INCONCLUSIVE；输入正确性错误优先INPUT_INCOMPLETE。只有输入/支持/coverage/宽度/对照同时满足才CONFIRMED_PRICE_DISTRIBUTION。小样本下达不到标准不通过扩宽区间或追加观测反复检验来解决。

低成本control与受控宽度共同检验“模型是否提供有用的价格分布”，不要求引入交易模拟器。利润/胜率对照不是本切片交付，也不能替代价格确认。新增阈值均在读目标结果前随设计/request冻结；后续修改产生新协议并保留旧结论。

## 7. Implementation Plan / 最小实现与接口

现有可复用：`load_frozen_price_range_bundle`、`build_advisory_feature_matrix`、旧回放的数据/投影内核、prospective严格读回和原子发布模式。旧CLI和旧aggregate固定activation=false，不修改其证据语义。

已实现：`backend/services/advisory_model_first/entry_price_confirmation_contracts.py`、`entry_price_confirmation.py`、`entry_price_confirmation_cli.py`。必要数据adapter限制在新confirmation文件内，通过已有公开只读来源消费，不改外部模块。

已实现CLI `python -m backend.services.advisory_model_first.entry_price_confirmation_cli`，子命令prepare/predict/settle/evaluate/inspect。prepare显式接收 `--spec --model-root --output-root --env-file`；其余接受`--request --model-root --output-root`，仅需要DB的阶段额外`--env-file`。全部写入仅artifact；返回0表示阶段成功（经济结果仍必须读status），2表示合同/输入错误，3表示未成熟/资源依赖waiting；禁止把0自动解读为模型确认。

资源执行合同细化：predict/settle/evaluate额外显式传`--qe-exclusive-slot`，消费由QE窗口已确认的只读JSON授权说明（authorization_ref、request_sha256、starts_at、expires_at，时间含时区），缺失/过期/其他请求不执行。消费者不能自行生成独占授权；此文件记录外部协调事实，不是QE原子锁或新审批平台。每个日块前后复查时窗和QE公开只读完整任务快照；非终态/未知立即WAITING，保留已消费事实，只能同request恢复。续约可以换slot，但不改变研究假设/窗口/预测；prepare/inspect仅元数据读回不要求占用时段。

实施顺序：用旧已消费样本完成合同与同核parity → 窗口资格spike → 冻结spec/control → 一次完整推理 → 目标结果揭示与评价 → 向delivery交付结论。实现无可用新窗口时继续完成源码与历史功能验证，报告确认阻塞；不通过改名伪造sealed，不默认依赖QE新训练。

## 8. Verification Plan

新测试：`backend/tests/advisory_model_first/test_entry_price_confirmation.py`、`test_entry_price_confirmation_contracts.py`、`test_entry_price_confirmation_cli.py`。覆盖窗口污染/非vintage降级、日期越界、训练时钟、同股票重复、目标毒化、先全量预测后结算、阶段恢复、未知缺行保留、统计边界、0退出码负结论、跨合同不混算。

复用旧`test_historical_price_replay.py`验证旧证据合同不变。统计测试使用小型可手算的IS例子、固定bootstrap确定性和按日分组；不写只断言实现常量的测试。真实批量仅在代码审核与输入就绪后运行，不与QE实验并行。

## 9. Design Acceptance Index

| ID | 验收要求 |
|---|---|
| F-520 | 新日期须证明窗口未消费、上游训练时钟及PIT，未知降级 |
| F-521 | 新历史推理入口运行冻结v4，不复用旧test预测伪造新确认 |
| F-522 | 所有预测先冻结，再揭示结果；单日/批量同核且无未来泄漏 |
| F-523 | 候选保留、正常缺失/系统错误分开、计数与重复控制 |
| F-524 | 固定control、coverage/width/IS判据、日期block推断与结论边界 |
| F-525 | 一次性window消费，exact retry只恢复，失败不回选 |
| F-526 | 输出可验证confirmation scope/hash，经济结果不由exit code替代 |
| F-527 | 仅Advisory只读数据/写artifact，无QE训练或生产操作 |

## 10. Design Acceptance Matrix

本矩阵验收四阶段源码及定向测试，不验收模型效果。SOURCE_VERIFIED不等于合格窗口存在、实际推理已运行或价格确认通过；这些仍待真实输入审核及外部QE独占时段，生产binding保持未发布。源码先行遵循已批准方案，不降低§4与§6条件。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-520 | §4；entry_price_confirmation_contracts.py/verify_confirmation_inputs | backend/tests/advisory_model_first/test_entry_price_confirmation_contracts.py | SOURCE_VERIFIED | none |
| F-521 | §5.2、§7；entry_price_confirmation.py | backend/tests/advisory_model_first/test_entry_price_confirmation.py | SOURCE_VERIFIED | none |
| F-522 | §5.2～§5.3 | backend/tests/advisory_model_first/test_entry_price_confirmation.py | SOURCE_VERIFIED | none |
| F-523 | §5.3、§6 | backend/tests/advisory_model_first/test_entry_price_confirmation.py | SOURCE_VERIFIED | none |
| F-524 | §6 | backend/tests/advisory_model_first/test_entry_price_confirmation.py | SOURCE_VERIFIED | none |
| F-525 | §4.2、§5.4 | backend/tests/advisory_model_first/test_entry_price_confirmation.py | SOURCE_VERIFIED | none |
| F-526 | §5.4、§7 | backend/tests/advisory_model_first/test_entry_price_confirmation_cli.py | SOURCE_VERIFIED | none |
| F-527 | §3、§7 | backend/tests/advisory_model_first/test_entry_price_confirmation.py；test_entry_price_confirmation_cli.py | SOURCE_VERIFIED | none |

## 11. Rollout / Rollback

离线源码合入不需后端重启；运行失败只停止本次阶段，保留预测/消费记录供exact恢复。CONFIRMED只允许交给delivery绑定同scope的shadow角色；INCONCLUSIVE/NOT_CONFIRMED不绑定，模型改进须新lineage。若没有独立历史窗口，自然前向是可选补充，功能开发、代码审核和旧窗口回归继续进行。

## 12. Risks / 不确定性处理

当前未证明合格连续新窗口存在；冻结v4含父Alpha/HMM等特征，缺少历史来源不能用最新值填充。数据库按日期查询不能自动证明公告/修订可见性；无法证明时只作探索。公司行动坐标已在开发数据形成独立v2投影身份，确认请求必须绑定该身份，不能改写旧v1权威结果。20日/300行是最低支持条件，不是整个功能完成百分比或足够统计功效的保证。

vintage审核文件必须包含 `entry_coordinate_review`：schema=`advisory_entry_coordinate_review_v1`、status=PASS、原validation标签文件hash、scope hash、projection producer版本、checked_validation_rows、unavailable_rows=0、tolerance_abs_gap=1e-6、maximum_abs_gap_difference。检查数必须等于原validation可建模行数，最大差不得超过固定1e-6 gap（0.01 bps，数值一致性容差，不是coverage/收益门槛）。只在已消费validation运行；缺失、不全、超差或身份不符，正式prepare失败。禁止把坐标审计放到新holdout或根据确认效果选容差。v3曾以validation early stopping，因此模型fit截止也必须覆盖这些T标签，而不能只报train分区终点。

## 13. Production Gates / 设计符合性

源码交付：DDL/DML、binding、后端重启、训练均noop；模型/输入为只读。用户拥有后端重启权。DESIGN-COMPLIANCE-001：四阶段及分类完整；错误不写成无推荐；不改旧研究协议；本设计所需控制仅服务一次确认且无新审批平台。本地测试与结构检查通过不表示窗口、模型或生产通过。

## 14. 实施审核状态

已修复/复审：control仅从原v3训练run的validation标签读取，检查实际T标签成熟时钟；预测冻结先于结果查询；每行重新核对校准量和tick/法规投影；crossing统计覆盖全部已输出预测，不能被后续停牌掩盖；重试绑定原消费收据；QE外部时段续约不改已有评价artifact。测试使用合成/已消费数据，不产生正式效果证据。

当前只读资源前检仍返回`WAITING_RESOURCE / QE_TASK_NONTERMINAL_OR_UNKNOWN`，完整资格材料尚未获得，因此没有prepare正式请求或提交实验。现有DB特征adapter保持D晚于父bundle continuation_cutoff的边界，不能借新入口绕回旧训练期。新日期还须满足本设计全部vintage/消费条件，不把日期后移当成自动OOS。

已消费validation的1,000行经v2生产器复核：检查1,000、不可用0、最大绝对gap差`1.1347649842008423e-7`，满足固定`1e-6`容差；3行公司行动全部保留。坐标前置项可签PASS，身份=`advisory_entry_price_core_v2`，标签SHA=`c4fc72b94e9e112bcc05405e8c7f6ec28bc2890b16c4ff978e3b7dfe0ee2b148`。该审计没有运行模型、写DB或消费新窗口。

同轮只读候选元数据spike：匹配冻结包的ENABLED Top20 Program=`advp_3126dd77f9774d94850f37ad012f640f`，当前binding=`advb_f860140caa314665ad60ac089ed84b3f`，全市场池；已有30个PUBLISHED目标日（2026-08-14至2026-09-29），另有1个2026-07-16 REPLAY。只读取日期/数量/身份，未读取目标收益或运行模型。现有30日包含缺口且只有较晚日期带原生universe receipt，不能拼接成最低20个连续合格日；不得重建候选、伪造receipt或降低门槛。冻结v4 test prediction历史回放可作为`NAVIGATION_ONLY`功能回归，但不能替代正式确认。

2026-09-30重新读回元数据：31个PUBLISHED目标日（新增09-30）及1个REPLAY；新增日期不自动解决历史缺口、原生候选身份和PIT资格问题。现有exploratory输入材料明确pit_visibility_verified=false，不能改签为ELIGIBLE_LOCKED_HISTORICAL_OOT；本轮没有读取目标效果或产生新的确认请求。

资源审核发现消费者误用了include_children=true：该视图按父实验分页却展开子行，不能用返回行数推进父offset。BUG-1632改用公开平铺include_children=false，逐行覆盖父实验和子运行，保留分页完整性/身份/状态检查。三项公开QE task详情仍paused（qe_20260716_042842_fd61、qe_20260810_221723_14ab、qe_20260824_101005_ce66）；legacy canonical_status缺失同样不推断idle。只有QE窗口解决状态可判定性并确认独占时段后才能执行，不能由Advisory终止任务或自造时段。已消费v4功能回放亦不绕过不并行要求。
