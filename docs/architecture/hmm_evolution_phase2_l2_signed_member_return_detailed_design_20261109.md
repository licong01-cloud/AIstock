# 申万L2固定成员组合有符号收益：目标对齐与匹配对照

> v1.2；2026-11-10；F2；父蓝图§1.32～§1.33/§7。当前状态：`RESEARCH_COMPLETE_STOP_NO_INCREMENTAL_ORDERING`。已按批准的L2-SIGNED-MEMBER-D1～D6完成源码、完整三季度文件预检、10/10新增fit、双fresh-process模型/预测封存及评价。两臂各142日×131=18,602行预测完整；L2主量及真实新买队列均未显示正向增量，按原规则停止本精确候选，不安排新QE/生产采用。正式研究源码HEAD为8f0a44dad48497ef5ce2cdaa0ded54831e56d684；完整结果见§11.1，原合同公式/预算/停止条件保持。§11.2追加零fit归因，唯一后继为独立待批准设计，不重开本S预算。

## 1. Background / 唯一问题与历史去重

终极目标仍是申万二级行业信号帮助真实股票消费改善收益及风险代价，而不是产生更多合法模型。旧成员C更好识别成员亏损，不代表能区分值得承担的上涨机会；其自然新买六账户和VIX辅助持仓参考均已完成，后者 `STOP_NO_INCREMENTAL_CASE` 不重跑。本次只检验：**在相同20维信息、成员、权重、季度和Ridge下，直接学习固定成员组合的正负收益，能否比仅学成员亏损更好排序未来收益，并迁移到已有真实新买队列？**

| 已有路线 | 事实及本次区别 |
|---|---|
| 官方L2有符号相对收益 | `rotation_l2_moneyflow_price_return_supervised.py`，20261008互补信息设计§15～§16；4特征、官方行业相对CSI300的10日收益、alpha=0.01、126训练日，已经执行。收益有正负并非新发明；该模型不是20维、固定成员组合、季度更新的本次候选 |
| 原B | `risk_l2_member_loss.py::pair_target`：`B=max(0,-sum(w_i*R_i))`，正组合收益被截为0 |
| 原C | 同函数：`C=sum(w_i*max(0,-R_i))`；保留成员损失却不扣除同组合的上行补偿 |
| 已存上行U | 同一标签已经保存 `U=sum(w_i*max(0,R_i))`；本次S=`U-C=sum(w_i*R_i)`，不新增风险厌恶系数、不单独训练上行头 |

定向核对当前源码/直接设计未找到“20维＋固定成员有符号收益＋相同季度Ridge”的已完成候选；这不是对所有历史临时笔记的穷尽证明。S是一个监督式L2收益研究候选，不是新的隐状态HMM；不能因模块名而宣称训练了HMM。

### 1.1 已完成P0：只读TRAIN载体核对

基线源码 `3146a812645729bf33bea3af782c9d73141b58dd`。一次fresh-process标准库检查仅读取下表三份已冻结TRAIN/特征载体，核对canonical/文件hash、完整131、日历、原mask和代数关系；未读取市场源文件或评价outcome，未fit/推断。这里的旧TRAIN包含后续季度允许使用的历史，不能给Q2模型训练读取Q4标签的权限。

| `F:/Dev/AIstock_runtime/hmm_l2_member_loss/20261106/prepare_v4/` | canonical receipt | 文件SHA256 |
|---|---|---|
| prepared.json | `203e4beaf175e453953bd81d860c2af9031265d2589347fb8ba971663c8b6929` | `c1239e87d0786a879707bc7dbae32b5c80771d46cf4f0e06226b56b90617158f` |
| features.json | `78472f692dac2cdfea27553b7d51e4ff053f046f969bfd24c88974be78a5c5ee` | `22dae9b94b2ab8aa714ae0d49c76a137a33ae6659d0ec7dd64f1225472d51b04` |
| train.json | `de3ce3723462beeb11e6fcc13abc640664772f9891f49427c7a90b20a84a5516` | `8b950d31cce6a336b9648379f0affeb82da7965cee8022a3986062f87384c352` |

Q4原共同mask为89,871行/716日，SHA `a774fd8382d5369e2094ba1fb96be716b7d90f6d30414dc56ae847f6ab387937`。S在该mask内45,877负、43,994正、0个精确零；范围−0.34395203996052603至1.0205176009027026。`B=max(0,-(U-C))`最大浮点差5.551115123125783e-17。另有316个可定义标签不属于原共同mask、3,609个合法固定成员端点NA、1,310个未成熟键；均不为了新候选增加样本或填值。正收益大于1可能合法，不能沿用损失的[0,1]校验。

结论仅为 `P0_TRAIN_CARRIER_FEASIBLE_NOT_MODEL_EFFECT`；不是新Q2/Q3全窗口预检、样本可学习性或经济价值证明。只复用紧凑结果和既有载体，不为此重建历史证据。

## 2. Scope / Architecture / 最小实现边界

本轮按已批准范围实施源码和更新两份设计状态：新增 `backend/services/hmm_risk/risk_l2_member_return.py`、`scripts/hmm_risk/run_risk_l2_member_return.py`、`backend/tests/hmm_risk/test_risk_l2_member_return.py`；仅在必要时修改HMM的 `risk_l2_loss_severity.py` 及 `test_risk_l2_loss_severity.py`，把日期加权Ridge数值核提成模块内语义中立函数，原loss wrapper的范围/输出/身份不变。不得复制整套训练核或放宽旧loss validator来接受负标签。

复用 `risk_l2_member_loss.FrozenMemberSource`、`pair_target`、成熟期裁剪、共享正式20维reader、`risk_l2_member_release.load_prepared`及manifest/PIT/security/provider/价格编码验证。新执行器独立组织三个vintage，不能把旧只支持Q4的CLI当作已经支持S。无新增表、API/UI、调度器、环境变量、数据导出、QE接线或记录平台。

## 3. Contracts D1 / L2-SIGNED-MEMBER-D1：目标与身份

候选版本为 `hmm_l2_signed_member_return_ridge_v1`。对decision日t，以t前一交易日可得的PIT成员和严格前置circ_mv构造固定成员集合及一次归一权重，沿用原成员设计20261102的D1/D2；每只成员的R_i是同release价格合同的 `P_i(t+10)/P_i(t)-1`。首个收益session为t+1，特征最多到t−1；不能将t收盘标签锚点误称09:15可成交收益。

S=`sum(w_i*R_i)=U-C`，币值单位为收益小数；不减CSI300、不做截面center/rank、winsor或裁剪，不改10日目标。不再输出event probability、warning或expected_loss；输出 `expected_member_return` 为Ridge原始有符号估计。真实标签须有限且>-1，没有人为上界；预测也须有限，但可能超出真实收益物理范围，必须原值报告并统计，不能截成概率或伪装保证收益。

标签明确继承 `price_basis=C010_unadjusted_integer_li`：它是未复权价格参考，除权/分红可能造成非投资损益的价格变化，不能称为完整total return。保持这个口径是为了本次只改变目标取正负部的顺序；不能在实施时悄悄改复权标签并仍声称只变目标。D5真实新买队列另用已冻结复权参考检查迁移；若原生价格与真实消费不对齐，不得以全L2正IC绕过迁移不足。

自然停牌、退出/进入及合法端点NA沿用原成员合同：必要成员端点不可定义时整条目标为typed NA，不删除成员后重新归一；未知缺失/hash冲突fail closed。标签NA不删除完整预测键。131目录、稀疏共享ID→正式申万文本代码、停发行业与membership分离保持。不生成HMM私有编号。

## 4. Contracts D2 / L2-SIGNED-MEMBER-D2：冻结输入及时间

输入显式固定为 `X:/AIstock_dataset_candidates/backtest_dataset_candidates/20261030-qe_hmm_full_v2-direct-monthly-v2-unified-candidate-correction-4388f6c6d166`；generation=`20261030-monthly-v2-unified-correction-4388f6c6d166`，release=`qe_hmm_full_v2_20261030`，cutoff=2026-10-30；manifest canonical=`772112c58f959748787b939e33ec874fb0766ca921d88eddbd49c131c3b3c43d`，文件SHA=`2242c44261dc0eaaf54cda3416a260cf6712ec960cff8bfbb46a527f6587b958`。不是源码全局常量，不读取其他active版本兜底。

原20维及顺序严格等于 `contracts.ALL_CORE_FEATURES` 的本设计基线版本，feature_definition SHA=`c9c197c189c580cf203332944d160b5c88fe414ca1145ef1e6d5a8221a25dccd`；所有具体feature/source/security/provider/calendar/C-013/release membership pins从上述已验证载体和manifest继承进新request并读回验证，不在运行时按当前文件重新授权。20261106新release设计D2的分类窗口/研究性release投影原样适用：`historical_as_known=false`，不能用10月观察伪造早期发布时间；不借数据库补齐。未成熟必须在读端点前判定。

| vintage | 训练decision范围 | 标签最晚端点 | 预测有效期 | 本次实际请求 |
|---|---|---|---|---|
| 2026Q2 | 2023-04-01..2026-03-31 | 2026-03-31 | 2026-04-01..06-30 | 同有效期真实交易日 |
| 2026Q3 | 2023-07-01..2026-06-30 | 2026-06-30 | 2026-07-01..09-30 | 同有效期真实交易日 |
| 2026Q4 | 2023-10-01..2026-09-30 | 2026-09-30 | 2026-10-01..12-31 | 2026-10-01..10-30真实交易日 |

每vintage先按 `pos(t)+10<=pos(train_end)`裁剪再读取训练标签，模型不得反向用于更早季度。request保留2026-04-01..10-30全部142日×131=18,602键；10月底尚未成熟的10日也必须预测/报告状态，不能缩为可评价区间。整体132个日期具有日历上的10日成熟端点，实际可评价集合再按合法NA逐键报告；Q4仅7个成熟日期不是独立强证据。

训练前一次完整file-only preflight，覆盖三vintage所需历史和预热，构造共同键、身份和所有必要标签；禁止fit边跑边补输入。可复用§1.1已匹配的Q4缓存，不要求重新导出或全量重建。Q2/Q3新增训练载体已完成并通过正式预检，实际样本/排除数见§11.1；若未来复核文件不能闭合，列精确组件/日期/行业owner，停止而非切旧数据或调整窗口。

## 5. Contracts D3 / L2-SIGNED-MEMBER-D3：公平对照与10-fit上限

S和匹配C每季度共享20维X、训练日期、成员/权重、标签事实、原C-010有效性共同mask及scaler。S训练值为U−C，控制值为C。不能用目标不同为由放宽mask，不能在评估后选季度、种子或模型。

保持旧Ridge全部数值设置：alpha=1.0，solver=svd，fit_intercept=true，copy_X=true，max_iter=null，tol=0.0001，positive=false，random_state=42。train-only mean/population std；常量维按原核识别并剔除。每条训练权重为 `N/(D*n_day)`，即日期等权，不把131行当独立时间样本；scaler仍为原核行等权mean/std，不将其私改为日期加权。数值环境沿既有Conda base：Python3.13.5、numpy2.3.3、scipy1.16.3、sklearn1.8.0、threadpoolctl3.6.0、hmmlearn0.3.3及既有线程变量=1；hmmlearn只保留环境身份，不调用隐状态拟合。不安装依赖或修改AIstock环境。

**为何不能直接把旧C当全部匹配控制**：原Q2/Q3 C在9月release训练（manifest `590b3b204085373ff4bd101efc50a45c6dd3844200200f987f9bc783e0c6b394`），11/6只是用10月输入重推，训练身份没有改变。与本次10月release训练的S直接比较会混入数据修订。不得称为“只改目标”。

新预算仅：S的Q2/Q3/Q4×双fresh-process=6；C匹配控制的Q2/Q3×双fresh-process=4；合计**10次新增fit**，一个新目标候选。C控制新建版本、保留原定义，旧C/原8＋4预算不回写、不覆盖。失败attempt先于fit持久化，失败即停，不自动重试/加seed/网格搜索。

Q4 C不重训：严格复用 `F:/Dev/AIstock_runtime/hmm_l2_member_loss/20261106/run_q4_v1/process_1/C.sealed.json`，文件SHA=`602a0027536f33414dbc31575130e0c4bb56da9038872721108cbde77ed9f449`，model SHA=`706719ca85bbca99fc1e9e8294974cdbc8fe9dcc794ef708f04180c213347f7b`；原acceptance canonical=`8342abe3b6c65b0a2e61e4a91cd1303174b04feaa62c25ddecea0f2fbaae52a6`，文件SHA=`b7faf857c082e836e7611a354a140ad944a70c150875be38fd12c42c7063b6ef`。实际训练release/feature/label/mask/rows/dates/scaler/numerical pins须与§1.1及新Q4公共训练载体逐项匹配。匹配失败为 `CONTROL_IDENTITY_MISMATCH`，停止，不自动把预算改成12。两process各加载此同一冻结控制、以相同新输入推理，保持旧模型身份；新源码提交不冒充旧训练HEAD。

## 6. Contracts D4 / L2-SIGNED-MEMBER-D4：封存及输出

正式执行须固定已交付的clean源码HEAD。prepare、fit、predict、evaluate分命令/权限，训练进程只挂载本季度TRAIN；先完成并封存全部模型和两process预测，再开放本次评价事实，不能用Q4已知标签回调选择Q2/Q3参数。既有历史已消费，隔离只防本轮实现泄漏，不恢复holdout资格。

新模型/预测schema明确带S目标合同、参数、scaler、20维序列、训练窗口/source/mask/label/hash、数值环境、source commit及推断输入身份。匹配C指针独立，旧loss schema仍拒绝S。每个decision完整131条、唯一文本代码；`trade_date=t`、`as_of=prev_open(t)`，不重复滞后。合法unavailable保留null/reason，未知输入失败，不能填默认收益/安全概率。新版本资产输出到任务专属runtime目录，不覆盖旧C、M0/M1、QE交付或dataset。

双fresh-process对每个新模型参数、完整预测的canonical及字节身份一致；可变运行时间/进程ID只存在各自执行receipt，不混入确定性payload。Q4复用控制也核对预测一致性。只记录完成本研究所需模型/预测/紧凑结果，不另建历史证据仓库、不绑定环境变量或要求后端重启。

## 7. Contracts D5 / L2-SIGNED-MEMBER-D5：价值分析与停止

### 7.1 固定评价对象及统计

主对象是完整可定义L2的实际S标签，排序方向均“越高越看好”：新score为S原始预测，匹配控制score为负的C正式clip[0,1]预测。每日期在两臂和标签共同可定义集合计算Spearman average-tie Rank IC；少于2个可用行业或常量序列为typed undefined，不填0。主量为日期等权 `mean(IC_S-IC_minus_C)`，同时报告各自均值；不能仅靠改善负基线就声称有用。相同集合报告原 `excess_return_Nd` 的固定方向排序作零fit背景，不据其结果选择新参数。

误差辅助：S的MSE/MAE与该季度TRAIN S日期等权均值常数预测比较；C不是收益预测器，不比较C与S的原始MSE。全期/Q2/Q3/Q4覆盖、逐日差和Newey-West Bartlett lag9、双侧95%区间均报告；lag内按真实session距离，不把NA日期压紧。三个QE seed共用市场历史，不增加独立日期数。区间跨零不伪称证明无效，也不新增显著性AND门。

精确定义：对可定义日序列x、均值m和n个共同日期，u_t=x_t−m，`SE²=(sum(u_t²)+2*sum[j=1..9]((1-j/10)*sum[两日均可定义且session距离=j](u_t*u_(t-j))))/n²`；区间为m±1.959963984540054*SE。n<2为undefined；若计算得负方差，无论是否疑似舍入都保留原值并将区间标为`NUMERIC_UNDEFINED`，不夹0、不增加epsilon或改lag；不得据此声称模型效果失败。MSE/MAE先每日平均再跨日平均，常数预测只来自对应TRAIN。

完整L2另报各臂高/低20%组的S、成员下行C与上行U。组容量k=ceil(0.20*n_available)；按score降/升序，边界同分组采用均分剩余名额的分数权重，组内权重归一，不靠文本代码决定赢家。高−低S和两臂差只是10日重叠参考，不拼账户NAV，不假设可交易行业组合；无实际订单/费用时不声明成本后收益。

### 7.2 已有真实新买队列的可迁移性

只读冻结 `F:/Dev/AIstock_runtime/hmm_l2_member_loss/20261106/value_dual_v1/cohort_facts.json`，文件SHA=`2a367e0f82bf540a5ab18e4863d97c6bef0e2413f78ba193e0c23531de7a0f7a`，canonical=`9058b93b4bf26ad68c45a4217508e99c9cffbf5e3250c03c84ce6c9d86529997`。只使用`rows`，保留2,053条原seed/交易日/股票记录；不使用`secondary_rows`的更晚成熟结果择优。旧carrier/analysis pins保持，不把内嵌旧B/C预测当本次控制。事实是同release的固定10D复权close-to-close股票收益，不是账户实际持有期净收益。

先按(day,stock)去除跨seed重复，要求重复键的行业、事实和reason完全一致，再与本次两臂同日L2预测连接，计算共同可定义日期的日均股票收益Rank IC及两臂差；复用原股票→L2文本映射并核对其冻结来源，不以事后上涨/亏损选择股票。seed分项仅诊断，不视三份独立样本。合法NA/未成熟和不构成日截面的日期完整列账，不强求每个seed/月都有足够样本、不缩原清单。该队列是固定旧基线产生的样本，有选择性，只检验迁移线索；不能代表新策略的成交、退出、再投资或因果增益。

### 7.3 事前资源取舍规则

- 技术/身份/复现失败：`TECHNICAL_BLOCKED`，不下经济结论，不自动追加fit。
- 若L2主量不能定义：`INSUFFICIENT_EVALUABLE_EVIDENCE`；若IC_S<=0或主差<=0：`STOP_NO_INCREMENTAL_ORDERING`，不交QE新实验。
- L2两项点估计均正，但固定新买队列无法形成共同日：`INCONCLUSIVE_TRANSFER`；若队列IC_S<=0或相对负C增量<=0：`STOP_NO_TRANSFER_LEAD`。保留全部结果，不搜索另一队列、阈值或消费公式。
- 两层均有正向点估计：仅为 `RESEARCH_LEAD_REQUIRES_VALUE_REVIEW`。结合分段、区间、组收益/上行机会、集中度与物理范围异常给出继续或停止的有据建议；不将这些诊断再变成全月份/全seed/全指标必胜门。正点估计只是是否值得讨论QE的资源筛选，不是业务最低效应、正式admission或经济通过。

不新增0.02 IC或成本后门槛，不从历史最大值反推阈值。即使有研究线索，后续QE须由其owner冻结消费及匹配基准，HMM不得自动生成权重、套1-p或启动账户。全历史反复使用、分类回溯、非total-return价格、重叠10D、Q4仅7成熟日及旧队列选择偏差始终披露。没有新独立样本时不自称泛化已证实。

## 8. Contracts D6 / L2-SIGNED-MEMBER-D6：一个完整工作包

批准后在一个连续包内完成源码、反例测试、至少两轮作者审核修复、最小门禁/CI、固定源码预检、10-fit及零fit对照评价、完整结果/停止建议与父蓝图状态同步；不把CLI、测试、PR各拆研发阶段。按预算失败即停止，不为时长凑实验。P0、源码交付、正式预检和10/10 fit、封存预测及评价均已完成，结果触发§7.3既定停止条件。本包结束，不追加fit或以另一目录重试。

若到达可讨论QE的线索，仅交明确新版本资产/统计/边界和消费问题；QE负责其接线/实验，荐股和LocalSIM后置。本设计不是它们的消费公式批准。旧生产、preset_A、数据集与active profile不动；本包不为研究技术加载申请重启。

## 9. Implementation Plan / Verification Plan

直接矩阵：正/负/大于1的S和U−C代数；旧loss wrapper仍拒绝负值；冻结成员及合法NA不重归一；训练成熟先于读取端点；三vintage生效/训练隔离；同X/mask/scaler/日期权重；Q4复用身份失配即停；fit attempt计数及最多10、不得失败重试；raw有符号输出不clip；完整131/未来日期/重复键/hash漂移；双process确定性；分数ties、NA session lag、同股票跨seed去重与共同集合；DB/训练外调用/数据写入poison。共用数值核提取须证明旧loss固定输入输出未变。只跑直接最小矩阵，广域剩余矩阵交CI，不盲跑全部历史测试。

### Design Acceptance Index

F-011：D1/D3/D5目标、公平比较和资源终态；F-012：D2/D4固定文件、时间/身份及完整分母；F-013：D6单包交付和跨owner边界。

### Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | backend/services/hmm_risk/risk_l2_member_return.py；risk_l2_loss_severity.py | test: backend/tests/hmm_risk/test_risk_l2_member_return.py；artifact: F:/Dev/AIstock_runtime/hmm_l2_signed_member_return/20261109/evaluation_v1.json | completed | 无 |
| F-012 | backend/services/hmm_risk/risk_l2_member_return.py：load_request/prepare/validate_q4_control | test: backend/tests/hmm_risk/test_risk_l2_member_return.py；artifact: F:/Dev/AIstock_runtime/hmm_l2_signed_member_return/20261109/preflight_v2.json | completed | 无 |
| F-013 | scripts/hmm_risk/run_risk_l2_member_return.py：prepare/preflight/fit/predict/evaluate | artifact: F:/Dev/AIstock_runtime/hmm_l2_signed_member_return/20261109/fit_v1/acceptance.json；§11.1 | completed | 无 |

本矩阵的completed表示批准研究包已经完整执行，经济结果触发停止；无实现缺口不等于有经济价值。三vintage预检、10-fit、两process预测和评价依次封存；§11.1记录真实文件结果，非fixture。授权来自§11用户明确批准，F2 validator只检查设计与引用。

## 10. Risks / Production Gates / Rollout / Rollback

模型类和特征没有新增信息，直接有符号目标仍可能不可预测；本设计不是保证通过。C对照更新是隔离数据修订，不是增加可择优候选。收益标签/归因不是实际可成交收益；同input比较不能消除历史回溯分类和多轮研究选择偏差。负结果应停止精确候选，不以再调horizon/alpha/特征或扩展市场模型逃避终态。

本轮源码以actual changed-files分类，不将hmm_risk共享依赖声明为none；独立离线CLI不以在线服务加载为研究前提。production_ddl_gate=noop；production_dml_gate=noop；dependency_gates=noop；dataset_write=false；active_profile_write=false；runtime_activation=false；service_control=false。无生产回滚动作：原资产和消费者保持；新研究输出只能版本化，不覆盖旧文件。工作树cleanup未授权。

## 11. 批准、审修与真实进度

`L2-SIGNED-MEMBER-D1～D6=APPROVED_BY_USER`；2026-11-09用户在P0交付后明确“批准执行下一步”，批准唯一S候选、匹配C及10-fit上限。原P0只授权可行性/设计的历史仍保留。源码交付按AUTO-REVIEW-DELIVERY-001；F2/CI/merge、实际研究和经济采用分别记录。

P0阶段已完成历史去重、Q4 TRAIN代数/共同mask和Q2/Q3旧训练release差异核查，当时未执行新Q2/Q3 prepare、新模型或评价outcome读取。后续已按批准整包执行，当前以§11.1结果为准；原C模型/训练身份不改写，QE/生产动作保持0。设计审修及最终F2结果在本节和PR记录，不另建重复报告。

第一轮作者审核（非独立第三方）：修正Q2/Q3旧C训练release混杂，改为匹配控制；精确复用Q4避免两次无必要fit；区分raw收益/损失范围及新买复权参考，补充scaler仍行等权。F2初验发现矩阵把“设计提案”混写为未来实现验收，已收敛为文档范围DESIGN_COMPLETE_ONLY、具体可读载体证据和单独待批准/未实现声明，未伪造源码或训练通过。

第二轮作者审核（非独立第三方）：明确未复权价格/除权风险与实际账户回报不同；固定共同mask、C正式clip方向、同分分组、NA日期距离HAC、跨seed去重及阶段终态，禁止事后切换队列/指标/标签。48个精确分数算术用例核对U−C、B和正负/大于1收益；五处父蓝图历史区块/原验收矩阵/旧变更历史自动比对保持。两份F2与diff检查通过；这些是设计算术/文档检查，不是新模型业务测试。DESIGN-COMPLIANCE-001逐项：完整包未被简化或冒报实施；NA/误差/经济失败不掩盖；旧模型/消费/数据及他模块不改；新目标/对照/停止规则均明确作为待批准整包，不作为已生效门禁。最终交付HEAD和CI记录于PR。

本轮源码第一轮作者审核：提取共用Ridge数值核但保留旧loss范围/输出字段，S允许负值和大于1；补齐季度TRAIN隔离、Q4实际scaler/mask一致性、fit前独占attempt和同一prepared预算占用。第二轮作者审核：修正两臂IC均值必须使用共同可定义日期，补齐HAC真实session距离、分数ties、全期/季度误差与组收益、原队列跨seed去重/seed仅诊断、预测物理范围及完整142×131矩阵。24项新直接测试、15项旧loss回归通过，含两fresh-process合成数值核一致性；这些合成测试不计正式研究fit，也不冒充真实模型结果。该段保留源码审核时点；后续正式交付/预检/10-fit已完成，见§11.1。

源码交付阶段历史门禁：Ruff/py_compile、模块归属6/6、validation_module_registry_l0（8项）、L0零finding、fresh-process新service/CLI及hmm_risk/health imports通过。canonical actual changed-files为runtime_impact=backend、target_ids=[backend-main]、catalog_error=null；不降级none，未执行线上激活。独立研究CLI使用交付后clean源码，不要求用户为研究记录重启后端。广域HMM矩阵交CI/Nightly，本地不重跑全量。DESIGN-COMPLIANCE-001逐项：源码覆盖完整批准包而非POC/placeholder；必要缺失/身份/复现失败显式停止，无silent fallback；旧模型/默认消费者/共享数据/其他模块不改；参数/预算/资源停止均来自批准合同，无新增经济门禁。该段为源码交付时点；正式研究现已完成，见§11.1。

### 11.1 已执行正式研究及资源终态（2026-11-09）

源码#7170已合入。首次prepare因旧Q4 request指向已清理worktree中的provider文件而在零fit停止；BUG-2206只允许显式同canonical hash定位，保留原request/provider身份，不做fallback。流水线#7175/#7176修正该精确离线reader分类后，修复#7181（e59ad2964b139e43e82c615034b0e2f5d05870de）及close-sync #7182均已合入，BUG关闭。provider canonical SHA仍为494a9b3ff725792592cf668505691e57a000ef10f160015dc3c45c28552565ba；新source_request_v2/request_v2只更新显式locator及对应request身份，缓存/模型/数据集原pins保持。修复的actual runtime_impact=none、target_ids=[]，无需后端重启；原#7170包含共享数值核提取时的backend分类保留为历史，不据此宣称线上激活。

研究固定clean HEAD=8f0a44dad48497ef5ce2cdaa0ded54831e56d684，使用D2同一10月release。三季度TRAIN全部在fit前通过正式file-only构造、校验与读回：

| vintage | 共同训练行 | 共同训练日期 | 合法固定成员端点NA | 原共同mask排除 | 未成熟键 |
|---|---:|---:|---:|---:|---:|
| Q2 | 88,929 | 714 | 4,123 | 482 | 1,310 |
| Q3 | 89,569 | 715 | 3,750 | 346 | 1,310 |
| Q4 | 89,871 | 716 | 3,609 | 316 | 1,310 |

Q4 mask=a774fd8382d5369e2094ba1fb96be716b7d90f6d30414dc56ae847f6ab387937，原C model=706719ca85bbca99fc1e9e8294974cdbc8fe9dcc794ef708f04180c213347f7b及原训练HEAD保持。正式新增attempt与完成fit均为10，未重训Q4 C；双fresh-process模型及完整预测canonical/字节一致。两臂各142日×131=18,602条，全部available；S预测范围−0.02863133653255744..0.08239480679971016，未裁剪。评价共有17,290个可定义L2目标、2个合法固定成员端点NA、1,310个未成熟键；后10个日期未成熟，不删预测、不认作数据缺失或全局失败。

任务结果目录：F:/Dev/AIstock_runtime/hmm_l2_signed_member_return/20261109/。只保留本研究必要结果和冻结载体：

| 结果文件 | canonical receipt / 文件身份 |
|---|---|
| prepare_v2/prepared.json | 06d3779b02a8fa295827f6d0e396c5f95c99f0ec7b1ed31ba414811b5f6bc82a |
| preflight_v2.json | f0e9ce098ca17791bd6aa6be4715204139b3aed97cb1bfd163cdf44886ab71e5，PASS |
| fit_v1/acceptance.json | a20f6b3e5db867ba78775b56c2ad6acb1d6adfd5a06550c44ea03a2b534bf95d，10/10 |
| predict_v1/acceptance.json及process_1.json/process_2.json | 完整预测canonical=d58daf75098914efcf03a59aa2db5e60352040b5c6f5cfcacb79addd36b2c36a；两文件SHA=c3cc99b28f5ff648d587c884d3a30f2d88d97fe62d096a780876865318d61126，均14,024,993字节 |
| evaluation_v1.json | canonical=cfe20a7bd96adf8a138549a01c13c0d18c6a28ec69c96e85d230f502767d9d17；文件SHA=feaf987131950f46ce365ef828f4a9658d4e21313b308cdf0c859726ef51c46b，133,523字节 |

L2主对象按相同132个共同可定义日期比较，C列均为“负C，越高越看好”：

| 区间 | 日期数 | S收益Rank IC | 负C收益Rank IC | S−负C |
|---|---:|---:|---:|---:|
| 全期 | 132 | −0.0341349 | +0.0681971 | −0.1023320 |
| Q2 | 60 | −0.0001093 | −0.1140729 | +0.1139636 |
| Q3 | 65 | −0.0536247 | +0.1853340 | −0.2389586 |
| Q4已成熟部分 | 7 | −0.1448063 | +0.5428121 | −0.6876185 |

全期S的HAC 95%区间=[−0.1658506,+0.0975808]，paired差区间=[−0.4017086,+0.1970445]，都跨零。原固定方向excess_return_Nd背景IC=+0.0295260，只展示，不据此重选模型。Q4仅7个高度重叠成熟日期，区间和高C点估计不构成独立强证据或自动采用依据。

S全期MSE=0.00548205、MAE=0.05498945；各季度TRAIN日期等权均值常数基线分别0.00491039、0.05196805。S误差分别高11.6419%、5.8140%。S高/低20%组的平均10日S为−1.89191%/−1.06541%，高−低=−0.82650个百分点；负C相应高−低=+0.69023个百分点，差=−1.51673个百分点。它们是重叠目标参考，不是实际组合、净值或成本后收益。

固定新买队列保留原rows全部2,053条，去重为1,269个(day,stock)键，重复事实一致；88个未成熟键保留。124个有源记录的日期中，114日形成共同可定义截面。S收益IC=−0.0241277、负C=+0.0688134，差=−0.0929411（HAC 95% [−0.2143851,+0.0285030]）。Q2 S/负C/差为+0.0389103/+0.0157079/+0.0232024，Q3为−0.0917502/+0.1257811/−0.2175314；无Q4队列，不伪造补样本。三个seed分项差分别−0.1654209/−0.0827465/−0.0045314，均为同一市场历史诊断，不当作独立三次确认；不使用secondary_rows或事后选队列。274项独立算术复核验证共同日期均值、真实session lag HAC、手工average-tie队列相关和日等权误差，全部通过。

**取舍**：按D5预先规则，IC_S<=0且主差<=0，终态STOP_NO_INCREMENTAL_ORDERING，结束此冻结20D/季度/10D/Ridge/S目标候选。原因是当前收益排序和误差未显示改进，不是身份、预测分母、自然停牌/未成熟或显著性门禁未过。区间跨零意味着不能宣称统计证明所有收益模型无效，也不能只挑Q2改善或翻转S分数继续寻找通过。无新QE请求、默认替换、生产采用或追加fit；旧C也不因本次相对较好自动晋升。未来研发若继续，应基于明确的新信息或目标适配依据另立单一问题，不能机械重复同一组合。

收尾作者审核两轮：第一轮按真实request/预检/attempt/模型/预测/评价读回对齐当前摘要、任务、Index/Matrix；第二轮逐项核对D1～D6公式及旧历史未改、来源/研究/经济状态分开、单位和共同日期/未成熟/原队列一致。DESIGN-COMPLIANCE-001四项复核：完整批准研究已执行，无简化/placeholder；NA/区间和负结果完整披露，无fallback；旧模型/数据集/消费者及他模块不变；没有新增门禁或第二候选。数据库/数据集/profile写入、QE任务、生产DDL/DML、依赖安装、runtime activation及服务控制均0；工作树未清理。此为研究终态，非经济通过。

### 11.2 零fit失败归因与后继取舍（2026-11-10）

只读§11.1四个正式receipt及其固定TRAIN/feature载体，在分析基线b076ff79e5402d86dff2357a846bcbfb21cb20ec完成已冻结参数的代数回读，未新增模型拟合或原始outcome。拟合/网络/数据库调用poison后运行；先验证输入canonical，再复算分数与正式预测逐键一致（绝对容差1e-14）。日期等权分解总方差/特征协方差，另用行权重总矩及手工tie-rank路径完成2,340项算术复核。不是独立第三方，也不是新的统计显著性检验。

结果F:/Dev/AIstock_runtime/hmm_l2_signed_member_return/20261110/cause_v1.json，canonical=`7212d9b6f70d758f67adf6e2cbef68f2f32685d912b5465c81fca02c557917fc`，文件SHA=`36a88ed26f0c5c35560f92f8ec45e842b3d9c9417203ef119b57694e50ee1b4b`，72,402字节。只保留紧凑结果，不重建历史证据或复制逐行标签。

| TRAIN vintage | 拟合内日均Rank IC | 原始总R² | 同日去均值R² | 日期间目标方差占比 | 日期间预测方差占比 |
|---|---:|---:|---:|---:|---:|
| Q2，714日 | +0.0532673 | +0.0253860 | −0.0008969 | 46.0779% | 65.7396% |
| Q3，715日 | +0.0614804 | +0.0210456 | +0.0051410 | 46.1653% | 58.4648% |
| Q4，716日 | +0.0516974 | +0.0104038 | −0.0019699 | 44.1763% | 47.8843% |

这些IC为拟合内，不算预测能力；同日R²相对同日真实均值的0残差基线，不能和§11.1绝对收益MSE基线混用。总损失确实分为日内与日期均值两部分；原S全局拟合可以靠日期间部分降低损失，而产品消费是同日排序。Q2/Q4的`sf_intraday_range_5d_neg`同日目标协方差分别+0.0006042/+0.0002050，日期间分别−0.0046559/−0.0025901；Q3两部分同为负，不能声称全面方向冲突。该特征对冻结预测的日内score方差协方差份额Q2/Q3约67.90%/130.50%；大于100%源于与其他贡献相互抵消，不是独立重要性或经济因果。负向range特征配负coef，使较大区间幅度关联较高S分数；不能由此自动删特征或翻转分数。

将各季度系数还原原单位后映射到固定Q2尺度，cosine Q2/Q3=0.962806、Q3/Q4=0.914291、Q2/Q4=0.823715。存在参数/分布差异，但未进行可识别的机制漂移检验，不能诊断“关系时变已证实”。旧标签载体与封存预测的有限重叠仅用于核验：Q2 7,860行/60日、Q3 7,203行/55日、Q4无重叠；Q3少最后10日，不替代原全65日或正式132日结果，未用Q4标签回调拟合早期模型。

**取舍**：支持“原始平方损失与同日排序用途存在可检验的目标适配问题”，不证明这是唯一根因，更不证明中心化可创造信息。未复权与真实新买复权口径差异仍披露，但本轮未量化其占比，不将其称为已证实主因。下一步仅提出[同日目标对齐W详细设计](hmm_evolution_phase2_l2_within_date_member_return_detailed_design_20261110.md)：同标签/20D/季度/scaler/Ridge下隔离日内损失，6-fit须另批；不是新HMM或恢复旧日中位数路线。W如不批准则停止当前扩展；如执行后仍不足，则结束本20D/Ridge目标变换路线，不再换中心方法、seed或alpha。原S终态/10-fit/资产不变，本轮新增fit=0、QE=0、DB/数据集/profile/服务动作=0。
