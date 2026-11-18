# 已有分钟回撤修复输入与跨期排序研究 F1

日期：2026-11-18；范围：Advisory / model_first，P0～P4离线输入及P5～P7单次配对实验。
依据：[蓝图§16.0](advisory_strategy_conditioned_model_blueprint_v1_20260710.md)及[长任务§11](../analysis/advisory_lstm_next_research_plan_20261107.md#minute-path-long-task)。P0～P4已合入；用户随后“开始执行下一步任务”承接此前明确的九折两臂最多18fit提案，具体执行补充见§13。§1～§12中的零训练状态保留为P4交付时的事实，当前授权与实耗只以§13及task.json为准。

## 1. Background / Goal / Feature Card

原生Top50已经确定；本轮检验D日回撤修复路径能否形成区别于旧日频/分钟摘要的可消费输入，不改变选股池。最终问题是固定5TD的Top10净增量，本次仅完成输入、程序及可恢复工程工件，不声称Alpha成立。

首个对象固定LSTM包`pkg_cc7eccb6202e4816b9d7c7d6b748bd37`、manifest=`93a7fb7c5c0745c96d2d97376db3c669e527256666ed1e014a710c38799a7dc6`。TCN只作既有来源盘点，不训练、不按收益择包。原生产Top20、原生分数/排名、V2终态、旧269fit＋1旧index及负面结论不变。

## 2. P0/P1/P2事实、输入与时钟

权威任务根：`X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1`。`task.json`记录授权、源码和精确范围；`p1_receipt.json`、`p2_receipt.json`、`measurement_spec.json`及对应脚本/原始证据记录本次真实操作，非复制旧经济结果。

- 父研究：`X:/AIstock_temp/ADVISORY/multiperiod-top10-5td-20261021/study/advmultitop10_6c632adf532818cfcd059113`，其`prepared/inputs.parquet` SHA256=`c8d6ed3a1bc59aa365a15b0abcefb5b99cc19f84726337744ca048fa34f7a08b`。原52,400行中只按包投影26,200行/524D/2,057股，不重新生成Selection或Top50。
- 原binding=`589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`，release=`qe_hmm_full_v2_20260831`，generation=`20261015-v18-unified-quote1`。显式绑定原文件，绝不回退active profile；manifest、日历及分钟元信息实际hash匹配。
- P1只读取身份/原分数、日历及4字节bin头；2024-07-04～2026-08-28所有524D、26,200槽的10,285个OHLCV文件均符合原stat lease和物理范围。371D日历241槽、153D为240槽，全部包含09:31～11:30、13:01～15:00的240个核心时钟。物理覆盖不等于数值非缺失，完整数值验收留在P4。
- P2固定合成路径交换同一15分钟块内两次回撤的低位停留长度，53项旧输出相同（数值atol=1e-10/rtol=1e-12、同NaN），恢复耗时为6/239与2/239。保持比例在该反例相同，不虚构每个新坐标都已被分别证明独立。真实固定样本D为2024-07-04、2025-10-09、2026-08-28，各50槽，108未恢复/41已恢复/1无回撤；只读144,400分钟close字节，无未来收益/排名/fit。
- 测量规范物理SHA=`c054ea17d5103e20a909f09b31c54ec2aa89c6855c11b1c4c4d5dc02b6b06b89`，在样本读取前写入。首个合成断言误要求保持比例也不同，在历史样本读取前失败；仅修正为信息块存在差异，原规范未改，失败记录保留。

该反例仅排除与已核对旧摘要完全同构，不证明经济独立、全库创新或分钟信号必然有效。旧16段、量价压力、序列相关和P9同步性结果直接引用，不重做收益研究；P9同群关系并非本轮新依赖。

输入仍为`NON_VINTAGE / RECONSTRUCTED_EXPLORATION`：原父预测创建于2026年，历史原生运行资格、逐D首次可得时间和完整父训练链样本外性未新证明。保留原身份与来源，不能由本轮顺序正确推导整个父模型链无泄漏。D收盘可见输入只允许在下一session T使用，目标H为T+4；本次不读取T～H价/收益或正式V2结果，也不复算日频源字段。

## 3. Contracts / 精确测量、对照和缺失

只用D的核心240个分钟close，排除可选09:30/13:00锚；午休不计入交易分钟耗时。输入必须单D、无时区、严格递增无重复、无盘外时间；核心槽缺少或任一核心close为NaN时，返回`UNKNOWN_INCOMPLETE_CLOSE_PATH`，全部测量/状态编码为NaN，不压缩、插值或前填。已知Inf、非正close、形状或时钟错误直接失败，不能变成正常UNKNOWN。

设核心close为c[0..239]，p[i]=max(c[0..i])，q为`1-c[i]/p[i]`最大值首次出现的索引，p=p[q]，v=c[q]。选择最早最大相对回撤，而非事后收益最大事件，不推断分钟内high/low先后。三坐标：

| 坐标 | 唯一定义 |
| --- | --- |
| `recovery_delay_fraction` | 第一个r>q且c[r]≥p时为(r−q)/239；到收盘未恢复时为(239−q)/239，是已观察等待下界，并携带未恢复标志，不能解释为已经恢复 |
| `recovery_hold_fraction` | q之后核心close≥p的比例；q=239时定义0，表示无剩余观察时段；它不是实际持仓胜率 |
| `unrecovered_fraction` | max(0,(p−c[239])/(p−v))；表征末端未修复程度，不能转成买卖价格 |

同时输出匹配控制`event_drawdown_bps=10000*(1-v/p)`、`trough_clock_fraction=q/239`，以及候选块的两个确定性状态位`is_no_drawdown/is_unrecovered`。最大回撤为0时状态`NO_DRAWDOWN`，三坐标定义0/1/0、两个控制0/0、状态位1/0；这是完整已知无事件，不是缺失填零。其他完整路径为`RECOVERED`或`UNRECOVERED`。状态字符串、真实覆盖及已知坐标与源缺失分开。

匹配B为原8个日频/父排序字段＋旧43个单股分钟字段＋D1两个量价条件坐标＋上述2个控制，共55数值字段；B+F另加3修复坐标及2状态位，共60字段。列顺序在源码常量固定。P4只产原始观测，不fit中位数/标准化；未来两臂同训练KEY、标签、支持和更新日程，train-only编码及原生主比较必须保持。增加坐标仍改变模型容量，B/B+F不是完全无混杂的机制因果试验。

原KEY为`package_id/decision_as_of_trade_date/target_trade_date/instrument`，每D保留原rank1～50、parent_score及行序。支持标志只表示路径已知且旧八字段有可用输入；其它控制缺失原样保留，未来训练编码另依固定合同。不删除未知股票/日期，不把未知槽让给第51名，不生成新排序。训练支持与经济共同完整D是不同分母。

## 4. Scope / Architecture / P4精确实现与资源

精确tracked写范围：本F1、原蓝图、原接续计划；`backend/services/advisory_model_first/minute_recovery_v1.py`（纯测量/列合同）；`minute_recovery_inputs_v1.py`（冻结来源、D切片与阶段）；`backend/tests/advisory_model_first/test_minute_recovery_v1.py`；`scripts/advisory_minute_recovery.py`（显式离线CLI）；`scripts/ci_change_classifier.py`（仅将新CLI映射到既有荐股测试通道）。共8文件。后者是P4验收发现的必要CI登记，已先记入task.json，不增加生产门禁或研究方向。

复用FrozenDataset及原stat lease/metadata pins、旧分钟纯聚合、D1纯计算和`economic_entry_pipeline.publish_stage/read_stage`，不修改公共叶。只允许`preregistered/prepared`两个阶段，记录本地源码闭包及输入内容身份；没有train/evaluate子命令，也不调用原study的prepare/run。复用publisher仅作原子写入，不调用其标签/训练入口。

CLI读取已存在父study，核对原plan/manifest链及输入sha后按显式列投影LSTM；保留原18列，拒绝外来包/manifest、重复KEY、重复rank、缺D/缺槽和D/T不一致。minute读取限原5字段OHLCV，逐股开文件，每块最多20D，单D≤300槽，总原D≤524/行≤26,200；只seek原D切片，不解码其后的行情。

资源上限CPU2线程、RSS2GiB、总分钟读取≤200MB、一次阶段≤1800秒。2,057股×5文件的头与每个D切片形成摘要，记录缺源/覆盖、实际bytes/耗时/内存；按输入规模估算所需约126MB。源数据前后stat/pin和消费切片身份核验，不以整bin内容hash冒充历史PIT。资源不足直接保留失败，不控制其它任务，不以QE running作为否决。

输出为一个新root下的不可变协议和prepared：`inputs.parquet`（完整原身份及两臂原始特征）、`daily_support.parquet`（完整D覆盖）、`receipt.json`及必要source读取索引。新进程重试必须核对协议、代码闭包、源身份及原stage全部hash，只返回同一已发布stage；不同参数/代码/源不能覆写或静默重算。目标必须是显式新目录，不得位于父源内或覆盖输入/既有研究。

本次没有UI/API/DB或调度器接入。未来产品若获准接入，后台按包/D一次计算并存聚合，页面只读结果；不能将分钟读取挂在主页加载或滚动上。本轮不操作服务、数据采集、数据集切换、依赖、模型绑定或生产名单。

## 5. P5后续实验提案：本次不执行

九块时间与截止保持接续计划§11.5：2025年4～5、6～7、8～9、10～11、12～2026年1月、2～3、4～5、6～7、8月1～28。各折训练从2024-07-04开始，截止为块首前一日按官方交易日历投影，且H≤截止。固定同参数Ridge(alpha100,lsqr,tol1e-6,max_iter2000,fit_intercept=False)，每折两臂最多18次物理fit，无最终重训/额外seed或调参；现在预算仍0。P4不实现/运行金融训练及评价入口，P5接续需明确实施范围和覆盖这18次的授权。

未来训练数值编码为各列训练中位数加missing bit，训练内IQR缩放（0且非恒定则训练std，否则1），按D中心化；候选新增块只用训练参数，两臂原55字段同编码。目标沿原T开盘到T+4收盘扣买0.95/卖5.95bps后的十槽业务收益，原生Top10是主比较，B是增量解释。模型只在同日支持槽中重排并保留未知槽原rank，稳定破同分；不改变原生产Top20。

所有预定预测先冻结后汇总经济结果；后折可用前段已成熟标签作预声明训练更新，不能据前折收益换方案。开发2025-04～09和扩展2025-10～2026-08均报告全部原D及各自共同完整成熟D；评价末端H超出原2026-08-28价格消费范围仍IMMATURE，不读取旧sealed范围补成熟。

事前研究判定建议沿原目标：两报告段点增量各≥100bps、两段主95%日期块区间下界>0、完整成熟配对覆盖≥80%、干预≥20D且≥20%成熟D、最差10%批次均值恶化≤100bps、额外UNKNOWN≤2百分点才支持讨论新确认；主增量≤0或上述尾部/UNKNOWN风险失败关闭精确候选，其余证据不足。10D circular block、10,000次、seed20261118，保持原日期轴及缺洞，主/辅各自配对，不以多重事后挑季度替代总目标。它们是未来实验提案，不追溯改判旧研究，也不作为原生荐股运行门禁。F1完成P4后的训练授权需明确接受该提案，不用本次零fit工程授权替代。

上述全是已消费历史探索，不能把九折视为九个独立验证。新确认只能另做隔离性/捕获/一次判断设计；原V2终态不重开，当前没有经济确认窗口已验收或可生产模型。

## 6. Design Acceptance Index

| design_item | requirement |
| --- | --- |
| F-981 | 原524D/26,200行、冻结manifest/输入/元信息/lease身份与逐日覆盖，零标签/SQL/fit |
| F-982 | 核心240时钟、确定性事件/三坐标/已知状态及真实UNKNOWN；非有限与非法时钟显式失败 |
| F-983 | 55/60字段匹配合同、完整原rank/行序/槽与数值前冻结；查重不冒充Alpha |
| F-984 | 有界D切片、2GiB/2线程/200MB预算、源不变及真实读取证据 |
| F-985 | 原子不可变阶段、新进程同输入复用、源/代码漂移拒绝，无金融执行入口 |
| F-986 | 真实全人口特征、第二算术路径、故障合同、多轮审核/源码交付与P5零授权分态 |
| F-987 | 九折同KEY/目标/编码/支持、train-only参数、原rank保留及最多18物理fit |
| F-988 | D/H投影后解码金融标签，修复旧窗口尾端IMMATURE，全部预测冻结后一次评价 |
| F-989 | 完整D轴配对、扣费收益、分段/季度/折/风险/100bps判定及第二算术 |
| F-990 | 不可变协议/阶段、STARTED/FINISHED恢复、源码身份/资源/自审/CI及生产分态 |

## 7. Design Acceptance Matrix

P3设计已由PR #7342合入；下表为P4真实输入及工程验收，源码CI/合入状态另记task.json和对应PR。全部引用绑定§12的同一prepared stage；不表示P5模型或经济效果已完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-981 | backend/services/advisory_model_first/minute_recovery_inputs_v1.py:load_source/validate_population | artifact: X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/study/prepared/receipt.json | VERIFIED_P4_INPUTS | none |
| F-982 | backend/services/advisory_model_first/minute_recovery_v1.py:recovery_features | artifact: X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/p4_independent_receipt.json | VERIFIED_P4_INPUTS | none |
| F-983 | backend/services/advisory_model_first/minute_recovery_inputs_v1.py:attach | artifact: X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/p4_independent_receipt.json | VERIFIED_P4_INPUTS | none |
| F-984 | backend/services/advisory_model_first/minute_recovery_inputs_v1.py:Budget/read_minutes | artifact: X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/study/prepared/receipt.json | VERIFIED_P4_INPUTS | none |
| F-985 | scripts/advisory_minute_recovery.py; minute_recovery_inputs_v1.py:prepare | artifact: X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/task.json | VERIFIED_P4_INPUTS | none |
| F-986 | backend/tests/advisory_model_first/test_minute_recovery_v1.py; §12 | artifact: X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/task.json | VERIFIED_P4_INPUTS | none |
| F-987 | minute_recovery_experiment_v1.py:train/predict; §13 | artifact: X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/p5_independent_receipt.json | VERIFIED_EXPERIMENT_CLOSED_NEGATIVE | none |
| F-988 | minute_recovery_pipeline_v1.py:FinanceReader/forecasts; §13 | artifact: X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/experiment/forecasts/trained/manifest.json | VERIFIED_EXPERIMENT_CLOSED_NEGATIVE | none |
| F-989 | minute_recovery_experiment_v1.py:comparison/summarize; §13 | artifact: X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/experiment/evaluated/report.json | VERIFIED_EXPERIMENT_CLOSED_NEGATIVE | none |
| F-990 | minute_recovery_pipeline_v1.py:Study/run; §13 | artifact: X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/task.json | VERIFIED_EXPERIMENT_CLOSED_NEGATIVE | none |

## 8. Verification Plan / Design compliance / Production gates

最小直接合同：已恢复/未恢复/无回撤/最后槽谷值、平局事件、缺分钟/午休/可选锚、Inf/非正价、旧摘要反例；冻结身份/重复KEY/原rank、D/T未来污染、错lease/截断源/运行中漂移、输出拒绝覆盖、预算、exact retry与改代码拒绝。真实输出全26,200行由不调用主测量的第二实现逐行重算；状态/原身份严格一致，有限数值rtol1e-12/atol1e-10，NaN位置一致。

源码只做直接测试、changed-file lint、F1/DAI、diff/scope和当前HEAD CI；不跑无关模型套件或靠mock冒充真实输入。设计与实现各至少三轮当前Agent自审，记录在同一任务/对应PR，不称独立外审；第二算术验证也不等于独立经济样本。

DESIGN-COMPLIANCE-001逐项：不把零fit输入工程当训练模型；不吞异常或用0补未知；不改Top50→Top10/5TD/费用/100bps/原生政策；不增生产审批或恢复暂停方向。生产DDL/DML、服务/启动脚本/流水线、依赖、模型激活、通知均noop。无需后端重启。未授权cleanup，旧源/工件/工作树保留。

## 9. Non-goals

P0～P4不含金融标签、模型训练、收益筛选、新数据集、生产接入、服务操作、DB写入或通知。P5仍是另需具体授权的后续提案。

## 10. Implementation Plan

1. 完成三轮设计审核和F1检查，先合入设计再实现P4（PR #7342已完成）。
2. 按§4实现纯测量、冻结读取、不可变阶段和离线CLI。
3. 验证数值、身份、缺失、资源和恢复合同；提取全部26,200槽，由第二实现复核。
4. 三轮源码审核修复、验收矩阵及必需CI通过后合入；精确工件与源码SHA写入task.json。

## 11. Risks / failure modes

历史预测和重建行情不证明历史PIT；不同路径测量不证明Alpha。可选竞价锚缺失可使旧控制列缺失，保留NaN；新测量坚持240核心槽定义。资源超限或源漂移直接失败，观察不完整保留UNKNOWN。未来匹配比较仍改变模型容量，并使用已消费历史，不能冒充独立确认。源码静态导入闭包采取保守校验；其他依赖修改也会拒绝恢复，必须使用记录中的冻结源码树，不能改工件身份迁就当前main。

## 12. P4真实验收与恢复入口（2026-11-18）

工件根`X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/study`，prepared stage SHA256=`89a1754f5fd09c4161316f5e8120c892a8ce1bc60271eecce0fcc7dba17111b9`。完整2024-07-04～2026-08-28的524D/26,200槽及原18列顺序、dtype、分数、排名均保留，输出约15.54MB。

- 可测量26,159槽（99.84%）：7,225已恢复、18,861未恢复、73无回撤。41个UNKNOWN分布于40D，其中28槽全部核心close未知、13槽缺一个核心close；不丢槽、不填0。484D的50槽路径完整（92.37%），这是输入分母，不是成熟收益日期数。
- 原旧控制列仍可能缺失：首段收益14,005槽缺失，16段成交量占比各6,390槽缺失，具体每列分母在receipt中。未用全样本统计填补；P5若获准必须按§5的train-only合同处理。
- 提取174.20秒、分钟读取126,172,140字节、全过程采样峰值RSS388,583,424字节、数值线程上限2。独立算术读取25,234,428字节、50.98秒，对全部状态和183,113个有限值验证，最大差异0、NaN一致，close消费片段hash匹配。
- 新进程精确恢复24.67秒，返回`EXACT_ARTIFACT_RETRY_VERIFIED`及同一stage SHA，未重新计算特征。31个直接测试覆盖测量、身份、未来价格/金融标签毒值、源变化、内容篡改、预算、发布恢复及CLI/CI路由；实际分次验证记录和当前HEAD CI保存在task.json/PR。

恢复使用`F:/Dev/AIstock_worktrees/advisory-minute-recovery-source-20261118`的冻结源码（精确commit见task.json）；CLI为`python scripts/advisory_minute_recovery.py --source-study X:/AIstock_temp/ADVISORY/multiperiod-top10-5td-20261021/study/advmultitop10_6c632adf532818cfcd059113 --output-root X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/study`。读取已有输入无需重启服务。P0～P4新增金融fit=0，历史累计269＋1旧index不变；P5预算仍0，没有可激活模型或100bps收益结论。

## 13. P5～P7执行补充：单一18fit配对实验

2026-11-18用户在上述P4交付及明确的18fit提案后指令继续下一步；本轮执行该单一实验、一次评价与交付，不追加候选、调参、seed、最终重训或确认窗口。既有task.json的p5节点记录授权、实际fit及源码/工件SHA，不覆写P4记录。当前执行根为`X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/experiment`；若执行前发生技术失败，原目录及失败原因保留，新协议另记精确恢复根，不能覆盖或赠送fit。

本轮tracked范围8文件：本F1、原接续计划、原蓝图；新增`backend/services/advisory_model_first/minute_recovery_experiment_v1.py`、`minute_recovery_pipeline_v1.py`、`backend/tests/advisory_model_first/test_minute_recovery_experiment_v1.py`、`scripts/advisory_minute_recovery_experiment.py`；既有`scripts/ci_change_classifier.py`只增加该CLI到已有荐股通道的映射。P4特征代码、输入和源码封存不改。

- 协议：保持§5的九个两月块、Ridge和55/60字段，截止日期按原官方日历投影到块首D之前最后交易日。共同训练KEY为该截止前D且第五session H已成熟、金融标签可用或明确CASH、P4共同支持成立的全部槽。按支持槽内D中心化，目标同样中心化；每D总样本权重相同并归一到均值1。整列训练缺失则两臂共同停用该基础列；其它列按训练中位数/缺失位/IQR→std→1编码，逐字段验证两臂基础参数一致。未知槽预测NaN、原rank保留，稳定按预测值/原rank破同分。
- 标签：复用原研究prepared/train_finance与evaluated/finance，两文件和manifest在协议冻结前只核对身份。每次解码先按包、D范围和H≤该折截止过滤；H与原官方D+5核对。旧文件中已经成熟但仍标IMMATURE的行，使用既有`finance_inputs`、同一冻结binding和原估值叶补算，仅读取到当前截止的日频/限制/停牌信息；不替换已确定的原标签、不把UNKNOWN改现金，不读2026-08-28之后价格。评价阶段才解码截至该日的完整评价标签。旧窗口尾端缺标签不能静默丢掉。
- 阶段：每折训练金融输入prepared、两臂各一个trained模型及预测，全部18个完成后发布forecasts/trained；再发布评价金融输入和evaluated。每次真正Ridge.fit之前独占写STARTED，完成阶段后写FINISHED；已开始未发布拒绝自动重训，已发布重试只验收/恢复同工件。无额外final模型。协议绑定P4 stage、金融文件、原binding、当前源commit、源码静态闭包和环境版本，变化拒绝续跑。
- 一次评价：原生Top10为主、B为辅，开发2025-04～09、扩展2025-10～2026-08分别报告，并汇总原D/成熟D/三臂共同完整D、费用、干预、UNKNOWN、最差10%尾部、季度/折和换入换出收益分解。三臂共用完整D仅用于配对，不掩盖各自未知槽。10D circular block、10000次、seed20261118，保留每段原日期轴/缺洞；按§5判定，两段均达100bps且区间/覆盖/风险通过才支持新确认设计，否则负面关闭或证据不足。它们均非生产模型验收。
- 性能与验证：同一进程2数值线程、采样RSS≤2GiB、每次执行≤1800秒、总输出≤200MiB；不重新读取全窗口分钟数据。直接测试覆盖D/H未来毒值、CASH/UNKNOWN、相同训练编码、18次预算、预测先冻结、恢复不fit及原日期轴。真实执行后第二实现复算预测/排名、净费用、分母、收益分解、置信区间及判定；源码至少三轮当前Agent审核，F1/CI/合入与研究经济结论分开。生产DB、服务、通知、模型激活及清理仍不包含。

协议与代码在读取本轮经济结果前冻结为`a4b776e81be3d6e9aff73d694e6e2ddd6e519c83`。实际结果见下节；源码交付最终commit/CI/合入身份另记task.json，后续仅更新文档，不改变该执行源码或结果。

## 14. P5～P7真实终态（2026-11-18）

终态为`CLOSED_NEGATIVE_EXACT_CANDIDATE`，evaluated stage SHA256=`2e0185009b27d4cb4cbf349bb415e08930a28ed1a8f11f99eefe26e7d476fcf7`。18次物理fit均有STARTED/FINISHED及不可变模型/预测，无最终重训，旧269＋本轮18＝累计287金融fit，另1旧index，剩余预算0。该精确修复块候选关闭，不追加seed/模型或改名复活；不能从这一结果推导所有分钟信息均无用。

| 报告段 | 原D / 成熟D / 三臂完整D | B+F对原生净增量bps | 95%日期块区间bps | B+F对B净增量bps |
| --- | --- | --- | --- | --- |
| 开发2025-04～09 | 126 / 126 / 116 | +10.689743 | [-51.311415, +71.871194] | -4.837152 |
| 扩展2025-10～2026-08 | 219 / 214 / 206 | -48.006355 | [-122.118115, +22.430745] | -2.440663 |
| 全部 | 345 / 340 / 322 | -26.861177 | [-80.499904, +24.086365] | -3.303994 |

按三臂共同完整322D，原生、B、B+F的5TD扣费批次均值分别+117.121351、+93.564169、+90.260175bps；它们是重叠批次均值，不能当累计账户收益或独立样本。B+F绝对均值为正也不满足业务主目标，扩展段相对原生为负，且两段均未达到100bps。覆盖分别92.06%/96.26%；最差10%尾部变化+49.74/-19.67bps、额外UNKNOWN为负，风险指标未单独触发停止，关闭来自原定增量规则。2026Q3相对原生-300.34bps也保留在季度报告中，不据此事后缩短区间。

旧2025-09窗口末端250槽在后折按同一估值叶恢复已成熟标签，不能把这个修复误记为候选增益。窗口结束的2026-08后5个D共250槽仍IMMATURE；其余65个评价金融UNKNOWN、50个明确CASH均保留。原41个分钟UNKNOWN槽不丢弃，未知槽仍保留原rank。

真实执行52.31秒，数值线程2，峰值采样RSS402,595,840字节，输出约6.53MB，没有重新读取分钟数组。第二实现复核18个模型的训练编码、34,418个有限分数（最大差1.71e-13bps）、全部排名（差异0）、16,885条可用费用换算、50条现金、345D批次、所有窗口/季度/折的收益分解及10000次区间，结论一致。新进程恢复17.40秒，同一stage SHA、没有FIT_FINISHED事件或新增claim；18次预算没有重复消耗。

第二实现首次检查出现两项脚本错误：NumPy float32标量未先转float64、费用换算遗漏D参考坐标到T开盘的observed_gap因子；均在原脚本修正并保留失败记录，实验代码/工件/容差未改、未追加fit。最终算术PASS回执为`p5_independent_receipt.json`。

工程交付保留可复用特征、固定配对训练/预测、成熟度修复、经济评价和恢复能力；18个研究模型均不具备激活资格。旧V2终态不重启，独立确认、生产接入、数据扩容及新方向均未执行。后续只可先复盘业务目标与现成P9等未覆盖问题，任何新候选和真实fit仍需另行具体规划/授权。
