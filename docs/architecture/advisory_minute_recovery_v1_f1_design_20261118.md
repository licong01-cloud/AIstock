# 已有分钟回撤修复输入与跨期排序研究 F1

日期：2026-11-18；范围：Advisory / model_first，P0～P4离线输入能力。
依据：[蓝图§16.0](advisory_strategy_conditioned_model_blueprint_v1_20260710.md)及[长任务§11](../analysis/advisory_lstm_next_research_plan_20261107.md#minute-path-long-task)。本设计先合入，再实施P4；P5金融训练授权仍0。

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

精确tracked写范围：本F1、原蓝图、原接续计划；`backend/services/advisory_model_first/minute_recovery_v1.py`（纯测量/列合同）；`minute_recovery_inputs_v1.py`（冻结来源、D切片与阶段）；`backend/tests/advisory_model_first/test_minute_recovery_v1.py`；`scripts/advisory_minute_recovery.py`（显式离线CLI）。共7文件，新增范围先更新任务记录。

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

## 7. Design Acceptance Matrix

P3只验收详细设计与既有P1/P2依据；以下`DESIGN_VERIFIED_P4_NOT_RUN`不表示源码、全人口数值或P5已完成。P4后在本表更新实际测试/工件引用，不另建验收文档。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-981 | §2/§4，计划minute_recovery_inputs_v1.py | artifact: X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/p1_receipt.json | DESIGN_VERIFIED_P4_NOT_RUN | none |
| F-982 | §3，计划minute_recovery_v1.py | artifact: X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/measurement_spec.json | DESIGN_VERIFIED_P4_NOT_RUN | none |
| F-983 | §3，计划两个新叶 | artifact: X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/p2_receipt.json | DESIGN_VERIFIED_P4_NOT_RUN | none |
| F-984 | §4，计划冻结D切片与预算 | artifact: X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/p1_receipt.json | DESIGN_VERIFIED_P4_NOT_RUN | none |
| F-985 | §4，计划CLI与原publisher | artifact: docs/architecture/advisory_minute_recovery_v1_f1_design_20261118.md | DESIGN_VERIFIED_P4_NOT_RUN | none |
| F-986 | §5/§8，计划全人口、第二实现与源码审核 | artifact: X:/AIstock_temp/ADVISORY/minute-recovery-20261118-v1/task.json | DESIGN_VERIFIED_P4_NOT_RUN | none |

## 8. Verification Plan / Design compliance / Production gates

最小直接合同：已恢复/未恢复/无回撤/最后槽谷值、平局事件、缺分钟/午休/可选锚、Inf/非正价、旧摘要反例；冻结身份/重复KEY/原rank、D/T未来污染、错lease/截断源/运行中漂移、输出拒绝覆盖、预算、exact retry与改代码拒绝。真实输出全26,200行由不调用主测量的第二实现逐行重算；状态/原身份严格一致，有限数值rtol1e-12/atol1e-10，NaN位置一致。

源码只做直接测试、changed-file lint、F1/DAI、diff/scope和当前HEAD CI；不跑无关模型套件或靠mock冒充真实输入。设计与实现各至少三轮当前Agent自审，记录在同一任务/对应PR，不称独立外审；第二算术验证也不等于独立经济样本。

DESIGN-COMPLIANCE-001逐项：不把零fit输入工程当训练模型；不吞异常或用0补未知；不改Top50→Top10/5TD/费用/100bps/原生政策；不增生产审批或恢复暂停方向。生产DDL/DML、服务/启动脚本/流水线、依赖、模型激活、通知均noop。无需后端重启。未授权cleanup，旧源/工件/工作树保留。

## 9. Non-goals

P0-P4 excludes financial labels, fitted models, performance screening, new datasets, production runtime integration, service operations, DB writes and notifications. P5 is a separately authorized proposal, not a deliverable of this task.

## 10. Implementation Plan

1. Complete three design review passes and the F1 validator; merge this design before implementing P4.
2. Implement pure measurement, frozen source reader, immutable stage publication and explicit offline CLI within the seven-file scope.
3. Verify direct numerical, identity, missingness, resource and retry contracts; prepare all 26,200 original slots and independently recalculate the new measurements.
4. Perform three source review passes, repair findings, validate the final acceptance matrix, and merge with required checks green. Record exact artifacts and source SHAs in task.json.

## 11. Risks / failure modes

Historical predictions and reconstructed prices do not establish historical PIT availability. Distinct path measurements do not establish alpha. Optional auction clocks can leave old control columns missing; these remain missing, while the new core-clock measurements use the frozen 240-slot definition. Resource exhaustion or source drift fails the stage; incomplete observations remain explicit UNKNOWN rows. The future matched comparison still changes model capacity and uses previously consumed research history; neither a positive input audit nor a future exploratory result establishes independent confirmation.
