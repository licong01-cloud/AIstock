# Advisory 分钟序列压力 Top20→Top10 F1 详细设计 v1.0

日期：2026-10-21。假设 `SERIAL_PRICE_PRESSURE_TOP10_5TD_1`。
本次交付是**查重、训练端文件可产性和详细设计**，不是模型源码、拟合、收益结果或启用。
状态 `DESIGN_REVIEW_PASS / TRAIN_CLOSE_INPUT_AUDITED / NOT_IMPLEMENTED / NOT_STARTED`。
目标合同 `ALPHA_RANKING`；拟登记 `LEARNABILITY_AUDIT / NAVIGATION_ONLY`。
现行方向与进度以[蓝图§16.0](advisory_strategy_conditioned_model_blueprint_v1_20260710.md)为准。

## Background / 单一业务问题及查重

问题：在原生合格Top20内，**分钟价格的延续/反转结构及其与收盘压力的关系**，能否在原父分数、
日频量和既有分钟汇总之外，改善固定五交易日Top10名单的成本后收益？不是用微观结构替代QE Alpha，
不是再换协方差估计器，也不将更小预测误差直接当作收益。

旧GP5-MINUTE已用开尾盘收益、RV、方向效率、VWAP距离、成交额份额和涨跌停压力；
GP5-VOLUME已有加权bar收盘价、signed-volume压力和尾盘量份额；GP5-ORDERED已有16段收益与量份额。
旧return-volume的18对跨日量价滞后也已存在。它们不是本次待重跑项目；更换bin数、阈值、seed或loss不构成新信息。

已只读解析两原生包50份冻结动态因子代码：LSTM26份46,165bytes、TCN24份50,632bytes，
未发现minute/1min来源，输入声明为daily_pv/daily_basic/sector H5；20个Alpha158使用日频OHLCV。
这是已检查代码的输入证据，不声称完整外部依赖都经运行验收或与父信号统计独立。

构造对照把同一个15分钟段内三次回到原价的涨跌次序由(a,b,c)改成(a,c,b)，其它bar不变。
真实调用旧聚合函数，日OHLCV/amount及**43个旧分钟特征**在1e-8内相同，非零收益比例也相同；
相邻分钟收益相关从−0.7857142857变为−0.8214285714。因此新统计不是这些汇总的确定性重编码。
这是功能非冗余证据，**不是条件独立、五日Alpha或盈利证明**。
第三轮再用可精确表示的float32价格101/102/103复核：43旧量最大差为0，日OHLCV/amount完全相同，
S仍为−0.7866138324与−0.8221764675。没有再次读行情或收益，不依赖浮点微差伪造新信息。

经济动机仅为可证伪假设：[Roll原论文](https://onlinelibrary.wiley.com/doi/abs/10.1111/j.1540-6261.1984.tb03897.x)
讨论特定假设下成交成本与序列协方差；[Aït-Sahalia/Yu](https://www.nber.org/papers/w13825)
研究高频噪声与流动性。A股分钟收盘不等于逐笔成交/盘口，本文不输出Roll点差或真实流动性估计。
序列噪声本身没有方向性收益承诺；预登记与收盘方向的交互，是否延续至T开盘以后五日必须实测。

## Scope / Non-goals

只消费父包原名单，派生独立Top10研究角色，不改原评分、排序、Top20、PIT、UNKNOWN、名单版本或执行资格。
原包直接使用；没有包ID/模型类型/股票池白名单。包与股票池字段只是来源身份，不作为收益特征。
首轮使用既有两包不等于已证明新包/多池经济泛化。

当前写入范围仅本文及蓝图；可产性脚本、计划和收据在X任务目录，非生产运行依赖。
下一源码阶段精确范围预定为：

- `backend/services/advisory_model_first/serial_price_pressure_top10_5td_inputs_v1.py`
- `backend/services/advisory_model_first/serial_price_pressure_top10_5td_study_v1.py`
- `backend/services/advisory_model_first/serial_price_pressure_top10_5td_pipeline_v1.py`
- `backend/tests/advisory_model_first/test_serial_price_pressure_top10_5td_v1.py`
- 本文及蓝图；若需其它文件先更新范围，不顺手改公共模块。

不改QE、StrategyPackage、Selection、LocalSIM、HMM、数据发布或分钟执行代码；不训练父包、补数据、
更改active profile/PIT、写数据库或控制服务。公告仍暂缓，旧联合风险已停止，不重跑/补证。
本模型不输出买卖价区间、胜率概率、资金仓位、现金择时或最优分钟；不能称收益型价格模型完成。
Alpha合同不承诺绝对盈利，Risk-managed合同及生产启用须分别评价。

## Architecture / Input and Clock Contracts

### 1. 原人口、来源和五日标签

只读父run `advgp5parentscore_48e050329b8e910166e52d4c`，原plan SHA
`48e050329b8e910166e52d4c35fe9d72d414961f1faa3a26bb5bc1dd592f5bb6`，prepared SHA
`c6e00780d442939801fcf22515787483b8675c0f71cbb11f0676d6cf7930aeb2`。
30,500原Top50行不覆盖；本研究按原rank≤20投影为12,200行/610包D，原candidate_group_size=50不改写成20。
源两包manifest、run、D/T、原分数dtype及排序完整保留；不重新Selection或推理。

train D=2024-07-04..2025-05-30，evaluation D=2025-06-03..2025-09-30。
训练只允许label_information_end≤train_end；T为紧邻D的交易日，H=T+4，固定五会话、buy0.95/sell5.95bps各一次。
原标签`GENERIC_ENTRY_FIXED_5TD_V1`及valuation policy hash原样验证，不用close/close风险标签替代。
UNKNOWN、不可执行、未成熟及原86评价D完整保留，不延长H或删除末五日；日级cohort不是实际fill/NAV。

使用已冻结v18文件binding
`589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`，不是运行时回退active版本。
全局canonical PIT供当前任务正常消费；既存父名单/历史证据不重建、不改判原生完整性。
源等级继续`QE_FROZEN_BACKTEST_SCORE_CURRENT_DATABASE_NON_VINTAGE_HISTORICAL_UNIVERSE_UNPROVEN`；
不因本次Bin输入通过升级旧native/vintage/OOS身份，也不据此增加包使用门禁。

### 2. D内文件读取与新变量

沿FrozenDataset校验绑定、元数据、bin generation stat lease；一次索引calendar、按股票复用stream，
只seek原股D内实际calendar片段。open/high/low/close/volume一份读入供两臂复用，禁止为每臂/每D复制数据树。
source记录完整元数据hash与实际消费片段hash、stat、字段、日期；stat lease不冒称整个bin内容hash。
不读数据库/active指针、T以后分钟或当前股票池来修补原名单；重试使用原binding和输入身份。

新统计S使用close的log差。只有三个slot严格相邻各一分钟、均为同D同交易连续段且三close有效时，
形成相邻收益对(r_t,r_{t-1})；午休、缺slot/NaN断点绝不桥接。S为这些对各自去均值后的Pearson相关。
分母0（包括全平或恒定收益）为`CONSTANT_RETURN_SERIAL_VARIANCE_UNDEFINED`，不是0信号；
有效对不足2或少于声明calendar合法三元组的80%为`INSUFFICIENT_OBSERVED_ADJACENT_TRIPLES`。
这是特征可观测性，不阻断包/原名单；NaN保留逐字段UNKNOWN，不补价、前填或删股。
Inf/已知非正价、负量、OHLC矛盾、坏头/截断、路径越界、读取漂移或身份不一致明确失败。
浮点相关仅允许1e-12内的边界舍入，不裁剪真实越界。

当前241slot日有237个合法三元组，不硬编码241/240作为其它日的分母；09:30/13:00 anchor按原calendar处理。
只允许D的09:30～11:30、13:00～15:00合法声明slot；非交易clock不是可丢弃噪声，应明确报告源合同错误。
同日价格乘统一正尺度不改变S；日内复权/单位只沿原文件合同，不引入未来复权锚。

### 3. 同信息对照与编码

matched原始字段共52个：原九日频量；39个既有分钟量（开尾30m收益/RV/方向效率、三volume量、
16段收益及16份额）；原Top20父score的组内z、原rank百分位；非零分钟收益比例、S观测覆盖比例。
不消费VWAP/amount坐标未知项或未纳入本次来源的limit flag，避免重新引入旧单位问题；两臂同取舍。
父score的组内z不当作跨日期可比绝对分；组内方差0或真实缺失仅该量UNKNOWN，原score不覆盖。

candidate仅新增S和S×closing_30m_return_bps/10000两列，共54字段，是一个“序列压力”信息块，
不逐列挑选两次候选。交互任一项未知则未知；flat/stale非零比例与coverage两臂均可见，
不把数据可观测性差异误归序列结构。所有旧分钟量复用既有纯聚合函数，不重写其定义。

训练列中位数及IQR仅train拟合；零IQR使用固定1作编码尺度，不冒称真实价或观测0；
全train未知列两臂共同停用并明示。每字段missing指示保留；若未来才观测到train全未知字段，不临时启用。
模型编码可填训练中位数，原始输入的UNKNOWN不抹去。无任何训练可识别输入时`NO_RANK_ADVICE`保留原Top10。

## Model / One Study Contract

### 4. 固定预算、拟合与派生名单

仅两个Ridge：matched/candidate各一次，alpha=10、solver=lsqr、tol=1e-6、max_iter=2000，fit_intercept=false。
以原package/D的可监督行对X和五日净收益Y做同组中心化；缺标签只不进入损失，不能删除源行或原日期。
两臂共享同监督KEY、训练median/IQR/共同列和权重，每D总权重相同、D内包等权。
重复stock/D不视为独立观测；真实行/unique stock-D及按D有效样本另报，统计按D成簇。
不读取evaluation标签拟合编码/参数/符号/窗口，不用标签状态选择推理输入。

推理对原20行的已编码X按package/D中心化，输出相对收益score及派生rank；不是绝对收益或校准胜率。
两臂各从原Top10最多替换两只：候选按预测排序、同分按原rank，只有严格提升才交换；
加性目标下选原Top10最低预测与外部最高预测配对，不复写原生Tradability/递补/TopK算法。
20行派生顺序、选择标记与原生顺序并存；禁止将研究名单混成Selection结果。

最多1candidate/2物理fit，sklearn版本、训练键hash、编码/列顺序、收敛状态及源码commit在事前登记。
监督拟合在既存WSL环境，两线程、RSS≤1GiB，预计分钟内/最长30min；不安装依赖或启动WSL API。
Windows只做有界文件prepare、纯预测/评价；X是临时及本次研究输出根，资产在原地读取。
prepare上界12,200行/1,253股/每D≤300实际slot，五字段约60MB消费量级；输入矩阵与每股stream生命周期有界。
排序O(G×20log20)、统计按D聚合，禁候选×分钟/标签many-to-many join。
超预算明确报状态，不调小人口、改模型或重试另一seed来凑成功。

### 5. 一次学习与经济判断

使用原已消费开发窗口，非独立OOS，不读取sealed holdout。源码/完整输入验证后才预登记新run；
registry记录objective_contract、study_type、decision_use、唯一新增信息、数据/policy/源码身份、fit数和lineage。
本次零fit输入审计不是模型试验/收益结果；未来两fit属于一个candidate，不伪装零多重检验成本。
固定两臂预测及所有原日期后才读取evaluation finance；必须先公布选择列表，不能看未来收益挑Top10。

业务基线原Top10；两个经济端点candidate−native、candidate−matched，均为每D十固定槽五会话净收益。
不可执行原槽按既有CASH_ENTRY_NOT_EXECUTABLE语义，UNKNOWN/未成熟为null并保留；不是模型SKIP或实际成交。
同D先包等权、再按D统计，跨包同股不当独立样本；不把重叠cohort复利为NAV/MDD或与沪深300日收益硬比。
同时报告预测MSE/RankIC、盈利/亏损幅度及胜率、尾部分布、丢失上涨、干预次数与日期、UNKNOWN/成熟分母。
预测技能不是经济成功，尾损改善不能抵偿Alpha收益失败，也不事后切换Risk-managed合同。

两端点共用三角色/全部原包的共同完整D；某包或某角色真实未知则该D配对值null，不以其它包平均代替。
固定原轴block5/2000次bootstrap、seed20261021。两主端点各报97.5%双侧区间，Bonferroni家族0.05；
完整原轴共同成熟D≥50且占原评价D≥80%时才做区间，block仅取原轴连续五D都已知者，不能压缩缺洞或跨洞拼接；
无合法块或支持不足则区间null、标INCONCLUSIVE，仍保留原日期及可测点值。
一次结果只有两端点均值均≥5bps/5TD、各下界>0，且相对两对照分别≥20个干预D、每原包≥10个干预D，
才支持另立确认设计。按D已知csi300_ret_5的正/负/零/未知四类分别报告train/evaluation支持，
不事后拆出“最佳区间”替代全期。
序列参数不随结果改判；任一不支持只停止本candidate，绝不回选matched、换阈值/窗或为旧候选补证。
training尺度MDE代理在fit前报告，真实paired MDE随一次结果报告；欠功效/已消费窗口均不支持激活。
即使上述导航标准通过，也不是自然前向/独立确认/实盘收益或上线许可。

## Implementation Plan / 已完成及下一源码任务

本次已完成：原生因子来源查重、旧43量构造对照、训练端全窗口close可产性、F1和蓝图一致性修订。
下一步按上面六文件精确范围实现：原绑定批读/共享两臂输入→编码/两头/JSON parity→冻结预测/原finance评价→
同窗口多轮源码审核修复/定向测试→一次正式prepare及WSL两fit/评价→真实结果回写蓝图/PR交付。
不以设计验证冒称源码完成，不等待后端重启、LocalSIM适配、UI或QE上游试验。

已完成的训练端审计仅close：219D/438包D/8,760原行、8,212 unique stock-D/975股；
8,173可测、39 UNKNOWN（34恒定方差/5不足三元组），没有删除/补0。237合法三元组为该样本日历事实。
11.185秒、峰值197,795,840bytes，消费7,920,268 Bin bytes，0fit/SQL/evaluation price/收益列解码/大型复制。
这不覆盖剩余OHLCV字段、evaluation特征、监督模型、WSL运行或经济结果；这些是下一实现验收项，不是假PASS。
factor输入审计收据SHA256=`6df43b103bc5479cca3bee3ec5f174defcdb864f08f64beda651e7e170b33c28`；
minute输入审计收据SHA256=`8c9a778e616a99ff2e9df0034507820d133672490a4115cb5aa26914c4a03e3f`。
收据位于X任务目录，是本机执行证据而非跨机器自动可用的发布资产；下一prepare必须重新验证其实际输入身份。

## Verification Plan / Design Acceptance Index

| ID | Requirement |
|---|---|
| F-361 | 代码来源查重、43旧量不变的构造对照；不把新统计等同Alpha |
| F-362 | 原Top20投影、分数dtype/排名/UNKNOWN、D/T/H/policy与文件身份保留 |
| F-363 | 严格同D连续三元组、真实分母/平坦未知/尺度不变、未来隔离 |
| F-364 | 52/54字段同监督两fit、train-only编码、序列交互与收敛预算 |
| F-365 | 三角色Top10/最多两替换、原生排序保留、NO_ADVICE及标签不驱动选择 |
| F-366 | 双净端点/日期簇/干预/MDE、单次结论、证据等级/合同隔离 |
| F-367 | 精确Advisory范围、文件资源预算、当前零fit与后续实现分态 |

源码定向用例：同旧43量而S不同；午休/缺slot/不同D/未来毒化；平价与恒定收益UNKNOWN；NaN保留、坏数失败；
正尺度不变；重复KEY/多包同股身份；train-only median/IQR、eval毒化不改拟合、两臂同监督与JSON parity；
同分保原序、最多两替换、原score float32不变；恢复身份漂移/已有stage不覆盖；完整86D/未知洞/两包日期簇分母。
每条保护业务或泄漏合同，不添加实现快照/重复fixtures；本地一轮小矩阵，失败节点定向修复，广测交CI。
真实模型结果另验原生LSTM/TCN名单，不把合成样本或单元测试当收益证明。

## Design Acceptance Matrix

本表仅设计验收，源码/模型尚未交付不是被掩盖的功能成功。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-361 | Background / 单一业务问题及查重 | artifact: X:/AIstock_temp/ADVISORY/minute-innovation-top10-20261021/factor-input-audit.json；artifact: X:/AIstock_temp/ADVISORY/minute-innovation-top10-20261021/minute-input-audit.json | DESIGN_REVIEW_PASS | none |
| F-362 | Contracts §1～2 | artifact: docs/architecture/advisory_serial_price_pressure_top10_5td_v1_f1_design_20261021.md | DESIGN_REVIEW_PASS | none |
| F-363 | Contracts §2及训练端实际审计 | artifact: X:/AIstock_temp/ADVISORY/minute-innovation-top10-20261021/minute-input-audit.json | DESIGN_REVIEW_PASS | none |
| F-364 | Contracts §3 / Model §4 | artifact: docs/architecture/advisory_serial_price_pressure_top10_5td_v1_f1_design_20261021.md | DESIGN_REVIEW_PASS | none |
| F-365 | Model §4 / Verification Plan | artifact: docs/architecture/advisory_serial_price_pressure_top10_5td_v1_f1_design_20261021.md | DESIGN_REVIEW_PASS | none |
| F-366 | Model §5 | artifact: docs/architecture/advisory_serial_price_pressure_top10_5td_v1_f1_design_20261021.md | DESIGN_REVIEW_PASS | none |
| F-367 | Scope / Implementation Plan / Production Gates | artifact: docs/architecture/advisory_serial_price_pressure_top10_5td_v1_f1_design_20261021.md | DESIGN_REVIEW_PASS | none |

## Risks / Rollout / Rollback / Production Gates

分钟序列可能只反映bid/ask bounce、stale quotes或短暂噪声，并不持续至T开盘后五日；新信息不等于新Alpha。
219训练D/同开发窗限制统计功效，多研究族试验偏差和非vintage限制继续披露，不因一次正点值去生产。
price-only可产性不证明完整OHLCV、标签或WSL环境已就绪；真实错误交相应owner，不用异常fallback改源。
下一代码是离线研究叶；原日频业务、旧产物/策略包/股票池不变，无需运行加载本研究来继续荐股。
本次文档不需后端重启/DDL/依赖安装，未修改运行态。#6453共享任务输入加载仍待用户方便重启，不阻断本研究。
回退仅不消费新研究角色，不覆盖原结果/父名单。LocalSIM不消费本模型结果/收益/缓存/断点。

## Review / 多轮自审与DESIGN-COMPLIANCE-001

第一轮输入查重：排除已做的RV、signed-volume和16bin重命名；以真实旧聚合函数构造非冗余对照，
纠正“因子名有intraday就已经消费分钟”的推断。
第二轮业务与统计：加入收盘方向交互且matched共享stale/coverage/父score，明确S非点差/五日Alpha；
固定两fit、双端点多重检验、原日期簇及干预标准，未知不造0或删日期，源码/当前审计分报。
第三轮时钟与交付：训练审计截止2025-05-30，不碰评价价/收益；冻结旧binding而非更新active；
保留原Top50分母与浮点dtype，补float32构造对照；明确非交易clock错误、双端点共同原包分母、
block不跨未知洞、市场分区按D已知值固定。监督拟合WSL、所有临时文件X，下一实现不阻塞日频原包。
以上是本窗口多视角自审，不声称独立外部审核。

四项符合性：①本次仅设计/可产性，不冒称源码或盈利模型完成；②平坦、真缺失、坏数、身份漂移分别表达；
③不改原包/原名单/费用/政策，新角色和Alpha合同显式独立；④没有包使用或用户重启前置门禁，
观察度与经济条件仅用于这个预登记研究，不限制原业务运行。
