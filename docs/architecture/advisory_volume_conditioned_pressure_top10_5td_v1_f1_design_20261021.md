# Advisory 成交量条件化价格延续 Top10 / 固定5TD F1 v1.0

日期：2026-10-21。状态：DESIGN_READY，输入可产性抽样完成；源码与监督研究未执行。
假设：`VOLUME_CONDITIONED_SERIAL_PRESSURE_TOP10_5TD_1`。
目标合同：ALPHA_RANKING；类型：EXPLORATORY_SCREEN；用途：NAVIGATION_ONLY。
本设计接续[蓝图§16.0](advisory_strategy_conditioned_model_blueprint_v1_20260710.md)，不是上一轮3:1组合的改权重版本。

## Background / Goal

189次已发生研究fit及历史1次index不清零。多时段、成熟更新及3:1组合的精确阴性结论不改判。
目标仍是对既存策略包原Top50重排、选Top10，在固定T开盘至T+4收盘的成本后cohort上超过原生Top10。
价格区间、退出、仓位暂不扩展；无需先证明所有角色，也不因研究结论阻断原包日频使用。

新机制：相同价格轨迹下，成交量集中在随后延续或反转的分钟，可能表示不同的短期买卖压力。
旧普通自相关只看价格，旧量份额/量加权收盘/符号压力没有保留相邻收益与成交量的全部联合关系。
[Campbell、Grossman、Wang（1993）](https://www.nber.org/papers/w4193)提出成交量与收益自相关的关系及流动性压力机制。
原文为日频市场/个股研究；本设计外推到分钟描述量与A股未来五交易日，**尚未被证明**，尤其可能在入场前已衰减。
不声称分钟成交量等于主动买卖量、机构订单、投资者成本或真实资金净流入。

查重：M12/入场timing已覆盖隔夜—日内分解，M9覆盖日量价、GP5覆盖分钟总量及16段路径，
GP5-RETURN-VOLUME-LAG-1已覆盖20日内日量与次日收益及下行压力的协动，
旧serial候选覆盖普通分钟自相关与尾盘乘积；本轮均不重新命名或单独重试。
本轮不是新的流动性理论，而是尚未被上述压缩描述量保留的分钟三元组联合信息检验；
构造反例的每日OHLC与总量也不变，因此旧日频量价滞后量不受影响。
两包冻结动态代码50份本轮逐一hash复验一致；既有来源审计未发现分钟输入。
Alpha158是日频输入；上述事实只支持本信息未直接进入这两包的已审计闭包，不能证明经济正交、所有未来包均无此信息或父包没有Alpha。

## Scope / Non-goals

不重训QE父模型、挖掘/入库通用因子、不改策略包/共享选股/数据/HMM/LocalSIM代码。
不搜索权重、TopK、分钟频率、窗口、阈值、种子、模型家族或表现好的包；公告仍待执行。
不建设API/UI/ModelOps，不运行生产回放、数据库操作或服务控制，不将研究上限当预测效果。

## Input / 时钟、身份及未知

复用已冻结多时段prepared人口：2024-07-04～2026-08-28，524D、52,400候选，两包每D各50；
来源为`advmultitop10_6c632adf532818cfcd059113`，prepared实际hash及binding写入新plan。
binding=`589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`。
原评分、原rank、包/股票/D/T/来源/UNKNOWN保留，不重跑Selection，不使用当前包配置回填。
包ID仅用于身份/等权分组，不进模型，不建立示例包白名单。
文件按该binding就地消费；节点locator转换不改变身份，不重新读取active profile改变范围。
既存文件是回溯冻结开发输入，不升级原候选native/vintage证明或宣称独立OOS。

特征只使用该股D及以前的数据；分钟三元组必须在D内且两个边各精确相隔一分钟。
不跨午休、隔夜或缺失分钟连接；原始时钟不压缩。价格正有限，量非负有限或明确缺失；
已知非法值/身份漂移报错，合法停牌/常数/零量/不足观察返回typed UNKNOWN，股票和日期不删。
仅当原日历合法三元组中，三价及中间时刻成交量共同观察比例≥80%且至少两组时计算。
这仅为离线数值定义，不是策略包或日频荐股准入门禁。

## Formula / 唯一信息块

对每个合法三元组(t−1,t,t+1)，令a=log(Pt/Pt−1)，b=log(Pt+1/Pt)，w=Vt。
在完全相同的共同观察三元组上计算普通Pearson相关ρ和以w为权重、分别加权去均值的Pearson相关ρv。
权重先除最大值再归一化，避免量纲及数值溢出；零总量或任一相关方差为零时不补0。
新输入仅两项：`volume_conditioned_serial_delta`=δ=ρv−ρ；
`volume_conditioned_closing_pressure`=δ×原`closing_30m_return_bps/10000`。
ρ/ρv在[−1,1]，δ在[−2,2]。尾盘输入复用原实现；尾盘端点缺失时乘积UNKNOWN。
最终边界时刻是D收盘，t+1是同日下一分钟，不是下一交易日或收益目标。
均匀正量必须δ=0；原成交量共同放大不改变结果。不能将相关性命名为因果冲击估计。

matched输入固定为旧`serial_price_pressure_top10_5td_inputs_v1.CANDIDATE`全字段，
加本共同三元组的ρ、联合覆盖率；父分数/原rank的旧归一化语义不变。
candidate只再加上述两项，其他编码、训练人口、排序、成本和标签完全一致。
两臂共同支持要求两项新增量可测且至少一项旧输入可测；不支持的股票两臂均保留原rank槽。
旧输入局部缺失由训练期中位数/IQR及缺失标记显式编码，全缺列两臂停用并报告；不得从评价期估计编码。
两臂仅重排相同可建议原槽，按预测分数降序、原rank破同分；原Top50不截断，派生rank≤10。
原生Top10单独保留为经济主基线，无输入支持不返回空荐股或修改原包结果。

## Research / 单候选、控制及时间切分

固定Ridge(alpha=100, solver=lsqr, tol=1e-6, max_iter=2000, fit_intercept=false)。
训练目标为原五会话成本后净收益的同包同D中心化值；特征同组中心化，按包/D等权，重复股票不冒充独立时间样本。
只有label_information_end≤当次训练截止的已成熟AVAILABLE或原合同合法CASH标签进入拟合；
先做时间投影再解码目标，未来poison不能在编码、拟合或异常检查中泄漏。
未知标签保留在原人口与评价分母，不填0，合法不可入场现金仅沿用已有冻结标签合同。

冻结三个前推fold（训练从2024-07-04开始）：

| 训练截止 | inner预测D |
| --- | --- |
| 2025-03-31 | 2025-04-01～2025-05-30 |
| 2025-05-30 | 2025-06-01～2025-07-31 |
| 2025-07-31 | 2025-08-01～2025-09-30 |

每fold两臂各1fit，共6；不是选择最佳fold/超参。inner按该fold评价截止投影成熟标签，未成熟保留。
全inner同时满足以下预声明导航条件才做最终两fit（训练截止2025-09-30）：
candidate−native及candidate−matched的共同D均值均>0；各端点至少2/3fold为正；
三fold均有共同可评价D、共同成熟日覆盖≥80%；两个比较各至少20个实际干预D、占成熟D≥20%、覆盖至少两个fold。
否则`INNER_NOT_SUPPORTED`完整结束，不解码outer收益，不改筛选条件或重新拟合。
最多8fit、1个新增信息候选；6fit停止时累计195，全部8fit执行时累计197，历史index仍1。
按实际fit claim计数，失败尝试也记录；候选/拟合/诊断次数分开，不把fit数直接当独立DSR试验数。

outer为2025-10-01～2026-08-28，先冻结两臂全部预测，再读此窗口评价目标。
该outer曾被前序研究消费，严格为探索；2026-08-31起sealed金融值本研究不读取。
匹配和经济计算复用现有cohort/valuation与成熟投影，不再实现一套收益算法。
固定T开盘至T+4收盘、10等额名义槽；成本买0.95bps/卖5.95bps沿用原合同，
合法不可入场、停牌估值与真实缺价规则不改，不将名义净cohort冒充组合NAV、实际成交或完整冲击成本收益。

主端点candidate−native；机制端点candidate−matched。三臂/两包在同一共同D先包等权后D等权。
原轴10D block bootstrap、10,000次、seed=20261021，保留UNKNOWN洞、不把完整日压紧。
预注册产品主端点用95%双侧区间，MDE80按相同block方差/α计算；机制区间另列描述，
不能把两个普通95%区间称为同时95%，更不能按哪个更好看来换主终点。
主均值≥10bps且主95%下界>0、相对原生至少20干预D/20%/两个季度、共同成熟覆盖≥80%、
额外未知槽≤2百分点、最差10%cohort均值恶化≤100bps时，为`WORTH_NEW_CONFIRMATION_DESIGN`。
只有主均值≤0、主区间上界<0或尾部恶化>100bps时停止本候选；其余`INCONCLUSIVE`，不自动调参追试。
机制增量、区间及相对matched干预支持独立分类，不设第二个产品激活门槛；主端点满足而机制未明时，
只可确认产品政策，不能宣称新信息已被证明有效。inner双正均值只是本新信息研究的低成本导航，
不改判旧候选或把新的matched标准追溯套用到旧结果。
同时披露两包各自、季度、全可测人口与共同人口、收益/尾部/胜率、避亏/漏盈、UNKNOWN及资源。
cohort胜率不是成交胜率，尾部均值不是MDD。部分包表现好不能单独被事后启用。

## Feasibility / 已执行的零fit输入检查

X根：`X:/AIstock_temp/ADVISORY/next-mechanism-20261021`。
构造反例只交换同日同15分钟段、同价格、同收益方向两个时刻的成交量；保持原OHLCV日总量、
旧分钟描述量及普通自相关不变，δ从1.346064变为−0.447655。
旧45个数值输出逐项相等，其中含覆盖及计数诊断，不将45全部宣称为模型特征。
该反例证明函数非冗余，不证明未被其他非线性代理间接学到，更不证明收益。

预登记抽样：三个inner区间各首末实际D，每日按股票代码取原候选前20个唯一股票，共120 stock-D。
120/120相关差均可测，输入扫描15.196097秒、采样RSS最高232,943,616bytes、消费231,840 Bin bytes。
这是六个训练期D的close/volume抽样，**不是**全窗口/全部OHLCV/乘积输入/未来发布能力验收。
探针RSS按进程节点采样，不是OS高水位；正式研究需要记录真正高水位，不拿本值承诺资源上限。
0SQL/收益解码/outer特征/fit/sealed；第一次因Windows locator失败发生于数据打开前，
等价WSL locator修复后沿用原采样预登记，未覆盖旧数据或改变样本。
`input-receipt.json` SHA256=`33f036397d1e54f404296f5be90059c5842db532f73e6e39a2f0350bb0cd288c`。
五个直接unittest通过：非冗余、等量零差、未知/非法量、未来/午休隔离、locator身份分离。
探针为本地设计依据，不能替代正式持久源码、全输入/模型/经济验收。

## Implementation Plan

设计先合入；后继从最新main独立树仅实现：
`backend/services/advisory_model_first/volume_conditioned_pressure_top10_5td_v1.py`（数值/拟合/排名），
`backend/services/advisory_model_first/volume_conditioned_pressure_top10_5td_pipeline_v1.py`（输入/阶段/研究），
`backend/tests/advisory_model_first/test_volume_conditioned_pressure_top10_5td_v1.py`（直接合同测试），以及本文/蓝图。
复用FrozenDataset、旧分钟聚合、cohort、projected_finance及现有原子阶段/registry；
不改旧已运行模型的合同、源文件或共享选股算法，不使旧研究变为可重试的新候选。
读取器按股票打开一次OHLCV流、缓存分钟日历索引、仅seek原D切片；同输入两臂复用，不复制H5/Bin。
计划绑定输入实际hash/文件lease/读切片摘要、代码及全部政策；消费范围不是全文件内容hash时明确标注。
原分阶段输出不可覆盖，exact retry仅复用已完成同身份结果；漂移/缺行明确失败，不回退active profile。
在X新研究根保存完整人口、特征可用性/原因、每次fit、模型、预测屏障、结果及资源。
CPU≤2线程、无GPU；一次研究计算≤30分钟、RSS≤2GiB、新产物≤200MiB，不复制大资产。
失败恢复不重做完成fit；公共缺陷交owner，不自行修改QE/LocalSIM/HMM/数据源码。

## Verification Plan / Design Acceptance Index

| ID | 验收目标 |
| --- | --- |
| F-431 | 经济机制/历史查重/非冗余与局部可产性，不宣称经济通过 |
| F-432 | 同日三元组、量权重、午休/未来/UNKNOWN/单位不变性 |
| F-433 | 原人口/身份/完整排名/共同建议槽与原生Top10基线 |
| F-434 | 训练成熟投影/编码/三fold/固定两臂/最多8fit |
| F-435 | inner分流、预测屏障/封存隔离、双端点同人口统计 |
| F-436 | 原子阶段、漂移与exact retry，不覆盖或重复fit |
| F-437 | 批量输入资源、完整结果及模块/运行边界 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-431 | Background / Feasibility | artifact: X:/AIstock_temp/ADVISORY/next-mechanism-20261021/input-receipt.json | DESIGN_AND_INPUT_SAMPLE_PASS | none |
| F-432 | Formula / Input | artifact: X:/AIstock_temp/ADVISORY/next-mechanism-20261021/test_input_probe.py | DESIGN_READY | approved_by_user: 按蓝图先交付设计；正式源码尚未实现，不宣称功能完成 |
| F-433 | Input / Formula | backend/tests/advisory_model_first/test_volume_conditioned_pressure_top10_5td_v1.py（后继计划） | DESIGN_READY | approved_by_user: 设计阶段；正式实现和测试未完成 |
| F-434 | Research / Implementation Plan | backend/tests/advisory_model_first/test_volume_conditioned_pressure_top10_5td_v1.py（后继未来poison/fit计数） | DESIGN_READY | approved_by_user: 设计阶段；0监督fit |
| F-435 | Research | backend/tests/advisory_model_first/test_volume_conditioned_pressure_top10_5td_v1.py（后继停止/继续分支） | DESIGN_READY | approved_by_user: 设计阶段；未做经济评价 |
| F-436 | Implementation Plan | backend/tests/advisory_model_first/test_volume_conditioned_pressure_top10_5td_v1.py（后继漂移/重试） | DESIGN_READY | approved_by_user: 设计阶段；恢复源码未完成 |
| F-437 | Implementation Plan / Production Gates | artifact: X:/AIstock_temp/ADVISORY/next-mechanism-20261021/input-receipt.json（仅局部资源） | DESIGN_READY | approved_by_user: 设计阶段；全人口及资源验收未完成 |

正式定向回归必须覆盖上述每项；特别比较同总量不同分配、等权、量纲缩放、零量/常数/缺分钟、
两臂同人口/无建议原槽、重复键/缺候选/身份漂移、未来poison、内层失败不读取outer、exact retry零新fit。
不以五个探针测试代替完整验收。多轮自审修复、F1、Ruff/scope/diff及必需CI通过后方可源码交付。

## Risks / 决策边界

一分钟关系可能在次日开盘前完全衰减，成交量不是交易方向；新函数仍可能只是噪声或冗余代理。
限定日内观察的高频相关不是因果识别。两个包共享股票/日期，不能当独立重复试验。
窗口已被多轮研究消费，哪怕区间为正也只能导航至真正独立确认，不直接激活；
设计抽样有选择限制，正式全窗口可用性可能更低。负结果只停止本候选，不证明全局不可学。

## Production Gates / DESIGN-COMPLIANCE-001

1. 本次交付设计和有限输入检查，不把它说成源码/模型/经济成果完成；后继预定义inner停止可为完整研究终态。
2. 不补价/补收益/删日期/填未知0；文件与身份错误明确失败。
3. 不改原策略包、Top10经济目标/成本/时钟或旧研究；不能在结果后选权重/包/季度/模型。
4. 不新增策略包准入、业务审批或生产发布阻断；研究导航标准不是日频业务门禁。

backend restart、client/dependency、DDL/DML、data activation、生产任务操作均noop。
不训练QE、不触碰其它模块；即使研究通过，确认、角色接入及运行加载仍分别验收，重启用户所有。

## Review / 多轮审核记录

R1：查重删除隔夜/日内分解候选；补充联合量价与旧无条件自相关的构造区别，排除“新名字即新信息”。
R2：将普通相关改为同一共同三元组控制，保证缺量不制造伪增量；两臂同支持/原槽保持。
R3：显式冻结训练内分流、主端点与机制分报/实际fit计数、sealed隔离和抽样限制；不将输入PASS升级收益。
R4：交叉核对蓝图F-288，取消初稿把机制下界设为第二个产品门槛的写法；区分采样RSS与OS峰值。
R5：补核GP5日频量价滞后原实现，明确这是同一流动性机制的新联合信息检验，不称全新理论；冻结两项字段名。
以上为本窗口逐项自审，不冒称独立外部审核。完整源码和经济结论尚待后继任务。
