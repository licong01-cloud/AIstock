# Advisory 买入价格价值：历史隔夜／日内收益分解假设 H-TIMING-1（F2）

> 日期：2026-10-03；版本：v0.2；状态：DESIGN_REVIEWED_NO_EXPERIMENT。本次交付研究设计，不交付已训练模型、收益确认或角色启用。
> 父级：[蓝图§16](advisory_strategy_conditioned_model_blueprint_v1_20260710.md)、[共享D内核接入](advisory_economic_common_core_daily_consumer_f2_design_20261003.md)。业务目标仍为日频、价格条件化的净收益与下行风险建议，不预测最佳分钟买卖点。

## 1. Background / 已核对的事实

- v1模型TAKE=0；v3日增量相对Selection为-8.8880bps；v4相对matched九字段为+8.7336bps，但相对基线为-0.1545bps，描述性区间均跨零。三者没有经济确认，不追加旧窗口、资格恢复或确认实验。
- #5313统一12个D字段计算，#5319交付有界只读来源；它们不证明旧模型训练／每日语义相同。旧v4十三字段顺序中query位于第九，不能把新core矩阵直接交给旧权重。
- #5324已合入`68ff7aaaa9358c160c75916231207d5f87f3c014`。用户重启后health=ok、runtime identity精确匹配；三个实际Program的`entry-value/status`均为NOT_CONFIGURED、deployable=false、automatic_capture_enabled=false。六UI只证明展示行为，经济合格模型数量仍为0。
- 现有经济九／十三字段模型没有历史隔夜／日内收益分解。旧通用103字段含当日`open_gap`，因此不能声称系统从未用过跳空信息；本假设也不是重新运行旧103字段排名研究。

问题：相同收盘涨幅，可能由隔夜高开、随后回落形成，也可能由低开、随后上涨形成。只有收盘收益和ATR不足以直接表达这两种历史构成；但这种区别是否能预测下一次买入的policy净价值，尚无本项目证据。

## 2. Scope / 单一可证伪假设

**H-TIMING-1：在原候选、D截止、真实价格条件、exit policy、成本、标签、模型参数及共同支持人口相同时，增加历史隔夜／日内收益构成与隔夜波动两个D描述量，能够改善进入价格的收益／风险估计，并产生相对同核控制及原无保护基线的成本后正增量。**

这是新的特征表达假设，不是新外部数据源、独立alpha证明或新上游选股策略。基础OHLC仍来自既存日线；不把“增加维数”本身当作创新或收益证据。若与已有字段高度重叠、不能学习或没有动作增量，允许该candidate停止。

本轮只设计，不读收益、查业务数据库、拟合、回放或访问sealed。后续先实现两个纯计算量及同核来源复用，再按固定方案作一次探索；不先建立新ModelOps、确认平台或完整新family UI。

### 本次允许写入的精确文档

1. 本文。
2. `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`：当前状态、§16唯一主线及本设计引用。
3. `docs/architecture/advisory_economic_common_core_daily_consumer_f2_design_20261003.md`：消费者已合入／重启验证状态，以及新recipe随实质假设实施的顺序。
4. `docs/architecture/advisory_economic_entry_daily_consumer_v1_f2_design_20261002.md`：当前工程／运行态状态，历史审核记录保留为历史，不改模型和功能合同。

## 3. Non-goals / 边界

不修改QE、Selection、StrategyPackage、HMM、数据准备、公共验证、Paper或Execution源码；不重选Top20，不提交QE实验，不把描述量发布为QE因子或策略包。保持日频DB-only，不读取分钟、新闻或财报，不执行分钟择时、下单、仓位分配或真实成交推断。

不放宽800bps风险参考、gap支持、旧scope或旧风险标签，不改v1／v3／v4权重和工件，不重复其导航以补证。不为历史证据归档排期。临时X、持久F；DDL/DML、profile激活、依赖安装、用户进程控制均不执行，后端重启用户负责。

## 4. Architecture / 同核输入与唯一改变项

冻结原候选／日历 → 同一只读快照的20D原值 → 既有12D core与新增两个纯计算量 → 显式query条件 → 同eligible的13字段控制／15字段candidate → 唯一条件节点与原shadow policy → 固定比较。

控制顺序固定为`(*D_FEATURES, query_gap_bps)`；candidate顺序为`(*D_FEATURES, overnight_intraday_contrast_19, overnight_volatility_19, query_gap_bps)`。分别13／15维，query均在末尾；完整顺序、公式、实现hash、实际两腿权重、PIT规则、package/policy/cost在拟合前绑定新request/scope。

两个控制／candidate均以新core生成所有训练、validation和日常输入；不能复用旧v4模型或在旧prepared追加recipe。新控制只服务本假设的matched归因，不是复活旧负候选；core修正导致的差异不算新增两个量的功劳。原始输入相同才承诺单D／批量一致。

来源当前只返回12D值与receipt，尚不提供原值packet。后续在该Advisory source内部复用已有原值读取，增加显式timing入口，旧load_day/load_batch输出和默认行为不变；不得用返回的12D摘要反推OHLC，也不得另按股票逐次查询。纯计算仍不认识收益／label／模型或资格。

## 5. Contracts / 两个描述量、时钟、缺失与身份

### 5.1 固定公式

取恰好20个权威交易日`s0…s19=D`，只使用原候选自己的真实open、close及各日D可见adj_factor，统一同一个D复权anchor。对后19日`j=1…19`：

```text
O*_j = raw_open_j / 1000 × adj_j / adj_D
C*_j = raw_close_j / 1000 × adj_j / adj_D
ON_j = log(O*_j / C*_(j-1))
ID_j = log(C*_j / O*_j)
overnight_intraday_contrast_19 = mean(ON_j - ID_j)
overnight_volatility_19 = std(ON_j, ddof=1)
```

两项单位均为log-return，非bps；十九个配对因现有20D边界固定，非搜索出的最优窗口。`ON+ID=log(C*_j/C*_(j-1))`仅为坐标和计算校验，不是获利恒等式。不将它转换成买价／卖价预测，不预设“低开必反转”或contrast某个符号必买。

真实价格／factor必须为有限正值；OHLC、唯一键、symbol、实际D/T、原rank/名单继续按现有core校验。股本变更需使用真实各日因子；不拿今天factor回填，不将拆分的原始价格跳变当作市场跳空。D参考／T法规价继续独立沿原价格坐标合同，不用本公式替代。

可手算的区分性测试：两条历史都每日收10元、相同高低价及成交量，一条每日开10.10元，另一条开9.90元。其收盘收益与当前12D摘要可相同，contrast分别为`2×log(1.01)`与`2×log(0.99)`；这是遗漏历史构成的计算例子，不是未来收益／alpha证据。公式有表达差异，是否可学习仍须固定研究验证。

### 5.2 缺失与停牌

每项都要求其19个配对所需原值完整。缺原open使两项UNKNOWN；仅缺D close时contrast UNKNOWN，volatility可在其所需open／前close完整时计算。更早缺close会使相关ON未知。nullable／停牌／盘中S-R歧义按需传播，不补零、不缩短至“最近19个有数据日”。

只从已证明可用的真实bar计算，不把停牌合成平价bar计作19个正常样本。仅有供应商OHLC行不证明真实交易：所需bar须有原始正成交量且非synthetic标记，遇整日停牌、零量占位、成交量未知或S/R时钟矛盾，该bar参与的描述量为UNKNOWN；不将盘中有效真实bar机械删除，其open/close身份可证实时仍可计算。受影响候选与日期完整保留，分别输出字段UNKNOWN及原因；其它完整股票可继续。两个描述量缺失不是股票池删股、日期删减或全局数据补齐任务。

### 5.3 信息时钟与来源

新描述量与原12D字段均只能读≤D原值；下一T仅为日历时钟。T开盘只在监督或实际观察时提供`query_gap_bps`，不进入D描述量；T high/low/close、未来退出、成熟状态与labels禁止进入feature builder。

训练与日常使用相同纯函数、窗口、复权、缺失语义。输入／计算hash仅为COMPUTATION_ONLY；数据库历史读回仍按其实际NON_VINTAGE/RECOVERED_LIMITED等级报告，不制造native receipt。正式资格继续要求原生scope及严格源身份。

包／名单／run/list／policy／pool／hash矛盾fail closed；新索引仅使用原候选exact键，不补第6名、不用当前配置重建历史成员。源读取保持至多20D块、有界原候选、repeatable-read readonly／finally rollback、原查询预算，不扩大universe×date。

## 6. 固定研究合同 / 不挽救旧candidate

### 6.1 先来源可产，再一次探索

源预检只核对现有已消费开发窗口的元数据、D原值及两个量的覆盖／缺失／分布／与原12D重叠，不读取标签或任何新窗口收益。先在已消费首中末D验证，能复用同一来源后才按块准备完整输入，不重复建立daily worktree。不按相关性／缺失统计回选公式、窗口或删除某一新增字段；这些结果只说明当前固定方案的可执行性和限制。

预检可发现无变化、全UNKNOWN或无共同价格支持等不值得拟合的情形；它们只终止当前可执行candidate或触发明确输入错误说明，不证明整个价格方向不可学。缺失是正常业务状态，不要求数据准备填造正常停牌bar。没有新增原始数据需求时不转交数据窗口。

后续研究仅用原已消费开发序列（2024-07-04～2026-02-02，386D），沿原train／validation／81D探索test的冻结切分和label-end purge；准确日期与源hash从已发布父请求读取并在新plan绑定，不从本文近似日期推导切分。累计已消费窗口不声称独立OOS。本轮没有prepare或新study。

### 6.2 共同人口与真实trial预算

共同监督集合为：原return和entry-loss标签均AVAILABLE、未跨切分信息截止、原12D＋两个新量＋真实query均有限的精确交集。控制和candidate的两个头均在这一集合拟合；全原候选/日期仍保存，不能为提高收益改变mask或删除UNKNOWN。

固定一个新增特征块candidate，两个模型配置（13／15）、每配置mean净收益及q0.9 entry-loss两个头，共4次头拟合。参数沿原200轮/lr0.05/depth3/leaves7/minleaf30/seed20261002/2线程，不early stop、不扫seed/loss/window/阈值。控制的新拟合也登记trial，不是exact retry。不在本轮附加交叉拟合、oracle、其它模型族或多期限头来制造新搜索。

research registry记录独立lineage、study_type=EXPLORATORY_SCREEN、objective_contract=RISK_MANAGED_ADVISORY、decision_use=NAVIGATION_ONLY、父研究族已消费窗口和真实累计配置/头数。文档合入不产生TRAINED登记；exact retry只能核验相同已发布plan及工件，不增拟合／收益读取。

### 6.3 支持与动作合同

价格支持继续train-only：gap桶100bps、每桶至少30个真实观察／5个D、真实min/max与合法法规tick域。比较时控制和candidate统一使用candidate的15字段train-only支持mask；不能让控制多吃几批缺失样本后把动作差异归于两个特征。

支持mask仅由D特征、明确query和训练统计形成，不能用未来label AVAILABLE或事后盈利决定预测是否UNKNOWN。分别报告共同mask的限制、全人口UNKNOWN、原基线覆盖；共同mask归因不是15维模型全市场可用性。

标签保持原policy/cost的净收益及entry-anchored日级净损失，不重新选择持有期。ACCEPTABLE仍要求受支持、mean净值>0、entry-loss q90≤既有800bps研究参考及合法可交易条件；不把参考升级为用户资金预算，不根据TAKE反调。期望收益／风险分位数不是盈利概率／均值置信界。

## 7. Evaluation / 价值而非“分解预测准确”

固定比较臂仅三种：原无保护Top5／同核13字段matched控制／同核15字段candidate。原shadow policy、Top5固定空槽、成本和共同估值终端不变，不新加规则或重复旧±300bps比较。UNKNOWN时的基线控制仅属研究处理，其收益与真实模型TAKE严格分开，不成为生产fallback。

主要成对指标同时报告：15减13的全共同估值日净收益增量、15减原基线的净收益增量，以及各臂绝对名义净收益。同步报告MDD、尾损、错失盈利／避免亏损、TAKE/SKIP/UNKNOWN、实际进入动作不同日、真实episode和控制贡献。不能只用模型RMSE、胜率、收益分解相关性或几笔成功干预判定业务有效。

固定moving-block bootstrap block5/reps2000/seed20261002，双侧95%描述性区间；功效/MDE和干预支持报告不能把旧29.6721bps代理当本candidate保证。regime只有预先已批准、PIT正确的定义才报告跨区间支持；没有定义标UNPROVEN，不看结果后分组。

不预承诺该candidate值得确认。净增量非正、零真实干预或没有支持时停止本candidate，不回选参数；正点估计也仅属探索导航，CI/MDE和支持不足如实报告。必须同时看到15减13与15减原基线的正点估计及非零真实动作增量，才能提出“值得继续评估”的导航建议；该条件不是确认通过、方向闸门或激活门槛，不能用一笔干预支持有效结论。只有形成值得继续的新候选，才另行在既有确认合同下预注册最低干预／日占比／regime、经济MDE、独立合法窗口和一次选点规则；本设计不读取该窗口、不授权激活，也不关闭整个研究方向。

## 8. Implementation Plan / 精确分阶段范围

1. 本轮：完成设计、蓝图当前任务及#5324进度一致性修订，至少三轮本窗口不同视角自审；结构validator与diff check。只交付设计，不借设计矩阵标记模型实现。
2. 后续计算／来源切片：新增`backend/services/advisory_model_first/economic_entry_timing_features_v1.py`和对应`backend/tests/advisory_model_first/test_economic_entry_timing_features_v1.py`；在`economic_common_core_daily_source_v1.py`新增显式原值复用入口及对应原source叶测试。实施前该切片另行登记完整F1验收，不修改原core默认合同。
3. 后续一次研究切片：新`economic_entry_timing_{contracts,training,inference,pipeline,evaluation}_v1.py`及五个对应叶测试（全部同一Advisory目录）；实际写入前在该切片设计/索引展开每个精确路径。既存标签、core、模拟器只消费不改；无需改QE或其它公共模块。
4. 预登记后才允许fit／一次导航；研究训练与QE训练资源不并行。只读准备及回放可按已批准资源规则执行；无法获得公开资源状态只暂停fit，不停止其它窗口任务。
5. 结果值得继续才接既有daily serving/grid/API/UI的新显式family，精确路径另登记；未值得继续不花工时补证、扩窗或为失败candidate建新UI。买入形成业务价值后才推进Exit，不开第二条训练主线。

预期后续最小实施约8～12小时，非硬性烧满工时：纯计算／复用来源2～3h、配对合同／唯一节点2～3h、预检／一次研究1～2h、多轮审核及交付3～4h；前置错误或无支持时提前报告，CI等待另记。不创建通用特征平台或历史固化任务。

## 9. Verification Plan / Design Acceptance Index

| ID | 设计验收要求 |
|---|---|
| F-611 | 实质新表达、经济假设与旧负结果界限 |
| F-612 | 19配对的固定公式、复权坐标、真实bar与未知保留 |
| F-613 | 唯一train/daily core、13／15显式新scope、旧模型不补侧车 |
| F-614 | D/T/PIT、精确身份、有界只读来源及模块边界 |
| F-615 | 同监督／预测支持人口、2配置4头和真实registry预算 |
| F-616 | matched收益／风险／实际干预与功效，研究不冒充激活 |
| F-617 | 分阶段精确范围、最小测试、多轮审核、退出条件 |
| F-618 | 文献只支持动机、T+1／分钟／可成交／跨包限制 |

后续最小计算测试：同收盘路径不同ON/ID构成可区分；19配对恒等式；拆分调整后不产生假gap；D后原值毒化不能消费；正常缺open/close、停牌与SR歧义分别UNKNOWN并保留股票；规范重复键fail closed；单D／批块同值。研究测试只保护同eligible／label-end purge／test毒化不改fit、共同支持、真实trial、非法输出、空／多段／未知及旧9维／旧v4拒绝新scope，不建实现快照或重复fixture。

## 10. Design Acceptance Matrix

此表仅验收研究设计；源码、真实输入、模型、经济确认和激活都未交付。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-611 | 本文§1/2/7；蓝图§16 | artifact: docs/architecture/advisory_economic_entry_timing_hypothesis_v1_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-612 | 本文§5/9 | artifact: docs/architecture/advisory_economic_entry_timing_hypothesis_v1_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-613 | 本文§4/6/8 | artifact: docs/architecture/advisory_economic_entry_timing_hypothesis_v1_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-614 | 本文§3/5/8 | artifact: docs/architecture/advisory_economic_entry_timing_hypothesis_v1_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-615 | 本文§6 | artifact: docs/architecture/advisory_economic_entry_timing_hypothesis_v1_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-616 | 本文§7/8 | artifact: docs/architecture/advisory_economic_entry_timing_hypothesis_v1_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-617 | 本文§8/9/11/12 | artifact: docs/architecture/advisory_economic_entry_timing_hypothesis_v1_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-618 | 本文§3/5/11 | artifact: docs/architecture/advisory_economic_entry_timing_hypothesis_v1_f2_design_20261003.md | DESIGN_VERIFIED | none |

## 11. Risks / 文献、经济限制及审核

[Lou、Polk、Skouras，JFE 2019](https://personal.lse.ac.uk/polk/research/TugOfWar.pdf)报告隔夜／日内成分的持续与跨时段反转，以及收益构成与策略收益的联系。这只支持分解动机，不证明A股Top20、19配对窗口、两个特征或本冻结退出policy的alpha。本文公式／最小实验为本项目待证设计，不是论文推荐合同。

A股T+1禁止将T开盘买入后T收盘卖出当作可执行标签。本设计**历史ID仅为≤D已结束日的描述量**，未来监督仍来自原T+1与停复牌约束的policy episode；不重写为当日close收益。日线open/close也不证明限价成交或最佳分钟，较低查询价不必更值得买。

窗口短、时段分解与动量/ATR高度相关、公司行动／停牌可能降低覆盖，原弱信号与已消费开发偏差可能使假设失败。全部正点估计也只导航；不能靠增加模型复杂度、阈值或种子补救。包／股票池变化仍需模型scope验证，不承诺跨包共享权重。

自审1（业务／新意／因果）：核对现有经济9／13字段和旧103字段的open_gap；本假设只宣称新增历史构成表达，不称独立信息源或已证alpha。加入同收盘路径的手算区分例子，明确不以T日收盘作为可执行当日退出标签。

自审2（PIT／缺失／归因）：固定20日内19配对，不压缩停牌交易日；补充零成交量占位不能当真实bar、部分真实bar不机械删除。共同监督集合与预测支持mask分开，不能用未来标签的成熟／缺失状态影响预测；13／15均新拟合，同核与新增块归因不混淆。

自审3（边界／投入／交付一致性）：核对只改四份Advisory文档；将#5324已合入及用户重启后默认未配置验证同步到蓝图／两从属设计，不把该验证写成真实合格角色通过。新假设先最小计算和来源、后一次研究，值得继续才接新family，不为旧负模型补证；不建平台，不修改QE或开启Exit并行线。确认窗口及生产动作均未执行。

DESIGN-COMPLIANCE-001逐项：①本次完整交付研究设计，不声称模型／源码／业务有效已完成；②缺失、不可产、矛盾和负结果显式，不填零或空结果伪成功；③目标、候选、政策、成本、T+1及旧结果不改判，新增两个描述量唯一归因；④沿原预登记／确认合同，不增加审批平台或必须等待未来实盘日期的门禁。三轮均为本窗口不同视角自审，不宣称独立外部审核者或子代理审核。机器结构与diff复验结果由实际命令另报。

## 12. Production Gates / Rollout / Rollback

本轮source_changes=0、database_reads=0、database_writes=0、model_fits=0、research_runs=0、sealed_access=0、binding_activation=0、qe_public_changes=0、process_control=0。仅设计文档；不得将validator PASS等同于可盈利模型或实际可用role。

发布：先设计审定后以独立Advisory切片实施，真实fit前绑定新request/implementation/scope及累计trial；新family逐步接到已有消费者，不提前发布BUY。无合格角色时继续NOT_CONFIGURED，不等待未来交易日进行功能验证。

回滚：本轮只回退本研究计划的主动引用；以后停止本独立版本消费，不覆盖旧模型、工件、名单或生产数据库。source merge、工程测试、经济确认、用户重启及role启用分别报告；设计合入无需重启，后端变更仍由用户加载。
