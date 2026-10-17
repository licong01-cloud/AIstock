# PT-NEXT-022：R8 基本面筛选与同股长期持有对照详细设计

> 版本：v1.1；日期：2026-09-19；Feature tier：F1
> 状态：P1_R8_REPLAY_VERIFIED_P2_P3_INPUT_UNAVAILABLE；离线研究完成，不上线。
> 主蓝图：[F2 §9.25](position_timing_advice_f2_redesign_20260903.md)
> 唯一开发权威：`docs/standards/aistock_development_standard_v1.5_20260523.md`

`DESIGN_VERIFIED` 仅表示设计闭合。当前 P1 reader、两政策、WSL 并行回放和不可变产物已经实现并完成验证；P2/P3 财报 PIT 输入仍未交付，四个 hypothesis slot 明确为 `UNAVAILABLE`。P1 结果不能冒充三个组全部完成，也没有产生可上线策略。

## 1. Background / 目标与依据

终极目标是为特定持仓股、自选股找到**成本后超过同股长期持有**的择时方法，而不是提高仓位、降低回撤或构建选股平台。本轮只检验基本面筛选后的适用范围：P1/P2/P3三个入选条件、T1/T2两个政策、共同BUY_AND_HOLD，共六项主要假设。不重复执行A0～C2全矩阵，不把原策略列为新比较器。

PT-NEXT-021 r7结果仍是历史事实：5,144股已回放，54项family-wise比较无正下界，selected=0；全人口因unknown无法识别。不能用新筛选删掉旧失败记录，也不能称新组为未见holdout。新研究不以盈利、MDE、样本量或人工审批决定是否可以研发。

方法依据：

- [Asness/Frazzini/Pedersen，Quality Minus Junk](https://link.springer.com/article/10.1007/s11142-018-9470-2)：盈利、增长、质量可以作为待检验的选股特征；不证明本设计50～500亿元、ROE10%或A股择时有效。
- [Tushare每日指标定义](https://tushare.pro/document/2?doc_id=32)：`total_mv`单位万元；本设计绑定的是冻结本地文件，不调用网络数据接口。
- [Tushare财务指标定义](https://tushare.pro/document/2?doc_id=79)：区分`ann_date/end_date/q_dtprofit`；字段存在不等于数据具备历史版本和可见性证据。
- [前次策略设计§4～7](position_timing_strategy_evolution_plan_f1_20260918.md)：复用纯执行、报告与WSL并行机制，不继承旧实验R7硬编码入口或旧统计family。

## 2. R8 source-only核验与可用性（F-001）

### 2.1 三类结论分别报告

| 检查对象 | 2026-09-19实际核验 | 可得结论 |
|---|---|---|
| Windows R8 manifest | 文件SHA、去自摘要canonical SHA相符；49项直接组件逐项hash/size一致 | 可作为显式冻结输入 |
| Windows factor inventory | 12项文件逐项hash/size一致，包括daily_basic、bak_basic、static parquet | 市值来源可绑定；文件清单名字含r7但内容引用仍有效 |
| WSL R8发布 | 原生ext4目录存在，manifest文件SHA与Windows相同 | 发布入口身份一致；本次没有重新全量hash WSL所有bin/因子文件 |
| R7→R8 manifest差异 | 旧组件记录变更0；新增4个sector context组件 | 不可声称R8新增完整财务PIT历史 |
| daily_basic HDF | 8,124,082行；5,340个symbol；键唯一；2018-08-01～2026-08-31；total_mv非有限/非正均0 | 物理源可用，不等于本研究PIT人口覆盖已完成 |
| 50～500亿元物理源筛选 | 4,055,602个symbol-date满足范围 | 仅source计数，不是入选股票数、收益或完整PIT覆盖 |
| 财报输入 | bak_basic有同比/每股指标，但没有本设计要求的季度、可见版本、现金流/ROE完整契约 | P2/P3=`FINANCIAL_PIT_INPUT_NOT_DELIVERED`，不可用备用同比替代 |
| 现行研究入口 | `pattern_strategy_evolution_benchmark.py`固定R7文件/canonical hash、54项family和旧父实验 | **不能仅换路径直接运行**；新增PT-NEXT-022命名空间，不改旧入口 |

设计冻结时未读取新策略收益；正式实现随后按本文顺序在 source preflight 完成后才读取收益，证据见§14。全程未查询数据库、未训练模型。49项只覆盖manifest直接引用，不冒充全部日线/分钟文件已重扫。R8内继承的r5/r7旧报告只能作为谱系，不能作为新R8实验完成收据。

### 2.2 冻结身份

- Windows：`X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260831-qe_hmm_full_v2-direct-20260918-r8-candidate`
- WSL：`/mnt/wsl/aistock-qe-data-v1/releases/20260831-qe_hmm_full_v2-direct-20260918-r8-candidate`
- manifest文件SHA256：`07db01d8c24108ebfe2b85bc42f18f33c77fb0e09451999d7520faa08ebe1b18`
- manifest canonical SHA256：`6bb6096aada59541f58d05e1c44d5dabd8838661936d98539adcb79d7ac39283`
- daily_basic.h5文件SHA256：`3962873854ae212278d7e2020eb1401c5f64589e882c6124ff29bf16e0e7388f`
- bak_basic.h5文件SHA256：`78ecbec13c22443e0e61bfbe9c10f2a2890662e19491d80283170503fb1d127e`；仅字段发现，不作正式财务筛选源。
- factor inventory文件SHA256：`9583849d94b647bd82347a10a06752f02f9e40f73cb848bd7f0479f6995be910`
- 日历SHA256：`ce017cfbf1d9dde630c0d7f39e33b767e95293acd5258104f80491239826207a`。

R8新增`market_context/sector_code_map/sector_context_receipt/sector_membership_spans`不进入本轮策略。其历史起点不同，尤其membership spans从2024-07-01开始，不能拿来给2018年股票补分类或缩短研究历史。

## 3. Scope / Non-goals / 隔离（F-002）

仅离线`position_timing`消费者及直接测试。只读R8、交易日、停牌/涨跌停、指数PIT和另行交付的财务快照；只写timing-owned新研究artifact。代码实现不在本次文档PR内。

禁止修改QE、HMM、Selection、Advisory、Paper、MiniQMT、local_data；不接荐股，不使用Agent，不新增训练、在线卡片/API/页面、scheduler、worker服务、监控平台或全局registry。N0/current、timing在线registry/current、共享guard默认值、旧研究artifact零写入。数据库读写、行情API、candidate/profile激活、后台服务控制均为false。代码仓库协作网络与行情网络分开记录。

数据缺口交数据窗口，不在择时内查询DB临时补齐、下载或重建源。待交付的财务快照可独立于R8，但必须显式绑定R8、日期、schema和不可变hash，不偷偷替换发布。P1不依赖该快照，缺口不阻断P1。

## 4. Contracts / 筛选数据与定义（F-003）

### 4.1 历史时点和字段

决策截点固定T交易日20:00（Asia/Shanghai），使用已可见源，交易最早T+1收盘。daily_basic缺精确入库时刻，统一使用T−1对应交易日值；该日市值缺失不能向未来或无限向前填补。总市值采用原始CNY市值，不乘复权factor：50～500亿元对应`500000 <= db_total_mv <= 5000000`万元，边界含等号。历史20:00只是因果截点，不要求等到现实当天20:00才能回放。

拟议财务快照schema：`position_timing_fundamental_pit_source_v1`。由数据窗口交付，文件格式Parquet＋JSON manifest，无DB API；reader只验证与归一化，不负责生产。必需字段：

| 类别 | 契约字段/语义 |
|---|---|
| 身份 | canonical symbol、report_end_date、statement_scope、source_record_id、revision_id、row_sha256 |
| 时间 | published_at或publication_date、revision_available_at、availability_precision、source版本来源；可见时间为记录及其组成项可见时间最大值 |
| P2 | 单季度扣非归母利润、单季度营业收入及其季度/版本身份；不使用累计值冒充单季 |
| P3 | 单季归母利润、归母净资产、合并经营现金流净额；TTM为连续四季；金融分类及有效期/来源 |
| 单位 | 货币统一CNY，利润/净资产合并范围与归属声明，ROE内部为比例而非百分数 |
| manifest | schema、candidate文件/canonical hash、文件hash/size/rows、可见日期覆盖、字段覆盖、缺失原因、producer、版本来源 |

日期精度只有日时，保守在公布后的下一交易日20:00决策可用，再于其下一交易日执行；有可信时间戳时按T决策截点可见。固定`Asia/Shanghai`。不能以本次下载时间代替历史披露时间，也不能把当前更正值挂回首次公告日。仅有最终修订值而无历史版本证据的记录=`PIT_REVISION_UNVERIFIABLE`。数据需求覆盖2016-01-01起的季度历史及可见版本（为2018研究起点提供同比/加速/TTM前史），至2026-08-31；研究收益日历仍从2018-08-01开始，前史不足保留typed原因。

对每个T，先逐季度选择当时可见的最新版本，再派生单季度/TTM；累计值差分必须使用当时可见、口径相容的累计记录。同一季度多条冲突不能任选；组成记录和运算均hash-bound。最新可用季度期末距T超过180个自然日标`FINANCIAL_STALE`。不填0、不把未知当成筛选不通过。

### 4.2 三组冻结公式

P1：既有当日PIT入选资格＋滞后一交易日总市值50～500亿元。

P2：P1＋以下全部成立。令q为最新完整可见季度，Q为单季扣非归母利润，S为单季营业收入：

```text
g(q)   = Q(q) / Q(q-4) - 1
g(q-1) = Q(q-1) / Q(q-5) - 1
g(q) > g(q-1) >= 0
Q(q), Q(q-1), Q(q-4), Q(q-5) > 0
S(q-4) > 0 且 S(q)/S(q-4)-1 > 0
```

负基数/扭亏不满足该正增长假设，标`NONPOSITIVE_GROWTH_BASE`，不是数据缺失。极小正基数不擅加阈值、截尾或剔除：报告分母金额和增速分布，解释低基数敏感性；下一规格才可增加稳健条件。缺记录为UNKNOWN，不是FAIL。该口径与上一轮建议一致，不再把“微小基数单列”误写成尚未定义的隐性过滤。

P3：P2＋非金融适用＋以下全部成立：

```text
NI_TTM = sum(最近连续4季度归母净利润)
E_avg = (归母净资产(q) + 归母净资产(q-4)) / 2
ROE_TTM = NI_TTM / E_avg >= 0.10
归母净资产(q)>0 且 归母净资产(q-4)>0
OCF_TTM = sum(最近连续4季度合并经营现金流净额) > 0
```

该ROE是本设计定义的期初期末平均权益口径，不冒充供应商加权ROE。金融分类须使用入选时可见的分类：金融记NOT_APPLICABLE，历史分类缺失记UNKNOWN；不把2024后的行业成员回填到2018，也不把金融股从P1/P2总体删除。

筛选结果四态PASS/FAIL/UNKNOWN/NOT_APPLICABLE，附值、来源及reason。PE/PB/股息率只作描述，不增加过滤、排名或参数搜索。P1/P2/P3跨组收益差不是基本面条件的纯因果效应。

## 5. 人口、账户起点及覆盖（F-004）

以R8既有PIT全市场人口作为source audit范围，不按今天是否存续筛股票。保持2018-08-01～2026-08-31日历，不为缩短计算删除历史。物理源5,340个symbol不等于研究人口；正式人口从R8 frozen stock_universe恢复（前批5,144股身份只用于核对）。

每个`(screen_id,symbol)`仅首次入选一次：T当日筛选PASS、T当日PIT资格有效、T1/T2共同所需历史指标ready时，签发entry cohort。ready取SMA60及lag5、ATR20和R0加速特征所需数据的共同输入就绪条件，不要求趋势向上、不看未来收益或未来终值。T收盘两账户均为1,000万元现金，BH从T+1尝试买入，T1/T2可按政策等待。严禁用择时首次成交日回填BH起点。

历史特征计算与交易资格解耦：采用当时已经发生的有效原始行情计算复权指标，不能先按当日ST/PIT资格抹掉过去行情再计算均线。没有真实数据、停牌或无效值的窗口仍typed unavailable；不把缺失交易日压缩成“最近N根有效K线”。当日不能入选不等于过去行情不可见。

入选后筛选状态继续可记录，但**不作为新增退出/补仓资格**；市值超上限、财报减速、离开指数均不能删除既有账户。继续适用当天买入PIT资格及方向性成交限制；退出不要求买入资格。BH首次未成交仅在后续买入资格有效且可交易日重试，不因筛选值变化重置账户；未知交易数据不假定为未成交。

没有入选机会的股票记录NOT_ENROLLED；筛选所需源未知导致无法确定首日的股票标ENROLLMENT_UNKNOWN，不能选一个后来数据完整日冒充真实首次入选。P2/P3快照整体未交付记组级INPUT_UNAVAILABLE，保留两个hypothesis slot，不将整组静默置空。源已经完整但条件不满足才是正常未入选。

同股跨筛选组是不同研究账户，组内不注入新本金、不共享现金、不反复新建获利episode。主结果为每股共同起点至固定截止的连续账户；按入选年份分层，报告持有长度，不把新近入选的短样本年化收益混成长期证据。

## 6. T1/T2状态机与成交（F-005）

### 6.1 共同约束

adjusted OHLC=raw×factor、volume=raw÷factor、下游factor=1，单位为虚拟复权单位；不模拟现金分红、配股或真实股份到账，不读取公司行动authority。保留factor完整性与restatement校验；10 bps只是material变化统计，不恢复公司行动逐条绑定。

独立现金账户：本金1,000万元，现金利息0，无杠杆、追加资金、市场冲击、跨股调拨。买入金额＋逐腿费不得超过现金，保留最低净佣金与规费分拆。snapshot冻结现行`PERSONAL_MANUAL_COMPONENT_COST_V1`及hash，不改共享默认值。沿用实际board-lot、T+1、停牌、方向性涨跌停和terminal口径。

趋势定义B(T)：`C(T)>SMA60(T)`且`SMA60(T)>SMA60(T-5)`。普通趋势退出E(T)：连续两个全局交易日可评价且`C<SMA60-ATR20`；中断/缺指标重置连续计数。ATR20按既有`_trend_features`的true range简单平均。风险退出沿用冻结`rule_default`（硬止损600 bps等完整snapshot），优先于趋势退出，未成交保持pending直至可卖。首轮不同时优化风险阈值。

新买入使用T决策收盘作为参考，纯研究复用`NO_OPEN_GAP_POLICY`：开盘缺口阈值非绑定，其余100 bps追价、近涨停和REDUCE语义保持；如造成仓位不足必须报告。T+1收盘仅执行T的指令，不用T+1收盘重新决定方向。单日每账户最多一个方向，卖出当日不反手买入。

### 6.2 T1：TREND_CONTINUATION_REENTRY_V1

- 空仓且B(T)、当日买入资格及指标可用：T+1尝试以可用现金建仓。
- 持仓：风险退出→E(T)全退出→否则HOLD；没有加速放量止盈。
- 风险卖出完成后从下一决策日重新检查B(T)，不要求重新出现底部形态；不能在卖出前发出相反买单。
- 普通买单只在T+1有效，未成交由新的T重算；部分/REDUCE建仓后不自动每日补满。

该趋势逻辑复用前批B0作为既定机制，并非宣称发明新算法；新假设是筛选人口、共同起点和历史特征可见性修正下相对BH的结果。不重新跑B0−A0或给旧矩阵添行。

### 6.3 T2：CORE_70_TACTICAL_30_RECOVERY_V1

首次建仓、全退出与T1一致。趋势仍有效且无风险/趋势退出时，按以下状态执行，基础/战术比例按**减仓时持仓单位**定义，不承诺每天占总权益70/30，不建立两个资金账户：

1. `ARMED`：eligible=`R0加速事件 AND 当前持仓净盈利`，eligible由false→true时，T+1计划卖出当前持仓的30%；盈利=`units×C−估计卖出费 > 剩余持仓成本`，不使用账户回本条件。成功卖出才转`TRIMMED`；no-fill不进入已减仓状态。普通减仓单T+1失效后不每日重发，已观察eligible状态仍更新，必须重新观察false→true才可再签发。
2. `TRIMMED`：不重复每天减持。最早在减仓成交之后的决策日，事件已明确false、B(T)成立且C>SMA20时，T+1以当时可用现金尝试恢复；不追加外部资金，不按虚构70%净值每日再平衡。
3. 恢复买入若被REDUCE，仅记录一次实际补回，成功后回`ARMED`且需要后续新的false→true事件才可再次减持；没有成交则新日重算，不能永久携带旧单。
4. 风险或趋势破坏退出全部优先；全退出复位状态。首次持仓周期的可观察eligible=true按之前false处理，但不得把UNKNOWN当false重新武装；未知时保留状态且不生成战术单。减仓状态与eligible前值分开记录：no-fill不消耗仓位状态，不意味着事件边沿可被无限复用。

R0谓词直接复用`pattern_strategy.acceleration_volume_exit(...,'R0')`，锁源digest：C>C(T−3)、SMA5>SMA10、SMA10上升、acceleration_atr≥0.25、distance_ma10_atr≥2、volume_ratio≥2；R0不强制exhaustion。不扫描加速/放量阈值。部分数量按raw等价股数合法取整，不能合法部分卖出则typed no-fill，不强制清仓。

30%减持后剩余成本按实际卖出比例分摊；现金、单位、费用与成本守恒。T1/T2使用同一费用/执行规则；BH不受技术信号及追价guard限制，但同样受真实成交限制和费用。终点不强制伪造成交：保留mark-to-market与可清算净值两列；末日买入T+1未满足、停牌、跌停等导致可清算净值UNKNOWN，不能删掉该账户或提前用事后最优日期卖出。

## 7. 评价合同（F-006）

主要经济目标：同股同本金同起点`terminal_liquidatable_nav(Timing) − terminal_liquidatable_nav(BH)`及除以初始本金的百分点差；同时报告总收益、CAGR（短窗口不滥用）、回撤、逐股超额中位数/分位数/胜率、尾部贡献、费用、持仓日比例、持仓时仓位、全期平均仓位。

P1/P2/P3各两项对BH为一个固定6假设family。六池视图仍保留：筛选全市场、沪深300、中证500、中证1000、科创50、科创100。指数成员使用滞后一日PIT归组；子池不重新初始化账户，成员退出不平仓。入选年份固定cohort及年度表现均只作诊断，不变成新的调参入口。

全市场和个股参考000300.SH；五指数子池分别000300.SH/000905.SH/000852.SH/000688.SH/000698.SH。指数在同一账户起止区间计算；动态子池归组属于日收益描述，不能拿不同起点的累计收益相减。指数缺失只影响该参考列，不用沪深300替代。指数是价格指数，复权股票研究不是同口径真实账户总回报；BH是主基准。用户所说“只比较长期持有”指取消旧择时比较器，指数沿用背景展示、不进入选胜者。

统计推断单独命名estimand：每筛选组的配对日收益等权合成之差的日均值，不把其区间当作逐股终值差的区间。以共同日历25交易日时间块、5,000次bootstrap、seed=20260919，对所有6格共用日期抽样；名义95%与Bonferroni双侧`1−0.05/6`并报。日内跨股票相关性保留，不把股票/重叠年份当独立样本。无入选账户的日期两侧贡献0并记active_count=0；有账户但valuation未知不能填0，完整人口推断不可用，仅另列paired-observed诊断。

P2/P3输入不足时hypothesis仍保留为UNAVAILABLE，family不缩小；P1可独立发布两格结果。所有样本曾被观察，结论最多EXPLORATORY。pool终值、日收益统计、风险改善分别解释；只有具备完整适用覆盖和相应校正支持的指定estimand可标SUPPORTED，不能据日均差直接断言每股终值胜出。缺数据、无显著性、负结果、低功效是报告状态，不是工程合入或研究启动门禁；selected=0，不自动上线。

输出expected/PASS/FAIL/UNKNOWN/NOT_APPLICABLE/NOT_ENROLLED及enrolled数量、unknown时间段；完整人口与条件配对分开。不得仅留下正收益、成功成交或最终有价格的股票。重点诊断空仓错过/避开收益、延迟入场、风险退出、趋势退出、战术减持及补回间隔。事件后收益只作描述，不能静态加回当作重放结果。

## 8. Architecture / 最小复用与产物（F-007）

无需第二套回测引擎。拟新增三个模块（尚不存在，不是可运行命令）：

| 拟议位置 | 职责与现有复用 |
|---|---|
| `backend/services/position_timing/fundamental_screen.py` | 只读source adapter、as-of季度版本选择、P1/P2/P3/入选表；复用`DailyCandidate`和manifest/file_reference |
| `backend/services/position_timing/fundamental_timing.py` | T1/T2轻量状态机；复用`pattern_close_cash_replay.Account/execute/fee`、`pattern_strategy`特征、冻结guard；不复制费用和成交实现 |
| `backend/services/position_timing/fundamental_timing_benchmark.py` | 新prepare/run/inspect入口、6格报告；复用现有hash/seal/index/report及ProcessPoolExecutor模式 |

受控扩展只允许在position_timing内以显式参数、旧默认值不变的方式适配cash执行/特征。现有`pattern_strategy_evolution_benchmark.prepare`没有screen参数且锁定R7；禁止文档宣称旧命令可直接执行新研究，禁止全局替换R7常量或放宽hash校验。helper抽取须证明旧入口旧request语义不变，不以复跑10条旧策略来验证。

新artifact namespace：`<timing-root>/research/fundamental_timing_v1/`，不得写旧pattern bundle。request冻结candidate文件/canonical身份、实际消费文件hash、可选财报快照、source审计、人口、screen/policy/cost/feature/execution spec、源码/环境、family及日期。源码清单必须包含三个新增模块和所有实际复用实现，不能直接沿用旧`sources()`的固定文件名单漏记新代码。财报未交付时绑定typed absence，而不是虚构hash；交付后新request保留旧产物，不覆盖。

产物：`source_audit.json`、`screen_audit.parquet`（季度刷新＋入选/状态变更记录，不存无界原始payload）、`enrollment.parquet`、sealed chunks、stocks/pool_daily、report、receipt、manifest。报告审计可从绑定源确定性重建。每个键唯一，as-of join输出行数不得超过预期symbol-date输入；禁止季度×日历笛卡尔积。采用按symbol分块/向量化，行情特征每股只算一次，BH按相同起点复用，不能跨不同起点/身份借结果。

先完整source preflight（日线factor正数/有限、restatement、日期、买卖限制、PIT、所需财务版本及hash），再读本轮label/策略收益。源缺口按组/账户如实分型；candidate漂移与损坏必须停止受影响计算。不得恢复公司行动解析、等待实时行情或未来财报。

WSL原生ext4源码checkout和artifact，Windows只作控制/设计。固定8个spawn计算进程、128股chunk、最多16在途symbol、父进程按canonical顺序唯一seal；source和bootstrap单进程。实现后固定32股检验1-vs-8一致，不按收益抽股；失败chunk不seal，相同输入exact retry复用相同身份。新增财报快照/规格变动必为新request，旧r7/r8 chunk不得因symbol相同跨研究复用。

## 9. Implementation Plan / 三个连续实施块（F-008）

### 块一：输入与合同落地

读取本设计、`action_value_data.DailyCandidate`、`open_candidate_pool_memberships`、现有source审计及本地导出字段映射。按§4实现只读screen adapter与coverage/entry表；修正新入口的历史特征可见性与交易资格耦合，不改旧实验。产出source-only audit，冻结参数后才读取新收益。P2/P3若未交付，仅向数据窗口输出§4.1字段/版本/单位/日期/hash需求，P1继续。

验收：R8身份、全日历、万元换算、季度/修订因果性、四态与首次入选均闭合；没有财务输入时不冒充严格业绩加速。禁止临时DB查询、下载、套用当前股票名单或恢复旧公司行动系统。

### 块二：两政策、验证与一次并行回放

按§6实现状态机及最小执行适配，复制现有父唯一写者/纯worker使用模式而非另建调度服务；按§10完成直接不变量和并行等价检查。新增正式入口后执行prepare→run→inspect→exact retry，P1两格及可用的P2/P3四格分别如实报告；不加旧策略实验臂、不以中间收益改变规格。

验收：账户现金/成本/单位/T+1守恒、择时/BH共同起点、真实限制、失败恢复及hash闭合。实现前不提供冒充已存在的CLI参数；确切CLI由实现的`--help`和定向测试确认后回填。

### 块三：横向解释、审核与交付

按§7输出完整维度，判断是筛选提升了长期持有，还是择时有净增量；不同组入选日期差异必须披露。多轮审核/修复、定向测试与CI通过后按授权提交合入，源码交付与实验完成/财务依赖/在线状态分开。不因为阴性结果继续自动扫描参数；后续新假设另行记录。

三个块可在一次连续任务中执行，不设任意12小时等待，不等新交易日。当前授权只执行设计文档，不开始这三个实施块；研究预计耗时待实现后以固定小样本吞吐估算，不能承诺尚无依据的完成时间。

## 10. Verification Plan / 多轮审核（F-009）

实现测试最多集中到两个直接文件`test_fundamental_screen.py`、`test_fundamental_timing.py`，引用共享小fixture，不复制旧研究全快照。必要验证：

1. 市值单位/边界、key唯一、输入hash漂移、错candidate拒绝；T+1/date-only可见、累计改单季、财报修订版本及180日过期。
2. 正负/小基数、金融不适用、未知不当FAIL、无源不伪造P2/P3；首次入选不看成交/收益，退池不删账户。
3. 风险优先、两日趋势破位中断、T2一次减仓/实际补回/再武装、UNKNOWN非false、no-fill不消耗状态；同日不双向。
4. 每腿费用、现金不足、board lot、T+1、停牌、方向涨跌停、终点不可清算；减仓成本守恒，不从factor生成账户权益。
5. 新特征预热不依赖PIT历史掩码、当日新买入仍校验资格；旧helper默认行为小型合同测试，不全量重跑旧实验。
6. 1-vs-8、父唯一seal、失败chunk恢复、三组共同paired键、指数缺失、6格family不缩水、source前outcomes_read=false、exact retry身份。

本次设计交付审核记录（不是实现测试）：

- 第一轮：输入与事实核验。修正“R8可直接运行”的过强表述，拆成source可用、WSL manifest一致、旧入口不兼容；明确R8未新增财报PIT。
- 第二轮：经济与因果审查。明确首次入选不等首次成交、后续退池不平仓、财报日期精度及版本、小基数不隐性过滤、T2无连续减仓/凭空补资金、终值UNKNOWN不删除；修订20:00截点与财务前史范围、区分减仓no-fill与事件边沿失效、补齐新增源码identity清单。
- 第三轮：范围与统计审查。仅6项对BH、指数为背景、数据缺口不阻断P1；分清日均差区间与终值目标；检查主蓝图/前批计划引用一致，不把设计通过写成实现完成。
- 最终自动检查：本文件F1、主蓝图F2、前批F1 validator及`git diff --check`；实际结果见提交/PR检查摘要，不预写尚未执行的PASS。

### DESIGN-COMPLIANCE-001

无简化交付：本文只承诺设计交付，两个财务组数据未交付和实现未开始均明确；无静默错误：未知/缺失/终值不可用可见；无擅改业务：保留独立资金、R8、六池和对应指数、不接其他模块；无新增审批：数据正确性检查限受影响输入，研究阴性/样本功效均为报告，不阻止开发合入。

## 11. Risks / Production Gates / 回滚

主要风险：财报历史版本不可恢复、低基数假加速、行业可比性、筛选入选时点差异、特征热身缺口、频繁6%风险退出、T2补回成本及同一历史多次探索。各自在source/report明确计数和限制，不转为监控平台。根本目标仍是同股成本后增量，不把低回撤或部分股票获利当普遍有效。

生产DDL/DML、依赖安装、dataset/profile激活、服务重启、在线发布均noop；无需后端重启。设计回滚只回退本次文档提交，既有artifact不动。将来研究失败保留immutable evidence，不覆盖旧产物。财务补充由数据窗口负责，本模块只消费合格输入，不反向要求QE/HMM改架构。

## 12. Design Acceptance Index

| design_item | 设计验收条款 |
|---|---|
| F-001 | R8身份、source-only检查范围和可直接使用边界 |
| F-002 | position_timing独立、零DB/runtime/其他模块修改 |
| F-003 | 三筛选组、财报PIT/schema/单位/缺失语义 |
| F-004 | 人口、首次入选、共同起点及无幸存者静默删除 |
| F-005 | 两完整政策、成交、成本、风险、再入场和终点 |
| F-006 | 只对BH六假设、指数背景、收益/统计/覆盖边界 |
| F-007 | 最小复用、不可变artifact、WSL确定性并行 |
| F-008 | 三块连续实施、数据需求分组、不开跑的本次边界 |
| F-009 | 多轮审核、直接测试和设计符合性 |

## 13. Design Acceptance Matrix

矩阵验收设计与当前 P1 实现映射；`DESIGN_VERIFIED` 仍只表示条款闭合，实际运行证据见§14。财务数据待交付按§2/§4处理，不隐藏在`gap_or_exception=none`中；该列none只表示设计合同无未定义条款。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §2、§8；`fundamental_screen.open_r8_candidate_identity`、`fundamental_timing_benchmark.prepare` | test: `backend/tests/position_timing/test_fundamental_screen.py`；artifact: §14 request source audits | DESIGN_VERIFIED | none |
| F-002 | §3/§11；新增文件仅在`backend/services/position_timing`及直接测试 | test: `backend/tests/position_timing/test_isolation.py`；artifact: §14 receipt 六项 side-effect 均 false | DESIGN_VERIFIED | none |
| F-003 | §4；`fundamental_screen.py` | test: `backend/tests/position_timing/test_fundamental_screen.py`；P1 四态和 P2/P3 typed unavailable | DESIGN_VERIFIED | none |
| F-004 | §5/§7；`p1_mask_for_symbol`、`_replay_symbol_task` | test: `backend/tests/position_timing/test_fundamental_timing.py`；artifact: §14 coverage | DESIGN_VERIFIED | none |
| F-005 | §6；`fundamental_timing.py`及既有成交/guard复用 | test: `backend/tests/position_timing/test_fundamental_timing.py`；artifact: §14 fills/terminal evidence | DESIGN_VERIFIED | none |
| F-006 | §7；`fundamental_timing_benchmark._build_report` | artifact: §14 bundle `report.json`；六格family、逐股、六池、年度和覆盖结果 | DESIGN_VERIFIED | none |
| F-007 | §8；`fundamental_timing_benchmark.py` | test: `backend/tests/position_timing/test_fundamental_timing.py`；artifact: §14 1-vs-8、sealed chunks、inspect/exact retry | DESIGN_VERIFIED | none |
| F-008 | §9；P1三块已完成，P2/P3等待独立财务输入 | artifact: §14正式bundle；未引入旧策略矩阵或其他模块 | DESIGN_VERIFIED | none |
| F-009 | §10/§11；直接测试、旧策略回归、F1/F2 validator | test: `backend/tests/position_timing/test_fundamental_screen.py`、`test_fundamental_timing.py`；artifact: §14验证记录 | DESIGN_VERIFIED | none |

## 14. Implementation / 正式结果（2026-09-19）

### 14.1 实现和不可变身份

实现文件为`fundamental_screen.py`、`fundamental_timing.py`和`fundamental_timing_benchmark.py`，直接测试为`test_fundamental_screen.py`和`test_fundamental_timing.py`。正式 request canonical SHA 为`52218b6026c85073f5c0b11fa14dbaa8bb2c97f15a53ecb67c6186f5a7ba1bbd`，request 文件 SHA 为`49835e331c86c638d24e9be4dcd397b802bb84789c42ee28bb1032609a0f6388`；bundle manifest canonical SHA 为`7de1867179bd53d1c2fb9b9243ecd8cab573f75f8bdd362ee9d7ae53c5c52401`。源码提交为`6935ac0841238c4fa9f5c2b548e3a14893439d24`；最终文档/测试修订的PR身份以合入记录为准，不能用后续文档提交冒充该研究计算源码。

运行环境为 WSL2、Python 3.10.19、pandas 2.2.3、numpy 2.1.2、pyarrow 21.0.0，environment SHA 为`677d5fd6f1a188b86a0a28a8abf667d4d698c9a1296e93d7713d647c8da64626`。R8 factor audit覆盖5,144股、7,970,157行，invalid/insufficient均0；factor series SHA 为`fc6db8af41ab0a5550da37ed3b02f5f90bb449eab558d7a724d3436bcc4043d6`，restatement coverage完整。市值源8,124,082行、5,340个物理symbol，候选人口缺失symbol为0。preflight时`outcomes_read=false`，收益读取只发生于`RUN_AFTER_FULL_SOURCE_PREFLIGHT`。

16股固定样本的1进程与8进程结果逐股hash完全一致，audit SHA 为`30266eacfd00c72fa90ab55a861eacc4e1b5f3332b284c31261874e88ff95933`。正式运行完成41个sealed chunk（末块24股），`inspect=VERIFIED`；相同request重试返回同一bundle的`ALREADY_MATERIALIZED`。receipt中database read/write、network、runtime action、service process control、corporate-action authority、account economics和broker clearing均为false；research worker process为true。

### 14.2 覆盖和结果

P1日级状态为expected 10,087,384、PASS 3,983,142、FAIL 6,087,608、UNKNOWN 16,634、NOT_APPLICABLE 0；公共特征ready后的enrollment eligible为3,778,624个symbol-date。最终4,360股入选、784股正常未入选，首次可判定入选前的`ENROLLMENT_UNKNOWN`为0；4,335股有双方可清算终值。P2/P3因正式财务PIT快照未交付，四格保持`FINANCIAL_PIT_INPUT_NOT_DELIVERED/UNAVAILABLE`，六假设family没有缩水。

全市场动态池的 T1 复合账户收益为+5.75%，同股 BH 为+142.03%，沪深300价格指数背景为+43.55%，Timing-BH为-136.28个百分点；T2分别为+5.43%、+142.03%、+43.55%和-136.60个百分点。逐股可清算终值方面，T1相对BH中位数为-27.05个百分点、胜率27.34%；T2中位数为-26.75个百分点、胜率27.96%。底部5%对总体均值的贡献约-28.9个百分点，说明左尾不是可忽略的小样本。

全市场配对日收益差的T1点估计为-5.2255 bps，nominal 95%区间[-9.6755,-1.0436]，六假设family-wise区间[-11.2225,+0.6081]；T2点估计-5.2402 bps，nominal区间[-9.6789,-1.0454]，family-wise区间[-11.2301,+0.6029]。两项按冻结0 bps经济阈值均为`INCONCLUSIVE`，不是`NEGATIVE`，但逐股、六池和点估计均没有显示超过BH。`selected_trial_count=0`，不进入card、alert、registry/current或serving。

中证300/500/1000、科创50/100视图均保留各自指数背景；两个政策在六池的累计Timing-BH均为负。科创100价格指数源在共同区间不可用时保持typed unavailable，没有改用沪深300替代。年度cohort只作诊断，不从其中回选年份、阈值或政策。

### 14.3 结论与下一步边界

本轮已经回答“50～500亿元初筛后，冻结T1/T2是否能超过同股长期持有”：当前R8 P1证据不支持，且经济量级明显落后。不能把family-wise区间跨0解释成策略可能已有效，也不能据此扫描趋势、减仓比例或止盈阈值。P2/P3是否改变适用人口仍是未回答问题，唯一正当后续是由数据窗口交付§4.1的严格财务PIT快照后，以同一冻结六格family补齐；在此之前不重复P1、不接QE/HMM/Agent、不训练模型，也不修改在线L1/L1a。
