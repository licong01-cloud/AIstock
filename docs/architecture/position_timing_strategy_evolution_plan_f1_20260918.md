# Position Timing 策略演进与横向验证长任务（F1）

> 版本：v1.0；日期：2026-09-18；状态：`DESIGN_ONLY_NOT_EXECUTED`
> 任务：`PT-NEXT-021 / CLOSE_CASH_STRATEGY_EVOLUTION_V1`
> 所属主蓝图：[持仓与自选池择时建议系统](position_timing_advice_f2_redesign_20260903.md) §9.24。
> 本次仅交付设计与规划。预计后续实施、回放和审核合计 **14～18 小时**，不是已经运行、固定时长保证或自动开始授权；不为凑时长等待行情。

## 1. Background / 目标与现状

终极目标是对用户指定的同一只自选股或持仓股，提供成本后优于直接长期持有的买入、持有、减仓、退出及再入场建议。低回撤、低仓位、更多交易或胜过指数均不能替代“同股择时增量”。不保证所有股票、年份均盈利，也不宣称本轮会找到最优规则。

上一轮 `pattern_close_cash_benchmark_v1` 已离线运行，request/bundle identity 为 `745c4ea3b5038532535bb86c71ac192891ac30cbac532ae6f81c543511c9f2ff`。2026-09-18 只读核验：其源码 [PR #4906](https://github.com/licong01-cloud/AIstock/pull/4906) 为 OPEN，CI verdict SUCCESS，尚非 main 能力；本次文档合入不代替该代码 PR 的授权或合入。原 worktree 与冻结 artifact 保留。

已知结果（不是新实验）：5,144 个身份、5,138 条路径、161 个分块；4,807 只完整配对股票中 1,613 只胜过长期持有（33.56%），收益差中位数 -26.89 个百分点。平均仓位 12.41%，持仓时间占比 14.58%，持仓条件下仓位均值 83.30%；1,000 万元账户没有预算/最小买入数量失败。低暴露主要来自长时间空仓，而非每次买得太少。

62,669 次确认入场中 34,112 次（54.43%）被价格 guard 拒绝。源码仍使用原突破日收盘作后续确认单参考价、100 bps 追价限制及 300 bps 开盘缺口限制；这是 CLOSE 成交研究值得拆解的政策组合，不等于已经证明 guard 错误或取消后收益提高。27,198 次成交卖出中，风险退出 19,797 次、加速放量退出 7,401 次。加速退出的盈利条件实际是整账户权益大于初始本金，不是本轮持仓盈利；调整必须是显式新策略，不静默修正 R0。

完整人口含未知成员日；上轮六池收益表是 `paired_observed_only` 条件样本诊断，不是完整全人口收益。科创50前21日无可评价账户与特征前置 PIT 掩码相关；科创100官方指数序列在 r5 中缺失。不得补零、删股或以沪深300替代科创100。

## 2. Documentation Discovery / 已核接口与最小复用

下列签名以 main `05ba6b8ab67e2007bb329752e426b306893fc0b5` 和 PR #4906 冻结实现 `f2f2fda533d8840bc3cec7c7e4f3914394ab2056` 为发现依据；实施开始时重新核验 main，不复制旧 worktree 覆盖新代码。

| 能力 | 实际接口或源位置 | 复用与缺口 |
|---|---|---|
| 日线/交易日/PIT/factor | `action_value_data.DailyCandidate.open/bars`；`pattern_universe_benchmark.open_candidate_pool_memberships`、`_qlib_adjusted_bars`、`_audit_qlib_adjusted_factor_integrity` | main 已有，只读 r5；不调用 DB/API，不开发数据生产 |
| 原形态事件 | `pattern_strategy.pattern_feature_frame`、`breakout_observed`、`advance_entry_event`、`acceleration_volume_exit` | R0 原参数与事件语义保留，不复制改造共享默认 |
| 独立现金账户 | PR #4906 `pattern_close_cash_replay.replay(symbol, bars)`、`execute(account, *, symbol, bar, ordinal, side, reference, guarded, risk=False)` | 当前只有全量 SELL，无部分数量 API；需在择时离线侧显式扩展，旧入口默认不变 |
| 研究生命周期 | PR #4906 `pattern_close_cash_benchmark.prepare(*, timing_root, repository_root, parent_pattern_bundle, candidate_root)`、`run(path)`、`inspect(root, *, request_hash=None)` | 当前 CONTRACT 固定 R0，**没有**策略列表或 variant CLI 参数；新合同/分派尚待实现 |
| 分组与收益 | PR #4906 `pattern_close_cash_report.build_report(chunks, *, memberships, calendar, index_frame)`；`comparison(timing, hold, index)` | 复用收益口径，新增 strategy_id 维度和共同起点对照；不另建回测平台 |
| 费用与规则 | `policy.component_cost_for_parent_notionals`、`frozen_price_guard_policy`、`frozen_exit_guard_policy` | 分项费用、手数与历史涨跌停保持；策略侧变体只用 timing-owned 快照，不写共享 defaults |

代码依赖处理：实施时若 #4906 已合入，从最新 main 复用；若仍 OPEN，明确报告源码依赖，先按当时授权完成其审查/交付，不能假装接口已在 main，也不让文档 PR 顺带合入代码。本规划不启动这一步。

## 3. Contracts / 共同研究合同

### 3.1 固定输入与身份（F-001）

- candidate：`X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260831-qe_hmm_full_v2-direct-20260915-r5-candidate`；manifest 文件 SHA256 `7b5402c38b4b279140375fa6517595f88bdfb472e617faf8b032c04f0d33d1c1`。
- 历史：2018-08-01～2026-08-31，5,144 个候选身份全部保留；停牌、退市、ST 的历史资格由冻结 PIT 语义处理，不因结果删除股票。
- 父 pattern bundle：`F:/Dev/AIstock_model_artifacts/position_timing_advice_v1/research/pattern_strategy_v1/bundles/014547fa46994c7b01b809580a8db5d4cce449c723661454bb3a29e6f0d9eeb2`；manifest canonical SHA `7481afad8bcb6bde45cfc4fbe90fc53ac14719ef048aa464d91498dce0da9541`。
- Qlib contract SHA `e0f3f1d67f782965c1647213b6bbe8d879cb3b7e416c3f341c01cc9d98460daa`；factor audit SHA `1131d05c5fa7f4c84d50eaec428cdd8359ee2cf1119ccea12afbef806ccc36bd`；restatement audit SHA `48b78cc7237fc02d4c0d840f3ef63c7cb9ddc6ef46325330b8b7acc3b2dd32ac`。
- 新 request 必须绑定文件与 canonical 身份，禁止沿用 active profile 或覆盖旧 artifact。
- OHLC×factor、volume÷factor、下游 factor=1；数量是虚拟复权单位，不模拟分红、配股认购、股份到账或券商清算。material factor 变化 >10 bps 只统计，不重新要求公司行动绑定；非正/非有限/历史不足/重述漂移/身份错误继续 fail closed。

### 3.2 现金、时钟与成交（F-002）

每股、每策略与 BUY_AND_HOLD 各有独立初始 10,000,000 元；自然复投、现金不计息、无杠杆/追加资金/跨股调拨。买入额加费不超过可用现金，减仓所得留在该股票账户。策略仓位不足不是通过外部资金补足。

T 日收盘观察，最早 T+1 收盘尝试成交；不是观察 T 收盘后仍以 T 收盘成交。无市场冲击/额外滑点为冻结研究假设，不是实盘承诺。继续使用原分项费用与父订单最低佣金；停牌、方向涨跌停、历史限制数据缺失、T+1 与板块合法数量约束不放宽。guard 开盘条件的对照仅改变研究政策，不取消交易所限制。

主终值为共同终日 MTM，另报终日可清算净值/无法清算原因；不提前截断以便卖出。BUY_AND_HOLD 在共同评价起点后首次合法机会买入，之后不主动调仓；起点前双方不隐含赠送仓位。

### 3.3 起点与可比性（F-003）

输出两套明确命名结果：①沿用各策略自然就绪起点的全历史描述，A0 用于与旧 receipt 对照；②主横向对照按每股各预注册策略所需的最晚**历史指标就绪日**共同起点，各账户重新以相同本金开始，BH 同起点买入。共同起点只按历史输入窗口确定，不按首个获利信号或事后完整终值选择。指标不足股票保留 unavailable 身份，不从人口清单消失。

本批保留既有 PIT 特征掩码，不将“历史特征可见性与交易资格解耦”混入策略效果；该方法学改进排入后续独立对照。共同起点后任何路径缺失不能静默取交集：保留 expected/observed/unknown 计数，完整人口结果 unavailable；仅另报条件配对诊断并明示选择偏差。不同策略配对覆盖不同时不按这些点估计直接排名。共同日期切片亦不冒充共同现金起点重放。

## 4. 冻结候选与机制（F-004）

本批共 **10 条策略路径 A0～A3、B0～B2、C0～C2，外加一个共同 BH comparator**。A0 是历史控制，9 个新增候选全部登记；不从中挑最佳再交叉组合、不扫描参数网格。参数是机制型首版假设，不是文献已证最优值。

### 4.1 A 组：入场执行政策 2×2（优先）

| 策略 | 买入参考价 | CLOSE 成交仍检查开盘缺口 | 其余 |
|---|---|---|---|
| A0 | 原突破日 adjusted close | 是，原300 bps | 原 R0 完整控制 |
| A1 | 回踩确认日 adjusted close | 是，原300 bps | 只改参考锚点 |
| A2 | 原突破日 adjusted close | 否 | 只移除买入侧开盘缺口政策 veto |
| A3 | 回踩确认日 adjusted close | 否 | 两项同时改变 |

100 bps 追价、近涨停规则、REDUCE 分档、卖出 guard、风险退出、加速退出参数均保留。A2/A3 不伪造 open_gap=0：用显式研究 policy 开关或等价纯适配分支，并测试除这一条件外行为一致。分别报告 A1−A0、A2−A0 和交互效应 `(A3−A2)−(A1−A0)`；改善成交率不直接等于提高收益。

### 4.2 B 组：三种可恢复入场的趋势策略

统一 adjusted 日线；SMA 使用完整观察窗口，ATR20 为20个有效 true range 的简单均值（含前收盘，不用未来或填零）。窗口按冻结candidate全局交易日计，缺失/停牌不交易，所需窗口有无效值则指标typed unavailable，不压缩为跨缺口的“最近N条有效记录”。B 组采用 A3 的买入执行政策，以当次决策收盘为参考；保留共同 frozen risk exit（优先）与普通卖出执行规则，禁用原加速放量清仓。

- **B0 趋势持有/破位退出/恢复再入场**：现金态 `close > SMA60` 且 `SMA60(T) > SMA60(T−5)` 时买；持仓连续两个可评价交易日 `close < SMA60 − ATR20` 时退出。风险退出优先；退出后最早下一决策日重新评价同一入场条件，无额外冷却/底部形态条件。
- **B1 中期突破/跟踪退出**：现金态 close 严格超过此前60个交易日的最高 high（不含T，窗口完整），并满足 B0 的趋势条件时买。持仓止损线 `max(上一止损线, 自入场以来最高close − 3×ATR20)`，只升不降；T 收盘跌破该线，T+1 才卖，不回填盘中成交。首次止损线在成交日收盘建立、下一决策日才可触发。退出后同样突破条件可再入场。
- **B2 上升趋势内回踩恢复**：现金态先满足 B0 趋势；T−1 close 不高于当日 SMA20，T close 高于 SMA20 且高于 T−1 close 时买（两日均趋势有效）。不再附加底部、放量、三均线等条件；退出与 B0 相同。量能只作预设描述性分层，不成为选股/入场条件。

这些是完整策略对照，不把 B−A 的多处差异解释成某一个阈值的因果效果。B0/B2 的双日破位遇缺失有效指标时不能把不连续观察凑成连续两日；停牌不成交且不伪造指标观察。B组普通买卖计划只在下一交易日有效，失败后从新决策日重新判断；风险退出沿用原pending-risk持续至可成交的语义。每日每账户最多一个方向，完全退出当日不再反手买入。

### 4.3 C 组：单笔盈利与部分止盈

均以 A3 为固定父策略，不依据 A 组成绩选择父版本；入场、原6% frozen risk exit、原加速/量能阈值不变。

- **C0**：只把加速退出的盈利条件改为 `Q_t × adjusted_close_t − estimated_sell_fee > remaining_cost_t`，其中Q为本次持仓的剩余虚拟单位，remaining_cost含对应买入费、预估卖出费只在左侧扣一次，不包含账户现金或其他已结束交易盈亏。不再要求账户收复初始本金。A3→C0 单独量化该口径变化。
- **C1**：C0 首次符合加速退出时只减持当时可卖数量的50%；同一持仓周期最多成功减仓一次，剩余仓位继续原风险退出，不再次按日减半。失败尝试不得消耗“已减持”状态；按原普通事件边沿/有效期决定重试，不能把旧信号无限携带。剩余数量仍受合法卖出单位限制；不能合法部分成交时记 typed no-fill，不强制清空。完全退出后状态复位，只有新R0入场事件才可再买，不在部分持仓期自动补回。
- **C2**：与C0相同入场/风险退出，但完全取消加速获利退出，继续持有至风险退出。C0/C1/C2 分别隔离“全卖/半卖/不因加速卖”的影响；不顺带改变成交量或涨速阈值。

部分数量按执行日 raw 等价数量与卖出规则取整，再映射虚拟单位，绝不将 source factor 变化当真实到账股份。分摊剩余持仓成本、已实现损益、每腿最低佣金和T+1须守恒。C1 的余仓趋势跟踪属于下一次可选对照，**本批不叠加**，以免把部分减仓和新退出混为一个原因；B组已覆盖结构/跟踪退出方向。

## 5. 横向评价与诊断（F-005）

| 维度 | 必须同时输出 |
|---|---|
| 个股 | 各策略、同起点BH、沪深300；终值、总收益/CAGR/最大回撤、净超额金额及百分点、费用、成交次数、时间仓位、条件仓位、unknown |
| 六类股票池 | 全市场、沪深300、中证500、中证1000、科创50、科创100；逐股路径再按滞后一日PIT归组，不建立共享总资金账户 |
| 指数基准 | 全市场/个股用000300.SH；五指数池依次000300.SH/000905.SH/000852.SH/000688.SH/000698.SH；缺失不替代、不阻止本池策略−BH |
| 日期 | 各自完整可评价期间、所有策略共同现金起点、统一2023-08-08～2026-08-31区间的持续账户收益切片、逐年；指数不存在的早期不回填 |
| 人口/分布 | 动态PIT等权日百分比收益合成指数、年初PIT固定成员子群；超额均值/中位数/分位数/胜过BH占比、最好最差尾部贡献、expected/valid/unknown |
| 机制 | 信号数→确认→拦截原因→成交→持有→各退出→再入场间隔；成本与现金拖累、持仓收益贡献、卖出后5/20/60日表现 |

池级指数是虚拟独立账户的日收益合成，不是可执行共享组合；所有账户同本金不等于可以汇总百分比时简单累加净值差。官方指数为价格指数，股票为Qlib复权信号路径，两者不伪称同口径总回报；同股BH才是主要择时基准。完整人口 unavailable 与 paired-observed-only 分表；不得将后者冠名完整全市场业绩。

退出后表现是事件描述，不使用未来最高/最低点决定当时动作；重叠事件不当独立样本。入场拦截后收益同样只能诊断，不能将因股数/现金/费用反馈改变的路径用静态“加回错失涨幅”冒充真实反事实。

统计为 `EXPLORATORY_HYPOTHESIS_GENERATED`，全时段已观察；滚动分年不变成未见holdout。主结论先报告同股终值差及覆盖、年份/人口稳定性，不因Sharpe/低回撤单独宣布有alpha。可识别的比较固定输出区间：25交易日时间块、5,000次重抽、seed=20260918，同一次时间抽样共用于所有策略/股票，避免把股票和事件误当独立；六池×9新候选=54项策略−BH为一个family，名义95%及Bonferroni双侧 `1−0.05/54` 区间并报。无法识别的比较记录typed原因，family大小不因缺失缩小。A0、机制差分、年度/量能分层只描述，不参与selected；若额外作显著性宣称，须纳入新的显式family而非暗中扩试。

该family区间估计的是共同可评价日期下**池级配对日收益差均值**，不是逐股终值胜率或复合收益差区间；必须标明estimand。覆盖不足不能宣称完整人口SUPPORTED。证据分型/功效/成本敏感性是报告，不是研究准入或工程合入门禁。所有结果无论正负均收录；本批不自动选出上线策略，selected=0。

## 6. 研究依据、边界与后续方向（F-006）

| 依据 | 支持的研究思路 | 不能推出 |
|---|---|---|
| [Han、Yang、Zhou，2013](https://www.cambridge.org/core/product/identifier/S0022109013000586/type/journal_article) | 波动分组股票组合的均线择时研究，支持把趋势持有作为可检验候选 | A股逐股MA60/ATR参数一定盈利 |
| [Kaminski、Lo，2014；MIT作者稿](https://dspace.mit.edu/entities/publication/bb69ca4b-0cdc-487f-831d-63b2e84fafee) | 止损可能增加或减少价值，应与持有政策成对评价 | 固定止损必然改善绝对收益；其指数期货证据可直接外推个股 |
| [George、Hwang，2004作者论文](https://www.bauer.uh.edu/tgeorge/papers/gh4-paper.pdf) | 52周高点与横截面动量，为突破/延续提供机制背景 | 60日突破与无杠杆单股择时已获验证 |
| [Zhu等，2015](https://arxiv.org/abs/1504.04254) | 中国指数简单技术规则研究提醒成本与多重尝试可能消除表面盈利 | 当前个人费率/个股/时期已被证明无效 |

上述文献仅给机制与反证，不据此注册庞大参数搜索。优先级为：执行/退出诊断 → 本批10路径 → 历史特征可见性与交易资格解耦 → 固定基础条件分层 → 模型动作价值增强。后两项不在本批实施：50～500亿元市值及业绩增长加速须先核历史PIT、财报真实可见时间与单位；过滤后的择时必须对同一过滤人口BH，不能把选股优势算择时alpha。因子/模型以“相对WAIT/HOLD的成本后动作增量”为目标，HMM/新闻/Agent/QE融合均不在关键路径。无需训练才能开始规则研究，规则优劣也不能靠文献背书或提高仓位来保证。

## 7. Implementation Plan / 14～18小时连续任务（F-007）

仅三个实施块，块间不设人工审批或收益正值门槛。以下是后续执行计划，不是本次执行记录。

| 块与预计用时 | 实施任务与实际依赖 | 验证检查与反模式 |
|---|---|---|
| 一：合同与最小实现，4～5h | 同步main核#4906；冻结本表10策略、输入/费用/起点/hash；复用§2接口扩展离线policy分派、部分减仓与多策略报告；保留A0自然起点等价路径。先做机制诊断，不据其收益改变候选集合 | 首轮审核时钟/现金/单位/隔离；定向测试只保护账户守恒、T+1、涨跌停、因果、部分卖出、身份与API契约。不新增平台、依赖、调度、参数优化器或低价值重复快照 |
| 二：按A→B→C完整历史实验，7～9h | 新timing-owned命名空间；先source-only audit，再10策略及BH全市场回放；每股数据/特征缓存共用，账户独立，有界chunk/checkpoint；输出逐股、六池、年度/共同起点/统一期间 | source通过后才读新收益；完整登记9个新候选及失败。第二轮审核检查收益计算/复权一次/缺失状态/未成交/重复减仓。不因A表现好坏临时跳B/C；不等实时行情/HMM/新交易日 |
| 三：横向比较与最终审核，3～4h | 汇总§5全部维度、机制诊断、时间块区间与成本披露；inspect→同输入exact retry；更新蓝图及不可变结果清单；按届时授权处理代码PR/CI | 第三轮审核源→request→chunk→bundle→报告hash和设计逐条符合性。A0自然起点回放须复现旧数值；共同起点A0明确新estimand。所有失败/不足也入总表，不以最好池代表全部；无上线激活 |

估时基于10策略全历史和源码扩展，实际受机器吞吐/CI影响；不保证12小时内结束，也不空等至12小时。source-only问题交回数据窗口；特定指数缺失仅使该指数对照不可用。新代码真实BUG登记独立Issue，只修position_timing及直接测试；多轮修复后重跑受影响不可变新request，不覆盖旧失败证据。未完成任务标明明确进度及续跑身份，不称全量完成。

拟新增生产源码上限为两个离线文件（策略集合与编排）；优先扩展已有report/replay的显式研究接口；不改在线L1/API/router/shared defaults。仅增加最小定向行为测试，复用现有测试lane；不因多策略而复制十套fixtures/快照。新schema/CLI具体签名在实现时随合同冻结，§2未存在的参数不能先写成可执行命令。

## 8. Verification Plan / 合入与交付

文档阶段：主蓝图F2与本文F1 validator、链接/identity/算术检查、`git diff --check`，三轮复核；它们只证明设计闭合。

2026-09-18文档审核记录（不是新实验记录）：第一轮核对现有request文件SHA、bundle manifest、report计数与PR#4906实时状态，纠正蓝图旧阻断/错误完成态；第二轮修订共同现金起点、部分卖出状态、指标缺口不能压缩窗口及统计estimand；第三轮检查三文档链接、优先级/历史版本适用范围、设计验收映射。首次validator指出验收证据引用不具体，补齐真实artifact及明确标为未来的定向测试路径后，F2 51/51、本文F1 8/8、历史F1 10/10通过。文档相对链接与diff检查通过；不把这些结果写成代码测试或新策略运行成功。

未来代码阶段：定向现金/数量/时钟/市场限制/缺失/immutable/旧行为测试，changed-file L0与CI、必要模块回归；避免为了每个实验新增测试框架。正式结果必须具备prepare/source audit/run/inspect/exact retry和输入/产物hash，不以测试绿或source覆盖冒充收益验证。

完成报告：源码/PR/CI与合入状态、所有候选request/bundle/结果、同股BH和对应指数各维度、覆盖与排除原因、outcomes_read阶段、retry身份。database/network-market/runtime/process/serving均false；仓库协作网络与行情网络分开记。

## 9. Scope / Risks / Production Gates（F-008）

本轮不改QE/Selection/Advisory/Paper/数据准备，不读写DB、不抓数、不改candidate/profile，不控制backend/worker/scheduler，不写卡、alert、交易、N0/current或serving。不引入公司行动账户清算。仅用户可执行backend重启，纯离线实验按设计不需要重启。

历史已观察、复权虚拟单位/价格指数差异、CLOSE无冲击假设、未知估值、共同窗口缩短以及多重尝试均须披露。成本后正结果仍是历史探索证据，不是最优策略或未来收益承诺。负结果同样完成研发，不阻塞下一机制研究；优先避免研究循环被未来行情或强制HMM耦合拖慢。

## 10. Design Acceptance Index

F-001～F-008定义见上文。`DESIGN_VERIFIED`仅表示本文设计闭合，所有新候选实现/实验均待执行；不将设计通过计为实现完成。

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §3.1输入与复权合同；未来择时离线编排 | `backend/tests/position_timing/test_pattern_universe_benchmark.py`现有source合同；新request测试待执行 | DESIGN_VERIFIED | none |
| F-002 | §3.2与§4.3账户/成交规则 | 拟扩展`backend/tests/position_timing/test_pattern_close_cash_benchmark.py`现金/部分减仓/T+1测试，未执行 | DESIGN_VERIFIED | none |
| F-003 | §3.3共同起点与PIT | 拟扩展`backend/tests/position_timing/test_pattern_close_cash_benchmark.py`起点/因果/缺失测试，未执行 | DESIGN_VERIFIED | none |
| F-004 | §4十条策略 | 拟新增`backend/tests/position_timing/test_pattern_strategy_evolution.py`状态行为测试，未执行 | DESIGN_VERIFIED | none |
| F-005 | §5横向评价 | 拟扩展`backend/tests/position_timing/test_pattern_close_cash_benchmark.py`分组/公式/覆盖测试，未执行 | DESIGN_VERIFIED | none |
| F-006 | §6文献与后续边界 | 设计依据artifact: https://arxiv.org/abs/1504.04254；§6原始文献与§1本地诊断，不代表策略收益验收 | DESIGN_VERIFIED | none |
| F-007 | §2/§7/§8最小实施与三轮审核 | 设计交付artifact: docs/architecture/position_timing_strategy_evolution_plan_f1_20260918.md；未来run/inspect/retry/CI未执行 | DESIGN_VERIFIED | none |
| F-008 | §9隔离 | 拟扩展`backend/tests/position_timing/test_pattern_close_cash_benchmark.py`identity/零副作用测试，未执行 | DESIGN_VERIFIED | none |
