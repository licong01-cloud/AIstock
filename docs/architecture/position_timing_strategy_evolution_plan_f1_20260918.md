# Position Timing 策略演进与横向验证长任务（F1）

> 版本：v1.1；日期：2026-09-18；状态：`R7_WSL_PARALLEL_DESIGN_APPROVED_NOT_EXECUTED`
> 任务：`PT-NEXT-021 / CLOSE_CASH_STRATEGY_EVOLUTION_V1`
> 所属主蓝图：[持仓与自选池择时建议系统](position_timing_advice_f2_redesign_20260903.md) §9.24。
> 本次只交付 r7/WSL/并行重设计，不启动新的正式回放。原 r5 串行任务已按用户指令终止；8 个已封存分片只是中止检查点，不是研究结果，也不得进入横向比较。

## 1. Background / 目标与现状

终极目标是对用户指定的同一只自选股或持仓股，提供成本后优于直接长期持有的买入、持有、减仓、退出及再入场建议。低回撤、低仓位、更多交易或胜过指数均不能替代“同股择时增量”。不保证所有股票、年份均盈利，也不宣称本轮会找到最优规则。

上一轮 `pattern_close_cash_benchmark_v1` 已离线运行，request/bundle identity 为 `745c4ea3b5038532535bb86c71ac192891ac30cbac532ae6f81c543511c9f2ff`。[PR #4906](https://github.com/licong01-cloud/AIstock/pull/4906) 已合入 merge commit `e3e156496f6cb2878c3268681ddd535cc4c754d2`，它是本轮最小复用基础，不代表策略已支持 alpha 或进入在线运行。

已知结果（不是新实验）：5,144 个身份、5,138 条路径、161 个分块；4,807 只完整配对股票中 1,613 只胜过长期持有（33.56%），收益差中位数 -26.89 个百分点。平均仓位 12.41%，持仓时间占比 14.58%，持仓条件下仓位均值 83.30%；1,000 万元账户没有预算/最小买入数量失败。低暴露主要来自长时间空仓，而非每次买得太少。

62,669 次确认入场中 34,112 次（54.43%）被价格 guard 拒绝。源码仍使用原突破日收盘作后续确认单参考价、100 bps 追价限制及 300 bps 开盘缺口限制；这是 CLOSE 成交研究值得拆解的政策组合，不等于已经证明 guard 错误或取消后收益提高。27,198 次成交卖出中，风险退出 19,797 次、加速放量退出 7,401 次。加速退出的盈利条件实际是整账户权益大于初始本金，不是本轮持仓盈利；调整必须是显式新策略，不静默修正 R0。

完整人口含未知成员日；上轮六池收益表是 `paired_observed_only` 条件样本诊断，不是完整全人口收益。科创50前21日无可评价账户与特征前置 PIT 掩码相关；科创100官方指数序列在 r5 中缺失。r7 不因版本切换而自动修复这些研究覆盖问题，不得补零、删股或以沪深300替代科创100。

第一次 PT-NEXT-021 实施在独立 Windows worktree 形成两个未合入提交 `21d25dd807575a7a07c6fb9e2f19537c1132be5d`、`5f28e22faf26ec930dcb70d6bd16ea7239ece871`，定向 26 项通过，并修复 BUG-1574：未知终值估值必须序列化为 typed `null`，不能把 `NaN` 写入 canonical JSON。其 r5 正式 request `a67dddf8f8e5dce87db1c591493b9746bc0da805c79bb8551e9a2a5c5016452a` 只完成 8×128=1,024/5,144 股后被用户明确终止；分片 0000～0007 均有 manifest、没有未封存分片，但不存在最终 bundle/report/inspect/exact retry。因此它只证明串行工程路径可运行，不证明任何候选策略收益；旧分片永久留在 r5 命名空间，r7 不读取、不续跑、不覆盖。

本轮改用已激活但在 request 中显式冻结的 r7 candidate，并在 WSL2 ext4 上执行。r5 与 r7 的 63,312 个日线 candidate 文件、6 个股票池文件、2 个指数上下文文件及 11 个择时 source-authority 文件逐文件一致；当前策略实际输入差异只有 r7 停牌表新增 `688766.SH` 的 2025-11-27～2025-12-05 七个 `S` 记录。r7 新补的 `daily_basic.volume_ratio` 不被本策略读取，策略量比继续由冻结日线 volume 因果计算。上述差异说明继续等待 r5 完成没有最终研究价值，但不授权把 r5 分片改名复用为 r7：最终结论必须在 r7 身份下重算全部 5,144 股。

## 2. Documentation Discovery / 已核接口与最小复用

下列签名以 2026-09-18 `origin/main` `0a784b1831ff2a8db61a7c45bce3b180ad76ea3c` 为发现依据；其中 #4906 已合入，BUG-1575 已把活跃 QE/WSL 绑定纠正到 r7。实施开始时仍重新核验 main，不能从旧 Windows worktree 直接运行正式 WSL 研究。

| 能力 | 实际接口或源位置 | 复用与缺口 |
|---|---|---|
| 日线/交易日/PIT/factor | `action_value_data.DailyCandidate.open/bars`；`pattern_universe_benchmark.open_candidate_pool_memberships`、`_qlib_adjusted_bars`、`_audit_qlib_adjusted_factor_integrity` | main 已有；只读 WSL r7 的冻结路径，不调用 DB/API，不开发数据生产 |
| 原形态事件 | `pattern_strategy.pattern_feature_frame`、`breakout_observed`、`advance_entry_event`、`acceleration_volume_exit` | R0 原参数与事件语义保留，不复制改造共享默认 |
| 独立现金账户 | `pattern_close_cash_replay.replay(symbol, bars)`、`execute(account, *, symbol, bar, ordinal, side, reference, guarded, risk=False)` | main 只有全量 SELL；未合入 PT-NEXT-021 分支已有部分数量原型，后续须重基于 main 审核，旧入口默认不变 |
| 研究生命周期 | `pattern_close_cash_benchmark.prepare/run/inspect` | main 合同固定 R0；未合入分支已有策略集合编排，但仍是 Windows 串行实现，不得直接视为 r7/WSL/并行完成 |
| 分组与收益 | `pattern_close_cash_report.build_report`、`comparison` | 复用收益口径，新增 `strategy_id` 维度和共同起点对照；不另建回测平台 |
| 费用与规则 | `policy.component_cost_for_parent_notionals`、`frozen_price_guard_policy`、`frozen_exit_guard_policy` | 分项费用、手数与历史涨跌停保持；策略侧变体只用 timing-owned 快照，不写共享 defaults |

实施时先把未合入的 PT-NEXT-021 代码重基于最新 main，保留 BUG-1574 typed-null 修复，再增加 r7 身份与确定性并行；不得把两个旧提交机械 cherry-pick 后直接运行。完成定向审核并形成干净源码提交后，即可从 WSL ext4 的原生干净 checkout 按该精确提交运行；不把PR合入或CI排队设置为研究启动门槛，最终交付前再完成CI与合入。Windows linked worktree 的 `.git` 指向 `F:/...`，在 WSL 中不可作为 Git 仓库；`/mnt/f` 主仓库也存在跨系统状态/性能风险，二者均不是正式运行源。

## 3. Contracts / 共同研究合同

### 3.1 固定输入与身份（F-001）

- 正式 candidate：`/mnt/wsl/aistock-qe-data-v1/releases/20260831-qe_hmm_full_v2-direct-20260918-r7-candidate`。manifest 文件 SHA256 `084ffe869dafe919e73e884afa6a833c497df4ab09e1bfb7f649993043bc6900`，canonical `dataset_manifest_sha256=c11e16ee15719c0b96af4a6fafcaa338858f541eedc6b3a0768b34621e202836`，deployment content SHA256 `372c41a147fe252596379878ebbeadc6601b195d744a144026a6308fdc73f257`。Windows controller path与远端路径只作同一发布的节点映射，不参与 WSL 运行时寻址。
- 历史：2018-08-01～2026-08-31，5,144 个候选身份全部保留；停牌、退市、ST 的历史资格由冻结 PIT 语义处理，不因结果删除股票。
- 父 pattern bundle：`/mnt/f/Dev/AIstock_model_artifacts/position_timing_advice_v1/research/pattern_strategy_v1/bundles/014547fa46994c7b01b809580a8db5d4cce449c723661454bb3a29e6f0d9eeb2`；manifest canonical SHA `7481afad8bcb6bde45cfc4fbe90fc53ac14719ef048aa464d91498dce0da9541`。父 bundle 只读少量合同文件，不承载全量计算。
- r7 calendar SHA `ce017cfbf1d9dde630c0d7f39e33b767e95293acd5258104f80491239826207a`；PIT instruments SHA `b4f3ceca16dbdf7745d907b6697d5aeea591ff29f4b7a2ea07c4879e97dd2dcf`；index SHA `875c9560e10c9a6d13f30f5fbfb44bc9aba16a3ef39a41fe1acd4a91e4f4c5dd`；suspend parquet/meta SHA `7f1895fe3c3a025708c1afa525d17e594f536180a86d6a9cf22b3c25d87bee3f` / `f56c1f4aa91a74eb30a086987bc22a41f0b36f1cd861f35844c2a8599c4326aa`；factor meta SHA `3574b589c84965cfca07f02dfa8dcdb8491e5a6bcd4fb49201ea98b6e07390c8`；coverage receipt SHA `dcee5835cdcbd4965d8937c7da80fbdee3aceb1e86a8069405410c264775815f`。
- Qlib adjusted-factor contract SHA `e0f3f1d67f782965c1647213b6bbe8d879cb3b7e416c3f341c01cc9d98460daa`。正式 `prepare` 重新生成并绑定 r7 factor audit/restatement audit，不能继承 r5 audit hash 充当 r7 证据。
- 活跃 profile 只用于发现 r7 路径；`prepare` 后 request 必须绑定上述路径、文件/canonical身份和源代码提交，`run` 不再解析可变 active profile。旧 r5 request、分片与 bundle 全部只读，禁止覆盖或纳入 r7 bundle。
- OHLC×factor、volume÷factor、下游 factor=1；数量是虚拟复权单位，不模拟分红、配股认购、股份到账或券商清算。material factor 变化 >10 bps 只统计，不重新要求公司行动绑定；非正/非有限/历史不足/重述漂移/身份错误继续 fail closed。

正式执行环境冻结为 WSL2 原生 ext4 checkout 与 `/home/lc999/miniconda3/envs/rdagent-gpu/bin/python`；request 记录 Linux/kernel、Python、NumPy、pandas、PyArrow 版本及 `environment_sha256`。研究 artifact 写入独立 ext4 根 `/home/lc999/data/position_timing_artifacts/position_timing_advice_v1`，不会逐 chunk 写 `/mnt/f`。完成 inspect 与 exact retry 后，只把已封存的 request/bundle/receipt 导出到 Windows timing artifact 根，逐文件 hash readback；导出不是激活、serving 或 current pointer 更新。

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

## 7. Implementation Plan / r7 WSL 确定性并行连续任务（F-007）

仅三个实施块，块间不设人工审批、收益正值、MDE 或最新交易日门槛。时间只是资源估算，不是等待时长或完成承诺；以下均为后续执行计划，本次没有启动 r7 `prepare/run`。

| 块与预计用时 | 实施任务与实际依赖 | 验证检查与反模式 |
|---|---|---|
| 一：代码收口与并行合同，2～4h | 在最新 main 上重做/审核未合入 PT-NEXT-021 变更，保留 BUG-1574 typed-null；将 candidate 常量改为§3.1 r7 显式身份；只在 `position_timing` 增加父写者/纯worker并行；形成干净提交后在 WSL ext4 建立该提交的原生checkout | 先以固定32股诊断集比较 `worker_count=1` 与 `8` 的 canonical 输出完全相同；包含 `688766.SH` 及沪深/创业/科创样本。该检查只验证并行确定性，不根据收益筛策略。失败即修代码，不降级为静默串行成功，也不增加审批平台 |
| 二：r7 WSL 全量回放，2～5h | 在 ext4 artifact root 执行 `prepare→run`；单进程 source preflight 后按固定128股 chunk、8个worker、最多16个在途symbol task 回放5,144股×10策略及BH；每个chunk封存后才推进 | source preflight通过后才能读取策略收益；任何worker异常取消当前chunk且不写manifest，已封存chunk可按同request续跑。不读取r5分片、不因A表现改变B/C、不等行情/HMM/新交易日，不写DB/active profile/在线仓库 |
| 三：汇总、审计与交付，2～4h | 父进程按canonical symbol顺序聚合，完成§5报告和单进程向量化bootstrap；`inspect→完全相同输入exact retry`；封存后导出Windows并hash readback；更新蓝图/结果，执行多轮审核、CI及合入后清理 | 核对source→request→chunk→bundle→receipt全链hash、完整人口/unknown及A0版本差异；所有正负结果入表，selected=0。只有封存结果可引用；不激活serving、不要求后端重启 |

### 7.1 并行执行合同

1. 使用 `concurrent.futures.ProcessPoolExecutor(max_workers=8, mp_context=multiprocessing.get_context("spawn"))`；worker 数与最多16个在途任务写入request，正式运行中不可自适应改变。WSL主机当前28逻辑CPU、约55 GiB可用内存，8 worker 是保守固定值，不把全部CPU占满。
2. 外层 chunk 边界仍为按 canonical symbol 排序的128股，chunk之间串行。父进程唯一负责 request/source identity、任务枚举、聚合、canonical JSON/Parquet、manifest和seal；worker不写artifact、不改registry/current、不访问DB/网络。
3. 首版由父进程按symbol加载已冻结bars并连同只读参数传给worker；单股约1,961日、有限列，序列化开销可控，且避免多进程共享/修改 `DailyCandidate.references`。worker只执行纯 `replay_strategy_set(symbol, bars, frozen_contract)` 并返回有界结果。
4. future完成顺序不得决定产物顺序。父进程只按预先冻结的symbol序列消费/排序结果；随机统计仍使用固定seed，浮点汇总顺序固定。并行与单进程的canonical hash必须一致。
5. 任一worker异常退出、返回非有限/错误schema或父进程中断时，当前chunk没有manifest即视为未完成；续跑只复用身份匹配且已封存的chunk，从整个未封存chunk重算，不拼接半成品。
6. bootstrap当前为单进程NumPy向量化，未有证据表明它是瓶颈，不再叠加第二层并行。source preflight同样保持单一确定性读者；本批不建设worker服务、队列、调度器、分布式框架或共享缓存平台。
7. 运行receipt区分 `service_process_control_performed=false` 与 `research_worker_processes_used=true`，并记录worker_count；不能因为禁止控制backend/worker服务，就虚假声明没有研究子进程。

### 7.2 r5→r7 版本差异与回归口径

r7正式A0成为新的当前基准。旧r5 close-cash A0只作回归诊断：差异审计认定输入未变的5,143股必须逐股等价；`688766.SH`由停牌版本差异解释，单独报告因果差值，不能硬要求与r5相同，也不能把它删出人口。受影响symbol集合由candidate逐文件差异审计生成并hash-bound，不在代码中手写豁免。immutable r5 close-cash baseline和父pattern bundle可作为诊断/谱系输入，但r5 candidate与中止的1,024股演进分片都不是r7计算输入；后者request身份不同，禁止跨版本复用，即使逐股文件相同。

新代码真实BUG登记独立Issue，只修 `backend/services/position_timing/` 及直接测试；修复后新建不可变request或重算未封存chunk，不覆盖失败证据。拟新增/修改仍限两个离线实现文件及最小测试；不改在线L1/API/router/shared defaults，不复制十套fixture，不新增依赖、平台、调度或参数优化器。

## 8. Verification Plan / 合入与交付

文档阶段：主蓝图F2与本文F1 validator、链接/identity/算术检查、`git diff --check`，三轮复核；它们只证明r7/WSL/并行设计闭合。

2026-09-18 v1.0审核记录（不是新实验记录）：三轮核对现金基准、共同起点、部分卖出、统计estimand、文档链接与验收映射，当时F2 51/51、本文F1 8/8通过。v1.1另行核对：①r5任务确已停止且只有8个封存chunk；②active profile、WSL路径及r7文件/canonical身份；③r5/r7实际策略输入逐文件差异；④父写者/纯worker/固定排序/失败chunk的并行确定性；⑤计划、蓝图和验收矩阵状态一致。文档验证不能写成r7代码测试或策略收益成功。

未来代码阶段：除现金/数量/时钟/市场限制/缺失/immutable/旧行为外，增加1-vs-8 worker exact equivalence、父进程唯一写者、worker失败不seal、续跑只复用sealed chunk、r7身份/环境与`688766.SH`停牌差异测试；执行changed-file L0、必要模块回归与CI。正式结果必须具备prepare/source audit/run/inspect/exact retry和输入/产物hash，不以测试绿、吞吐或source覆盖冒充收益验证。

完成报告：源码/PR/CI与合入状态、WSL checkout/环境/worker合同、r7及父身份、request/chunk/bundle/receipt、所有候选结果、同股BH和对应指数、覆盖与排除、outcomes_read阶段及retry身份。`database_read/write=false`、`live_market/network_api=false`、`runtime_action/service_process_control/serving=false`，同时如实报告`research_worker_processes_used=true`；仓库协作网络与行情网络分开记。

## 9. Scope / Risks / Production Gates（F-008）

本轮不改QE/Selection/Advisory/Paper/数据准备，不读写DB、不抓数、不改candidate/profile，不控制backend服务、共享worker或scheduler，不写卡、alert、交易、N0/current或serving。不引入公司行动账户清算。`ProcessPoolExecutor`的短生命周期研究子进程属于本任务内部计算，不是服务控制。仅用户可执行backend重启；纯离线实验不需要重启。

历史已观察、复权虚拟单位/价格指数差异、CLOSE无冲击假设、未知估值、共同窗口缩短、多重尝试及WSL环境迁移均须披露。成本后正结果仍是历史探索证据，不是最优策略或未来收益承诺。负结果同样完成研发，不阻塞下一机制研究；优先避免研究循环被未来行情、旧r5任务或强制HMM耦合拖慢。

## 10. Design Acceptance Index

F-001～F-008定义见上文。`DESIGN_VERIFIED`仅表示本文设计闭合，所有新候选实现/实验均待执行；不将设计通过计为实现完成。

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §3.1 r7/WSL输入、环境与复权合同；未来择时离线编排 | `backend/tests/position_timing/test_pattern_universe_benchmark.py`现有source identity测试；active profile generation `20260918-v10`及§3.1固定hash；新r7 request测试待执行 | DESIGN_VERIFIED | none |
| F-002 | §3.2与§4.3账户/成交规则 | 拟扩展`backend/tests/position_timing/test_pattern_close_cash_benchmark.py`现金/部分减仓/T+1测试，未执行 | DESIGN_VERIFIED | none |
| F-003 | §3.3共同起点与PIT | 拟扩展`backend/tests/position_timing/test_pattern_close_cash_benchmark.py`起点/因果/缺失测试，未执行 | DESIGN_VERIFIED | none |
| F-004 | §4十条策略 | 未合入分支的`backend/tests/position_timing/test_pattern_strategy_evolution.py`曾有26项定向通过；重基于r7并行版后必须重跑，不能当正式验收 | DESIGN_VERIFIED | none |
| F-005 | §5横向评价 | 拟扩展`backend/tests/position_timing/test_pattern_close_cash_benchmark.py`分组/公式/覆盖测试，未执行 | DESIGN_VERIFIED | none |
| F-006 | §6文献与后续边界 | 设计依据artifact: https://arxiv.org/abs/1504.04254；§6原始文献与§1本地诊断，不代表策略收益验收 | DESIGN_VERIFIED | none |
| F-007 | §2/§7/§8最小实现、WSL原生checkout、确定性并行与三轮审核 | 设计交付artifact: docs/architecture/position_timing_strategy_evolution_plan_f1_20260918.md；r5 request `a67dd...452a`仅8个sealed chunks后终止；r7 run/inspect/retry/CI均未执行 | DESIGN_VERIFIED | none |
| F-008 | §9隔离；§7.1父写者/研究子进程边界 | `backend/tests/position_timing/test_pattern_strategy_evolution.py`拟扩展identity/零副作用、1-vs-8等价、失败不seal测试，未执行 | DESIGN_VERIFIED | none |
