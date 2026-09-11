# 自选股与持仓股形态择时研究设计

> 版本：v1.7；日期：2026-09-11；Feature tier：F1（本模块研究扩展）
> 状态：`ENGINEERING_SOURCE_PREFLIGHT_VERIFIED_FORMAL_EVIDENCE_SOURCE_BLOCKED_NOT_SERVING`
> 首项任务：`PT-NEXT-018 / TREND_PULLBACK_ACCELERATION_V1`
> 所属蓝图：[持仓与自选池择时建议系统](position_timing_advice_f2_redesign_20260903.md)
> 权威规范：`docs/standards/aistock_development_standard_v1.5_20260523.md`
> 设计源提交：`535180c15`；因子覆盖预检实现提交：`ab1a20ba2`。当前已完成离线实现、直接测试与训练股开发烟测。前三次正式运行分别暴露根 manifest 漏项、`002086.SZ` 特殊非同比例资本变动、`300506.SZ` 未绑定复权因子基准拼接缝；三个不可变 request/bundle 与人口均永久保留，覆盖不完整时不读取比较值、不据其结果调参。第三次 request `ab159bebedee3ce1a0a200d400e57523921d296d00bcc4d93b09ebca60c406a9` 已通过 inspect 与 exact retry，但评价覆盖仍为63/64，因此不是有效收益证据。新实现把全量因子—公司行动覆盖审计前移到 request 生成前；第四批128股在未读取收益时发现4个未绑定区间并拒绝生成正式 request。当前需要数据所属模块补齐非同比例资本事件 authority 并修复增量复权因子历史重述，当前没有 serving 模型。

## 1. Background / 目标与现状

目标是为用户已经选定的自选股和持仓股产生可解释的买卖时机建议，并用逐腿成本后的历史收益检验价值。首个研究问题来自用户提出的路径：近期低点附近重新站上 MA5/MA10，回踩确认后买入；已有盈利持仓出现上涨加速和放量时管理退出。用户随后明确允许参考机构实践和论文、逐步微调或优化，不要求照搬概要。所有数值都是首版研究假设；“最优”指限定候选集合与数据条件下经样本外检验的较优方案，不承诺永久或全局最优。

当前正式 L1 仍为规则风险管理。`backend/services/position_timing/policy.py` 的 `PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1.buy.breakout_addon.enabled=false`、`EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1.take_profit.enabled=false`。`action_value.py::MARKET_FEATURES` 有收益、EMA20、波动、收盘位置与量比，但没有本设计的突破后回踩事件状态。已有 ATR 增量与 PT-NEXT-017 结果不能直接证明本组合有效或无效。

`backend/strategies/trend_following_strategy.py::TrendFollowingStrategy` 只有均线放量突破/跌破逻辑，且 `run()` 调用执行器。本设计只参考可核验的公式，不导入该策略、执行器或订单对象。全量EXIT保留为可解释的原型基线；减仓和衰竭确认进入有界优化候选，不假定清仓一定优于减仓或继续持有。

## 2. Scope / 范围与 Non-goals

设计覆盖日频形态、两项独立机制对照、完整规则组合回放、有界参数优化、模型条件化、相似策略研究队列与未来 QE 只读融合边界。原型、优化器与模型现已在同一个离线研究工具中实现，以一个顶层 request 在读取新评价收益前同时冻结两个统计 family；它们仍是两个解释边界而不是两个产品或审批阶段。

产品人口仍为“唯一权威持仓 ∪ 用户显式勾选的已确认自选”；同股持仓身份优先。当前名单只能用于当前或所声明的追溯展示，不能倒填为历史自选名单。首轮正式研究使用冻结 PIT 样本，报告向真实自选/持仓人口迁移的局限。

不修改其他模块、生产数据库、共享 guard 默认值或当前 L1/L1a；不新增订单、自动交易、数据库表、API、页面、scheduler、worker、模型服务或策略市场。HMM、分钟新信号、新闻 Agent 与 QE 成果不进入首轮模型或规则。后续运行接线可复用原页，研究成功不等于运行发布。

## 3. Architecture / 最小实现与复用

```text
冻结日线/复权/指数 + 公司行动/停牌快照 + 全局交易日
                    ↓
       全量因子—公司行动覆盖预检（不读收益）
                    ↓
  pattern_strategy.py：纯特征、事件状态、固定候选动作
                    ↓
  既有 daily_fill / 持仓现金记账 / 逐腿成本 / 终值与统计
                    ↓
  pattern_research.py：prepare / run / inspect / 不可变结果
          ↙                                  ↘
 pattern_optimizer.py：8模板月度选择    pattern_model.py：同状态双头模型
          ↘                                  ↙
        同一冻结评价人口上的研究比较与逐股历史影子解释
```

实际新增 `pattern_strategy.py`、`pattern_research.py`、`pattern_optimizer.py`、`pattern_model.py` 及四个同名直接测试文件；没有修改既有 `action_value_research.py` 或其他模块。新代码只调用其纯函数及既有成交、公司行动、现金/库存、模型训练实现，并在本模块内维护形态政策状态，未复制第二套共享 guard、数据库或运行框架。

| 基础 | 复用位置与约束 |
|---|---|
| 历史日频源 | `action_value_data.py::DailyCandidate`；冻结 manifest、消费字段和文件 hash，读取现有候选 |
| 行情与核心特征 | `action_value.py::market_features`；历史窗口、复权和完整性与现有实现一致 |
| 公司行动/停牌/因子覆盖 | `action_value_corporate_actions.py`、`action_value_suspensions.py` 与 `pattern_research.py::audit_pattern_factor_action_coverage`；只读已有快照/已授权只读源，正式 request 前穷举未绑定因子变更，禁止修改或猜测上游 |
| 交易日与合法交易 | candidate 全局 calendar、`action_value.py::daily_fill`、现有 board-lot；不使用自然日或统一100股规则 |
| 成本和仓位 | `policy.py::PERSONAL_MANUAL_COMPONENT_COST_V1`、已有现金/库存/持仓成本更新；净佣最低5元与规费分别计 |
| 持久化和统计 | `artifact_store.py` 的 hash、锁及原子提交；`action_value_research.py::circular_block_interval` |
| 后续模型 | `action_value_model.py::estimator_parameters/monthly_training_windows/numeric_matrix`；固定 LightGBM 4.6.0 |

未来代码写入范围只含 `backend/services/position_timing/`、`backend/tests/position_timing/` 及本模块设计文档。本次实际文档写入范围只有本文件与主蓝图。研究输出只到 timing-owned `research/pattern_strategy_v1/requests|bundles|source_diagnostics`；不写全局 N0、QE/Advisory/Selection registry、任何 current/serving 指针、生产 card/event/alert。`source_diagnostics` 只是按内容寻址的单份只读事实回执，不是监控平台、调度器或试验门禁。首轮试验清单保存在自有 request/receipt，不另建试验管理平台。

## 4. Contracts / 价格、时钟与状态

决策时点固定为全局交易日 T 的 20:00（Asia/Shanghai），动作有效期为 T+1 一个交易日。T日最终收盘价和全天成交量只能在收盘数据已可用后用于决策；禁止看完日线后假设当日收盘前成交。盘中未来若展示本策略，也只是提示已冻结建议；日频研究不能宣称捕捉到了当日冲顶最高价。

request展开并绑定candidate manifest、全局calendar hash、实际列/文件hash、公司行动与停牌snapshot、费用和guard snapshot及代码commit；bundle级request/manifest提供统一`source_identity`。事件、成交、连续净值和模型标签行显式携带`decision_as_of/feature_available_at`；缺少历史精确到库时间时明确采用上游日频可见时钟假设，不伪造观测过的时间戳；正式来源晚于cutoff时该日不可用。inspect复核输入和输出hash，源码/data/spec变化创建新request；retry不得覆盖旧bundle。

记 C/H/L 为同基准复权 OHLC：raw OHLC × factor，与 `market_features` 一致；MAk为包含当日的k日简单均线，ATR14为14日简单平均 true range。归一化的全部价格使用同一复权基准；成交/费用/涨跌停检查仍用原始人民币价格。复权因子与公司行动校验复用既有契约，不把除权缺口当突破/跌破。

公司行动 source snapshot v5 在同一 `record_date` 内按“共享 `end_date` 或共享 `base_date`”的连通关系识别同一分配方案；两者都不同才是同日独立分配。`end_date` 与 `base_date` 都可能被同一事项的不同源版本改写，任取一个作唯一键都会重复计数。相同方案的已实施条款覆盖更早预案；多条已实施记录的经济条款互相冲突时 typed fail-closed。不同方案才按行动前每股口径相加现金与送转比例。每个方案优先使用最早非空 `imp_ann_date`；若全部修订均缺该字段，仅在唯一 `record_date` 严格早于除权日时，以其日频cutoff作为保守可见代理。禁止任选一条、使用更早普通公告日或静默覆盖。v1～v4 不可变快照继续可读；新快照一律生成 v5，不改写旧文件。

形态研究另冻结 `position_timing_pattern_corporate_action_application_policy_v2`，逐个送股事项比较“事项前最后一个有效 qfq factor”到“事项日或其后首个有效 factor”的比值与数量倍数乘积，容差2%。普通送股必须满足 `observed_factor_ratio >= quantity_multiplier_product × 0.98`。若同一因子区间只有一个合并行动、账户与参考价现金均为零，且源倍数大于观测倍数，则显式分类为非同比例事项：观测因子在1的±2%内时记 `NON_PRO_RATA_STOCK_ACTION_IGNORED`；观测因子为正增长且该行动仅含一个经济事项时，以观测因子比作为合成研究持仓的有效数量倍数，记 `NON_PRO_RATA_STOCK_ACTION_FACTOR_MAPPED`。后者只保持跨股票合成sleeve与candidate复权身份一致，不声称还原特定真实股东的权益。含现金、多日期行动、多个经济事项的非零映射或其他不一致全部 `PATTERN_CORPORATE_ACTION_FACTOR_MISMATCH` fail-closed。request 同时绑定源 snapshot hash、候选源 hash、应用策略 hash、逐事项审计与规范化 action-set hash；receipt/inspect 再次校验。该策略只属于离线形态研究，不改变共享公司行动数据、不修改其他研究的旧 request，也不构造 survivor filter。

新 request 另冻结 `position_timing_pattern_factor_action_coverage_policy_v1`：在2018-08-01至2026-08-31的完整训练股与评价股范围内，按连续有效 factor 观测穷举绝对变化超过10 bps的区间；只有规范化公司行动 book 在前一有效因子日至当前有效因子日之间至少包含一个行动时才算已绑定。审计记录候选源、规范化行动集、人口、日期、material/bound/unbound计数与每个未绑定区间的股票、日期、因子和变化幅度，明确 `outcomes_read=false`。覆盖不完整时 `prepare` 立即返回 `PATTERN_FACTOR_ACTION_COVERAGE_INCOMPLETE` 且不发布正式 request；覆盖完整时 request v2 绑定策略与审计 hash，run 重算逐字段一致性，receipt/inspect继续校验。旧 request v1 仅保持不可变 inspect/run兼容，不可伪装拥有新审计。该检查不使用点估计、MDE或最新交易日，不是收益准入/人工审批；它只阻止来源语义未知的错误计算，也不得通过换股、缩短历史或放宽10 bps阈值制造完整覆盖。

所有窗口以全局交易日索引、完整有效 observation 计算，不删停牌日压缩时钟，不向前填价格来造形态。特征不可用时输出 `PATTERN_SOURCE_UNAVAILABLE`，中止当前等待事件；持仓继续按现有估值/风险路径处理。零波幅导致 ATR=0 时输出 `PATTERN_SCALE_UNAVAILABLE`，不能除零或填成正常形态。unknown 不等同于没有信号。

### 4.1 首版固定参数

| 参数 | v1值 | 解释 |
|---|---|---|
| MA周期 | 5、10 | 首版原型采用SMA；后续允许有记录的窗口/形式研究 |
| 近期低点窗口/最大年龄 | 20/10交易日 | 低点在 b-20..b-1 中取最低L，相等取最近日期 |
| 突破时距低点上限 | 3 × ATR14_b | 固定“近期低位”代理，不称真实底部 |
| 回踩等待期 | b+1..b+5 | b为首次突破收盘日；不把未回踩事件删掉 |
| 回踩容差 | 0.25 × ATR14_(T-1) | 比较上一日已知MA10带，避免盘中均线自反馈 |
| 量能基准 | V_T / mean(V_(T-20..T-1)) | 不含当天；V为原始股数，实施数量变更时按公司行动映射到T日可比股数 |
| 放量门槛 | 2.0 | 假设值，不以历史最大收益确定 |
| 加速/过度延伸 | 见§4.3 | ATR标准化，避免绝对涨幅跨股票失真 |
| 首轮清仓目标 | 当前全部可卖持仓 | 目标EXIT，实际数量仍受T+1/合法申报/成交约束 |
| 共同评价终点 | anchor+20，终值最多顺延5日 | 卡片有效期不随终值顺延 |
| 参考资本 | 每股独立sleeve初始100,000元 | 本身不代表用户仓位建议 |

以上值作为首块v1基线一起冻结，冻结范围是一次可复现试验，不是永远禁止优化。第二块按§6.1预先声明的候选和选择算法在开发数据中调优；后续还可根据失败机制提出新版本。修改之前记录理由、范围、训练与评价边界，旧request和结果保留。数据覆盖报告说明可计算性，不能靠换活跃股票冒充调整后普适有效。

### 4.2 入场：突破后回踩确认

在无活动事件时，b日同时满足以下条件才建立 `BREAKOUT_OBSERVED`：

1. C_(b-1) ≤ max(MA5_(b-1),MA10_(b-1))，C_b > max(MA5_b,MA10_b)。这定义一次跨越上沿，不要求同一天分别穿过两条均线。
2. 上述20日低点年龄≤10，且 0 < C_b-low20 ≤ 3×ATR14_b。

在 b+1..b+5 依序读取完整日线，每日先检查失效，再检查确认。若 C_T < MA10_(T-1)-0.25×ATR14_(T-1)，记 `PULLBACK_INVALIDATED`。否则首次同时满足以下条件即为 `PULLBACK_CONFIRMED`：

- abs(L_T-MA10_(T-1)) ≤ 0.25×ATR14_(T-1)，且 C_T ≥ MA10_(T-1)；盘中刺穿超过容差不能冒充“不破”。
- C_T > MA5_T > MA10_T，MA5_T > MA5_(T-1)，MA10_T > MA10_(T-1)。

确认后只在 T+1 提出一次OPEN；若未成交，记 `POLICY_FILL_UNAVAILABLE_EXPIRED`，不能追溯改成T日均线价成交。部分成交按真实数量持有，无自动补单。5日无确认记 `PULLBACK_WINDOW_EXPIRED`。单事件最多一次OPEN，不连续每日重复发出；重开事件规则见§5。

首轮研究OPEN，不把空仓模型或入场候选直接用于已有持仓ADD。持仓股仍可展示形态，但ADD须以后使用持仓状态一致的标签单独研究。

### 4.3 出场：盈利持仓加速放量

仅对已有可卖持仓，且按T日raw close扣除预计清仓费用后的累计持仓经济收益为正时判断。成本与分红送转后的数量/成本更新沿用现有持仓账本语义，不用未经调整的原买入价格替代。冻结规则风险退出优先；已触发风险退出的日子不计作本止盈机制的成功案例。

定义加速度 a_T = [(C_T-C_(T-3))/3 - (C_(T-3)-C_(T-8))/5] / ATR14_(T-1)。在所有输入可用时，以下条件同时成立即发出 `ACCELERATION_VOLUME_EXIT`：

- C_T > C_(T-3)，MA5_T > MA10_T，MA10_T > MA10_(T-1)；
- a_T ≥ 0.25，且 (C_T-MA10_T)/ATR14_T ≥ 2.0；
- 同股数口径 volume_ratio_T ≥ 2.0。

首版基线在T+1请求EXIT，不隐含上影线/收盘走弱条件。第二块显式比较加入衰竭确认和减仓的版本，分别记录其策略身份。全量不可卖时报告剩余数量和原因，不能伪造全仓平掉。一次事件只尝试一次，风险退出仍每日处理；未成交后只有条件先退出再重新满足才构成新的形态边沿。

## 5. 历史反事实与有效性评价

### 5.1 两项机制：从共同起点观察两条路径

入场机制以突破b建立所有事件，双方同股、同初始现金和共同b+20终点。E0在b+1尝试OPEN；E1等待首次确认T，在T+1尝试同一计划股数。数量在b按已有board-lot与预算计算并冻结；公司行动后映射同等经济数量。延后路径现金不足记真实 `NO_FILL_BUDGET` 并保持现金，不删除该事件。两侧NO_FILL都保留；缺源UNKNOWN单列并令该比较覆盖不完整，不能只取成功成交交集。入场后的共同续持规则为冻结risk exit或HOLD，不引入加速止盈。

计划数量为按b日raw close估算、预留对应买入费后不超初始现金的最大合法数量；不能只用cash/price再发现最低佣超预算。计划执行复用`daily_fill`及冻结price guard，T+1行情导致的yellow减量/部分成交都记录为真实结果，所以两侧计划经济数量相同不保证实际成交数量相同。日频OHLC无法唯一确定触价先后时沿用冻结保守成交语义并披露代理误差，未来分钟核对不倒改既有结果。

所有未回踩、假突破、涨停买不到、延后涨价买不起、等待期结束的事件均计入。若只在成功确认日抽样，就无法回答等待是否划算。单股机制事件从b开始到b+25不重叠，此间忽略新突破；下一全局交易日起重新捕捉新的跨越。实际账户内则由现金/持仓与§5.2约束。

出场机制的持仓来源为E0真实因果入场路径，按上述事件规则逐日演化；每条持仓首次满足§4.3时锚定s。X0从s状态继续risk exit/HOLD；X1从完全相同的s状态在s+1尝试清仓，已卖出现金留到共同s+20终点。两边不再应用第二次加速止盈，部分/未成交剩余仓位继续共同risk exit/HOLD。买入历史与沉没费用在两边相同，只比较新增动作的增量成本与机会成本。这个人口是合成因果持仓，不宣称覆盖用户任意建仓历史。

E0仅在其预定终值退出之前提供s状态；复制之后X0/X1使用独立的s+20终点，不继承E0的b+20退出，否则出场标签会被剩余持有期混淆。prepare按源结束日分别列出能覆盖anchor+25的评价日期；窗口末尾尚无法成熟的事件计为`RIGHT_CENSORED`，不当作零收益或策略失败，不按实际收益选择结束日期。不同b来源的X事件可能重叠，不能用事件总数宣称独立样本量。

终值优先使用共同nominal date的可执行清仓；若任一路仍有库存且不可清仓，同步延长双方到最早可共同完成的日期，最多5个全局交易日；先退出的一侧维持现金。不可评价记 `UNAVAILABLE_AT_HORIZON`。收益=(候选终值财富-基线终值财富)/共同anchor财富×10000；禁止把回撤减少直接改名超额收益。

### 5.2 完整政策：保证终极目标有直接对照

完整P策略为空仓等待§4.2、持仓执行冻结risk exit或§4.3或HOLD。每次结束入场等待事件后，至少到该b+5结束才允许新突破；持仓期间不OPEN/ADD，实际退出之后只接受退出日之后新发生的突破，不复用旧事件。候选/基线都跨交易日连续记账，不在每个成功事件重置本金。无形态日保持现金或持仓，同样计入净值。

P每次新b按当时现金和§5.1的同一预算公式生成计划；risk exit与形态止盈同日出现只执行risk exit。终值清仓日及其后顺延期不再开仓；buy-and-hold首次尝试未成交时在下一个决策日重新按当时预算生成买入计划，直到完成一次持仓建立，部分成交后视为已建立，不自动补仓。

固定比较P减同股buy-and-hold、P减冻结L1。候选及两条基线共同从各股首个具备必要历史窗口且位于评价期的交易日开始，均以100,000元现金起步；buy-and-hold在首个合法执行日建立持仓，P等待形态，L1只按已有真实输入运行。没有历史intent的L1可能保持现金，receipt必须标明这一基线的信息限制，不能把跑赢现金单独称择时有效。共同窗口nominal terminal取源结束日往前5个全局交易日，预留最多5日终值退出。首次买入费用双方如实计算；持仓成本来自实际回放成交与公司行动。

已有持仓的主要机制证据由§5.1的因果E0入场路径提供，清晰记录合成入场偏差。PT-NEXT-010的`initial_holding_endowment`要求非空entry_reference，不能声称它支持未知成本；首轮不为新策略改该函数。真实产品持仓成本未知时按§6解释不可用，禁止拿20日前价格伪造用户成本。

完整政策允许“规则退出后再入场”，所以新增换手必须逐腿计算。同股buy-and-hold是净超额的基准；沪深300只作市场背景，跑赢指数不足以证明择时有增量。

### 5.3 人口、统计与证据限制

复用PT-NEXT-017所绑定candidate、原训练64股与2018-08-01..2026-08-31源范围。实现prepare时核验原request实际字段，不从名称猜路径。主评价沿用756个全局session的初始训练边界，即使首块无需训练也保持共同评价起点，确保第二块可比。按 `SHA256('20260911|'+canonical_symbol)` 升序从candidate身份集合中剔除所有已登记研究request股票并集及训练股后取64只；新并集必须包含PT-NEXT-017。先冻结清单，再报告PIT/窗口/事件覆盖，不据结果换股。若不足64只就记录实际数量；没有事件也是有效研究结果，禁止换一个“更活跃”批次。

真实自选历史缺失时，本研究只称PIT代理人口实验。由于研究者已经多次观察同市场日期，跨股票隔离不能抹掉时间数据复用；保留 `EXPLORATORY_USER_PROPOSED_HYPOTHESIS / CROSS_SYMBOL_HELDOUT_NOT_TEMPORAL_HOLDOUT`。候选策略清单中的其他项没有执行就不计成已完成trial；实际看过收益的变体、失败研究和后续选择历史均记录，当前family校正不冒充所有历史搜索的全局校正。

首块一次性固定4项主比较：E1-E0、X1-X0、P-buy-and-hold、P-L1。前两项每个anchor日先对股票事件等权取均值，再对有事件日期等权；后两项按交易日对固定人口等权聚合财富日收益差。单位分别为事件bps和bps/日，不互相代换。共同使用25日circular moving block、5,000次、seed=20260911、nominal95%、Bonferroni98.75%区间；稀疏事件在全局calendar上保存`daily_event_mean/active_day_indicator`，重抽日期块后以日均值之和除以有事件日期数，非事件日分子与计数均为0。若重抽分母为0，记录无效重抽，全部5,000次中有效比例低于95%时区间为null并报告原因，不追加重抽凑足结果。该按日等权口径不能换成按事件数量加权，也不能把非等间隔事件序号当交易日。

每项成本后threshold=0，adjusted lower>0为SUPPORTED，adjusted upper<0为NEGATIVE，其余（包括区间恰好为[0,0]）为INCONCLUSIVE；4项分别报告。仅两项完整政策比较均SUPPORTED才可称本规则组合取得局部统计支持；不能因为一个机制为正就称组合有效。没有足够有效日期块计算区间时字段为null与typed reason，不通过调块长、增股票或等明天补救。MDE/区间宽度/样本量为报告义务，不作为开跑或工程合入门槛。

receipt同时记录gross/net、逐腿费用、1/2/3父订单费用敏感性、每腿另加5/10bps摩擦敏感性（假设压力值，不是市场实测）、换手、暴露、最大回撤、等待错失上涨、止盈后的继续上涨及回撤变化。市场上涨/下跌和波动切片仅诊断，不按最赚钱切片重写生效条件。主paired路径必须对称；UNKNOWN和不可估值路径报告数量与影响，禁止静默剔除后宣称完整人口SUPPORTED。负结果同样完成研究任务。

## 6. 有界优化与模型：第二个工作块

原型规则回放用于建立可解释基线；规则未获SUPPORTED不阻止继续研发。同一离线实现还分别检验“少量规则细节调整能否改善效果”和“模型能否识别原型何时有用”。三者在读取本轮评价人口收益前一次性冻结，避免先看原型评价结果再条件化决定是否运行优化或模型；已经反复看过的历史始终说明探索性，不能因增加一层验证就宣称数据从未被研究者见过。

### 6.1 小范围调优与演进契约

首轮优化只用8个规则模板，全部保持原型低点、MA5/MA10、加速度和放量公式；不会先遍历笛卡尔积再挑8个：

| template_id | 相对原型的唯一变化（或明确组合） |
|---|---|
| R0 | §4原型，等待5日、容差0.25ATR、清仓 |
| R1 | 等待3日 |
| R2 | 等待8日 |
| R3 | 回踩容差0.10ATR |
| R4 | 回踩容差0.50ATR |
| R5 | 止盈只请求减持当前可卖数量50%，合法取整；不足最小合法减持量记NO_ACTION，不强行清仓 |
| R6 | 清仓须同时满足衰竭确认：收盘位置≤0.5，或上影线占当日振幅≥0.5；零振幅不可确认 |
| R7 | R5减仓与R6衰竭确认组合 |

衰竭确认在加速放量同一T日检查，用收盘可见事实，不等未来下跌后倒填信号。收盘位置=(C-L)/(H-L)，上影线=(H-max(O,C))/(H-L)。模板的等待超时/重新捕捉时钟按对应等待参数同步派生；单事件与共同终点仍保持20/25日，不沿用硬编码b+5覆盖R2。减持后剩余持仓继续原风险规则，形态信号须重新出现边沿才会再次减持。

优化算法Q每月只读取此前已完成的12个自然月连续回放结果：先找到不晚于当次决策前25个全局session的最后一个完整自然月末，再取截至该月末的12个月；只使用原训练股，在同样初始资本/持续路径下按成本后日均超额相对buy-and-hold排序，选择最高者，1e-9 bps内并列按R0..R7优先。8个候选在开发段各自连续记账，不因每月排名重置资本。候选源覆盖不对称时不以缺失补零参与排名；若无可比较模板，Q保持上次模板，首次则使用R0，并记录`OPTIMIZER_UNAVAILABLE_USING_PRIOR_TEMPLATE`，明确这只是规则回退而非模型成功。

选中模板在接下来的外层月份固定，用真实递推的Q持仓/现金运行；月界只更新规则，不交易重置仓位。已活动的突破/等待事件绑定创建时模板，直到失效/消费；已有持仓的退出阈值从新月决策日起使用新模板，边沿状态以新模板重算并绑定版本。训练股选择和评价股回放严格隔离；不能用外层月收益回选同月模板，也不在全样本看完后选一个固定赢家代替Q。比较对象是这套可部署的选择算法Q，8个内部模板的开发收益如实留存但不各自宣称8项独立支持。

参数邻域稳定性、换手、卖飞/回撤、不同时间段表现用于判断下一修改方向；不能只挑单个最高收益尖峰称最优。下一轮可以研究MA长度、量比、加速门槛、退出比例或其他策略机制，每次限定一个修改主题并事前记录候选数与外层边界。预算可以随证据和算力在新spec扩展，不是永久门禁；已看过的外层数据进入开发集后，只能称探索性或改用其后历史段验证。历史因果回放继续优先，不等待实时日期、人工放行或强制sealed holdout。

### 6.2 同状态模型条件化

保留两个单独监督头：入场在突破b预测“等待固定回踩政策E1相对立即E0”的成本后价值，输入只能来自b；不能把未来是否回踩成功输入b的模型。出场在真实持仓s预测X1相对X0的成本后价值，持仓状态、浮盈和可卖数量必须与监督一致。入场模型正值选E1，否则E0；出场模型正值执行X1，否则X0。模型阈值固定0，不把相关系数当成功率。旧ENTRY/EXIT heads不换标签、不覆盖旧model artifact。

出场训练人口同时取原训练股E0入场与R0回踩入场两条冻结因果路径的可评价持仓，按`(symbol,decision_as_of,state_sha256)`去重；每条标签从其自身真实状态复制X0/X1。原型机制对照仍只用§5.1的E0人口，不把这项模型训练扩展倒写为原型结果。未覆盖的持仓状态或事件时钟须记录分布差异，尤其不能把空仓ENTRY头用于ADD；模型运行路径、训练人口与原型事件人口的覆盖分别报告。

比较两组固定模型：同目标core-only与core+形态块；每组包含上述两个头，使用同一LightGBM 4.6.0规格，完整参数由`estimator_parameters()`在prepare显式展开并hash-bound，early-stopping=false、无搜索。每月expanding训练，初始756session；仅使用`label_available_at <= training_cutoff < prediction_as_of`的已成熟标签，预处理只拟合训练集，缺头/缺模型显式不可用。联合政策在相同可预测日期评价，并报告全计划窗口覆盖。

形态块字段固定候选为：低点年龄、距低点ATR倍数、距MA5/MA10的ATR倍数、两条MA的一日斜率/ATR、当日low距上一日MA10的ATR倍数、加速度a、同股数volume_ratio。全为当前可算的连续值；不增加未来事件标签、不做百指标特征筛选。MA/ATR/量比已有正式因子可经语义与PIT核对后读取同值列，否则本模块纯计算；全局因子桥接不是前置平台任务。

模型条件化先固定在R0目标上，不把8个模板再乘进模型网格。一个顶层 request 同时冻结原型 family=4 与演进 family=5；两组分别采用 Bonferroni 98.75% 与 99% 区间，不把9项误称为一个family，也不跨family挑赢家。演进5项为Q-P、Q-buy-and-hold，以及增强模型联合政策减core模型联合政策、减P、减buy-and-hold；内部8模板选择和两组各两头的训练次数另行完整记录，不能写成“只试5个模型”。模型和规则优化分别报告，不在同一外层结果上再挑赢家后声称无偏有效。单request比原先“看完原型后再准备第二request”更严格地避免结果驱动规格变化；具体样本数与正式收益仍须由不可变bundle给出，开发烟测不能替代。

逐股研究解释输出 `symbol/as_of/pattern_state/reason_values/proposed_action/target_session/valid_until/position_context/cost_estimate/evidence_ref`，预测值是成本后增量估计，不称个股胜率。当前持仓成本未知时不能生成盈利清仓结论；源或模型不可用时提供原因。runtime接线另按本蓝图已有版本化影子建议和提醒契约实施；首块不用新增API/UI去展示一个未验证策略。

## 7. 相似策略与场景选择

以下为候选研究队列，不是已有效策略，不一次性开跑，不形成自动策略路由平台。排序依据是机制差异、数据已具备、可证伪和交易成本敏感性，不是历史PnL排行榜。

| 优先级/候选 | 场景代理（待各自规格冻结） | 动作与要检验的机制 | 与首项的区别 |
|---|---|---|---|
| P0 本设计 | 低位突破后回踩；盈利趋势突然加速放量 | OPEN/EXIT；等待确认与提前退出的机会成本 | 当前唯一完整设计 |
| P1 波动收缩后区间突破 | 历史波动与量能收缩，突破过去区间上沿 | OPEN；新信息是否结束盘整并形成持续性 | 不依赖先见低点/均线回踩 |
| P1 突破失败或支撑失守 | 原突破后收盘重新落回事先冻结的区间 | EXIT；及早识别失败能否降低成本后损失 | 失败风险，不以浮盈或放量加速为前提 |
| P2 平稳区间中的过度偏离回归 | 低趋势斜率、相对波动的异常下跌后收复 | OPEN/WAIT；流动性冲击是否暂时 | 反转机制，强下跌趋势下可能失效 |
| P2 市场回撤中的相对强势修复 | 个股相对指数抗跌，市场修复时重新走强 | OPEN；共同市场波动与个股强弱的交互 | 市场背景条件，不直接复活已NEGATIVE的旧行业字段 |
| P3 缺口延续/回补 | 收盘可知缺口、成交接受程度 | 次日OPEN/WAIT；信息冲击延续还是回补 | 若要当日判断，另属分钟研究 |
| P3 结构化公告/业绩事件后漂移 | 发布时刻、已知公告类型与可复现事件字段 | OPEN/EXIT；消息是否缓慢被价格吸收 | 依赖历史可见事件，排在数据契约可用之后 |

首次场景说明用透明描述，例如“上行趋势/横盘代理”“波动扩张”“疑似失败突破”。在这些切片上报告策略增量，不能把事后最优场景回填为已验证路由。未来需场景选择时，只做一层固定的动作价值比较或同一模型内交互；HMM可用则作为独立context块研究，不等待HMM，也不将趋势标签与HMM隐状态混称。

未来冲突处理顺序保持：可执行性与用户上限→冻结风险退出→持仓状态允许的唯一研究政策→HOLD/WAIT。只有通过独立组合评价后才能把多个候选并行到同一股票；不采用多策略投票来虚构更多独立证据。同一源量价指标在多个策略中相关，不把票数当置信度。

## 8. 后续QE成果组合

先形成择时自身的固定规则/模型及receipt，再只读消费QE已导出的因子值、OOF预测或有训练截止时间的模型产物。预计最小字段为 `canonical_symbol/decision_as_of/feature_available_at/dataset_identity/experiment_identity/training_cutoff/prediction_or_factor_values/schema_sha256/artifact_sha256`；没有字段的既有产物保留`UNAVAILABLE`说明，需要上游补充时向用户提交精确需求，由其他模块窗口开发。

组合实验固定同一只股票、人口、资金、成本和可交易性：QE方向+原执行时点，与同一QE方向+择时时点对照；先只比较一个冻结择时政策。不得把最新训练模型的历史拟合值当OOF，不在看完择时结果后挑“最配合”的QE实验，不把自选股选得好产生的收益归给择时。因子与QE预测同源信息需报告重叠。当前任务不调用QE训练、不写其registry、不改其选股和回测代码。

## 9. Implementation Plan / 实施方案

| 工作块 | 实际产出 | 收益与交付的关系 |
|---|---|---|
| 规则历史研究 | 已实现纯形态与事件状态、四项固定比较、连续账户回放、不可变request/bundle/receipt及请求前因子—公司行动覆盖审计 | 工程与直接测试已完成；前三次正式run均因不同完整性问题不可作为收益证据，第四批在prepare前发现源缺口；不等最新交易日、不要求后端重启 |
| 有界优化、模型条件化与历史解释 | 已实现8模板训练期选择、同状态两头监督、core/增强对照、历史逐股解释 | 与规则在同一request中事前冻结；覆盖完整的正式收益仍待数据authority修复，上线仍按既有影子契约另行接线 |

其他策略和QE组合留在后续队列，不占当前实现量。当前工程不接卡片、提醒或serving；正式结果无论正、负或不确定都如实回填，不把研究分类作为源码合入门槛。

### 9.1 单次长任务执行包：实现与初步正式验证

用户要求将本策略实现和初步验证作为一次连续长任务执行。当前已在独立实现分支完成上表两个工作块的源码、直接测试和训练股开发烟测；设计PR #4538 的旧CI记录为 `pr_quality` 失败，而其已执行的后端选择性测试通过，该旧CI既不作为研究结论，也不冒充当前实现已经通过。最终以同步最新main后的当前实现PR重新执行全部适用CI。

只读预检已经确认：原PT-NEXT-017 request `c685a820dc4ed7ff0508350f6536832ab22777646e9adb5fbb1f972fe00d9c1e`存在，训练/历史评价人口各64股，源日期2018-08-01..2026-08-31；其candidate根`X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260831-qe_hmm_full_v2-direct-20260905-candidate`存在。`C:/Users/lc999/miniconda3/envs/AIstock/python.exe`读回numpy2.4.0、pandas2.3.3、lightgbm4.6.0；默认Python/根.venv缺少所需研究依赖，不作为本任务解释器。source-only人口选择已在不读取outcome时得到64只新评价股；现有公司行动和停牌snapshot不覆盖该批次，因此正式prepare前只需对训练64股与评价64股生成同一范围的新版只读快照。

| 连续工作段 | 时间预算 | 实施与正式交付 |
|---|---|---|
| 契约落地与源检查 | 0.5～1小时 | 核对设计commit、独立worktree和精确写入范围；source-only确定人口/文件hash；展开初版与优化/模型规格；先补下面列出的实际接口适配 |
| 策略与研究管线实现 | 3～4小时 | 形态/事件、两种机制反事实、连续P路径、8模板选择Q、同状态标签与四个模型head、逐股历史解释；针对性反例测试随实现推进 |
| 首次历史回放与训练 | 3～5小时 | 先跑已冻结R0四比较，再执行8模板开发选择与两组模型的5比较；复用同一源快照、特征缓存和同规格基线，保持不同request身份 |
| 审核修复、报告与合入 | 3～4小时 | 因果/交易正确性审核与研究统计审核；修复后只重跑受影响部分；bundle inspect/exact retry、隔离校验、验收矩阵、蓝图进度及研究报告、源码提交PR和CI后合入 |

上述时段是一个任务内的操作顺序，不增加产品阶段或人工审批。计时可交错：计算期间审核实现和文档，不能同时改变正在计算所绑定的源码。CI排队单独报告，不伪装成开发耗时。

可直接复用的已核验接口：`DailyCandidate.open(root)`、`daily_fill(plan,bar,*,sellable,parent_count=1,full_exit=False,slippage_bps=0)`、`estimator_parameters()`、`monthly_training_windows(calendar,initial_sessions=756)`与既有prepare/run/inspect不可变写入模式。`replay_continuous_cohorts`当前接受models和固定model_action_authority，没有任意`policy_callback`参数；必须在本模块真实增加显式扩展点并保持旧默认路径，不得调用想象中的参数。`circular_block_interval`当前是有限数值序列均值bootstrap，不支持稀疏事件比值；须在本模块增加专用小函数或明确mode实现§5.3，不把NaN填0交给旧函数。

实际新增四个职责文件：`pattern_strategy.py`（特征/模板/事件/动作）、`pattern_research.py`（反事实/CLI/报告）、`pattern_optimizer.py`（训练期模板选择）、`pattern_model.py`（同状态训练与推断），以及对应四个直接测试文件。授权只读快照首次真实输入又暴露 `603259.SH/2025-05-21` 同日两项独立现金分红被旧适配器误判为冲突；本任务据§4契约最小修正同属 `position_timing` 的 `action_value_corporate_actions.py` 及其直接测试，不修改生产表或其他模块。没有修改`action_value_research.py`或既有公开入口；allowed_write_scope仍仅为本模块源码、直接测试与两份设计文档。

研究预算固定：源人口按§5.3的64训练股/至多64评价股；8个规则模板仅在开发人口中选择；core/增强两组各2个head，每个可训练月最多4次fit，不追加种子/模型搜索。原型4项与第二块5项比较分别绑定独立family；所有实际尝试可追溯，不能把两family中任一赢家当作经过整个研发历史校正的最终最优。数据、形态特征和未变化的基线在hash一致时复用；初始只运行1个研究计算进程，线程上限4，实测首个小批次耗时与内存后更新完成时间估计，不在共享机器上同时拉起多套训练。

正式run前先以固定合成反例验证因果与交易逻辑；若需真实数据小样性能测量，只取预定训练股前2只与首个开发时间段，记录它为开发数据，不能从评价人口挑“容易通过”的股票。不得为赶时限缩短正式样本、删除亏损事件或只交付最赚钱模板；耗时超预算就保存已完成的request/bundle身份并如实续报。允许只缓存确定性的源/特征和独立月度fit，缓存键含代码、输入、训练cutoff、目标和参数hash；无现有续跑支持的连续路径从该request重算，不声称具备未实现的任意中断恢复。Windows中断后据git、request和输出hash恢复，禁止覆盖未核验的历史结果。

两轮审核分别覆盖：第一轮PIT、复权、可卖数量、最低佣、同状态对照、边沿/到期/跨月状态；第二轮训练与外层隔离、稀疏事件统计、覆盖分母、候选及多重比较计数、解释是否夸大收益。每次发现问题即修复和执行直接反例；最后再按F1各条及主蓝图约束逐项读回，不为“多轮”机械重复全套测试。

任务结束交付：可运行CLI和测试；原型及优化/模型的不可变request、receipt、逐日连续净值、事件/成交/费用明细、月度模板与模型身份；固定SHA排序的前20条可评价入场/退出案例及全部类型失败计数（不按收益选案例）；逐股历史建议；一份中文结果报告与更新的蓝图/验收矩阵；源码PR、CI及合入状态。病例不足20条按实际交付。数值不足以训练某head时交付已实现代码、真实零/稀疏样本统计和`MODEL_NOT_ESTIMABLE`原因，明确“训练未成功/模型效力不可判定”，不报虚假的模型完成；规则和其他独立比较继续。

本长任务验收分别报告工程完成度、原型证据、有界优化证据、模型增量证据、合入状态；不得把NEGATIVE/INCONCLUSIVE研究结果当工程失败，也不得把代码测试通过当有效策略。当前不激活正式card/alert/serving，不要求后端重启、不做生产DML。工程已完成并进入合入前复核；用户已授权只读生产源快照。前三次正式 request 分别因 artifact 完整性、特殊资本事项和复权因子历史拼接而不可用于收益结论。第四批不再重复长跑：source-only预检在正式request发布前发现4个未绑定因子区间并fail-closed。下一步是由数据所属模块修复历史因子重述并提供非同比例资本事件authority；修复后对同一第四批人口重新prepare，不换股、不等待最新交易日，也不把收益结果作为工程合入条件。

### 9.2 当前实施读回

- source-only人口：从所有既有择时研究request声明人口并集中排除训练股后，确定性选出64只新评价股；该步骤未读取其收益或标签。
- 开发烟测：固定训练股前2只完成原型、8模板选择、双特征集双头LightGBM与外层回放。观测到34条模型标签、19,184条模板开发日、60个月度选择记录、102次成功fit/118次尝试、2,054条模型外层日记录和34条历史预测；这些数字只证明实际执行路径可达，不进入正式统计结论。
- 审核修复：已修正延后买入现金不足的typed no-fill、持仓退出PIT边界、父订单费用净/毛拆分、除权停牌日估值、UNKNOWN覆盖、开发/训练/外层联合覆盖和递归artifact文件集校验。第二次正式运行后又据真实 source/factor 对照修正公司行动身份：同一 `(end_date, record_date)`、不同 `base_date` 是修订；唯一已实施条款覆盖较早预案，只有不同财报期的同日分配才相加。形态研究在源 snapshot 之上增加 hash-bound 因子一致性应用审计，特殊非同比例事项可以保留股票并显式 no-op，其他不一致仍拒绝。修复均有直接反例，不改变形态阈值、模型规格或已冻结评价人口。
- 验证状态：公司行动与 pattern 相关直接测试共53项、完整 `backend/tests/position_timing` 回归301项通过，其中请求前因子覆盖审计及request v2防降级有3项新增反例。position-timing源码/测试 ruff、compile、F1/F2 validator 与 `git diff --check` 必须再次通过；上述本地结果不替代最终CI或收益证据。
- 首次正式运行：request `df591f237f6263873e720a678218b59042cc9490c63a0f482ffd0891c60d4a12` 绑定提交 `888a6576e2fa54cccfcfbac91cbcc2c92c3e9be5`、64训练股、64只此前未评价股票、2个family和9项比较。计算生成了receipt与模型文件，但根 manifest 构造使用 `path.name != "manifest.json"`，错误排除了114个模型子目录 manifest；inspect得到 `PATTERN_BUNDLE_FILE_SET_MISMATCH`，故整份bundle按fail-closed处理，不读取或报告其中收益。这是artifact完整性缺陷，不是统计结论；旧request、bundle与评价人口永久保留，禁止原地补manifest。
- 修复与重跑：根 manifest 只排除bundle根自身的 `manifest.json`，嵌套模型manifest必须进入文件集并绑定hash；Pattern专属LightGBM参数只增加 `verbosity=-1` 以压制重复日志，不改树、目标、样本、阈值或预测语义。新request须绑定修复后的干净提交，并把首次64只评价股票纳入prior-request禁用集合后确定性选择新评价人口；所需公司行动/停牌快照重新按新128股范围只读冻结。首次公司行动快照 `bd6590e3888a305baf369106fef519a8151ee48d5a7faadf95961f2662fb9b08` 与停牌快照 `13be7919a67a7f98a1029023c2a5d95940bbf89234422c3e706211ea9e780d9a` 仅属于失败request的输入谱系，不覆盖未来新人口。没有研究模型current、selected或任何运行态写入。
- 新人口数据适配：新评价股 `600803.SH` 的 `2025-07-22` 已实施分红行缺少 `imp_ann_date`，但唯一 `record_date=2025-07-21`。快照按§4的record-date保守代理契约升级为v3并显式留证；不删除该股票、不使用更早 `ann_date=2025-03-27` 推定最终条款，也不修改生产数据。v1/v2不可变快照继续只读兼容。
- 第二次正式运行：request `b653d306802c0ba6e41204535260853f6d969af0e69868df4f8f4a38c2d9336b` 绑定提交 `3a75b22e4e06ac14c5d2b4f40f0387a8329d44de`、第二批64只评价股票、公司行动快照 `d9fef84118122e07fb6824b9d6a27035cfdf0448ed20d5d082b82b32c983fa64` 和停牌快照 `c71c9fdcece0d0bb7929ef400c10f7d928ff18b8fa7bc913970b92bb78942aae`。bundle包含365个绑定文件和114个模型子manifest，inspect通过；exact retry为`ALREADY_MATERIALIZED`且hash不变。但外层仅63/64：`002086.SZ/2023-12-29` 的数据库源行声明每股转增1.59，而candidate相邻有效qfq factor比为1；旧路径先在缺失日读到非有限factor，并且若机械应用会伪造159%持仓增长。因此全部9项比较按`coverage_complete=false`保持`INCONCLUSIVE`，selected=0；点估计、区间和诊断不得用于选下一版阈值或宣称策略收益。该bundle永久保留为结构完整、证据人口不完整的失败记录。
- 第二次后数据审计：对该128股全部91个含送股事项作独立因子核对，除 `002086.SZ` 外均满足冻结容差。另发现 `688188.SH` 两个年度事项各有同经济条款、同 `end_date/record_date` 但不同 `base_date` 的重复修订；v3曾把每股0.4送转误叠加为0.8。`600803.SH` 的0.81早期预案与1.03已实施条款也不应相加。v4只读快照 `0af455eed9976b5fac1c903a160b273021eac6c8669bd2c941d5fbf666c6a419` 从702条原始行形成685个行动/经济事项、折叠17条修订、record-date代理归零；应用审计 `45b1f10b89a940fca62b154ffff2186543cdeefba045c80c7bf49e031b45f69f` 检查91/91区间，只将 `002086.SZ` 记为 `NON_PRO_RATA_STOCK_ACTION_IGNORED`。同股单独回放已达到1/1完整、0 unknown，证明原63/64故障路径被修复；这仍只是正确性验证，不是正式收益结果。
- 第三批source-only预检：prior request 32份、禁用股票489只；确定性得到64只训练股与64只新评价股。v5公司行动快照 `3f0b8027b2e94d4fb6077666df5439108c1fb1eba67364a7bc508d0b41ddd8ac` 从741条源行形成722个行动/经济事项并折叠19条修订；停牌快照为 `923e062505dd68f9897062b531f97fa0be6a62a230195efb057e2b7561080f06`。96/96个送股因子区间完成核对；`002326.SZ/2019-09-30` 定向转增从源倍数1.1206188映射为factor倍数1.0756665，`300506.SZ/2025-12-22` 重整股份过入投资人/债权人且factor为1，故合成旧持仓不增加数量。应用审计为 `9a3eaedbc7ad4dd140c4be3aef2d1aedda32995e5805748c2f2c4ae79177cca6`。该预检不读取收益/标签，不是策略证据；两项非同比例语义另由公司公告与本地 source/factor identity 交叉核验。
- 第三次正式运行：request `ab159bebedee3ce1a0a200d400e57523921d296d00bcc4d93b09ebca60c406a9` 绑定提交 `6389c5b494d7d521d59747408744e56426e74cf2` 与上述第三批快照，bundle含365个绑定文件，manifest `9de1da44981b7fb021d994197e60056b2e5b04a5cb7ddad87d0bfd857f597f85`、receipt `3c370a205d0de93f04531daf4b975d7d7dc26041a9366a9089bfdb0503bf7b22`，inspect与exact retry `ALREADY_MATERIALIZED`通过。训练模型输入64股完整、优化器开发512/512模板—股票对完整；但评价原型和模型输入均只有63/64，`300506.SZ` 为 `UNBOUND_MATERIAL_FACTOR_CHANGE`，所以9项比较继续不可引用，`selected_trial_count=0`且无serving/registry/current/card/alert/order/DB/runtime写入。
- 第三次源诊断：`300506.SZ` candidate在2026-07-03至07-06由0.754176跳到1；本地 `market.adj_factor` 同期由4.844跳到6.4229，而Tushare当前对2026-06-25至07-15整个历史区间均返回6.4229。原始价格07-03收6.25、07-06收6.04，总股本持续142,559.6569万股且无实施分红。故这是上游历史因子整体重述后本地仅刷新新区间形成的基准拼接，不是07-06真实除权；不得在形态研究里把它映射成持仓数量或静默保留伪复权收益。
- 第四批source-only预检：33份prior request、553只禁用股票确定性得到原64训练股与第四批64评价股；只读v5公司行动快照 `126bcd984e1c8dc89b5a7694fc4c6b83066074d679c5c0fea6d05639a6582b00`、停牌快照 `171cb4dade3a6e13d30bfaa9146416041bbece023296560b6f2861170a94c24f` 已冻结。对128股全部697个大于10 bps的因子区间检查后，693个能绑定规范化公司行动，4个未绑定：`000970.SZ/2022-02-15→02-24`、`600008.SH/2020-09-18→09-29`、`601236.SH/2021-07-26→08-04`、`688109.SH/2026-07-08→07-09`。前三项都跨停牌、总股本增加而流通股本未同步增加，`dividend`无事项，不能凭臆测决定普通旧股东的数量/现金语义；`688109.SH` 的candidate与本地库由1.5415跳至1.5459，而Tushare当前已把两侧都重述为1.5459。无收益源诊断回执为 `F:/Dev/AIstock_model_artifacts/position_timing_advice_v1/research/pattern_strategy_v1/source_diagnostics/c01a8d3dd05ffda3c23eaa6be5c886f4989447e173cf2df34c33a5bbe537a822.json`，明确`outcomes_read=false/database_write=false`。
- 第四次运行边界：提交`ab1a20ba2`上的真实prepare在12.5秒返回`PATTERN_FACTOR_ACTION_COVERAGE_INCOMPLETE`，request文件数前后均为3、`new_request_files=[]`，证明新 `position_timing_pattern_strategy_request_v2` 不会在缺少完整因子—公司行动审计时发布。上述4个区间关闭前不运行训练/回放。该数据纠错不能通过改形态阈值、删除股票、缩短历史、把总股本变化强行当同比例送股或读取前三次比较值完成。数据authority修复后仍使用同一第四批人口和两family九项冻结比较；旧v1 request/bundle继续可inspect但没有新审计身份。

## 10. Verification Plan / 测试与验收

直接测试已落在 `backend/tests/position_timing/test_pattern_strategy.py`、`test_pattern_research.py`、`test_pattern_optimizer.py` 与 `test_pattern_model.py`，并复用/扩展 `test_action_value_corporate_actions.py`；当前相关53项、完整position-timing 301项通过，覆盖如下：

1. 因果形态：仅修改T之后数据不改变T事件；low并列规则、b+5边界、确认日次日才成交、停牌不压缩时间；除权前后经济等价样本不产生伪突破，送股导致的成交股数变化不造假放量；同日独立分红相加而同一方案冲突继续拒绝。
2. 状态与动作：不确认、假突破、一次入场、过期、退出边沿、持仓不可ADD、风险退出优先；EXIT目标与可卖/部分成交区别；真实成本未知不伪称盈利。
3. 对照与账本：从b锚定，保留无回踩和未成交；s复制完全相同持仓；预算不足保留现金；逐腿父订单最低佣、分红送转、停牌、终值共同顺延与未知覆盖；等待错失上涨和提前卖飞的反例必须得到负增量。
4. 研究身份：四项比较与family一致；source漂移拒绝复用；retry只返回既有bundle；旧研究/模型/current/N0/card/alert hash不变。稀疏事件统计不把非事件日当零收益、不按非等间隔事件序号抽块。request v2不可删除因子—公司行动审计降级成v1语义，旧v1 artifact仍可inspect。
5. 证据诚实：零事件返回完整人口计数和不可估计原因；UNKNOWN不偷换NO_FILL；相同代码/数据/request重放一致；不把机制SUPPORTED当组合SUPPORTED。

第二块另补选模的直接反例：改变外层月份收益不得改写该月模板；月界不能重置现金/库存或重复退出；已活动事件保持创建时模板；模型训练不含未成熟标签和未来确认状态；缺头/缺优化窗口明确回退身份。源文件/参数展开hash与实际计算值不一致须被检出。规则选择算法和模型的改动分别覆盖，不用复制实现的恒真测试充数。

本次设计验证使用本文件F1校验、主蓝图F2校验与`git diff --check`。实施时按直接测试和变更影响补充既有回放回归，不建立额外监控平台或审批机制。

## 11. Design Acceptance Index

| ID | 设计验收对象 |
|---|---|
| F-001 | 用户目标、持仓/自选身份与设计/实现状态一致 |
| F-002 | 价格、量、时钟和非未来低点定义完整 |
| F-003 | 突破回踩的状态、失效、到期与未成交完整 |
| F-004 | 盈利加速放量EXIT及可卖/未知成本语义完整 |
| F-005 | 入场/退出同状态反事实与缺失覆盖完整 |
| F-006 | 完整策略、样本和四项统计比较闭合 |
| F-007 | 最小代码面、基础复用、单模块隔离明确 |
| F-008 | 调优候选、时间选择边界、模型目标及阶段边界明确 |
| F-009 | 候选场景和QE延后组合边界明确 |
| F-010 | 直接测试、风险与证据交付要求明确 |

## 12. Design Acceptance Matrix / 设计验收矩阵

`ENGINEERING_VERIFIED`表示设计条款已有真实代码与直接测试，不表示模型取得增量或生产功能已加载。前三次正式运行都因artifact或人口覆盖不完整而不能作为收益证据；第四批只完成不读收益的source preflight且未生成request。不得引用该矩阵、预检或旧bundle声称策略有效。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | 本文§1、§2、§9；主蓝图§9.21 | `backend/tests/position_timing/test_pattern_research.py` | ENGINEERING_VERIFIED | none |
| F-002 | `pattern_strategy.py::pattern_feature_frame`；`action_value.py::market_features`；`action_value_corporate_actions.py::_snapshot_payload` | `backend/tests/position_timing/test_pattern_strategy.py`；`test_action_value_corporate_actions.py` | ENGINEERING_VERIFIED | none |
| F-003 | `pattern_strategy.py::breakout_observed/advance_entry_event`；`pattern_research.py` | `backend/tests/position_timing/test_pattern_strategy.py`；`test_pattern_research.py` | ENGINEERING_VERIFIED | none |
| F-004 | `pattern_strategy.py::acceleration_volume_exit`；`pattern_research.py::_simulate_exit_pair` | `backend/tests/position_timing/test_pattern_strategy.py`；`test_pattern_research.py` | ENGINEERING_VERIFIED | none |
| F-005 | `pattern_research.py::evaluate_entry_and_exit_mechanisms/replay_prototype` | `backend/tests/position_timing/test_pattern_research.py` | ENGINEERING_VERIFIED | none |
| F-006 | `pattern_research.py::replay_full_policy_symbol/_comparison_receipts` | `backend/tests/position_timing/test_pattern_research.py` | ENGINEERING_VERIFIED | none |
| F-007 | `backend/services/position_timing/pattern_*.py`；timing-owned artifact root | `backend/tests/position_timing/test_pattern_research.py`；`python -m pytest backend/tests/position_timing -q` | ENGINEERING_VERIFIED | none |
| F-008 | `pattern_optimizer.py`；`pattern_model.py`；`pattern_research.py::run_pattern_request` | `backend/tests/position_timing/test_pattern_optimizer.py`；`test_pattern_model.py` | ENGINEERING_VERIFIED | none |
| F-009 | 本文§7、§8；当前代码无HMM/QE/Agent import | `backend/tests/position_timing/test_pattern_research.py`；合入前显式import扫描 | ENGINEERING_VERIFIED | none |
| F-010 | 本文§10、§13、§14；四个直接测试文件 | `python -m pytest backend/tests/position_timing/test_pattern_strategy.py backend/tests/position_timing/test_pattern_research.py backend/tests/position_timing/test_pattern_optimizer.py backend/tests/position_timing/test_pattern_model.py -q` | ENGINEERING_VERIFIED | none |

## 13. Risks / 失败模式与 Production Gates

主要风险是：形态阈值任意性、稀少信号、等回踩错失上涨、加速止盈卖飞、同历史重复研究、历史自选不可恢复、无法成交与最低佣造成收益消失、既有风险退出掩盖形态影响。分别用固定参数和失败样本、两项组件对照、连续净值、研究谱系与成本压力报告解释；不以MDE或外部交割单建立准入条件。

本次 `production_ddl_gate=noop`、DML/dependency/restart/runtime均noop。后续研究读本地候选和timing-owned快照；源问题跨模块时只提交需求。Rollback：撤回本设计的下一任务链接或停止后续独立研究即可，既有生产卡和历史研究不变；未来运行版用既有版本化policy回退，不能覆盖旧artifact。

DESIGN-COMPLIANCE-001：①当前交付为完整离线工程实现，不冒充正式收益或运行功能；②未知/无事件/失败显式留证，不填零冒充成功，任何开发、训练或评价覆盖不完整均不能进入SUPPORTED；③用户概要作为原型，按最新授权允许后续依据论文与独立实验优化清仓/减仓/确认和其他参数；④不增加审批、双人确认、sealed holdout、MDE或最新数据等待。生产库只读快照仅遵守用户明确的数据操作授权边界，不由研究结果决定；研究结论不控制工程合入，正式服务仍沿主蓝图已有证据语义。

## 14. 方法论依据与设计审核记录

- [永太科技2019-09-24定向转增实施公告](https://static.cninfo.com.cn/finalpage/2019-09-24/1206944068.PDF)明确 `002326.SZ` 的1.206188/10只向排除控股股东及一致行动人后的其他股东实施；因此数据库每股比例不能无条件套到任意合成持仓。
- [名家汇2025-12-24权益变动公告](https://static.cninfo.com.cn/finalpage/2025-12-24/1224896786.PDF)说明 `300506.SZ` 重整转增后原持股5%以上股东数量未变、比例被动稀释；这与candidate factor中旧持仓数量不增加的身份一致。两项公告仅用于确认公司行动适用对象，不提供或筛选收益结果。
- [Lo、Mamaysky、Wang：Foundations of Technical Analysis](https://www.nber.org/papers/w7613)提出对主观形态做系统识别并检验条件收益分布。本设计借鉴客观编码方法；不把该研究当作MA5/MA10、回踩或A股收益证明。
- [Lee、Swaminathan：Price Momentum and Trading Volume](https://doi.org/10.1111/0022-1082.00280)研究成交量与动量持续性/反转的关联，研究尺度包括中长期。它支持研究量价交互，不证明“当日放量后下一日见顶”。
- [Sullivan、Timmermann、White：Data-Snooping, Technical Trading Rule Performance, and the Bootstrap](https://doi.org/10.1111/0022-1082.00163)说明技术规则搜索需要纳入尝试集合和数据窥探影响。本研究保留实际试验谱系，不把局部校正夸大为所有历史搜索的独立确认。
- [Bailey等：The Probability of Backtest Overfitting](https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf)研究回测选择的过拟合风险；本设计据此要求记录搜索集合及外层样本外路径，不额外建立CSCV平台或把过拟合概率当准入条件。
- [AQR：A Century of Evidence on Trend-Following Investing](https://www.aqr.com/Insights/Research/Journal-Article/A-Century-of-Evidence-on-Trend-Following-Investing)提供跨市场长期趋势研究背景。这不能直接推出A股数日尺度的均线或止盈参数；它提醒本研究同时衡量提前退出错失趋势的代价。

以上来源于2026-09-11公开摘要核验；NBER页面直读受限时使用检索返回的原始摘要，未声称阅读不可访问全文。所有具体窗口/阈值来自用户意图和本设计的可操作定义，尚无本地receipt支持。

第一轮语义审核已修订：清仓保留为原型；回踩等待从突破日锚定；没有回踩的事件保留；收盘确认只在次日尝试；模型只能在决策时看到已经出现的形态。第二轮契约审核已修订：机制收益与完整策略收益分开、两者统计单位分开；完整策略加入同股buy-and-hold与L1；成本未知不虚构浮盈；量比处理送转可比数量；旧研究不因新策略覆盖。用户补充授权后第三轮修订加入有界优化、减仓与衰竭确认，区分一次试验冻结和长期可调整，并修正`initial_holding_endowment`需要非空成本的复用误判。最终文档校验结果由本次PR的命令输出记录。

实现审核新增四组修正：其一，所有N线政策名与数值必须落到代码或receipt，不从名称推断语义；其二，净佣最低5元仅作用于佣金组件，gross必须同时移除已实现费用和估值清算费用，敏感性只在相同人口上标注；其三，PIT关闭只禁止新买入和新形态信号，已持库存仍按停牌/涨跌停/公司行动规则估值及退出，除权停牌日用factor映射最后价格；其四，原型UNKNOWN、优化器开发覆盖、模型训练/评价覆盖与外层覆盖均fail-closed，避免部分样本被标为SUPPORTED。上述均为正确性边界，不是新增研发门禁。
