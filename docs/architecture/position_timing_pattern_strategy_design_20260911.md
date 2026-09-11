# 自选股与持仓股形态择时研究设计

> 版本：v1.0；日期：2026-09-11；Feature tier：F1（本模块研究扩展）
> 状态：`DESIGN_ONLY_NOT_IMPLEMENTED_NOT_RUN_NOT_SERVING`
> 首项任务：`PT-NEXT-018 / TREND_PULLBACK_ACCELERATION_V1`
> 所属蓝图：[持仓与自选池择时建议系统](position_timing_advice_f2_redesign_20260903.md)
> 权威规范：`docs/standards/aistock_development_standard_v1.5_20260523.md`
> 设计源提交：`9722483e9`；本次只交付设计，不启动历史实验或模型训练。

## 1. Background / 目标与现状

目标是为用户已经选定的自选股和持仓股产生可解释的买卖时机建议，并用逐腿成本后的历史收益检验价值。首个研究问题来自用户提出的路径：近期低点附近重新站上 MA5/MA10，回踩确认后买入；已有盈利持仓出现上涨加速和放量时管理退出。用户随后明确允许参考机构实践和论文、逐步微调或优化，不要求照搬概要。所有数值都是首版研究假设；“最优”指限定候选集合与数据条件下经样本外检验的较优方案，不承诺永久或全局最优。

当前正式 L1 仍为规则风险管理。`backend/services/position_timing/policy.py` 的 `PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1.buy.breakout_addon.enabled=false`、`EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1.take_profit.enabled=false`。`action_value.py::MARKET_FEATURES` 有收益、EMA20、波动、收盘位置与量比，但没有本设计的突破后回踩事件状态。已有 ATR 增量与 PT-NEXT-017 结果不能直接证明本组合有效或无效。

`backend/strategies/trend_following_strategy.py::TrendFollowingStrategy` 只有均线放量突破/跌破逻辑，且 `run()` 调用执行器。本设计只参考可核验的公式，不导入该策略、执行器或订单对象。全量EXIT保留为可解释的原型基线；减仓和衰竭确认进入有界优化候选，不假定清仓一定优于减仓或继续持有。

## 2. Scope / 范围与 Non-goals

设计覆盖日频形态、两项独立机制对照、完整规则组合回放、有界参数优化、后续模型学习方式、相似策略研究队列与未来 QE 只读融合边界。首个实现块交付离线原型研究；第二块在同一研究工具内优化规则并研究模型条件化，形成逐股历史影子建议。这是两个连续工作块，不拆平台、审批或日历等待阶段。

产品人口仍为“唯一权威持仓 ∪ 用户显式勾选的已确认自选”；同股持仓身份优先。当前名单只能用于当前或所声明的追溯展示，不能倒填为历史自选名单。首轮正式研究使用冻结 PIT 样本，报告向真实自选/持仓人口迁移的局限。

不修改其他模块、生产数据库、共享 guard 默认值或当前 L1/L1a；不新增订单、自动交易、数据库表、API、页面、scheduler、worker、模型服务或策略市场。HMM、分钟新信号、新闻 Agent 与 QE 成果不进入首轮模型或规则。后续运行接线可复用原页，研究成功不等于运行发布。

## 3. Architecture / 最小实现与复用

```text
冻结日线/复权/指数 + 公司行动/停牌快照 + 全局交易日
                    ↓
  pattern_strategy.py：纯特征、事件状态、固定候选动作
                    ↓
  既有 daily_fill / 持仓现金记账 / 逐腿成本 / 终值与统计
                    ↓
  pattern_research.py：prepare / run / inspect / 不可变结果
                    ↓
  研究报告 → 第二块：同状态标签与模型 → 逐股历史影子解释
```

首块预计只新增上述两个服务文件及两个直接测试文件；按需在现有 `action_value_research.py` 增加明确 policy callback 或复用函数，不复制其成交、公司行动、现金/库存引擎。若现有入口的模型必需参数、review stride 或硬编码动作与本策略冲突，须用显式新模式扩展并回归旧路径，不把模型伪造为空实现来绕过。

| 基础 | 复用位置与约束 |
|---|---|
| 历史日频源 | `action_value_data.py::DailyCandidate`；冻结 manifest、消费字段和文件 hash，读取现有候选 |
| 行情与核心特征 | `action_value.py::market_features`；历史窗口、复权和完整性与现有实现一致 |
| 公司行动/停牌 | `action_value_corporate_actions.py`、`action_value_suspensions.py`；只读已有快照/已授权只读源，禁止修改上游 |
| 交易日与合法交易 | candidate 全局 calendar、`action_value.py::daily_fill`、现有 board-lot；不使用自然日或统一100股规则 |
| 成本和仓位 | `policy.py::PERSONAL_MANUAL_COMPONENT_COST_V1`、已有现金/库存/持仓成本更新；净佣最低5元与规费分别计 |
| 持久化和统计 | `artifact_store.py` 的 hash、锁及原子提交；`action_value_research.py::circular_block_interval` |
| 后续模型 | `action_value_model.py::estimator_parameters/monthly_training_windows/numeric_matrix`；固定 LightGBM 4.6.0 |

未来代码写入范围只含 `backend/services/position_timing/`、`backend/tests/position_timing/` 及本模块设计文档。本次实际文档写入范围只有本文件与主蓝图。研究输出只到 timing-owned `research/pattern_strategy_v1/requests|bundles`；不写全局 N0、QE/Advisory/Selection registry、任何 current/serving 指针、生产 card/event/alert。首轮试验清单保存在自有 request/receipt，不另建试验管理平台。

## 4. Contracts / 价格、时钟与状态

决策时点固定为全局交易日 T 的 20:00（Asia/Shanghai），动作有效期为 T+1 一个交易日。T日最终收盘价和全天成交量只能在收盘数据已可用后用于决策；禁止看完日线后假设当日收盘前成交。盘中未来若展示本策略，也只是提示已冻结建议；日频研究不能宣称捕捉到了当日冲顶最高价。

request展开并绑定candidate manifest、全局calendar hash、实际列/文件hash、公司行动与停牌snapshot、费用和guard snapshot及代码commit。每行携带`decision_as_of/feature_available_at/source_identity`；缺少历史精确到库时间时明确采用上游日频可见时钟假设，不伪造观测过的时间戳；正式来源晚于cutoff时该日不可用。inspect复核输入和输出hash，源码/data/spec变化创建新request；retry不得覆盖旧bundle。

记 C/H/L 为同基准复权 OHLC：raw OHLC × factor，与 `market_features` 一致；MAk为包含当日的k日简单均线，ATR14为14日简单平均 true range。归一化的全部价格使用同一复权基准；成交/费用/涨跌停检查仍用原始人民币价格。复权因子与公司行动校验复用既有契约，不把除权缺口当突破/跌破。

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

每项成本后threshold=0，adjusted lower>0为SUPPORTED，upper≤0为NEGATIVE，其余INCONCLUSIVE；4项分别报告。仅两项完整政策比较均SUPPORTED才可称本规则组合取得局部统计支持；不能因为一个机制为正就称组合有效。没有足够有效日期块计算区间时字段为null与typed reason，不通过调块长、增股票或等明天补救。MDE/区间宽度/样本量为报告义务，不作为开跑或工程合入门槛。

receipt同时记录gross/net、逐腿费用、1/2/3父订单费用敏感性、每腿另加5/10bps摩擦敏感性（假设压力值，不是市场实测）、换手、暴露、最大回撤、等待错失上涨、止盈后的继续上涨及回撤变化。市场上涨/下跌和波动切片仅诊断，不按最赚钱切片重写生效条件。主paired路径必须对称；UNKNOWN和不可估值路径报告数量与影响，禁止静默剔除后宣称完整人口SUPPORTED。负结果同样完成研究任务。

## 6. 有界优化与模型：第二个工作块

首块规则回放用于建立可解释基线；规则未获SUPPORTED不阻止继续研发。第二块分别检验“少量规则细节调整能否改善效果”和“模型能否识别原型何时有用”，允许研究者依据已有结果形成新假设，但用其后未用于该次选择的时间段评价。已经反复看过的历史始终说明探索性，不能因增加一层验证就宣称数据从未被研究者见过。

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

比较两组固定模型：同目标core-only与core+形态块；每组包含上述两个头，使用同一LightGBM 4.6.0规格，完整参数由`estimator_parameters()`在prepare显式展开并hash-bound，early-stopping=false、无搜索。每月expanding训练，初始756session；仅使用`label_available_at <= training_cutoff < prediction_as_of`的已成熟标签，预处理只拟合训练集，缺头/缺模型显式不可用。联合政策在相同可预测日期评价，并报告全计划窗口覆盖。

形态块字段固定候选为：低点年龄、距低点ATR倍数、距MA5/MA10的ATR倍数、两条MA的一日斜率/ATR、当日low距上一日MA10的ATR倍数、加速度a、同股数volume_ratio。全为当前可算的连续值；不增加未来事件标签、不做百指标特征筛选。MA/ATR/量比已有正式因子可经语义与PIT核对后读取同值列，否则本模块纯计算；全局因子桥接不是前置平台任务。

模型条件化先固定在R0目标上，不把8个模板再乘进模型网格。第二块在读取评价收益前落成独立request和验收补充，总共5项主比较：Q-P、Q-buy-and-hold，以及增强模型联合政策减core模型联合政策、减P、减buy-and-hold。family=5，Bonferroni99%区间；内部8模板选择和两组各两头的训练次数另行完整记录，不能写成“只试5个模型”。模型和规则优化分别报告，不在同一外层结果上再挑赢家后声称无偏有效。具体事件支持/持仓转移、训练文件hash和样本数属于该块prepare产物，本设计不伪装它们已经冻结或执行。

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
| 块一：规则历史研究 | 纯形态与事件状态、四项固定比较、连续账户回放、不可变request/bundle/receipt、测试与蓝图回填 | 无论正/负/不确定都交付；不等最新交易日、不要求后端重启 |
| 块二：有界优化、模型条件化与历史解释 | 8模板训练期选择、同状态两头监督、core/增强对照、历史逐股建议与证据说明 | 原型未获支持仍可优化；上线功能按证据和既有影子契约另行接线 |

其他策略和QE组合留在后续队列，不占首块实现量。当前文档合入只登记块一为下一计划项；实现、训练、回放均仍未开始。

## 10. Verification Plan / 测试与验收

直接测试落在计划文件 `backend/tests/position_timing/test_pattern_strategy.py` 与 `test_pattern_research.py`，本次尚不存在，不宣称已通过：

1. 因果形态：仅修改T之后数据不改变T事件；low并列规则、b+5边界、确认日次日才成交、停牌不压缩时间；除权前后经济等价样本不产生伪突破，送股导致的成交股数变化不造假放量。
2. 状态与动作：不确认、假突破、一次入场、过期、退出边沿、持仓不可ADD、风险退出优先；EXIT目标与可卖/部分成交区别；真实成本未知不伪称盈利。
3. 对照与账本：从b锚定，保留无回踩和未成交；s复制完全相同持仓；预算不足保留现金；逐腿父订单最低佣、分红送转、停牌、终值共同顺延与未知覆盖；等待错失上涨和提前卖飞的反例必须得到负增量。
4. 研究身份：四项比较与family一致；source漂移拒绝复用；retry只返回既有bundle；旧研究/模型/current/N0/card/alert hash不变。稀疏事件统计不把非事件日当零收益、不按非等间隔事件序号抽块。
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

`DESIGN_VERIFIED`仅表示文档级设计已逐项审核；所有新增功能仍为未实现、未运行，不得引用该矩阵声称有回测或收益证据。`implementation_refs`此处指设计规定的落点，未新增的代码位置由实施任务另填实证。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | 本文§1、§2、§9；主蓝图§9.21 | artifact:docs/architecture/position_timing_pattern_strategy_design_20260911.md | DESIGN_VERIFIED | none |
| F-002 | 本文§4；action_value.py::market_features | artifact:docs/architecture/position_timing_pattern_strategy_design_20260911.md | DESIGN_VERIFIED | none |
| F-003 | 本文§4.2、§5.1、§5.2 | artifact:docs/architecture/position_timing_pattern_strategy_design_20260911.md | DESIGN_VERIFIED | none |
| F-004 | 本文§4.3、§5.1 | artifact:docs/architecture/position_timing_pattern_strategy_design_20260911.md | DESIGN_VERIFIED | none |
| F-005 | 本文§5.1、§5.3、§10 | artifact:docs/architecture/position_timing_pattern_strategy_design_20260911.md | DESIGN_VERIFIED | none |
| F-006 | 本文§5.2、§5.3 | artifact:docs/architecture/position_timing_pattern_strategy_design_20260911.md | DESIGN_VERIFIED | none |
| F-007 | 本文§3、§9、§13 | artifact:docs/architecture/position_timing_pattern_strategy_design_20260911.md | DESIGN_VERIFIED | none |
| F-008 | 本文§6、§9 | artifact:docs/architecture/position_timing_pattern_strategy_design_20260911.md | DESIGN_VERIFIED | none |
| F-009 | 本文§7、§8 | artifact:docs/architecture/position_timing_pattern_strategy_design_20260911.md | DESIGN_VERIFIED | none |
| F-010 | 本文§10、§13、§14 | artifact:docs/architecture/position_timing_pattern_strategy_design_20260911.md | DESIGN_VERIFIED | none |

## 13. Risks / 失败模式与 Production Gates

主要风险是：形态阈值任意性、稀少信号、等回踩错失上涨、加速止盈卖飞、同历史重复研究、历史自选不可恢复、无法成交与最低佣造成收益消失、既有风险退出掩盖形态影响。分别用固定参数和失败样本、两项组件对照、连续净值、研究谱系与成本压力报告解释；不以MDE或外部交割单建立准入条件。

本次 `production_ddl_gate=noop`、DML/dependency/restart/runtime均noop。后续研究读本地候选和timing-owned快照；源问题跨模块时只提交需求。Rollback：撤回本设计的下一任务链接或停止后续独立研究即可，既有生产卡和历史研究不变；未来运行版用既有版本化policy回退，不能覆盖旧artifact。

DESIGN-COMPLIANCE-001：①本交付是完整设计，不冒充功能实现；②未知/无事件/失败显式留证，不填零冒充成功；③用户概要作为原型，按最新授权允许依据论文与实验优化清仓/减仓/确认和其他参数；④不增加审批、双人确认、sealed holdout前置或最新数据等待。研究结论不控制工程合入，正式服务仍沿主蓝图已有证据语义。

## 14. 方法论依据与设计审核记录

- [Lo、Mamaysky、Wang：Foundations of Technical Analysis](https://www.nber.org/papers/w7613)提出对主观形态做系统识别并检验条件收益分布。本设计借鉴客观编码方法；不把该研究当作MA5/MA10、回踩或A股收益证明。
- [Lee、Swaminathan：Price Momentum and Trading Volume](https://doi.org/10.1111/0022-1082.00280)研究成交量与动量持续性/反转的关联，研究尺度包括中长期。它支持研究量价交互，不证明“当日放量后下一日见顶”。
- [Sullivan、Timmermann、White：Data-Snooping, Technical Trading Rule Performance, and the Bootstrap](https://doi.org/10.1111/0022-1082.00163)说明技术规则搜索需要纳入尝试集合和数据窥探影响。本研究保留实际试验谱系，不把局部校正夸大为所有历史搜索的独立确认。
- [Bailey等：The Probability of Backtest Overfitting](https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf)研究回测选择的过拟合风险；本设计据此要求记录搜索集合及外层样本外路径，不额外建立CSCV平台或把过拟合概率当准入条件。
- [AQR：A Century of Evidence on Trend-Following Investing](https://www.aqr.com/Insights/Research/Journal-Article/A-Century-of-Evidence-on-Trend-Following-Investing)提供跨市场长期趋势研究背景。这不能直接推出A股数日尺度的均线或止盈参数；它提醒本研究同时衡量提前退出错失趋势的代价。

以上来源于2026-09-11公开摘要核验；NBER页面直读受限时使用检索返回的原始摘要，未声称阅读不可访问全文。所有具体窗口/阈值来自用户意图和本设计的可操作定义，尚无本地receipt支持。

第一轮语义审核已修订：清仓保留为原型；回踩等待从突破日锚定；没有回踩的事件保留；收盘确认只在次日尝试；模型只能在决策时看到已经出现的形态。第二轮契约审核已修订：机制收益与完整策略收益分开、两者统计单位分开；完整策略加入同股buy-and-hold与L1；成本未知不虚构浮盈；量比处理送转可比数量；旧研究不因新策略覆盖。用户补充授权后第三轮修订加入有界优化、减仓与衰竭确认，区分一次试验冻结和长期可调整，并修正`initial_holding_endowment`需要非空成本的复用误判。最终文档校验结果由本次PR的命令输出记录。
