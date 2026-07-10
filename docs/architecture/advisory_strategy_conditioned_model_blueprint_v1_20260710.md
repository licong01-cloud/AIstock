# AIstock 荐股策略条件化模型体系 F2 架构蓝图 v1

> 日期：2026-07-10
> 文档类型：F2 顶层架构蓝图，`docs-fast-new` 交付
> 当前状态：蓝图设计已形成，尚未进入任何阶段的详细设计或代码实施
> 适用模块：Advisory 荐股、Selection Center 结果消费、StrategyPackage 只读语义、行业 HMM、行情数据、模型训练、荐股页面
> 最终决策者：用户人工决定是否买入；系统不下单、不记录人工实际买入结果

## 0. 文档定位与权威边界

本文档汇总截至 2026-07-10 对 AIstock 荐股功能的分析、代码核对、机构研究和模型方案，作为后续分阶段详细设计与开发的总纲。

本文档负责锁定：

- 总体架构和模块隔离边界。
- 数据权威、PIT、防泄漏和模型版本规则。
- 多策略包独立运行、候选重排和 Top5 输出原则。
- 超跌反弹与长期趋势两类首批策略专家的目标差异。
- HMM、行业黑名单、收益预测、持股周期和价格区间的关系。
- 每个实施阶段的目标、进入条件、交付物、退出门禁和回滚边界。
- 后续详细设计文档清单及其 Feature tier。

本文档不负责锁定字段级 DDL、最终 API schema、具体模型超参数、页面像素级交互和生产调度参数。这些内容必须在对应 Phase 的 F1/F2 详细设计中完成并通过审批后，才可开始代码实施。

文档权威优先级如下：

1. 用户在本轮及后续明确批准的决策。
2. 本蓝图中列出的总体边界和阶段门禁。
3. 最新已批准的专项 F2 设计和当前实际代码/DDL。
4. 较早文档中与以上内容不冲突的部分。

若本蓝图与早期手工多包或荐股生命周期文档冲突，以已落地的原生单包契约为准：一个荐股程序绑定一个单 Alpha 包或一个原生多 Alpha 父包；多个策略包通过多个独立荐股程序并行运行，不在一个荐股程序中手工融合。

## 1. Background / 背景与现状差距

### 1.1 业务背景

当前策略包可以为指定交易日生成有序候选，但已有观察表明，候选前 20 只内部的原始排名在多数情况下与后续实际收益关系较弱。未来荐股目标希望从每个策略包的候选池中给出约 5 只更具决策价值的股票，并补充：

- 模型重排及胜率或成功概率。
- 扣除成本后的预期收益分布。
- 建议持股周期范围。
- 买入、止盈、止损的参考价格区间。
- 行业 HMM、行业景气、近期热度和风险解释。

不同策略包的经济目标存在显著差异。当前原生多 Alpha 父包偏向超跌反弹，信号衰减和持有周期较短；研发中的新策略包偏向数月级趋势跟踪，希望识别可能形成大级别主升浪的股票。两类策略不能共用同一个短周期收益标签和同一套退出逻辑。

### 1.2 当前实现基线

已经存在且必须复用的能力：

- StrategyPackage manifest 具有稳定的 `package_id`、`package_version`、`manifest_sha256`、`alpha_mode` 和 Alpha 组件信息。
- Alpha 组件已有 `factor_ids`、`model_id`、`holding_period`、`rebalance_frequency`、`risk_tags` 等字段，见 `backend/services/strategy_package/models.py`。
- 原生多 Alpha 父包按一个完整 StrategyPackage 运行，子 Alpha 分数、归一化分数、权重和组合证据可作为模型特征，但不得重新手工组合策略包。
- Selection runtime 在 HMM 启用时先用行业系数调整原始分数并重排，同时保留原始 rank/score、HMM snapshot、preset、行业代码和系数，见 `backend/services/selection_center/hmm_runtime.py`。
- Advisory runtime profile 具备 HMM 配置能力，但默认 `hmm.enabled=false`；只有明确绑定 snapshot/config 和 signal preset 的 Program 才能称为实际启用 HMM。
- `runtime_profile.industry_blacklist` 已按 PIT 行业元数据执行硬过滤，缺失行业映射时 fail-closed，见 `backend/services/selection_center/tradability.py`。
- Advisory 已有 `program_id`、binding version、review run、每日 list version、list item、episode、收益快照和 operation advice 等生命周期实体。
- 当前价格指引为 `rule_default`：使用规则生成买入区间、软/硬止损区间，止盈默认关闭，见 `backend/services/selection_center/price_guidance.py`。
- 当前 Advisory 默认复评策略仍使用固定进入/退出排名 20/40、确认 2 日、每日替换预算 5、止损 8%、追踪止盈激活 18%/回撤 7% 和 20 日 time stop，见 `backend/services/advisory_program.py`。
- Selection 的 `rule_default` 价格指引与 Advisory 生命周期退出策略是两个现有层，不能描述成同一套模型契约。

当前主要差距：

- Advisory 没有规范化 `strategy_style_profile`。
- `holding_period` 是自由文本，尚未成为 Advisory 的可验证模型条件。
- 没有 Advisory 专属训练观察、标签、数据快照、模型版本和预测实体。
- 没有候选 20 只内部的第二阶段模型重排。
- 没有收益分位数、信号存活、MFE/MAE、成交概率或价格区间模型。
- 当前 `candidate.rank` 同时参与进入、保持和退出判断，不能直接被影子模型覆盖。
- HMM 已经调整过分数，若在下游再次固定乘权会重复计算行业影响。
- 长期趋势目标没有候选池召回率、分层收益目标、趋势生存和捕获率评价。

### 1.3 前序设计承接

本蓝图承接并不重复替代以下文档：

- `docs/analysis/advisory_native_multialpha_only_f2_design_20260710.md`
- `docs/analysis/advisory_recommendation_list_lifecycle_design_20260608.md`
- `docs/architecture/strategy_package_platform_boundary_contract_20260520.md`
- `docs/architecture/price_guard_execution_acceptance_plan_2stage_20260602.md`
- `docs/architecture/hmm_daily_coefficient_generation_design_20260428.md`

其中，2026-06-08 生命周期文档中的手工多包设想已被后续原生单包契约替代；其 list version、episode、review run、操作建议和每日连续演进原则继续有效。

| 既有能力或设计 | 本蓝图处置 | 说明 |
|---|---|---|
| Program/binding/review/list/episode 身份链 | 继承 | 保留独立生命周期和 append-only 审计 |
| 单 Alpha 与原生多 Alpha 父包统一 `single_package` 路径 | 继承 | Advisory 不展开 leg、不接受子包或手工权重 |
| 手工 `fusion_pool`、`weighted_rank_fusion`、`union`、`intersection` | 废止 | 历史记录只读，不得用于新绑定、复评或回放 |
| Selection `rule_default` 价格区间 | 继承为基线 | 与新模型预测明确分状态展示 |
| Advisory 固定生命周期参数 | 待风格化设计 | 在正式晋级前保持现状，不能在影子阶段静默改变 |
| `selection_center_advisory_preview` 等旧诊断入口 | legacy diagnostic | 不得作为正式训练观察或荐股入口；物理清理另立设计 |
| PriceGuard 向 QE/Paper 强制推进路线 | 被最新边界覆盖 | 本蓝图只在 Advisory 落地，不自动注入 QE/Paper |

## 2. Scope / 范围

本蓝图覆盖：

- 每个策略包独立的 Advisory 候选重排和 Top5 shortlist。
- 单 Alpha 包与原生多 Alpha 父包统一接入。
- 策略风格画像、风格专家、包级校准和风格漂移。
- HMM 行业状态、行业黑名单和行业优先级的模型化使用。
- 预期收益、持股周期、MFE/MAE、信号存活和价格路径预测。
- 买入、止盈、止损参考区间的 Advisory 决策层。
- 数据库到不可变 Parquet 的训练数据流水线。
- 历史 PIT 观察构建、严格 OOS、模型注册、影子验证和晋级。
- 荐股页面的解释性展示，最终由人工决定是否买入。
- 模型缺失、数据不足、校准失效和版本不匹配的 fail-closed 行为。

多个策略包可以同时独立执行荐股。共享训练底座或风格模型不等于共享候选名单，任何跨包全局总榜都不在本蓝图范围内。

## 3. Non-goals / 非目标与硬边界

本蓝图明确不做：

- 不修改冻结 StrategyPackage manifest、Alpha 权重、模型资产或 HMM snapshot。
- 不把多个独立策略包在荐股页面内手工融合。
- 不使用 legacy Selection preview/aggregate 诊断入口生成正式训练观察或线上推荐。
- 不改变 Selection Center、StrategyPackage runtime、Paper v2 或模拟盘的原始 rank/score 和运行行为。
- 不使用 QE 回测结果、策略包回测摘要、Paper 账户、订单、持仓或模拟收益作为训练特征或标签。
- 不调用 QE 回测引擎构造 Advisory 训练标签。
- 不连接真实下单、资金分配、仓位管理或自动交易。
- 不建设人工逐股增删、覆盖排序或记录实际买入结果的工作流。
- 不承诺找到多倍股，不保证 30% 至 70% 收益。
- 不让模型覆盖行业黑名单或硬风险上限。
- 不把模型概率、收益区间或价格区间描述为确定性预测。
- 不在第一版直接建设大型神经 MoE、端到端深度交易系统或强化学习交易器。
- 不允许缺失模型或特征时静默伪装成模型预测结果。
- 不因蓝图结构校验通过而宣称功能已实现或可进入生产。

## 4. 设计原则与已确定决策

### 4.1 数据与隔离原则

1. PostgreSQL 中的真实行情、行业、资金、交易状态和 Advisory 观察是训练及推理数据权威。
2. Parquet 是带 manifest 和校验哈希的可复现派生快照，不是第二权威数据库。
3. 所有在线特征必须满足 point-in-time，记录 `feature_availability_ts` 和 `market_data_asof`。
4. 模型能力仅属于 Advisory，下游不能反写 Selection、StrategyPackage 或 Paper。
5. 模型制品、数据快照、包 manifest、HMM snapshot 和预测必须可追溯到不可变版本。
6. 市场样本身份、标签策略和 Program lineage 必须拆分：相同候选上下文只形成一个 canonical signal observation，实际改变候选/特征的 selection profile 与改变标签的 label policy 才进入训练/校准条件分布；Program/binding/review lineage 独立留证并用于部署，不得让两个等价 Program 把同一市场样本重复加权。

### 4.2 模型原则

1. 共享数据和特征，不强制共享目标函数。
2. 策略风格必须显式声明，模型在风格内部自动学习具体规律。
3. 第一版使用风格专属 LightGBM 排序、分类和分位数模型组合。
4. 新包先使用风格先验，只有正式 OOS 观察足够后才能启用包级校准。
5. 重排、收益、存活、路径风险和成交概率分别建模，不用一个模型直接猜全部绝对价格。
6. 允许模型拒绝预测；拒绝比静默给出低质量答案更正确。

### 4.3 投资建议边界

1. 系统输出的是 Advisory 参考信息，不是订单。
2. 人工决定是否买入，不要求程序记录人工最终选择。
3. 行业黑名单是硬过滤；HMM 和行业景气是模型条件或先验。
4. 硬止损属于独立风险规则，不能被模型无限放宽。
5. 买入、止盈和止损均输出较小范围及置信度，不输出保证价格。

## 5. 机构实践与研究依据

研究用于解释方法选择，不构成 AIstock 自有模型有效性的验收证据。

| 研究或机构结论 | 本蓝图采用点 | 限制 |
|---|---|---|
| [Cross-sectional Learning-to-Rank](https://arxiv.org/abs/2012.07149) 直接优化资产相对顺序 | 候选按 decision-as-of/target 日期、canonical signal context 和 label policy 形成去重 ranking group，优化 Top5 | 论文结果不能代替 A 股 OOS 验证 |
| [Gu、Kelly、Xiu 的机器学习资产定价研究](https://doi.org/10.1093/rfs/hhaa009) 强调非线性和高维交互 | 支持以树模型学习候选、行业与市场状态交互 | 论文预测目标和美股样本不能直接迁移 |
| [LightGBM LambdaRank](https://lightgbm.readthedocs.io/en/latest/Parameters.html) 支持 NDCG Top-K 目标 | 第一版候选重排采用 GBDT ranker | 仍需独立概率和风险模型 |
| [TRA, KDD 2021](https://arxiv.org/abs/2106.12950) 通过多个预测器处理不同交易模式 | 使用显式风格路由和专家模型 | 不直接照搬深度网络实现 |
| [Google MMoE](https://research.google/pubs/modeling-task-relationships-in-multi-task-learning-with-multi-gate-mixture-of-experts/) 建模任务关系 | 作为后续共享专家、任务专属门控的研究方向 | 第一版样本量不足以支撑复杂 MoE |
| [Robeco 短期反转研究](https://www.robeco.com/en-us/insights/2023/10/reversing-the-trend-of-short-term-reversal) 指出反转与行业/因子动量冲突 | 反弹专家使用 HMM 行业修复、行业动量和资金状态 | 机构样本不能证明当前 HMM 系数有效 |
| [Moskowitz、Grinblatt 行业动量](https://doi.org/10.1111/0022-1082.00146) 研究行业对个股动量的解释 | 行业趋势/宽度作为 HMM 之外的独立特征并做消融 | 历史美股结果不等于当前 A 股行业优先级 |
| [Time Series Momentum](https://w4.stern.nyu.edu/facdir/lpederse/papers/TimeSeriesMomentum.pdf) 发现约 1 至 12 个月持续性 | 长期趋势专家使用 20 至 180 日标签 | 研究以多资产为主，不等同 A 股个股多倍股 |
| [AQR 百年趋势研究](https://www.aqr.com/Insights/Research/Journal-Article/A-Century-of-Evidence-on-Trend-Following-Investing) 支持截断亏损、让盈利延续 | 长期策略采用趋势存活和移动保护，不用小幅固定止盈 | 不能作为选中科技大赢家的直接证明 |
| [Momentum Crashes](https://doi.org/10.1016/j.jfineco.2015.12.002) 说明动量在高波动反弹期可能崩溃 | 市场/HMM 状态进入趋势风险模型 | 需要 A 股独立校准 |
| [Conformalized Quantile Regression](https://proceedings.neurips.cc/paper/2019/hash/5103c3584b063c431bd1268e9b5e76fb-Abstract.html) 校准条件区间覆盖率 | 收益和路径区间增加滚动校准 | 分布漂移下仍需持续监控 |
| [DeepHit](https://ojs.aaai.org/index.php/AAAI/article/view/11842) 建模事件时间和竞争风险 | 长期目标、止损、趋势失效和超时采用生存/风险思想 | 第一版可先使用离散时间 GBDT 实现 |
| [AQR 因子时变研究](https://www.aqr.com/Insights/Research/Journal-Article/How-Do-Factor-Premia-Vary-Over-Time-A-Century-of-Evidence) 提醒因子择时在滞后和成本后较难 | HMM 只作为可验证条件，不作为必然增益 | 任何行业优先级必须做消融 |
| [Probability of Backtest Overfitting](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253) 量化多次试验的选择偏差 | 多期限、多模型和多 regime 搜索必须预登记并做多重检验治理 | 统计修正不能替代经济显著性和 forward shadow |

## 6. Architecture / 目标架构

### 6.1 总体流程

```text
Advisory 为每个 Program 触发现有正式 Selection pipeline
  -> StrategyPackage 独立生成 Alpha 原始候选
  -> [hmm.enabled=true 时] 现有 HMM 行业分数调整
  -> risk policy 的 can_buy/score multiplier/delta/rank penalty 与重排
  -> 行业黑名单、停牌及其余 decision-as-of cutoff 时已知的可交易性硬过滤
  -> 完成 Advisory 自有、权威且可追溯的 SelectionRun
  -> Advisory 候选池与完整退出观察深度
  -> strategy_style_profile 显式路由
  -> 风格专属候选重排专家
  -> Top5 质量、行业和相关性约束
  -> 收益分布、信号存活、MFE/MAE、成交概率模型
  -> 原始价格区间决策层
  -> Advisory list version 与页面影子/正式展示
  -> 人工决定是否买入
```

### 6.2 模块边界

```text
SelectionRun 已完成
  -> AdvisoryCandidateModelService
       -> FeatureSnapshotResolver
       -> StrategyStyleRouter
       -> AdvisoryCandidateRerankerBundle
       -> AdvisoryOutcomeDistributionModelBundle
       -> AdvisoryPricePathModelBundle
       -> AdvisoryPriceDecisionLayer
  -> AdvisoryProgramService._evaluate_review
```

最窄接入点位于 SelectionRun 转换为 AdvisoryCandidate 之后、Advisory 中央复评之前。模型服务只生成候选派生副本和 Advisory 证据：

- `alpha_raw_rank/alpha_raw_score`：HMM 前的 Alpha 原始证据，始终保留。
- `hmm_adjusted_rank/hmm_adjusted_score`：HMM 后、risk policy 前的中间证据；当前实现尚未完整持久化该 rank，Phase 1 必须补采。
- `risk_policy_adjusted_rank/risk_policy_adjusted_score`：risk policy 的 can_buy、multiplier/delta、rank penalty 和重排后的证据；禁用软 overlay 时也要记录 enabled/policy hash。
- `selection_effective_rank/selection_effective_score`：risk policy 与可交易性硬过滤完成后的正式 Selection 权威排名，当前 Advisory 生命周期的基线输入。
- `advisory_model_rank/advisory_model_score` 独立存储。
- 影子阶段不得覆盖当前用于进入/退出的 `candidate.rank`。
- Paper v2 继续读取原始 `SelectionRun.aggregate_results`。
- 正式复评所需候选深度必须覆盖 `rank_exit_threshold`；不足时延续现有 `ADVISORY_EXIT_OBSERVATION_DEPTH_INSUFFICIENT` fail-closed 语义。

模型关闭或回滚时恢复的是 `selection_effective_rank`，不是 HMM 前的 `alpha_raw_rank`。五层 rank 必须带各自 stage、生成配置和 hash，禁止继续引入含义不明确的“源排名”新字段。

### 6.3 权威候选运行与观察深度

- 每个 Program/target trade date 的候选必须来自 Advisory 自有的权威单包 `SelectionRun`，并继续通过现有正式 StrategyPackage/Selection artifact 路径生成；legacy preview、历史 aggregate 诊断或跨包合成结果不能作为候选源。
- Advisory 可以在自己的 effective runtime profile 中请求更深的 `selection.top_k`，用于模型候选召回和生命周期退出观察，但不得修改普通 Selection Center 或 Paper v2 的默认运行配置。单 Alpha 仍受 Selection v1 上限 50 约束；原生多 Alpha 父包只能使用 frozen manifest 已声明的 `topk/topk_variants/secondary_topk`，否则现有 runtime 会 `TOPK_RUNTIME_MISMATCH`。缺少所需深度变体时，必须发布经正常验证的新父包版本或另立 Advisory 深池契约，不能临时覆盖。
- 每次运行必须持久化 `requested_top_k`、manifest top-k variant、Alpha 总评分数量、HMM/risk policy 前后候选数量、硬过滤前后数量、eligible universe 标识及 hash、effective runtime config、SelectionRun id 和 artifact hash。
- 候选截断必须同时满足模型特征深度与 `rank_exit_threshold` 观察深度；前者不足使用 `ADVISORY_MODEL_FEATURE_DEPTH_INSUFFICIENT`，后者不足继续使用现有 `ADVISORY_EXIT_OBSERVATION_DEPTH_INSUFFICIENT`，两者不得混用。
- 为 HMM/risk overlay 消融，Phase 1 必须保存同一权威运行的 Alpha 原始深池、HMM 调整后深池、risk policy 调整后深池和硬过滤后正式深池。

### 6.4 多策略包独立性

- 一个 Advisory Program 只绑定一个单 Alpha 包或一个原生多 Alpha 父包。
- 多个 Program 可同时运行，每个 Program 单独生成候选、Top5、模型预测、列表版本和生命周期。
- 共享风格专家只共享模型参数，不共享候选名单、状态或排名。
- 原生多 Alpha 父包的各 leg 分数、权重、一致度和分歧度可以作为输入，但父包仍是唯一包身份。
- 不提供跨包总榜、交叉替换或自动择包。

## 7. 策略风格画像与专家路由

### 7.1 Advisory 专属风格画像

新增逻辑实体 `strategy_style_profile`，稳定键为 `(package_id, manifest_sha256, profile_version)`。它属于 Advisory 平台配置，不写入冻结 manifest。风格声明可以包级复用；训练/校准只绑定实际改变 signal/label 的 effective selection profile 与 label policy，Program/binding/review 仅作为 lineage 和 deployment scope，不默认生成重复样本。

最少语义：

```text
style_family
primary_horizon_days
supported_horizons
signal_decay_prior
label_objective
candidate_pool_policy
entry_hold_exit_policy_family
price_guidance_policy_family
calibration_policy_family
profile_version
```

首批风格：

- `SHORT_REBOUND`：超跌反弹、流动性修复、快速信号衰减。
- `LONG_TREND`：行业/个股趋势持续、右尾收益、较长持有周期。

模型可以根据候选的回撤、距高点、动量斜率、换手、行业状态和多 Alpha 结构估计 `observed_style`，但该结果只用于漂移诊断和候选级软路由，不能覆盖声明的业务目标。

### 7.2 层级收缩

```text
GLOBAL_PRIOR
  -> STYLE_PRIOR
      -> PACKAGE_CALIBRATED
```

- `GLOBAL_PRIOR/STYLE_PRIOR` 只能来自其他包的同目标、同口径合法 OOS 观察，或在研究前预先声明并冻结的通用候选生成器的合法 OOS；来源 package/generator、cutoff、scope 和 hash 必须进入制品证据。
- 当前包使用冻结后模型回看其研发期得到的 `RETROSPECTIVE_RESEARCH_ONLY` 数据只能训练内部 research bootstrap，不能包装成 `GLOBAL_PRIOR/STYLE_PRIOR` 数字预测。
- 新策略包或新 manifest 只有在存在兼容合法先验时才使用 `STYLE_PRIOR`；否则模型能力返回 `MODEL_UNAVAILABLE`，现有荐股基线继续运行。
- 包级正式 OOS 有效样本和市场阶段覆盖满足详细设计门槛后，才能升级为 `PACKAGE_CALIBRATED`。
- 包版本发生实质变化时，不得无条件继承旧包校准；详细设计需定义迁移或回退规则。

## 8. HMM、行业黑名单与行业优先级

### 8.1 现有 HMM 使用规则

现有 HMM 已经对候选分数执行一次乘法调整并重排。下游模型必须同时读取原始和 HMM 后证据，由模型学习 HMM 的边际作用，不再次执行固定乘权。

- `hmm.enabled=true`：必须匹配现有 snapshot、preset、日期与 artifact hash；证据缺失时模型通道 fail-closed，并返回 `ADVISORY_HMM_EVIDENCE_MISSING`。
- `hmm.enabled=false`：记录明确的 `hmm_enabled=false`，`hmm_adjusted_rank/score` 为空，由 no-HMM 路由/校准消费 `alpha_raw` 与 `selection_effective` 证据；这不是错误。
- 本蓝图不新增 HMM 日调度器，继续消费现有 generation-on-miss 产物；是否启用仍由 Program 的 effective runtime profile 决定。

基础特征：

```text
alpha_raw_score, alpha_raw_rank
hmm_adjusted_score, hmm_adjusted_rank
risk_policy_adjusted_score, risk_policy_adjusted_rank
selection_effective_score, selection_effective_rank
hmm_enabled
hmm_model_snapshot_id, hmm_signal_preset
sector_code, hmm_coefficient
hmm_as_of_trade_date, hmm_effective_trade_date
hmm_generation_mode, input_data_max_dates
hmm_freshness_lag, coefficient_artifact_hash
```

当前 HMM 证据主要记录 artifact 路径，正式训练数据必须补充内容 SHA 和可验证日期范围。

现有 daily artifact 的 `effective_trade_date=D` 只使用 `as_of_trade_date=D-1`。按 Advisory 日期契约，目标 SelectionRun 的 `D=T+1`，因此对应 artifact 是 `effective=T+1/as_of=T`。模型必须消费该 SelectionRun 实际使用的 artifact，不得事后替换；`effective=T/as_of=T-1` 属于 target date 为 T 的另一信号上下文，不能混入同一 ranking group 或校准 scope。

### 8.2 风格化语义

| 风格 | HMM 重点 | 禁止误用 |
|---|---|---|
| `SHORT_REBOUND` | 行业持续下跌概率、弱转强、止跌、行业宽度修复 | 把当前强势系数直接当作反弹成功保证 |
| `LONG_TREND` | 强势状态持续时间、未来转弱概率、行业宽度、资金扩散 | 只用单日行业系数决定数月持有 |

如果同一 HMM 状态识别器服务不同周期，必须为不同风格训练或校准不同的“状态到优先级”映射。

### 8.3 行业黑名单

- 行业黑名单在候选截断前执行硬过滤。
- 被屏蔽股票不得被模型恢复。
- 过滤后应继续向后寻找候选，直到达到候选池目标或真实候选耗尽。
- 黑名单配置和 PIT 行业映射必须进入预测审计证据。

### 8.4 Selection risk policy

- 现有 risk policy 可能执行 `can_buy` 排除，也可能通过 score multiplier/delta、rank penalty 和重排形成软 overlay；它位于 HMM 与最终 tradability 排名之间。
- 正式观察必须保存 `risk_policy_enabled`、policy/version/hash、每只候选的排除/乘数/delta/penalty 和调整前后 rank/score。
- 模型只把 risk policy 结果作为可验证条件，不重复机械施加；Phase 0B/3 必须同时比较 overlay enabled/disabled 或等价消融。
- risk policy 证据缺失时，不得声称已复算 `selection_effective_rank`；model route 返回 `ADVISORY_RISK_POLICY_EVIDENCE_MISSING`，现有 Selection/Advisory 基线处置仍遵守 §16.3。

## 9. 候选重排与 Top5 选择

### 9.1 候选条件重排

训练 ranking group 是 `(decision_as_of_trade_date, target_trade_date, signal_context_hash, label_policy_hash)` 下的一组候选，标签另存 `effective_entry_trade_date`。其中 `signal_context_hash` 覆盖 package/manifest、Selection deterministic content/stage hash、selection runtime code commit、adapter/query semantic version、决策 cutoff、PIT universe、HMM 状态和实际影响候选/特征的 effective selection profile；它必须排除 `run_id`、`created_at`、Program/binding lineage 等非内容身份。`label_policy_hash` 覆盖期限、入场基准、benchmark/cost、barrier 和企业行动口径。Program/binding/review lineage 随观察保存，但除非配置确实改变 signal context 或该模型的标签定义，否则不产生重复训练样本。第一版模型包：

```text
AdvisoryCandidateRerankerBundle
  CandidateLambdaRankModel
```

收益、MFE/MAE 和路径风险在每个 `(style_family, signal_context_scope, label_policy_scope)` 内只能有一个权威 `AdvisoryOutcomeDistributionModelBundle` 实例。SHORT_REBOUND 与 LONG_TREND 必须是不同实例和标签头，不是全平台共用一个短周期模型。Phase 3 排序器可以使用同口径 bootstrap 标签，但不得在同一 scope 发布第二套收益/风险预测；对应 style 的 Phase 4 能力就绪后，排序 bundle 只能按原子 bundle version 消费该权威结果模型。

候选特征包括：

- 原始 Alpha rank/score、分差和候选内分位数。
- 多 Alpha 各 leg 分数、权重、一致度和分歧度。
- HMM 原始/调整后证据。
- Selection risk policy 调整前后证据和 policy hash。
- 个股动量、反转、波动、成交额、换手和资金流。
- 行业相对强度、宽度、资金、估值和 HMM 状态。
- 市场风险状态、指数宽度和风格环境。
- 候选列表上下文、行业集中度和相关性簇。

### 9.2 Top5 不是机械前五

最终候选应在模型排序上应用可解释约束：

- 行业黑名单和不可交易性已经硬过滤。
- 可配置单一行业最大数量和高相关簇最大数量。
- 流动性、决策 cutoff 时已知的可交易性和数据完整性必须达标；T+1 涨跌停/停牌由价格路径模型作为未来标签处理。
- 目标为 5 只，但不静默用低质量股票补满；页面显示“合格 N/5”和观察候选。
- 最终买入仍由人工决定。

`shortlist_top_n=5` 是模型/页面 shortlist，不等于现有 Advisory Program 的 `target_count`。候选观察深度 `candidate_observation_top_k`、模型 shortlist 数 `shortlist_top_n` 和正式 active list 目标 `program.target_count` 是三个独立、版本化字段；蓝图阶段不得因展示 Top5 就把当前 20 只 Program 静默缩容。

### 9.3 候选池召回率

重排模型只能从候选池内选择。必须先测量：

```text
strategy_recall@K(q, h, label_policy_hash) =
  PIT eligible universe 中在期限 h 内满足 q 的股票且进入权威 TopK 的数量
  / PIT eligible universe 中在期限 h 内满足 q 的股票数量

conditional_recall@K(q, h, label_policy_hash) =
  权威最大深池中在期限 h 内满足 q 的股票且进入 TopK 的数量
  / 权威最大深池中在期限 h 内满足 q 的股票数量

q(h) in {MFE_h >= 30%, MFE_h >= 50%, MFE_h >= 70%}
```

`label_policy_hash` 固定 effective entry basis、horizon、企业行动、停牌/退市、benchmark/cost 和 censor 口径。`strategy_recall@K` 判断策略包是否把未来赢家召回到候选池，`conditional_recall@K` 判断在已生成深池内截断到 K 的损失。二者的完整参数、denominator、eligible universe hash、最大深池深度和数据可用时点必须一同落库；不同 h/policy 的 Recall 不得直接比较。

若长期大赢家的 `strategy_recall@20` 不足，Advisory 可以在自身权威 `SelectionRun` 请求不超过 50 的 `top_k` 后再重排，页面仍可只展示观察 Top20 和模型 shortlist。若 `strategy_recall@50` 仍不足，必须先完成 Advisory 专属深池观察路径设计，或对共享 Selection 上限做独立契约变更和 Selection/Paper 回归，才能评估 100；本蓝图不把 Top100 当作现有能力。候选池扩大必须由 OOS Recall、计算成本、特征深度和退出观察深度共同决定。

## 10. 策略风格专家

| 维度 | `SHORT_REBOUND` | `LONG_TREND` |
|---|---|---|
| 主要期限 | 1/3/5/10/20 日 | 20/40/60/120/180 日 |
| 排序目标 | 5 至 10 日扣费后风险调整收益 | 趋势持续、右尾收益和捕获率 |
| 主要标签 | 正收益、短期 MFE/MAE、信号衰减、成交概率 | 分层目标到达、趋势存活、峰前回撤、趋势失效 |
| 主要风险 | 接飞刀、流动性不足、交易成本、反弹失败 | 趋势反转、拥挤、深度整理中过早退出 |
| 生命周期 | 快速复评、较短 time stop | 入选与继续持有阈值分离、移动保护 |

两类专家共享 FeatureSnapshot 和数据构建器，但必须使用独立标签、模型、校准、验证报告和晋级状态。

## 11. 预期收益、持股周期与路径风险

### 11.1 结果分布模型包

```text
AdvisoryOutcomeDistributionModelBundle
  NetReturnQuantileModel
  PositiveReturnProbabilityModel
  MFEQuantileModel
  MAEQuantileModel
  [SHORT_REBOUND] SignalSurvivalModel
  [LONG_TREND] LongTrendCompetingRiskHazardModel

AdvisoryPricePathModelBundle
  EntryGapFillModel
  LimitFillProbabilityModel
  IntradayExecutionEventOrderModel
```

推荐输出：

- 各支持期限的扣费后绝对收益与 benchmark 超额收益 P20/P50/P80。
- `P(net_absolute_return > 0)`、`P(net_excess_return > 0)` 和经校准置信度。
- 信号在 1/3/5/10/20 或 20/40/60/120/180 日仍有效的概率。
- 未来最大有利波动 MFE 和最大不利波动 MAE 分位数。
- 持股周期下界、建议区间和失效上界。
- 模型、数据和校准状态。

不能只输出平均收益。长期趋势策略也不能以胜率为唯一目标，否则会偏好频繁小涨并排斥低频高赔率候选。

第一版建议使用可解释的独立模型头，而不是一个多输出黑箱：

```text
AdvisoryCandidateRerankerLGBM v1
AdvisoryHoldingHorizonLGBM v1
AdvisoryPricePathLGBM v1
AdvisoryLongTrendHazardLGBM v1
```

算法职责固定为：LambdaRank 只优化候选 Top-K 顺序，不发布“胜率”；收益正概率由 Outcome 的分类/分位数头统一校准，quantile LightGBM 输出收益/MFE/MAE 分位数。SHORT_REBOUND 使用离散时间 `SignalSurvivalModel`，LONG_TREND 用一个 competing-risk hazard 同时派生 stage survival、time-to-hit 和 competing event probability，不再并行输出同名 generic survival。`IntradayExecutionEventOrderModel` 只处理分钟级入场/限价/涨跌停/可执行止损触达顺序；各头可共享快照，但不得共用不相容标签、字段名或部署版本。

核心标签概念：

```text
r_total_gross_h = corporate-action-consistent executable total return
r_net_absolute_h = r_total_gross_h - explicit transaction costs
r_net_excess_h = r_net_absolute_h - benchmark_total_return_h
MFE_h = max future corporate-action-normalized path / normalized entry - 1
MAE_h = min future corporate-action-normalized path / normalized entry - 1
gap_1d = corporate-action-normalized target-day open / decision-as-of pre_close - 1
fill(candidate_price) = minute path crosses price while tradable
survival_h = signal remains valid and risk conditions have not failed
```

字段级可执行价格、成本、benchmark、企业行动、停牌和涨跌停口径必须由 Phase 1/3/4/5 详细设计锁定。排序 relevance 默认以风险调整后的 `r_net_excess_h` 为主要目标，用户收益区间同时展示 `r_net_absolute_h`，买入安全边际优先使用绝对净收益；最终组合权重在 Phase 0A 预登记。`benchmark_policy_id/hash` 与 `cost_policy_id/hash` 必须进入 label schema、dataset manifest、prediction 和 calibration scope；不同 benchmark 或成本版本的结果不得直接混合比较。

### 11.2 持股周期范围

持股周期由信号存活、扣费后收益下分位数、风格先验和路径风险共同决定：

```text
有效期限 h 当且仅当
P(style_specific_survival_to_h) 达标
且 P20(r_net_absolute_h) 与 P20(r_net_excess_h) 满足详细设计阈值
且风险/可交易性状态未失效
```

输出是范围而不是准确天数。初始历史构建可以使用严格 PIT 数据形成观察，不必等待新系统在线运行数月，但不合法的包版本历史只能作为研究先验。

## 12. 买入、止盈、止损价格区间

### 12.1 预测与价格转换分离

模型在企业行动一致的归一化路径上预测收益率、基点、跳空和路径分布，`AdvisoryPriceDecisionLayer` 再结合 signal-date 原始价与复权因子转换为价格区间：

```text
模型分布
  + raw CNY/yuan pre_close/current/open
  + 复权因子
  + tick size
  + 涨跌停、停牌、T+1
  + 流动性和成本
  -> 原始价格区间
```

模型不得直接学习不可比的绝对复权价格。

收益、MFE/MAE 和存活标签不得直接跨除权除息/送转使用未复权 high/low，否则会制造虚假跳空和风险事件。训练只学习相对、企业行动一致的路径；页面价格区间才转换为现有 `raw` CNY/yuan 契约。

本蓝图中的 `raw price` 固定指“未复权、单位为人民币元的展示/交易价格”，不等同于数据库物理存储整数。`market.kline_daily_raw.*_li` 等厘单位字段必须通过同一受测转换契约除以 1000；训练 builder 与在线服务禁止各自实现不同单位换算。

`decision_ref_price` 是 `decision_as_of_trade_date` 收盘时的参考基准，不能与旧字段 `reference_price`、legacy `episode.signal_date` 或实际成交成本混用。新契约禁止继续复用含义冲突的单一 `price_basis`：逻辑证据必须拆成 `price_reference_basis`、`execution_basis`（如 `next_open_executable/signal_close/next_close`）、`adjustment_basis=raw`、`currency=CNY`、`price_unit=yuan` 和物理 `storage_scale`。现有 Selection price guidance 与 Advisory Program adapter 分别保持各自既有字段枚举，详细设计负责显式映射；同时记录数据截止时间、policy/model hash、tick、涨跌停和停牌证据。

### 12.2 买入区间

买入区间综合：

- 次日开盘跳空分布。
- 指定限价的分钟级成交概率。
- 支付该价格后的 P20 净收益是否仍有安全边际。
- 最大追价、涨停距离、流动性和 tick 约束。

荐股系统没有实际成交价，因此只使用 `reference_entry_price`。如需按用户实际持仓成本复算，必须由独立人工输入能力设计，不得读取 Paper v2 持仓。

### 12.3 止盈和止损

- 止盈使用 MFE、收益分布和趋势状态形成“激活区间 + 移动保护”，不强制单一固定价。
- 软止损区间可由 MAE、波动和信号失效预测形成。
- 硬风险价格始终由独立风险规则约束，模型只能收紧，不能突破上限。
- A 股 T+1、停牌和跌停可能导致止损不可立即执行，页面必须展示可执行性状态。
- 当前 `rule_default` 保留为明确标识的规则基线；模型不可用时不得把规则结果标成 `model_predicted`。

分钟数据是窄买入区间、事件先后顺序和成交概率的必要条件。只用日线时只能提供较粗的收益、周期和路径区间。

## 13. 长期趋势与大行情捕获

### 13.1 有序收益状态

`+30%/+50%/+70%` 是嵌套目标，不是三个互不相关的事件。长期专家采用有序多状态：

```text
ENTRY
  -> HIT_30
       -> HIT_50
            -> HIT_70
            -> STOP_OR_TREND_BREAK
       -> STOP_OR_TREND_BREAK
  -> STOP_OR_TIMEOUT
```

模型输出：

```text
P(HIT_30 before stop)
P(HIT_50 given HIT_30)
P(HIT_70 given HIT_50)
time_to_hit quantiles
stage-specific stop/trend-break hazards
```

组合概率必须保持 `P70 <= P50 <= P30`。每个阶段的“达到下一目标、止损、趋势失效、超时”才构成竞争风险。

LONG_TREND competing-risk 的事件粒度固定为 20 至 180 个交易日的 `TARGET_STAGE_HIT/TREND_BREAK/TIMEOUT`，输出字段使用 `trend_stage_*` 前缀；它不处理分钟级 fill/limit/stop 事件。PricePath 的分钟输出使用 `intraday_execution_*` 前缀，两者不得复用 event type 或概率字段。

### 13.2 绝对收益与趋势捕获率

必须区分：

- `absolute_return_target`：从参考入场价实际上涨 30%、50% 或 70%。
- `trend_capture_ratio`：退出策略捕获整段最大有利行情的比例。

若目标是股票数月上涨数倍而系统捕获其中一部分，`trend_capture_ratio` 是退出策略的主要评价指标，分层绝对收益概率是入选和持有证据。

### 13.3 长期生命周期滞回

长期趋势不能每日重新机械取前 5：

```text
ENTER：模型前5且趋势概率、数据和风险达标
HOLD：排名下降但仍在观察深度内，趋势存活且行业未失效
EXIT：趋势存活、行业状态、风险或确认条件明确失效
```

进入阈值、继续持有阈值和退出阈值必须分离，避免几日整理淘汰真正的长期赢家。

## 14. 数据权威、PIT 与不可变 Parquet

### 14.1 数据来源

允许的主要数据库来源：

- `market.kline_daily_raw`
- `market.kline_minute_raw`
- `market.adj_factor`
- `market.daily_basic`
- `market.moneyflow_ts`
- `market.sector_data`
- `market.sw_index_member`
- `market.sw_index_classify`
- `market.sw_daily`
- `market.index_daily`
- `market.cyq_perf`
- `market.suspend_d`
- `market.stk_limit`
- `market.stock_basic`
- `market.stock_universe_pit_spans`
- Advisory 自有 selection/review/list/episode 观察实体

PIT 申万行业归属以 `sw_index_member`/`sw_index_classify` 为权威；`sector_data` 主要提供连续行业特征，不能替代离散 PIT 行业成员关系。

PIT eligible universe 必须按交易日重建并防止存活偏差：

- 记录 `universe_version/hash`、`list_date`、`delist_date`、证券/板块状态、新股 seasoning、ST/退市整理、长期停牌和流动性规则版本。
- 历史退市股必须保留在其当时合法 universe 和后续 outcome 中；禁止用“当前仍上市股票列表”回放历史。
- 只有在 T 日 cutoff 前可知的上市、停牌和风险状态可以决定 T 日 eligibility；未来退市、T+1 停牌或后续板块变化不得泄漏为特征。
- universe 构建、StrategyPackage 自身 eligible 规则与 Advisory 硬过滤必须分层留证，Recall 分母使用哪一层必须明确。

ST/风险状态历史复算以现有 `market.stock_universe_pit_spans` 为权威，`stock_basic` 只补充上市/退市等基础元数据，不能用当前状态替代 PIT span。

禁止的训练来源：

- StrategyPackage `backtest_summary`、`backtest_context`、`metrics_snapshot`。
- QE backtest/archive 的收益、成交和持仓结果。
- Paper v2 账户、订单、持仓、现金和模拟收益。
- 人工实际买入或主观选择结果。

### 14.2 历史观察构建

正式训练观察必须满足：

- 候选只使用当时可得数据和时间对应的策略包/模型版本。
- 先计算 `effective_strategy_package_oos_cutoff = max(所有 Alpha leg、model asset、预处理/特征资产的 training/selection/research/freeze cutoff，父包 freeze/promotion_at，实际 Selection runtime/adapter/query semantics freeze_at，以及 hmm.enabled=true 时实际 HMM snapshot 的 training/selection/freeze cutoff)`。正式 OOS 起点必须晚于 `effective_strategy_package_oos_cutoff + embargo`，且对应父包、全部组件/model/HMM/runtime semantics 当时已经存在；这里只读取冻结/决策时间元数据，不读取回测结果。
- 原生多 Alpha 父包不能只取父包字段、首个 leg 或主模型 cutoff；任一实际引用组件缺少可验证 cutoff/vintage 时，该区间为 `FORMAL_OOS_UNAVAILABLE`。
- 历史观察优先引用当时已持久化的权威 SelectionRun。使用后来代码重建时，必须证明同一 executable semantics/version 在 signal date 前已冻结；否则即使行情 PIT 正确也只能标记 `RETROSPECTIVE_RESEARCH_ONLY`。
- 仅满足训练截止日而不满足上述最晚决策时点的回放标记为 `RETROSPECTIVE_RESEARCH_ONLY`；缺少合法 vintage 时标记 `FORMAL_OOS_UNAVAILABLE`，不能进入包级校准或正式晋级。
- 权威深池中的所有候选都生成固定期限 outcome label；不得只给 ENTER/人工最终选择的股票打标签，以免产生选择偏差。
- 为计算 `strategy_recall@K`，Phase 1/0B 另构建 PIT eligible universe 的轻量 outcome denominator；它不进入正式候选或荐股列表，只用于召回审计。
- 所有标签按交易日历成熟，并记录 censor、停牌、涨跌停、成本、benchmark 和数据缺失状态。
- 长周期 120/180 日标签必须处理右删失和重叠标签。

决策时钟按现有 Advisory runtime 固定映射：

```text
decision_as_of_trade_date = T
selection_as_of_trade_date = T
target_trade_date = review_trade_date = SelectionRun.trade_date = T+1
effective_entry_trade_date = T+1
legacy episode.signal_date = review_trade_date = T+1
```

新 observation/prediction 必须同时保存 decision-as-of 与 target/review 日期，禁止把 legacy `episode.signal_date` 重新解释为 T 日信号截止日。特征只能使用 T 日 cutoff 前已知信息；历史库中已经存在的 T+1 停牌、涨跌停、开盘价和分钟路径只能作为 outcome/price-quality label，不能反向参与 T 日候选过滤。线上无法预知的 T+1 可交易性不得在历史 builder 中被“补知道”。

### 14.3 文件快照

数据库按批次导出不可变 Parquet，训练只读文件：

```text
dataset_snapshot_id
snapshot_state: BUILDING, SEALED, FAILED
query/template version
source watermark
feature cutoff policy
canonical signal context and label policy hashes
program/binding lineage and package/manifest/model/HMM versions
effective selection profile and review policy hashes
selection runtime code commit and adapter/query semantic versions
benchmark and cost policy hashes
row counts and date coverage
file list and SHA256
schema fingerprint
label maturity summary
durable_snapshot_uri, storage_backend, promotion_receipt
```

数据库仍是数据权威。训练不得反复对数据库执行逐股票、逐日期高频查询。

快照采用原子发布：构建期间为 `BUILDING`，所有文件、行数、schema 和 SHA 校验通过并提升到项目目录外、Windows/WSL 均可访问的 durable immutable dataset store 后，一次性转为 `SEALED`；失败为 `FAILED`。只有带 promotion receipt 的 `SEALED` 可训练；相同 build key 重试必须幂等，旧快照按详细设计的保留策略只读保留。

初始构建从最早可复算的 PIT 日期开始批量生成历史观察，不要求等待新系统在线累计数月。回看区间可以立即训练内部 research bootstrap 并标记 `evidence_level=RETROSPECTIVE_RESEARCH_ONLY`、`deployment_state=SHADOW`，但不得向用户显示为已校准数字预测；只有满足模型 vintage、最晚研究决策 cutoff 和 embargo 的区间才进入正式 OOS，之后随新交易日持续追加并逐步形成包级校准证据。

## 15. 训练、模型制品与运行环境

### 15.1 推荐训练路径

- Windows 负责触发和编排，WSL Conda 负责导出后训练。
- 首选已具备 LightGBM、Pandas、PyArrow、数据库驱动的 WSL 环境。
- WSL ext4 只存 Parquet staging/cache 和训练临时文件，避免跨文件系统小文件开销；它不是 durable dataset snapshot store。
- 第一版 LightGBM 以 CPU 训练为主；GPU 不是必要条件。
- 模型训练本身通常不是主要耗时，历史候选生成、标签成熟和分钟路径构建才是主要成本。
- 训练只从 durable `SEALED` snapshot 物化 WSL cache。通过验证的 bundle 必须提升到项目目录之外、Windows 在线服务可访问的不可变 `artifact_content_addressed_store`；线上和复现流程不得依赖 `\\wsl$` 临时路径。dataset 与 model 分别记录 `artifact_uri`、`storage_backend` 和 promotion receipt。
- Windows 后端负责受控加载已提升的 LightGBM bundle 并执行推理，WSL 不作为在线 runtime 依赖。Phase 1/3 的容量探针必须记录候选/标签/分钟数据行数与字节数、DB 导出时间、标签时间、训练时间和峰值内存；在实测前不承诺固定初训时长。

### 15.2 模型制品契约

每个模型版本必须记录：

```text
model_version_id
model_family and style_family
signal-context/label-policy compatibility scope
program/binding deployment lineage compatibility
training_dataset_snapshot_id
feature_schema_hash and label_schema_hash
train/validation/test date ranges
training cutoff and embargo
hyperparameter hash
calibration artifact
model file SHA256
metrics by package/style/regime
artifact_uri, storage_backend, promotion_receipt
created_at and code commit
```

单个模型文件不是部署原子单位。不可变 `model_bundle_version` 必须原子绑定其 `declared_capability_set` 所需的全部模型头、calibration、style、horizon、dataset 和 hash；未声明 capability 的模型头不要求存在，同一 bundle 声明多项 capability 时必须满足各项的完整依赖闭包。禁止在线把不同训练批次的子模型或 capability 临时拼接成一个 bundle。

### 15.3 模型状态

校准层级、不可变 bundle 和 Program 级部署状态分开：

```text
evidence_level: RETROSPECTIVE_RESEARCH_ONLY, FORMAL_OOS
calibration_level: NONE, GLOBAL_PRIOR, STYLE_PRIOR, PACKAGE_CALIBRATED
deployment_state: DRAFT, SHADOW, CHALLENGER, CHAMPION, DISABLED, REJECTED
```

`deployment_state` 属于 `app.advisory_model_deployment_binding`，不是模型文件属性；每次状态变化追加 `app.advisory_model_deployment_event`。同一 bundle 可在 Program A 为 `CHAMPION`、Program B 为 `SHADOW`；部署记录必须绑定 `program_id`、binding/runtime/review scope、bundle version、`deployment_expected_row_version`、生效时间和操作者。只有指定 Program 的 deployment binding 为 `CHAMPION` 且所有依赖版本匹配，才可影响该 Program 的 Advisory 正式排名。

能力清单随 bundle 原子发布：

```text
RERANK_READY
RETURN_HORIZON_READY
PRICE_RANGE_READY
LONG_TREND_READY
```

每个 capability instance 都绑定 `style_family`、horizon、数据范围和版本；`RERANK_READY(SHORT_REBOUND)` 与 `RERANK_READY(LONG_TREND)` 不是可互换能力。每项能力有独立指标门禁。任何详细模型设计都必须给出数值阈值、最小 OOS 交易日/有效样本、市场 regime 覆盖、统计置信区间和“未通过即停止”的结果，不得仅写“效果良好”。

`deployment_state` 与 capability readiness 是两条状态轴。Research bootstrap 只能记录 `deployment_state=SHADOW`、`evidence_level=RETROSPECTIVE_RESEARCH_ONLY`、`calibration_level=NONE` 和 experimental head set，不得声明任何 `*_READY` capability；只有合法 OOS/prior 与相应 artifact closure 全部过门后才可声明 READY，并供 Phase 6 展示。

最小 artifact closure：

- `RERANK_READY(style)`：该风格专属 reranker、ranking feature/label schema、score normalization 和 OOS 报告；不包含用户可见收益胜率。
- `RETURN_HORIZON_READY(style)`：该风格的 net return/positive-return probability、style-specific survival、MFE/MAE、benchmark/cost policy、校准和 OOS 报告。
- `PRICE_RANGE_READY(style)`：必须精确依赖兼容的 `RETURN_HORIZON_READY(style)` bundle version/hash，再包含 price path、fill/`IntradayExecutionEventOrderModel`、raw/CNY/yuan 转换、硬风险 policy、校准和分钟覆盖报告；没有 Outcome 依赖时只能显示执行可行性，不能声明价格区间 READY。
- `LONG_TREND_READY`：必须联合包含 `RERANK_READY(LONG_TREND)` 和 `RETURN_HORIZON_READY(LONG_TREND)`，其中单一 competing-risk hazard 负责 ordered barrier、time-to-hit、trend-stage survival 与 competing event；再加 capture label、校准和长期 OOS 报告。展示价格区间时还必须包含 `PRICE_RANGE_READY(LONG_TREND)`。

## 16. 每日推理与荐股生命周期

### 16.1 有界活跃列表

每日复评不能把历史候选与当日候选简单并集。正确语义：

```text
previous active episodes
  + current complete candidate depth
  + style-specific model predictions
  + review policy and replacement budget
  -> explicit ENTER/HOLD/EXIT/WATCH decisions
  -> bounded active list
```

- 稳态下 `active_count <= program.target_count`，允许因质量不足低于目标数量。
- 退出项保留在当日 list version 和历史中，但不计入 active pool。
- 替换必须有明确退出和进入配对，不得无界累加。
- 每日替换预算、确认天数和 time stop 按策略风格配置。

Top5 展示与 active list 迁移规则：

- `candidate_observation_top_k` 决定候选/退出观察深度，`shortlist_top_n=5` 决定模型页面 shortlist，`program.target_count` 决定正式 active episode 数，三者不得互相隐式改写。
- 现有 `target_count=20` 的 Program 在 Phase 7 前继续保持 20；启用 Top5 shortlist 不触发 15 只股票的批量退出。
- Phase 7 如要把正式目标改为 5，必须先给出迁移预览并由用户对该 Program 单独确认。默认迁移模式为 `DRAIN_TO_TARGET`：安全/模型退出照常发生，`active_count > new_target_count` 时不补位，逐步收敛后恢复正常 replacement budget。
- 需要立即缩容时只能使用另行批准的 `RECONCILE_TO_TARGET`，逐项记录退出原因；也可选择新建独立 Top5 Program。禁止以“重排更新”名义无解释批量退出。
- 迁移期允许 `active_count > new_target_count`，但必须有 `migration_state`、旧/新目标、remaining excess 和预计收敛规则；`rank_enter_threshold`、`rank_exit_threshold`、确认期和 replacement budget 随 style/target/policy version 一起版本化。
- 回滚模型不会复活已经退出的 episode，也不会删除模型生成的 ENTER/HOLD/EXIT 决策；恢复基线后的后续复评继续 append-only 演进。

### 16.2 推理审计

每次预测至少记录：

- Program、binding lineage、package、manifest、SelectionRun、canonical signal observation、`decision_as_of_trade_date`、`target/review_trade_date` 和 `effective_entry_trade_date`。
- signal context/label policy/effective selection profile/review policy hash、requested top_k、eligible universe hash 和各 stage 数量。
- 数据 snapshot、market_data_asof、feature availability。
- HMM snapshot/preset、as-of/effective 日期、generation mode、input max dates 和 coefficient artifact hash。
- `alpha_raw`、`hmm_adjusted`、`risk_policy_adjusted`、`selection_effective`、`advisory_model` 五层 rank 和 score。
- model bundle、Program deployment binding、校准层级、capabilities、预测状态和 reason codes。
- 价格基准、原始价格转换证据和规则硬上限。

正式复评的配置合并顺序继续遵守现有契约：active binding 是基础配置，请求配置显式覆盖，PIT cutoff 和目标日期上下文最后写入并保持最终权威；review run 保存实际生效配置。

### 16.3 模型失败与基线连续性

本蓝图中的 fail-closed 指“模型通道拒绝伪造输出”，不等于让现有 Advisory 无条件停摆：

- `SHADOW` 模型缺失、证据不足或版本错配时，影子预测标记不可用并写 reason code，当前基于 `selection_effective_rank` 的 review 继续运行。
- `CHAMPION` 部署必须显式配置 `deployment_failure_policy`。默认 `FALLBACK_SELECTION_EFFECTIVE` 会产生高优先级审计/告警并清楚标注已回退；需要严格停止的 Program 可配置 `BLOCK_REVIEW`/`WAITING_DATA`。
- 两种策略都不得把 `rule_default` 或基线排名标成模型结果，也不得静默保持陈旧预测。

## 17. Contracts / API、DB、UI、模型与数据契约

### 17.1 DB 逻辑实体

以下是蓝图锁定的逻辑实体。字段级 DDL 在 Phase 1/2 详细设计中确定：

| 逻辑实体 | 建议名称 | 作用 |
|---|---|---|
| 策略风格画像 | `app.advisory_strategy_style_profile` | 包 manifest 到风格、期限和目标的版本化映射 |
| 信号观察 | `app.advisory_signal_observation` | 决策时 PIT 候选、特征引用和来源证据 |
| 结果标签 | `app.advisory_outcome_label` | 多期限收益、MFE/MAE、生存和事件标签 |
| 数据快照 | `app.advisory_dataset_snapshot` | DB 到 Parquet 的 manifest、watermark 和哈希 |
| 模型版本 | `app.advisory_model_version` | 单模型、数据、代码、校准和不可变制品证据 |
| 模型 bundle | `app.advisory_model_bundle_version` | 原子绑定全部模型头、能力、期限和 hash |
| Program 模型部署 | `app.advisory_model_deployment_binding` | Program 级乐观并发版本、当前状态和生效范围 |
| Program 部署事件 | `app.advisory_model_deployment_event` | append-only 启停、晋级、回滚和操作者历史 |
| 模型预测 | `app.advisory_model_prediction` | 每个候选、期限和模型的不可变预测 |
| 校准/漂移快照 | `app.advisory_model_monitor_snapshot` | 概率覆盖率、漂移、晋级和停用证据 |

现有 `app.advisory_daily_review`、`selection.daily_selection_evidence`、`advisory_review_run`、`advisory_recommendation_list_version`、`advisory_recommendation_list_item` 和 `advisory_episode_return` 继续作为在线证据/生命周期权威，不用模型表替代。

### 17.2 API 契约方向

详细 API 设计至少覆盖：

- 读取 Program 当前风格画像、模型部署状态和数据截止时间。
- 读取某 list version 的五层排名和模型解释。
- 读取收益、周期、价格区间及校准状态。
- 读取影子对照和模型不可用原因。
- 受控启用、停用和回滚某 Program 的 Advisory 模型，不影响 StrategyPackage 或 Paper。

所有写入接口必须使用 `deployment_expected_row_version` 乐观并发语义，不能让过期页面覆盖新部署状态。

### 17.3 UI 契约

页面按 Program/策略包独立展示：

- Alpha 原始排名、HMM 调整排名、risk policy 调整排名、Selection 正式排名、模型排名及变化。
- Top5 和“合格 N/5”。
- 策略风格、校准层级和模型部署状态。
- 收益 P20/P50/P80、正收益概率和持股周期范围。
- 买入、止盈激活、软/硬止损参考区间。
- 行业状态、热度、风险和主要贡献理由。
- 数据截止时间、模型版本、包 SHA、HMM snapshot。
- `rule_default`、`model_predicted`、`model_unavailable` 明确区分。

页面按 capability 渐进开放，不允许占位数值：

- `RERANK_READY`：五层排名、Top5 shortlist 和排名解释。
- `RETURN_HORIZON_READY`：收益分位数、正收益概率和持股周期。
- `PRICE_RANGE_READY`：买入、止盈/移动保护、软/硬止损区间。
- `LONG_TREND_READY`：有序目标概率、time-to-hit、趋势存活与捕获率。
- capability 未通过时对应区块显示明确 `model_unavailable` 及 reason code，其他已通过区块和现有荐股功能保持可用。

不增加逐股人工编辑、人工买入记录或订单入口。

建议展示语义示例：

```text
alpha_raw_rank=8, hmm_adjusted_rank=4
risk_policy_adjusted_rank=5, selection_effective_rank=3
advisory_model_rank=2
style_family=SHORT_REBOUND, calibration_level=PACKAGE_CALIBRATED
5d_net_absolute_return=P20:+0.8% / P50:+2.1% / P80:+4.6%
5d_net_excess_return=P20:+0.2% / P50:+1.4% / P80:+3.8%
holding_range=3-8 trading days
buy_zone=10.05-10.16, skip_above=10.18
take_profit_activation_zone=10.82-11.08
soft_stop_zone=9.54-9.62, hard_risk_price=9.42
confidence=MEDIUM, guidance_status=model_predicted
market_data_asof=2026-07-10T15:00:00+08:00
```

所有数值只是契约形态示例，不是任何股票的实际预测或默认阈值。

### 17.4 Reason code 基线

后续详细设计应收敛至少以下语义：

```text
ADVISORY_MODEL_PROFILE_MISSING
ADVISORY_MODEL_NOT_READY
ADVISORY_MODEL_VERSION_MISMATCH
ADVISORY_DATASET_ASOF_MISMATCH
ADVISORY_FEATURE_MISSING
ADVISORY_HMM_EVIDENCE_MISSING
ADVISORY_PREDICTION_UNCALIBRATED
ADVISORY_MODEL_SHADOW_ONLY
ADVISORY_MODEL_FEATURE_DEPTH_INSUFFICIENT
ADVISORY_MODEL_STYLE_DRIFT
ADVISORY_FORMAL_OOS_UNAVAILABLE
```

## 18. Design Acceptance Index / 设计验收索引

本索引验收的是蓝图覆盖范围，不代表代码实施完成。后续详细设计和实现 PR 必须引用这些稳定编号，并生成真实 implementation/test evidence。

| ID | 蓝图验收项 |
|---|---|
| F-001 | Advisory 模型能力与 Selection、StrategyPackage、QE、Paper 严格隔离 |
| F-002 | 多个策略包通过独立 Program 并行荐股，禁止跨包候选融合 |
| F-003 | 建立 canonical signal/label scope、Program deployment lineage、Advisory 风格画像和层级校准 |
| F-004 | Alpha raw、HMM adjusted、risk-policy adjusted、Selection effective、Advisory model 五层 rank/score 可追溯且影子阶段不覆盖正式 Selection 排名 |
| F-005 | 权威单包深池、候选重排、Top5 shortlist 约束及 strategy/conditional Recall@K 分母完整 |
| F-006 | HMM 只作为可验证先验/特征，禁止二次固定乘权 |
| F-007 | 行业黑名单作为模型前硬过滤，模型不得绕过 |
| F-008 | 短期反弹专家具有独立期限、标签、生命周期和验收 |
| F-009 | 长期趋势专家具有有序目标、生存、捕获率和滞回生命周期 |
| F-010 | 预期收益输出扣费后概率和分位数而非单点保证 |
| F-011 | 持股周期由信号存活、收益下分位数和风格共同形成范围 |
| F-012 | 买入区间由跳空、成交概率、净收益和可交易性共同形成 |
| F-013 | 止盈/止损区间模型化，硬风险上限独立且 A 股约束完整 |
| F-014 | shortlist 与 active target 分离，每日复评有界、显式退出且旧 Program 缩容受控迁移 |
| F-015 | 数据库是训练/推理数据权威，禁止回测和 Paper 数据污染 |
| F-016 | 历史观察满足无存活偏差 PIT universe、最晚研究决策时点、模型 vintage、cutoff、embargo 和删失处理 |
| F-017 | DB 到不可变 Parquet 采用 BUILDING/SEALED/FAILED 原子快照、完整 manifest 和哈希 |
| F-018 | capability-closed 不可变模型 bundle、Program 级部署、校准、漂移和版本匹配可治理可审计 |
| F-019 | 数据/模型不足时模型通道 fail-closed，基线连续性策略明确且返回稳定 reason code |
| F-020 | UI 只展示决策证据，最终人工买入且不增加订单/逐股编辑 |
| F-021 | 所有模型先影子运行，晋级后仅影响指定 Advisory Program |
| F-022 | 分阶段详细设计、开发、验证、发布和回滚边界明确 |
| F-023 | 验证覆盖排名、概率、收益、风险、价格、生命周期和业务隔离 |
| F-024 | 生产 DDL、回填/影子 DML、数据快照/模型制品库、依赖、训练调度和 Program 激活分阶段独立门禁 |

## 19. Implementation Plan / 分阶段实施方案

所有阶段都遵循：详细设计先审批，代码后实施；未达到阶段退出门禁时不得进入正式晋级。

### Phase 0A：口径冻结与数据可用性审计

- 目标：在计算任何表现指标前锁定候选权威源、五层 rank、全部 Alpha leg/model/HMM vintage、Selection runtime/adapter/query semantics、effective package OOS cutoff、决策时钟、embargo、benchmark、成本、价格单位、标签/barrier 优先级和数据可用时间。
- 进入条件：本蓝图获用户确认；当前 StrategyPackage、Selection 和 Advisory 契约可核对。
- 交付物：指标/标签口径字典、候选深度与 eligible universe 规则、数据可用性审计、研究与正式 OOS 分类规则。
- 退出门禁：每个目标包都能判定合法历史起点；未知 vintage/决策时点有明确 `RETROSPECTIVE_RESEARCH_ONLY` 或 `FORMAL_OOS_UNAVAILABLE` 处置。
- 发布/回滚：只读分析，无 DDL、DML 或 runtime 变化。
- 详细设计等级：F1。

### Phase 1：最小 PIT 数据底座与不可变快照

- 目标：先建立可供基线审计的最小 historical observation、全候选 outcome label、dataset snapshot 和 Parquet 流水线。
- 进入条件：Phase 0A 口径获批准。
- 交付物：五层 rank 补采、权威深池和 pre/post-HMM/risk-policy 证据、DDL/保留周期、PIT builder、标签成熟/删失、防泄漏、DB 到 Parquet 详细设计。
- 数据要求：数据库权威、effective runtime/review scope、查询模板版本、水位、benchmark/cost hash、文件 SHA 和抽样回查。
- 快照状态：`BUILDING -> SEALED` 原子发布或 `FAILED`；只有 `SEALED` 可供 Phase 0B/训练使用，重试幂等。
- 退出门禁：同一 build key 可重复构建；DB/Parquet 抽样一致；所有深池候选有成熟/删失标签；T+1 信息未进入 T 日特征；非法 vintage 被拒绝或正确分级。
- 发布/回滚：DDL、历史回填 DML、durable dataset store 和 builder activation 各自需要独立门禁；默认离线禁用。
- 详细设计等级：F2。

### Phase 0B：基线质量与可建模性审计

- 目标：基于 Phase 1 的最小 `SEALED` snapshot，确认候选整体 Alpha、内部排名单调性、HMM 边际增益、硬过滤影响和长期赢家 Recall@K。
- 进入条件：Phase 1 数据最小闭环通过；目标包存在合法 OOS，或已明确只能做 retrospective research。
- 交付物：基线审计报告、strategy/conditional Recall 分母与报告、模型价值判断、建议候选深度和目标期限。
- 关键对照：`alpha_raw`、`hmm_adjusted`、`risk_policy_adjusted`、`selection_effective`、候选等权、候选内随机 5、HMM/risk overlay 启停和行业黑名单消融。
- 退出门禁：明确哪些包值得重排，以及每种风格的候选深度；每个包给出 `FORMAL_OOS_AVAILABLE` 或 `FORMAL_OOS_UNAVAILABLE` 结论。
- 停止条件：没有合法 vintage/OOS 或兼容合法 prior 的包只能保留研究报告和 `RETROSPECTIVE_RESEARCH_ONLY + SHADOW` bootstrap，停止该包 Phase 2 之后的用户可见数字预测、包级校准和 canary 路线。
- 发布/回滚：只读分析，无 runtime 变化。
- 详细设计等级：F1 数据分析设计。

### Phase 2：策略风格与模型平台

- 目标：建立 style profile、feature/label registry、模型版本、部署状态和层级收缩。
- 进入条件：Phase 1 数据契约稳定且 Phase 0B 已完成目标包/风格分类；没有正式 OOS 时，只有兼容合法 prior 才能建立 `STYLE_PRIOR + SHADOW`，否则仅允许内部 research bootstrap 且用户侧 `MODEL_UNAVAILABLE`。
- 交付物：风格路由、模型制品、校准层级、包版本迁移和 reason code 详细设计。
- 非目标：不训练正式交易模型，不影响当前排名。
- 退出门禁：新包可解析到明确 style；prior 训练来源、版本和 OOS 合法性可审计；版本不匹配 fail-closed；`STYLE_PRIOR` 与 `PACKAGE_CALIBRATED` 不得混淆。
- 发布/回滚：平台元数据默认不启用推理。
- 详细设计等级：F2。

### Phase 3：超跌反弹候选重排影子模型

- 目标：为当前多 Alpha 反弹包训练 LambdaRank 和风险感知 relevance 目标；用户可见收益胜率与路径分布仍由后续唯一权威 Outcome bundle 提供。
- 进入条件：Phase 1/2 完成并具有 SEALED 短周期观察；缺少正式 OOS/合法 prior 时允许训练 research bootstrap，但只能是 `SHADOW + RETROSPECTIVE_RESEARCH_ONLY + NONE`，不得声明 `RERANK_READY`、进入 Phase 6 或满足晋级门禁。
- 交付物：短反弹 ranking 标签、HMM/risk overlay 消融、rank score normalization、原子 bundle、`RERANK_READY(SHORT_REBOUND)` 能力和影子推理详细设计。
- 验证对照：原始前5、HMM前5、无 HMM 模型、含 HMM 模型、随机5、候选20等权。
- 退出门禁：详细设计预先锁定数值阈值、最小 OOS 交易日/有效样本、regime 覆盖和置信区间；时间样本外 NDCG@5、Precision@5、净收益、MAE、换手和 rank score 稳定性全部通过，否则停止。
- 训练窗口：至少比较 2/3/5 年滚动窗口，不预先假定年限越长越好。
- 发布/回滚：仅 `SHADOW`，不覆盖 `selection_effective_rank`。
- 详细设计等级：F2。

### Phase 4：预期收益与持股周期模型

- 目标：输出多期限净收益分位数、正收益概率、信号存活和持股周期范围。
- 进入条件：对应 style 的 reranker 候选、标签和特征链稳定；SHORT_REBOUND 首次由 Phase 3 进入，LONG_TREND 在 Phase 8B 按本阶段同一契约执行。
- 交付物：唯一权威 outcome bundle、分位数、存活模型、conformal/概率校准、`RETURN_HORIZON_READY` 和拒绝预测详细设计。
- 退出门禁：预设的数值阈值、最小 OOS 有效样本/regime/置信区间，以及概率 Brier/可靠性、分位数覆盖率、期限单调性、成本后结果和漂移分桶全部通过，否则停止。
- 发布/回滚：影子预测写 Advisory 模型表前必须通过 shadow prediction DML、容量和保留门禁；不影响列表动作，可独立停写且保留历史。
- 详细设计等级：F1 或 F2，由是否新增 DDL/API 决定。

### Phase 5：分钟路径与价格区间模型

- 目标：使用分钟行情完善同一 Outcome bundle 的 MFE/MAE 标签/版本，并由 PricePath bundle 训练跳空、成交和事件先后，生成买入、止盈和止损范围；不得发布第二套 MFE/MAE prediction head。
- 进入条件：对应 style 的 Phase 4 结果模型稳定且分钟数据覆盖率通过审计；LONG_TREND 在 Phase 8C 按本阶段同一契约执行。
- 交付物：分钟标签、新的 immutable Outcome/model bundle version、兼容 Outcome bundle version/hash、`raw + CNY + yuan` 单位转换、T+1/涨跌停/停牌/tick、硬风险上限、`PRICE_RANGE_READY` 详细设计。
- 退出门禁：MFE/MAE 标签或头升级后必须产生新 bundle，完整重跑 Phase 4 数值/OOS/校准门禁，并重新验证所有下游 compatibility；随后成交概率校准、分钟事件顺序、价格基准、除权、可交易性和真实行情 shadow 验证全部通过，否则停止。旧 READY bundle 保持不可变，不自动继承新能力。
- 发布/回滚：`rule_default` 继续作为明确基线；模型区间可独立关闭。
- 详细设计等级：F2。

### Phase 6：荐股页面按能力影子展示

- Phase 6A `RERANK_READY`：展示五层排名、Top5 shortlist、合格 N/5 和解释。
- Phase 6B `RETURN_HORIZON_READY`：增加收益分位数、概率、持股周期和校准状态。
- Phase 6C `PRICE_RANGE_READY`：增加买入、止盈/移动保护、软/硬止损和可执行性。
- Phase 6D `LONG_TREND_READY`：增加有序目标、time-to-hit、趋势存活和捕获率。
- 进入条件：对应 capability 的稳定 `SHADOW` bundle 已通过自己的数据/指标门禁；不得因任意一个 bundle 稳定就展示其他能力。
- 交付物：API capability manifest、前端状态、解释、空态、错误态和可访问性详细设计。
- 非目标：不增加逐股人工编辑、订单或买入记录。
- 退出门禁：每个开放区块分别通过真实 API E2E、移动/桌面 UI、长文本、状态不重叠、模型不可用和历史 list version 回看；“完整 Phase 6”仅在全部计划能力分别过门后成立。
- 发布/回滚：显示开关按 Program/capability 控制，缺失区块明确 unavailable，关闭后现有荐股页面不变。
- 详细设计等级：F2。

### Phase 7：Advisory Top5 受控晋级

- 目标：通过门禁的模型可以影响指定 Program 的 Advisory 排名和 Top5，但不影响 Selection/Paper。
- 进入条件：稳定 `SHADOW/CHALLENGER` bundle、Phase 9A 最小 ModelOps 已通过、用户批准 canary、拟部署 binding 的 `deployment_expected_row_version`、canary policy 和完整回滚证据。
- 交付物：`selection_effective_rank` 到 model rank 的动作接入、生命周期滞回、Program 级乐观并发启停、Top5 shortlist/active target 分离及缩容迁移详细设计。
- 退出门禁：canary 业务指标、业务 E2E、稳态/迁移态列表约束、退出补位、DRAIN_TO_TARGET、重跑幂等、跨日连续性和隔离 oracle 全部通过后，才可用 expected row version 将该 Program deployment binding 晋级为 `CHAMPION`。
- 发布/回滚：Program 级 canary；模型部署回退到 `selection_effective_rank`，价格能力独立回退 `rule_default`；不复活已退出 episode，保留全部决策与预测审计。
- 详细设计等级：F2。

### Phase 8：长期趋势专家

- 目标：建立 20 至 180 日趋势重排、有序目标、生存、峰前回撤和捕获率模型。
- 进入条件：Phase 1/2 完成；新策略包具有冻结版本和 SEALED 历史/在线观察。没有正式 OOS 时，仅在兼容合法 prior 存在时允许 `STYLE_PRIOR + SHADOW`；否则只做 `RETROSPECTIVE_RESEARCH_ONLY` 内部 bootstrap，不能通过本阶段门禁。
- 子阶段：8A 长期 reranker；8B 按 Phase 4 契约训练长期 outcome/horizon；8C 按 Phase 5 契约训练长期 price path；8D 组合 capability-closed shadow bundle。
- 交付物：长期候选 Recall@K、`RERANK_READY(LONG_TREND)`、`RETURN_HORIZON_READY(LONG_TREND)`、按覆盖率决定的 `PRICE_RANGE_READY(LONG_TREND)`、分层目标、趋势失效、HMM 持续性、长期生命周期和 `LONG_TREND_READY` 详细设计。
- 数据要求：优先比较 5/7/10 年严格 PIT 窗口；最新标签允许右删失。
- 退出门禁：详细设计预设的数值阈值、最小 OOS 有效样本/regime/置信区间，以及 Recall@20/50（仅在独立深池契约完成后才含 @100）、barrier AUCPR/Brier、time-to-hit、趋势捕获率、回撤和假退出率全部通过，否则停止。
- 发布/回滚：用户可见 shadow 只允许兼容合法 `STYLE_PRIOR`；research bootstrap 仅内部可见。合法包级 OOS 达标后可在 shadow 中标记 `PACKAGE_CALIBRATED`，但 Phase 8 不得直接影响正式列表；任何长期趋势 bundle 仍必须返回并完整通过共同 Phase 7 canary/champion 门禁。
- 详细设计等级：F2。

### Phase 9：ModelOps 与持续治理

- Phase 9A 最小正式运行保障：首个 Phase 7 canary 前建立 prediction/calibration drift、artifact/load health、告警、Program 手动停用、灾备读取和回滚演练；这些是 Phase 7 进入门禁。
- Phase 9B 持续治理：至少一个 bundle 经 Phase 7 晋级后，再建立可重复调度、自动重训、champion/challenger、自动停用候选和高级漂移治理。
- 交付物：调度、容量、保留、模型注册、告警、重新训练和灾备详细设计。
- 退出门禁：9A 必须通过加载健康、告警、手动停用、回滚演练和历史预测可读；9B 必须再通过重复训练一致性、制品哈希、失败隔离和自动治理验证。
- 发布/回滚：自动训练不等于自动晋级；自动停用策略和 champion 变更需受控审批，紧急手动回滚始终可用。
- 详细设计等级：F2。

Phase 8 在 Phase 2 后可以并行准备，但不得绕过数据、OOS、影子和共同 Phase 7 晋级门禁。Phase 3 与 Phase 8 严禁共用同一个收益标签头。

## 20. 后续详细设计文档清单

建议按以下顺序新建，每份文档必须声明依赖的蓝图编号、Feature tier、验收索引、验证矩阵和生产门禁：

1. 候选权威源、决策时钟、OOS/vintage、benchmark/cost 和数据可用性口径设计。
2. Advisory PIT 历史观察、全候选标签和原子 SEALED Parquet 快照设计。
3. Advisory 模型数据表、DDL、保留周期、回填和迁移设计。
4. 荐股候选质量、HMM 消融和长期赢家双口径 Recall@K 基线审计设计。
5. 策略风格画像、特征/标签注册、原子 bundle 和 Program 部署治理设计。
6. HMM、行业黑名单和风格化行业优先级设计。
7. 超跌反弹候选重排与 Top5 shortlist 约束设计。
8. 收益分位数、信号存活和持股周期设计。
9. 分钟路径、成交概率、raw/CNY/yuan 转换和价格区间设计。
10. 长期趋势、有序目标、生存和捕获率设计。
11. WSL 训练、项目外 content-addressed artifact store、模型制品、数据快照和调度设计。
12. Advisory 推理服务、API、缓存、幂等和 reason code 设计。
13. 荐股页面 capability 影子展示与解释性设计。
14. Phase 9A 最小 ModelOps、加载健康、漂移告警、手动停用和回滚演练设计。
15. Advisory 正式晋级、Top5 active target 迁移、生命周期、发布和回滚设计。
16. Phase 9B 自动重训、champion/challenger、高级漂移和灾备设计。

## 21. Verification Plan / 验证方案

### 21.1 数据验证

- DB 与 Parquet 按日期、股票、字段和聚合值抽样一致。
- PIT 字段不得晚于决策时点；财务和行业数据使用真实可用时间。
- 日期身份满足 `decision/selection_as_of=T`、`target/review/SelectionRun/legacy episode.signal_date=T+1`，新观察不得把 legacy 字段误作 T 日 cutoff。
- 父包及全部 leg/model/HMM 的 effective OOS cutoff、最晚研究/冻结决策时点、embargo、manifest 和 vintage 完整。
- `BUILDING/FAILED` 快照不可训练，`SEALED` 发布原子且同 build key 重试幂等。
- 权威深池的所有候选均有固定期限成熟/删失标签，ENTER 与否不影响打标。
- T+1 交易状态只进入 outcome/price-quality label，不进入 T 日候选特征。
- 长周期标签的 censor、停牌、涨跌停和企业行动处理可复算。
- 数据缺失不允许静默填充为中性成功。

### 21.2 模型验证

共同指标：

```text
NDCG@5, Precision@5
Top5 cost-after excess return by benchmark_policy_hash/cost_policy_hash
win rate, payoff ratio, MAE, MFE, max drawdown, turnover
Brier score, reliability curve, quantile coverage
industry/regime/package/version stability
```

长期趋势额外指标：

```text
Recall@20/50 for MFE thresholds; @100 only after an approved deep-pool contract
conditional +30/+50/+70 calibration
time-to-hit calibration
trend capture ratio
false early-exit rate
peak-before-stop path correctness
```

模型验收必须使用滚动时间样本外、purge/embargo 和按日期聚类的统计区间。行数多不等于独立样本多。

多期限、多模型、特征族和 regime 选择空间必须在训练前登记。详细设计至少选择并锁定一种适合该实验结构的多重检验/过拟合控制方法，例如 FDR、SPA/Reality Check、Deflated Sharpe 或 PBO，同时要求扣费后经济显著性门槛；只挑最优一次结果不得晋级。

### 21.3 业务与隔离 oracle

- 相同 SelectionRun 在模型关闭时保持当前行为。
- 五层 rank stage/hash 可追溯；影子模型不能改变 `selection_effective_rank`、Selection 结果或 Paper 意图。
- 多个 Program/策略包独立运行，互不覆盖模型、列表和生命周期。
- 行业黑名单股票不会被模型恢复。
- 稳态 active list 有界；DRAIN_TO_TARGET 迁移可解释收敛；退出股票有记录但不继续占 active count，也不会因回滚复活。
- 模型缺失、版本不匹配和数据过期返回明确 reason code。
- 每个 UI 模型区块只在对应 capability ready 时显示数值。
- UI 不把模型区间显示为确定收益或订单。

### 21.4 测试分层

- L0：schema、纯函数、哈希、标签、价格取整。
- L1：repository/service/model adapter 契约。
- L2：DB 数据快照、训练 smoke、模型加载、推理幂等。
- L3：Advisory API 与业务流程。
- L4：真实页面 E2E 和影子运行。
- L5/nightly：长窗口 OOS、跨模块回归、漂移和长期标签成熟。

长训练、广泛市场阶段验证和 UI/API 业务流交由 Validation Center/CI/nightly；交互开发窗口只保留最小充分本地门禁。

## 22. Design Acceptance Matrix / 设计验收矩阵

本矩阵表示蓝图条款已经在文档中定义。`design_ready` 不表示代码已实现；代码 PR 必须替换为真实实现引用和测试证据。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §3、§6.2、§17 | 隔离架构和业务 oracle 已定义 | design_ready | none |
| F-002 | §2、§6.4 | 独立 Program 和禁止跨包融合契约已定义 | design_ready | none |
| F-003 | §4.1、§7、§9.1、Phase 2 | canonical signal/label scope、Program lineage、风格画像和层级收缩已定义 | design_ready | none |
| F-004 | §6.2、§8.1、§8.4、§16.2、Phase 1 | 五层 rank/score、补采和影子写入边界已定义 | design_ready | none |
| F-005 | §6.3、§9、Phase 0B、Phase 3、Phase 8 | 权威深池、Top5 shortlist 和双口径 Recall@K 已定义 | design_ready | none |
| F-006 | §8.1、§8.2 | HMM 边际特征和禁止二次乘权已定义 | design_ready | none |
| F-007 | §8.3、§9.2 | 黑名单硬过滤及不可恢复 oracle 已定义 | design_ready | none |
| F-008 | §10、Phase 3 | 短反弹期限、标签和影子门禁已定义 | design_ready | none |
| F-009 | §10、§13、Phase 8 | 长趋势多状态、捕获率和滞回已定义 | design_ready | none |
| F-010 | §11.1、Phase 4 | 扣费后概率、分位数和校准指标已定义 | design_ready | none |
| F-011 | §11.2、Phase 4 | 存活与收益下分位数周期规则已定义 | design_ready | none |
| F-012 | §12.2、Phase 5 | 跳空、成交和净收益买入区间已定义 | design_ready | none |
| F-013 | §12.3、Phase 5 | 模型软区间、独立硬风险和 A 股约束已定义 | design_ready | none |
| F-014 | §9.2、§13.3、§16.1、Phase 7 | shortlist/target 分离、迁移、有界列表和显式退出已定义 | design_ready | none |
| F-015 | §4.1、§14.1 | DB 权威及禁止回测/Paper 污染已定义 | design_ready | none |
| F-016 | §14.1、§14.2、Phase 0A、§21.1 | 无存活偏差 universe、决策时钟、最晚研究时点、vintage、embargo 和 censor 已定义 | design_ready | none |
| F-017 | §14.3、Phase 1 | 原子 SEALED snapshot、manifest、watermark 和哈希已定义 | design_ready | none |
| F-018 | §15.2、§15.3、§17.1、Phase 9 | capability-closed 不可变 bundle、Program 部署、校准和漂移治理已定义 | design_ready | none |
| F-019 | §4.2、§16.3、§17.4、§21.3 | 模型通道拒绝、基线连续性和 reason code 已定义 | design_ready | none |
| F-020 | §3、§17.3、Phase 6 | 仅决策展示、无逐股编辑和订单已定义 | design_ready | none |
| F-021 | §15.3、Phase 3、Phase 6、Phase 7 | shadow、champion 和 Program 级晋级已定义 | design_ready | none |
| F-022 | §19、§20 | 分阶段路线和十六份详细设计输出已定义 | design_ready | none |
| F-023 | §21 | 数据、模型、业务、隔离和测试分层已定义 | design_ready | none |
| F-024 | §19、§23、§25 | DDL、DML、dataset/model store、调度和 Program activation 门禁已定义 | design_ready | none |

## 23. Rollout / Rollback / 发布与回滚

### 23.1 发布顺序

1. 先发布 expand-only DDL、只读数据和模型元数据能力。
2. 经独立门禁后执行历史回填、制品提升和离线训练。
3. 经独立门禁后启用 shadow prediction writer。
4. 再按 capability 发布 UI 影子展示。
5. Phase 9A 最小 ModelOps、告警、手动停用、灾备读取和回滚演练通过。
6. 完成规定的 OOS 和 shadow 观察后，由用户批准 Program 级 canary。
7. 最后才允许该 Program 的 champion deployment binding 影响 Advisory shortlist/正式列表。

每一步都可独立停止。自动训练不得自动晋级 champion。

### 23.2 回滚

- 首先用 `deployment_expected_row_version` 关闭受影响 Program deployment binding，排名恢复 `selection_effective_rank`；若价格能力已启用，再独立恢复当前 `rule_default`，优先恢复用户侧基线。
- 随后停止受影响的 online/shadow prediction writer 和训练调度，阻止继续产生新副作用。
- 已发生的 target migration 不随模型回滚自动反向执行；不得复活已退出 episode，后续 list 继续 append-only。
- 不删除历史 prediction、dataset、model 和 monitor 证据。
- 不修改 StrategyPackage binding，不回滚 Selection/Paper 数据。
- DDL 仅允许向后兼容 expand-only 新增表/列；canary 回滚不删除表、历史或制品，代码回滚后旧 Advisory 路径仍可读取。
- 模型制品回滚切换不可变 bundle/deployment version，不覆盖原文件。

## 24. Risks / Failure Modes / 风险与失败模式

| 风险 | 影响 | 约束或缓解 |
|---|---|---|
| 原始候选深池整体没有 Alpha | 重排无法凭空创造有效信号 | Phase 0B 先验证候选整体 lift |
| 长期赢家未进入前20 | Top5 永远无法选中 | 先评估 Recall@20/50；需要 @100 时另立深池契约 |
| 使用后来模型/Selection 代码回放研发期 | 严重泄漏和虚假效果 | 强制 model/runtime semantics vintage、cutoff 和 embargo |
| 用当前上市列表回放历史 | 存活偏差夸大候选质量 | PIT universe、退市股保留和 universe hash |
| 等价 Program 重复计样本 | 人为放大相同信号权重 | canonical signal/label hash 去重，Program 只留 lineage |
| 同一标签训练反弹和趋势 | 负迁移并错误淘汰趋势股 | 独立风格专家和标签头 |
| HMM/risk overlay 二次施加 | 行业或风险暴露被重复放大 | 保留逐 stage 证据，由模型学习边际贡献 |
| 只优化胜率 | 偏好小赚、忽略大赢家和大亏风险 | 同时优化收益分布、赔率、MAE、右尾和效用 |
| +70% 稀有事件样本不足 | 过拟合和概率失真 | 有序条件概率、层级收缩、长窗口和 AUCPR |
| 多期限/模型反复择优 | 多重检验导致虚假冠军 | 预登记搜索空间、统计修正、经济显著性和 forward shadow |
| 120/180 日标签重叠 | 有效样本被高估 | 按日期聚类、purge/embargo、生存删失 |
| 未复权路径跨企业行动 | 产生虚假 MFE/MAE、跳空和止损 | 企业行动一致标签，展示时再转换 raw CNY/yuan |
| 数据库频繁细粒度读取 | 训练 I/O 成为瓶颈 | 一次导出不可变 Parquet，训练只读文件 |
| 新包没有正式 OOS | 包级预测被误报为已校准 | 合法 prior 才能 `STYLE_PRIOR + SHADOW`；否则 `MODEL_UNAVAILABLE` |
| 模型和数据版本错配 | 不可复现或错误预测 | 版本/hash 校验和 fail-closed |
| 模型区间被理解为保证 | 用户决策风险 | 概率、区间、置信度和明确风险说明 |
| 每日列表简单并集 | active list 膨胀 | 显式 ENTER/HOLD/EXIT 和有界 active count |
| 趋势股短期整理 | 每日重排导致过早退出 | 入选/持有/退出阈值分离和确认期 |
| 规则 fallback 冒充模型 | 错误的可信度展示 | `rule_default` 与 `model_predicted` 强区分 |

## 25. Production Gates / 生产门禁

本蓝图文档交付本身：

- `production_ddl_gate=noop`
- `production_backfill_dml_gate=noop`
- `production_shadow_prediction_write_gate=noop`
- `production_dataset_snapshot_store_gate=noop`
- `production_model_artifact_store_gate=noop`
- `production_training_scheduler_activation_gate=noop`
- `production_program_model_activation_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_runtime_activation=not_performed`
- `production_service_restart=not_performed`

后续阶段：

- 任何 Phase 如新增或修改表结构，都必须提交正式 migration，并执行独立 `production_ddl_gate`。
- Phase 1 历史观察/标签回填必须独立执行 `production_backfill_dml_gate`，不能因 DDL 合入自动运行。
- Durable dataset store 配置、权限、容量、原子 promotion 和读取 smoke 必须通过 `production_dataset_snapshot_store_gate`。
- 任何 Phase 的 shadow prediction 持久化都必须先通过容量、保留和 `production_shadow_prediction_write_gate`。
- WSL 训练产物进入 Windows 可访问 `artifact_content_addressed_store` 前必须通过 `production_model_artifact_store_gate` 和 promotion receipt 验证。
- 新 Python/WSL 依赖必须提交 lock/环境变更并执行 backend dependency gate。
- 前端新增依赖必须执行 frontend dependency gate。
- 训练调度器必须单独执行 `production_training_scheduler_activation_gate`；Program shadow/champion/canary 激活必须单独执行 `production_program_model_activation_gate`，不能因代码合入或自动训练而启用。
- 每个实现 PR 必须单独证明 Selection、Paper、StrategyPackage 无行为副作用，并把生产 DB 的预期 DDL/DML 与“无非预期副作用”证据分开报告。

## 26. 开放决策与后续评审点

以下决策必须在对应详细设计中基于 Phase 0A/1/0B 证据确定，不阻断本蓝图：

- 每种风格在 v1 的内部候选池是 20 还是 50；是否需要 Top100 及其独立深池契约。
- 包级校准的最小有效 OOS 样本、市场阶段和时间跨度。
- 长期趋势的风险屏障、time stop 和移动保护口径。
- 行业集中度和相关性簇约束的默认值。
- 自动重训频率、champion 晋级审批人和漂移停用阈值。
- 分钟数据覆盖不足时允许展示的最粗价格区间等级。
- 模型预测表按候选、期限展开还是 JSONB 混合存储。

这些参数不得在代码阶段临时拍脑袋确定，必须回到相应 F1/F2 详细设计和验收矩阵。
