# AIstock 荐股策略条件化模型体系 F2 架构蓝图 v1

> 日期：2026-07-10
> 修订日期：2026-07-27
> 文档类型：F2 顶层架构蓝图，当前修订使用 `docs-fast-update`
> 当前状态：蓝图已形成；Phase 0A/Phase 1 历史研究、PIT 数据底座、Phase 1E 编译和 G5 基础设施已分阶段合入。O4 authoritative CLI、逐 Program orchestration、typed capacity policy producer 和 pending historical request ref 已完成代码合入；真实 O4/G5 前瞻证据链仍等待所选决策日的真实 DEV ingestion 完成，但该等待不阻塞历史范围研究或模型研发。Phase 1R R1-R4 已完成源码、DEV/production schema 和真实历史业务闭环：R3 单 Alpha与原生多 Alpha 父包完成 `2026-07-01` 至 `2026-07-21` 的 15 个交易日、30/30 package-day E2E；R4 由 PR `#2792` 合入（merge commit `f7cf3fb3c3e7417236671e1bef3cdb1f8a124ab9`），BUG close-sync 由 PR `#2793` 合入（merge commit `81de5f93b8e7326ad9cd13ed2cb520c66847d321`），完成 32,549 条 outcome、4 个 summary、2 个非空 SEALED retrospective snapshot、360 条 source correction 和 exact retry。生产历史读取与 Phase 1R 隔离写入不要求把生产数据复制到 DEV；服务重启和 runtime activation 未执行。当前下一阶段为 R5 API/UI/legacy cutover，唯一实施级设计 `docs/architecture/advisory_phase1r_r5_api_ui_legacy_cutover_f2_design_20260727.md` 已完成正式审核并可进入实现；Phase 0B 模型价值审计、WSL 模型训练和用户可见模型预测尚未开始。根据单用户、学术研究、无实盘交易边界取消人工审批、角色、运行时 DDL、运行时策略包二次验证、candidate-count 运行门禁和未经确认的 canary/champion/ModelOps 前置链；不存在实时荐股或交易执行路径
> 适用模块：Advisory 荐股、Selection Center 结果消费、StrategyPackage 只读语义、行业 HMM、行情数据、模型训练、荐股页面
> 最终决策者：用户人工决定是否买入；系统不下单、不记录人工实际买入结果

## 0. 文档定位与权威边界

本文档汇总截至 2026-07-10 对 AIstock 荐股功能的分析、代码核对、机构研究和模型方案，作为后续分阶段详细设计与开发的总纲。

本文档负责锁定：

- 总体架构和模块隔离边界。
- 数据权威、PIT、防泄漏和模型版本规则。
- 多策略包独立运行、候选重排和 Top5 输出原则。
- 历史范围研究、新策略上线前验证和逐交易日有界列表演进原则。
- 超跌反弹与长期趋势两类首批策略专家的目标差异。
- HMM、行业黑名单、收益预测、持股周期和价格区间的关系。
- 每个实施阶段的目标、输入条件、交付物、完成判定和回滚边界。
- 后续详细设计文档清单及其 Feature tier。

本文档不负责锁定字段级 DDL、最终 API schema、具体模型超参数、页面像素级交互和生产调度参数。这些内容必须在对应 Phase 的 F1/F2 详细设计中完成并通过自动验证后，才可开始代码实施。

文档权威优先级如下：

1. 用户在本轮及后续明确确认的决策。
2. 本蓝图中列出的总体边界和阶段完成判定。
3. 最新已确认的专项 F2 设计和当前实际代码/DDL。
4. 较早文档中与以上内容不冲突的部分。

若本蓝图与早期手工多包或荐股生命周期文档冲突，以已落地的原生单包契约为准：一个荐股程序绑定一个单 Alpha 包或一个原生多 Alpha 父包；多个策略包通过多个独立荐股程序并行运行，不在一个荐股程序中手工融合。

若较早 Phase 0A.2/Phase 1 文档把 `replay-program-range` 仅描述为诊断 CLI，或只允许单日 `MANUAL_HISTORICAL_RESEARCH` 进入所有历史数据链，以本文新增的 `HISTORICAL_RANGE_RESEARCH` 总体边界为准。后续 F2 详细设计必须同步修订这些从属契约：范围研究可形成隔离的 retrospective observation/dataset，但不能冒充当时真实运行、正式前瞻 OOS、`PUBLISHED` list 或模型已校准证据。

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
- 当前启用单 Alpha Program 已绑定真实包，但 current manifest 尚无 DSE；其 67 条历史 DSE 均属于旧 manifest，不能继承为 current identity 的研究证据。
- 当前只有显式单日 historical runner，没有产品级日期范围任务、逐日进度、断点恢复、独立研究列表和新策略上线前收益验证页面。
- `review_schedule` 只是 legacy Program metadata；历史研究不注册自动 daily scheduler，但允许用户显式启动一个有限日期范围任务并在任务内部按交易日顺序执行。

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
| Advisory 固定生命周期参数 | 待风格化设计 | 在用户确认模型排名启用前保持现状，不能在研究或展示阶段静默改变 |
| `selection_center_advisory_preview` 等旧诊断入口 | legacy diagnostic | 不得作为正式训练观察或荐股入口；物理清理另立设计 |
| `replay-program-range` 占位 CLI | 提升为正式范围研究能力 | 复用权威单日推理语义，生成独立 `HISTORICAL_RANGE_RESEARCH` 任务、逐日研究列表和收益验证；不得继续停留在一次性诊断输出 |
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
- 历史 PIT 观察构建、严格 OOS、模型注册、研究验证和用户确认后的 Program 级模型启用。
- 已准入新策略包的历史范围研究：显式起止交易日、逐日荐股、列表生命周期、收益成熟、任务恢复和上线前验证报告。
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
- 不把历史范围研究任务做成自动调度器、当前荐股列表发布器或 Selection/Paper/模拟盘回放入口。
- 不把使用当前 manifest/runtime/code 回看历史得到的结果描述为策略当时已经存在、当时真实荐股或正式前瞻 OOS。
- 不因蓝图结构校验通过而宣称功能已实现或可进入生产。

## 4. 设计原则与已确定决策

### 4.1 数据与隔离原则

1. PostgreSQL 中的真实行情、行业、资金、交易状态和 Advisory 观察是训练及推理数据权威。
2. Parquet 是带 manifest 和校验哈希的可复现派生快照，不是第二权威数据库。
3. 所有在线特征必须满足 point-in-time，记录 `feature_availability_ts` 和 `market_data_asof`。
4. 模型能力仅属于 Advisory，下游不能反写 Selection、StrategyPackage 或 Paper。
5. 模型制品、数据快照、包 manifest、HMM snapshot 和预测必须可追溯到不可变版本。
6. stable 市场样本、evidence version、标签策略和 Program lineage 必须拆分：同一 `canonical_signal_scope_hash` 只形成一个 economic sample，stage/artifact/source 修订只增加 observation version；实际改变 selection semantics/config 或 label policy 才进入不同训练/校准条件分布。Program/binding/review lineage 独立留证并用于部署，不得让两个等价 Program 把同一市场样本重复加权。
7. 历史范围研究使用数据库中的历史 PIT 行情和当前冻结策略语义逐日重算，统一标记 `HISTORICAL_RANGE_RESEARCH + RETROSPECTIVE_RESEARCH_ONLY`；它可用于功能验证、策略上线前研究和内部模型 bootstrap，但与真实前瞻 observation/OOS 分区隔离。

### 4.2 模型原则

1. 共享数据和特征，不强制共享目标函数。
2. 策略风格必须显式声明，模型在风格内部自动学习具体规律。
3. 第一版使用风格专属 LightGBM 排序、分类和分位数模型组合。
4. 新包先使用风格先验，只有正式 OOS 观察足够后才能启用包级校准。
5. 重排、收益、存活、路径风险和成交概率分别建模，不用一个模型直接猜全部绝对价格。
6. 允许模型拒绝预测；拒绝比静默给出低质量答案更正确。
7. 历史范围结果可以训练和比较 research bootstrap，但仅凭回看任务不得发布 `RERANK_READY`、`RETURN_HORIZON_READY`、`PRICE_RANGE_READY` 或用户可见已校准数字。

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
| [Cross-sectional Learning-to-Rank](https://arxiv.org/abs/2012.07149) 直接优化资产相对顺序 | 候选按 decision-as-of/target 日期、stable canonical signal scope 和 label policy 形成去重 ranking group，优化 Top5 | 论文结果不能代替 A 股 OOS 验证 |
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
用户可选择单日精确历史研究，或显式创建有限日期范围研究任务
  -> 单日精确研究读取 identity/hash 匹配的既有 Selection evidence
  -> 范围研究按冻结 Program/package/config/code 和交易日序列调用同一权威 StrategyPackage/Selection 推理语义
  -> StrategyPackage 独立生成 Alpha 原始候选
  -> [hmm.enabled=true 时] 现有 HMM 行业分数调整
  -> risk policy 的 can_buy/score multiplier/delta/rank penalty 与重排
  -> 行业黑名单、停牌及其余 decision-as-of cutoff 时已知的可交易性硬过滤
  -> 完成 Advisory 自有、权威且可追溯的候选证据：单日 SelectionRun 或隔离的 range day artifact
  -> Advisory 候选池与完整退出观察深度
  -> strategy_style_profile 显式路由
  -> 风格专属候选重排专家
  -> Top5 质量、行业和相关性约束
  -> 收益分布、信号存活、MFE/MAE、成交概率模型
  -> 原始价格区间决策层
  -> research list version 与页面学术研究展示
  -> 不产生投资建议、订单、仓位或执行动作
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
- 当前/legacy `AdvisoryProgramService._evaluate_review` 为行为 parity 暂时保留现有 final-list synthetic rank 和 `ADVISORY_EXIT_OBSERVATION_DEPTH_INSUFFICIENT` 诊断边界；该约束不外推为 Phase 1R package/candidate-count 门禁。Phase 1R 使用 candidate v2 raw/stage/universe evidence，并与 current wrapper 共同使用 `LEGACY_COMPATIBLE_MISSING_RANK_V1 = observed_max_selection_rank + 1`；即使 final top-k 小于 `rank_exit_threshold` 也能执行 baseline list，但不会人为强制增加弱确认。

模型关闭或回滚时恢复的是 `selection_effective_rank`，不是 HMM 前的 `alpha_raw_rank`。五层 rank 必须带各自 stage、生成配置和 hash，禁止继续引入含义不明确的“源排名”新字段。

历史范围研究不进入 `AdvisoryProgramService._evaluate_review` 的当前列表写入路径，而使用独立边界：

```text
HistoricalRangeResearchOrchestrator
  -> shared StrategyPackage/Selection inference components
  -> isolated range candidate evidence
  -> shared AdvisoryListTransitionEngine
  -> isolated range list/outcome/report repositories
```

共享的是无状态推理组件、PIT 数据访问和候选语义，不共享普通 Selection run repository、当前 Advisory list repository、模拟盘/Paper consumer 或可变任务状态。

### 6.3 权威候选运行与观察深度

- 单日 `MANUAL_HISTORICAL_RESEARCH` 的候选必须来自 identity/hash 匹配的既有单包 `SelectionRun` artifact/DSE；该路径不得现场重算 Selection，也不得把 current-semantics 结果冒充当时真实 evidence。
- `HISTORICAL_RANGE_RESEARCH` 为验证当前冻结策略语义，允许通过 Advisory 独立 orchestration adapter 按显式历史交易日调用与现有 Selection 相同的 StrategyPackage 推理、HMM/risk/tradability 和证据生成语义。该 adapter 只能写 Advisory 范围研究命名空间，不得写普通 Selection run、Paper/模拟盘 artifact 或当前荐股 list；不得复制、简化或重新实现第二套选股算法。
- legacy preview、aggregate 诊断或跨包合成结果在两条路径中都不能作为权威候选源。
- Advisory 可以在自己的 effective runtime profile 中请求更深的 `selection.top_k`，用于模型候选召回和额外观察，但不得修改普通 Selection Center 或 Paper v2 的默认运行配置。单 Alpha 仍受 Selection v1 上限 50 约束；原生多 Alpha 父包只能使用 frozen manifest 已声明的 `topk/topk_variants/secondary_topk`，否则现有 runtime 会 `TOPK_RUNTIME_MISMATCH`。缺少更深变体只会使依赖该深度的后续模型 capability unavailable；不能阻断已准入 package 的 Phase 1R baseline list，也不能要求为逐日荐股换包。
- 每次运行必须持久化 `requested_top_k`、manifest top-k variant、Alpha 总评分数量、HMM/risk policy 前后候选数量、硬过滤前后数量、eligible universe 标识及 hash、effective runtime config、候选运行 identity 和 artifact hash；单日精确研究额外记录来源 `SelectionRun id`，范围研究记录独立 `range_day_run_id`，不得伪造 SelectionRun。
- 模型特征深度不足使用 `ADVISORY_MODEL_FEATURE_DEPTH_INSUFFICIENT`，只影响对应模型 capability。现有 current/legacy wrapper 可为 parity 保留既有 final-list synthetic rank/诊断，但 Phase 1R baseline lifecycle 不再使用 `ADVISORY_EXIT_OBSERVATION_DEPTH_INSUFFICIENT` 作为 candidate-count gate；它依据 candidate v2 raw/stage/universe closure 把 active symbol 分类为 INCLUDED/EXCLUDED/ABSENT/OUTSIDE，并使用 R3 冻结的 current-compatible synthetic missing rank，不改变弱确认、退出或替换语义。
- 为 HMM/risk overlay 消融，Phase 1 必须保存同一权威运行的 Alpha 原始深池、HMM 调整后深池、risk policy 调整后深池和硬过滤后正式深池。

### 6.4 多策略包独立性

- 一个 Advisory Program 只绑定一个单 Alpha 包或一个原生多 Alpha 父包。
- 多个 Program 可同时运行，每个 Program 单独生成候选、Top5、模型预测、列表版本和生命周期。
- 共享风格专家只共享模型参数，不共享候选名单、状态或排名。
- 原生多 Alpha 父包的各 leg 分数、权重、一致度和分歧度可以作为输入，但父包仍是唯一包身份。
- 不提供跨包总榜、交叉替换或自动择包。

### 6.5 单日手工历史研究执行与日期边界

- `review_schedule` 仅是 legacy Program metadata，研究模块不读取它、不注册 scheduler。请求必须显式指定一个已完成交易日和一个或多个 Program。
- 唯一业务键是 `(program_id,decision_trade_date,HISTORICAL_RESEARCH_ONLY)`；binding、manifest、policy、effective config、artifact/DSE 和 source revision hashes 是不可变冲突谓词。
- 一个 Program 的 WAITING/FAILED/恢复不会回滚或跳过其他 Program；research run 必须单 Program 原子提交或按同一 deterministic identity 恢复，并输出逐 Program 与 batch receipt。
- 单 Alpha current manifest 与原生多 Alpha parent 只执行无业务写入 research preflight；旧 manifest DSE 不继承，研究路径不发布 package 或创建 binding。
- 单日手工路径不提供日期范围展开、自动 scheduler、`PUBLISHED` list 或实时荐股。PREVIEW、current-semantics range research、集中回补和正式前瞻 evidence 必须保持独立 lineage。

### 6.6 历史范围研究执行器与新策略上线前验证

`HistoricalRangeResearchRun` 是正式 Advisory 研究功能，不是诊断测试。用户可对一个或多个独立 Program 创建有限日期范围任务；每个 Program 仍只绑定一个已准入单 Alpha 包或一个已准入原生多 Alpha 父包。任务必须冻结：

```text
program_id
package_id / manifest_sha256 / alpha_mode
runtime_profile / review_policy / style_profile hashes
code_release_id / code_release_hash
start_trade_date / end_trade_date
calendar_id / calendar_version / ordered_trade_dates_hash
candidate_observation_top_k / shortlist_top_n / target_count
origin = HISTORICAL_RANGE_RESEARCH
research_scope = HISTORICAL_RESEARCH_ONLY
evidence_level = RETROSPECTIVE_RESEARCH_ONLY
execution_prohibited = true
```

- 已有 Advisory Program 时，任务冻结其当前 binding/runtime/review projection，但不会把当前 binding 伪装为历史有效 binding。已准入策略包尚无普通 Advisory Program 时，任务可以携带 hash-closed `research_program_spec` 创建仅存在于范围研究命名空间的稳定 `research_program_id`；它不创建或修改普通 Program/binding，仍保持一 Program 对一 package。
- `research_program_spec` 只提交 package id 和 Advisory 配置；alpha mode、package version、manifest 与多 Alpha component identity 必须由已准入 package projection 推导，页面不得手工声明或覆盖。
- 起止日期必须是显式范围，执行器从权威交易日历一次性展开有序交易日集合；不接收自然日循环、`latest` 推断或运行中自动扩展 end date。
- 每个 Program/原生多 Alpha leg 按冻结 lookback contract 读取 start date 之前的数据库 warmup 数据；warmup 不生成范围外荐股日、列表或 episode，也不改变 v1 首日空 seed。
- 完整交易日集合保存为 immutable date-plan；长区间按 ordinal cursor 分批物化 day rows，不在 create 事务一次插入全部 Program×日期记录，因此不需要用日期跨度业务上限换取事务安全。
- `end_trade_date` 不得晚于数据库已完成的最近交易日。收益、MFE/MAE 和持股周期标签可在其 horizon 数据成熟后追加；未成熟状态必须明确，不阻断已完成的荐股日结果。
- 对新策略包的历史验证固定使用当前已准入 manifest/runtime/code 作用于历史 PIT 数据，回答“当前策略如果从该历史日期开始会怎样”。它不要求、也不得伪造策略包在历史日期已经存在的 binding、model vintage 或真实运行记录。
- HMM、行业、ST、停牌、涨跌停、风险和行情输入必须逐日按 decision cutoff 解析。任务可以冻结当前 HMM/runtime policy 和代码，但不得把任务创建日的最新 HMM snapshot、行业状态或股票池直接复制到全部历史日期；每日日任务保存实际 as-of snapshot/ref/hash。
- T 日 ENTER/HOLD/EXIT/WATCH 只使用 T cutoff 已知事实；next-open/next-close 实际价格在后续作为 execution/outcome 追加，不得影响 T 日 action 或 list hash，也不得因为最后一日价格尚未成熟而阻塞范围荐股结果。
- 策略包准入已经完成后，执行器只读取冻结 package identity 和推理输入，不再次执行资产、模型、因子、组件或包可用性准入检查，不回写 package 状态。
- Selection current wrapper 的最新 refresh readiness、trade-enabled binding 和共享 repository 不进入范围 pure computation；范围 day 只依据 T/warmup 历史数据库分区和 read receipt，不会被最新交易日状态阻断。
- 每个 `(range_run_id, program_id, decision_trade_date)` 是独立、幂等、可恢复的日任务。相同 request hash 重试返回原结果；不同 payload 命中相同业务键必须显式冲突。一个 Program/日期失败不回滚其他 Program 或已完成日期。
- 日期必须按序推进列表状态；前一交易日的 active list 是下一交易日唯一生命周期输入。允许候选计算并行预取，但 list transition 必须按 Program/日期串行提交，禁止跨日乱序覆盖。
- 每日形成独立 candidate set、`ENTER/HOLD/EXIT/WATCH`、list version、active count、替换配对和 reason codes。稳态 `active_count <= target_count`，退出记录保留但不继续占 active pool，禁止将每日候选简单并集。
- 当 outcome 数据成熟后，程序追加逐候选、逐 episode、逐 list version 和整个 range 的收益、benchmark、成本、MFE/MAE、回撤、换手、持有天数、胜率/赔率及 Recall@K 研究结果；不得读取 QE 回测、Paper、模拟盘账户或人工买入结果。
- 范围任务提供持久化状态、进度、失败日期、可重试原因、resume cursor、最终确定性 receipt 和完整 artifact refs。等待输入、恢复和取消是任务状态，不是审批或人工放行门禁。
- 范围研究结果可以进入 Phase 0B 和内部模型 bootstrap 的独立 retrospective dataset partition；正式 OOS、package calibration 和用户可见模型 capability 仍要求符合 model/data vintage、cutoff、embargo 和 calibration 证据。
- 多个 Program 的范围任务可并行，但候选、列表、收益、状态和报告永久按 Program 隔离；不生成跨包总榜或自动择包。

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

训练 ranking group 是 `(decision_as_of_trade_date,target_trade_date,canonical_signal_scope_hash,label_policy_hash)` 下的一组候选，标签另存 `intended_entry_trade_date`。`canonical_signal_scope_hash` 只覆盖 package/manifest、selection runtime semantics、effective config、日期/cutoff/calendar 等稳定语义；Phase 0A evidence-rich `signal_context_hash` 覆盖 Selection deterministic content/stage/artifact、PIT universe、HMM/risk evidence，并进入 observation version/lineage而不进入 stable sample id。`label_policy_hash` 覆盖期限、入场基准、benchmark/cost、barrier 和企业行动口径。Program/binding/review lineage 随观察保存，但除非配置确实改变 stable scope 或标签定义，否则不产生重复训练样本。第一版模型包：

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

q(h) in {EXECUTABLE_MFE_h >= 30%, EXECUTABLE_MFE_h >= 50%, EXECUTABLE_MFE_h >= 70%}
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

两类专家共享 FeatureSnapshot 和数据构建器，但必须使用独立标签、模型、校准、验证报告和 capability 状态。

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
r_net_absolute_h = fixed-capital (residual cash + net exit cash) / reference notional - 1
r_net_excess_h = r_net_absolute_h - benchmark_net_total_return_h
PATH_MFE_h/PATH_MAE_h = extrema on the full E-to-X_h corporate-action-normalized path
EXECUTABLE_MFE_h/EXECUTABLE_MAE_h = extrema on the S-to-X_h sellable path under tradability policy
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

历史观察有两条合法来源，必须分区保存并由 lineage selector 显式选择：

- `MANUAL_HISTORICAL_RESEARCH`：只消费当时 identity/hash 可证明的既有 DSE/source evidence，可参与其证据等级允许的正式研究判断。
- `HISTORICAL_RANGE_RESEARCH`：使用当前冻结 package/runtime/code 对历史 PIT 数据逐日重算，服务功能验证、策略上线前研究和内部 bootstrap；固定为 `RETROSPECTIVE_RESEARCH_ONLY`，不得提升为真实历史运行或正式前瞻 OOS。

两条来源可以共享 stable economic sample 和 outcome calculation engine，但 observation version、source lineage、dataset partition、evidence level 和可用 capability 必须分别记录。任何 selector 都不得因 formal source 不足而静默退回范围研究数据。

正式训练观察必须满足：

- 候选只使用当时可得数据和时间对应的策略包/模型版本。
- 先计算 `effective_strategy_package_oos_cutoff = max(所有 Alpha leg、model asset、预处理/特征资产的 training/selection/research/freeze cutoff，父包 freeze/promotion_at，实际 Selection runtime/adapter/query semantics freeze_at，以及 hmm.enabled=true 时实际 HMM snapshot 的 training/selection/freeze cutoff)`。正式 OOS 起点必须晚于 `effective_strategy_package_oos_cutoff + embargo`，且对应父包、全部组件/model/HMM/runtime semantics 当时已经存在；这里只读取冻结/决策时间元数据，不读取回测结果。
- 原生多 Alpha 父包不能只取父包字段、首个 leg 或主模型 cutoff；任一实际引用组件缺少可验证 cutoff/vintage 时，该区间为 `RESEARCH_EVIDENCE_UNAVAILABLE`。
- 历史观察优先引用当时已持久化的权威 SelectionRun。使用后来代码重建时，必须证明同一 executable semantics/version 在 signal date 前已冻结；否则即使行情 PIT 正确也只能标记 `RETROSPECTIVE_RESEARCH_ONLY`。
- 仅满足训练截止日而不满足上述最晚决策时点的回放标记为 `RETROSPECTIVE_RESEARCH_ONLY`；缺少合法 vintage 时标记 `RESEARCH_EVIDENCE_UNAVAILABLE`，不能进入包级校准。
- 权威深池中的所有候选都生成固定期限 outcome label；不得只给 ENTER/人工最终选择的股票打标签，以免产生选择偏差。
- 为计算 `strategy_recall@K`，Phase 1/0B 另构建 PIT eligible universe 的轻量 outcome denominator；它不进入正式候选或荐股列表，只用于召回审计。
- 所有标签按交易日历成熟，并记录 censor、停牌、涨跌停、成本、benchmark 和数据缺失状态。
- 长周期 120/180 日标签必须处理右删失和重叠标签。

决策时钟按现有 Advisory runtime 固定映射：

```text
decision_as_of_trade_date = T
selection_as_of_trade_date = T
target_trade_date = review_trade_date = SelectionRun.trade_date = T+1
intended_entry_trade_date = E = T+1
earliest_sell_eligible_trade_date = S = next_trading_day(E)，通常 T+2
fixed_horizon_exit_trade_date X_h = shift_trading_days(E, h)，不包含 E
legacy episode.signal_date = review_trade_date = T+1
```

新 observation/prediction 必须同时保存 decision-as-of 与 target/review 日期，禁止把 legacy `episode.signal_date` 重新解释为 T 日信号截止日。特征只能使用 T 日 cutoff 前已知信息；历史库中已经存在的 T+1 停牌、涨跌停、开盘价和分钟路径只能作为 outcome/price-quality label，不能反向参与 T 日候选过滤。线上无法预知的 T+1 可交易性不得在历史 builder 中被“补知道”。A 股 T+1 下 `h=1` 的首个固定期限退出日是 S，不是 E；收益、`EXECUTABLE_MFE/MAE`、`PATH_MFE/MAE`、barrier 和 benchmark 必须使用 Phase 1 冻结的时间窗/可执行性契约。

### 14.3 文件快照

数据库按批次导出不可变 Parquet，训练只读文件：

```text
dataset_snapshot_id
snapshot_content_hash and manifest_core_sha256
snapshot_state: final rows are SEALED only
capture batch ids/receipt hashes
build id/checkpoint and attempt lease/fencing receipts
query/template version
append-only source availability/revision set
feature cutoff policy
stable canonical signal, selected observation/label version and label policy hashes
program/binding lineage and package/manifest/model/HMM versions
effective selection profile and review policy hashes
selection runtime code commit and adapter/query semantic versions
benchmark and cost policy hashes
row counts and date coverage
file list and SHA256
schema fingerprint
label lifecycle/terminal/censor summary
durable_snapshot_uri, storage_backend, publish_receipt
invalidation and blob-reference status
```

数据库仍是数据权威。训练不得反复对数据库执行逐股票、逐日期高频查询。

capture、build/attempt 和 final snapshot 分离：业务证据先进入 COMPLETE capture batch；build/attempt 用 checkpoint、lease 和 fencing 管理 materialize/verify/publish/seal；只有 durable CAS publish 完成后，才按 canonical manifest content 生成 immutable SEALED snapshot。失败只属于 capture/build attempt，final snapshot 表不写非 SEALED row。只有带 publish receipt、完整 blob refs 且未 invalidated 的 SEALED 可消费；旧快照按详细设计的保留策略只读保留。

checkpoint 一旦固定不可替换；坏 checkpoint generation 只能由程序在 expected row version、current attempt、fencing token 和 terminal reason 全部匹配时原子转 ABORTED，随后才能创建下一 generation；FAILED_TERMINAL 不得按同 logical key 重建。GC quarantine 在 v1 只记录逻辑状态、不移动 blob；删除前出现新引用必须追加 CANCELLED_REFERENCE_CHANGED，并在引用再次归零后用新 epoch 重新等待保留期。

初始构建从最早可复算的 PIT 日期开始批量生成历史观察，不要求等待新系统在线累计数月。回看区间可以立即训练内部 research bootstrap 并标记 `evidence_level=RETROSPECTIVE_RESEARCH_ONLY`、`configuration_state=SHADOW`，但不得向用户显示为已校准数字预测；只有满足模型 vintage、最晚研究决策 cutoff 和 embargo 的区间才进入正式 OOS，之后随新交易日持续追加并逐步形成包级校准证据。

## 15. 训练、模型制品与运行环境

### 15.1 推荐训练路径

- Windows 负责触发和编排，WSL Conda 负责导出后训练。
- 首选已具备 LightGBM、Pandas、PyArrow、数据库驱动的 WSL 环境。
- WSL ext4 只存 Parquet staging/cache 和训练临时文件，避免跨文件系统小文件开销；它不是 durable dataset snapshot store。
- 第一版 LightGBM 以 CPU 训练为主；GPU 不是必要条件。
- 模型训练本身通常不是主要耗时，历史候选生成、标签成熟和分钟路径构建才是主要成本。
- 训练只从 durable `SEALED` snapshot 物化 WSL cache。通过验证的 bundle 必须发布到项目目录之外、Windows 在线服务可访问的不可变 `artifact_content_addressed_store`；线上和复现流程不得依赖 `\\wsl$` 临时路径。dataset 与 model 分别记录 `artifact_uri`、`storage_backend` 和 publish receipt。
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
artifact_uri, storage_backend, validation_report_ref
created_at and code commit
```

单个模型文件不是部署原子单位。不可变 `model_bundle_version` 必须原子绑定其 `declared_capability_set` 所需的全部模型头、calibration、style、horizon、dataset 和 hash；未声明 capability 的模型头不要求存在，同一 bundle 声明多项 capability 时必须满足各项的完整依赖闭包。禁止在线把不同训练批次的子模型或 capability 临时拼接成一个 bundle。

### 15.3 模型能力与 Program 配置状态

校准层级、不可变 bundle 和 Program 级配置状态分开：

```text
evidence_level: RETROSPECTIVE_RESEARCH_ONLY
calibration_level: NONE, GLOBAL_PRIOR, STYLE_PRIOR, PACKAGE_CALIBRATED
configuration_state: DISABLED, SHADOW, ENABLED
```

`configuration_state` 属于 `app.advisory_model_deployment_binding`，不是模型文件属性；每次配置变化追加
`app.advisory_model_deployment_event`。同一 bundle 可在 Program A 为 `ENABLED`、Program B 为 `SHADOW`；配置记录必须绑定
`program_id`、binding/runtime/review scope、bundle version、正常乐观并发行版本、生效时间和操作者。只有用户为指定 Program
显式配置 `ENABLED` 且依赖版本匹配时，模型才可影响该 Program 的 Advisory 排名；不存在 champion/challenger 晋级链。

能力清单随 bundle 原子发布：

```text
RERANK_READY
RETURN_HORIZON_READY
PRICE_RANGE_READY
LONG_TREND_READY
```

每个 capability instance 都绑定 `style_family`、horizon、数据范围和版本；`RERANK_READY(SHORT_REBOUND)` 与
`RERANK_READY(LONG_TREND)` 不是可互换能力。每项能力有独立指标判定。任何详细模型设计都必须给出数值阈值、最小 OOS
交易日/有效样本、市场 regime 覆盖和统计置信区间；未达标版本准确标记 `MODEL_UNAVAILABLE` 并保留证据，不能仅写“效果良好”，
也不能永久淘汰研究方向。

`configuration_state` 与 capability readiness 是两条状态轴。Research bootstrap 只能记录 `configuration_state=SHADOW`、
`evidence_level=RETROSPECTIVE_RESEARCH_ONLY`、`calibration_level=NONE` 和 experimental head set，不得声明任何 `*_READY`
capability；只有合法 OOS/prior 与相应 artifact closure 完整时才可声明 READY，并供 Phase 6 展示。

最小 artifact closure：

- `RERANK_READY(style)`：该风格专属 reranker、ranking feature/label schema、score normalization 和 OOS 报告；不包含用户可见收益胜率。
- `RETURN_HORIZON_READY(style)`：该风格的 net return/positive-return probability、style-specific survival、`EXECUTABLE_MFE/MAE` 主 projection、`PATH_MFE/MAE` diagnostic、benchmark/cost policy、校准和 OOS 报告。
- `PRICE_RANGE_READY(style)`：必须精确依赖兼容的 `RETURN_HORIZON_READY(style)` bundle version/hash，再包含 price path、fill/`IntradayExecutionEventOrderModel`、raw/CNY/yuan 转换、硬风险 policy、校准和分钟覆盖报告；没有 Outcome 依赖时只能显示执行可行性，不能声明价格区间 READY。
- `LONG_TREND_READY`：必须联合包含 `RERANK_READY(LONG_TREND)` 和 `RETURN_HORIZON_READY(LONG_TREND)`，其中单一 competing-risk hazard 负责 ordered barrier、time-to-hit、trend-stage survival 与 competing event；再加 capture label、校准和长期 OOS 报告。展示价格区间时还必须包含 `PRICE_RANGE_READY(LONG_TREND)`。

## 16. 历史逐日推演、范围任务与研究列表生命周期

### 16.1 有界活跃列表

按连续历史交易日研究时，不能把上一研究日候选与当前研究日候选简单并集。正确语义：

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
- 每个历史研究日的替换预算、确认天数和 time stop 按策略风格配置。

Top5 展示与 active list 迁移规则：

- `candidate_observation_top_k` 决定候选/退出观察深度，`shortlist_top_n=5` 决定模型页面 shortlist，`program.target_count` 决定正式 active episode 数，三者不得互相隐式改写。
- 现有 `target_count=20` 的 Program 在 Phase 7 前继续保持 20；启用 Top5 shortlist 不触发 15 只股票的批量退出。
- Phase 7 如要把正式目标改为 5，必须先给出迁移预览并由用户对该 Program 单独确认。默认迁移模式为 `DRAIN_TO_TARGET`：安全/模型退出照常发生，`active_count > new_target_count` 时不补位，逐步收敛后恢复正常 replacement budget。
- 需要立即缩容时只能使用显式配置并由程序验证的 `RECONCILE_TO_TARGET`，逐项记录退出原因；也可选择新建独立 Top5 Program。禁止以“重排更新”名义无解释批量退出。
- 迁移期允许 `active_count > new_target_count`，但必须有 `migration_state`、旧/新目标、remaining excess 和预计收敛规则；`rank_enter_threshold`、`rank_exit_threshold`、确认期和 replacement budget 随 style/target/policy version 一起版本化。
- 回滚模型不会复活已经退出的 episode，也不会删除模型生成的 ENTER/HOLD/EXIT 决策；恢复基线后的后续复评继续 append-only 演进。

Phase 1R v1 范围任务固定从首个交易日的空 active state 开始。禁止读取当前线上/当前荐股 active list 作为历史首日状态，否则会把未来状态泄漏到历史研究；未来若需要非空 seed，必须另行修订详细设计并证明来源、日期和 identity 与范围首日严格相邻。

同一范围任务重跑不得复制 list version、episode 或 outcome。执行器必须以日任务 receipt 和前日 list hash 形成 hash chain；缺少前日终态时后续日期保持 `WAITING_PREVIOUS_DAY`，不能跳日后再回填隐藏状态。

### 16.2 推理审计

每次预测至少记录：

- Program、binding lineage、package、manifest、SelectionRun、stable canonical signal + selected observation version、`decision_as_of_trade_date`、`target/review_trade_date` 和 `intended_entry_trade_date`。
- signal context/label policy/effective selection profile/review policy hash、requested top_k、eligible universe hash 和各 stage 数量。
- 数据 snapshot、market_data_asof、feature availability。
- HMM snapshot/preset、as-of/effective 日期、generation mode、input max dates 和 coefficient artifact hash。
- `alpha_raw`、`hmm_adjusted`、`risk_policy_adjusted`、`selection_effective`、`advisory_model` 五层 rank 和 score。
- model bundle、Program deployment binding、校准层级、capabilities、预测状态和 reason codes。
- 价格基准、原始价格转换证据和规则硬上限。
- 范围任务 id/request hash、起止日期、有序交易日集合 hash、当前进度、前日 list hash、日任务 attempt/receipt 和 retrospective lineage。

正式复评的配置合并顺序继续遵守现有契约：active binding 是基础配置，请求配置显式覆盖，PIT cutoff 和目标日期上下文最后写入并保持最终权威；review run 保存实际生效配置。

### 16.3 模型失败与基线连续性

本蓝图中的 fail-closed 指“模型通道拒绝伪造输出”，不等于让现有 Advisory 无条件停摆：

- `SHADOW` 模型缺失、证据不足或版本错配时，影子预测标记不可用并写 reason code，当前基于 `selection_effective_rank` 的 review 继续运行。
- `ENABLED` 模型缺失、证据不足或版本错配时，必须显式记录模型失败并回到 `selection_effective_rank`；不得提供会阻断整个 Program review 的 `BLOCK_REVIEW` 或等价隐藏门禁。
- 两种策略都不得把 `rule_default` 或基线排名标成模型结果，也不得静默保持陈旧预测。

## 17. Contracts / API、DB、UI、模型与数据契约

### 17.1 DB 逻辑实体

以下是蓝图锁定的逻辑实体。字段级 DDL 在 Phase 1/2 详细设计中确定：

| 逻辑实体 | 建议名称 | 作用 |
|---|---|---|
| 策略风格画像 | `app.advisory_strategy_style_profile` | 包 manifest 到风格、期限和目标的版本化映射 |
| Phase 0A handoff | `advisory_phase0a_handoff_bundle_v2` | 从不可变审计制品确定性生成 scope、稳定语义和可消费范围；无审批表、角色或授权链 |
| Source availability/revision | `app.advisory_source_availability_event/revision_set` | 未来 first-seen、纠正和 stable source revision evidence |
| 稳定信号 | `app.advisory_signal_observation` | 不受证据修订影响的 canonical economic signal |
| 信号证据版本 | `app.advisory_signal_observation_version` | PIT 候选、stage、runtime/HMM 与 source evidence revision |
| 结果标签 | `app.advisory_outcome_label` | 多期限收益、EXECUTABLE/PATH MFE/MAE、生存和事件标签 |
| Capture/build control | `app.advisory_capture_batch/advisory_dataset_build_attempt` | capture receipt、checkpoint、lease/fencing 和失败恢复 |
| 数据快照 | `app.advisory_dataset_snapshot` | final SEALED manifest-content identity、selected versions、blob refs 与 invalidation |
| 历史范围研究任务 | `app.advisory_historical_range_run` | 冻结 Program/package/config/code、日期范围、交易日集合、状态、进度和最终 receipt |
| 历史范围日任务 | `app.advisory_historical_range_day_run` | 每 Program/日期的候选生成、attempt、幂等状态、前后 list hash 和失败恢复 |
| 历史范围研究列表 | `app.advisory_historical_range_list_version/item` | 隔离的逐日 ENTER/HOLD/EXIT/WATCH、active list、替换配对和原因；不写当前荐股 list |
| 历史范围结果 | `app.advisory_historical_range_outcome/summary` | 成熟标签、逐候选/episode/list/range 收益、风险、换手、Recall 和报告 lineage |
| 模型版本 | `app.advisory_model_version` | 单模型、数据、代码、校准和不可变制品证据 |
| 模型 bundle | `app.advisory_model_bundle_version` | 原子绑定全部模型头、能力、期限和 hash |
| Program 模型部署 | `app.advisory_model_deployment_binding` | Program 级乐观并发版本、当前状态和生效范围 |
| Program 部署事件 | `app.advisory_model_deployment_event` | append-only 启停、版本切换、回滚和操作者历史 |
| 模型预测 | `app.advisory_model_prediction` | 每个候选、期限和模型的不可变预测 |
| 校准/漂移快照 | `app.advisory_model_monitor_snapshot` | 概率覆盖率、漂移、模型可用性和停用证据 |

现有 `app.advisory_daily_review`、`selection.daily_selection_evidence`、`advisory_review_run`、`advisory_recommendation_list_version`、`advisory_recommendation_list_item` 和 `advisory_episode_return` 继续作为当前荐股证据/生命周期权威，不用模型表或历史范围表替代。

### 17.2 API 契约方向

详细 API 设计至少覆盖：

- 创建历史范围研究任务，显式提交 Program、起止交易日和冻结配置；返回 request hash、交易日数量和任务 identity。
- 查询范围任务进度、逐日状态、失败原因、resume cursor、每日候选/list/actions、成熟收益和最终报告。
- 对可重试失败执行幂等 resume/cancel；cancel 只停止未开始日期，不删除已完成研究事实，也不形成审批流程。
- 读取 Program 当前风格画像、模型部署状态和数据截止时间。
- 读取某 list version 的五层排名和模型解释。
- 读取收益、周期、价格区间及校准状态。
- 读取影子对照和模型不可用原因。
- 受控启用、停用和回滚某 Program 的 Advisory 模型，不影响 StrategyPackage 或 Paper。

Program 模型配置写入必须使用 `configuration_expected_row_version` 乐观并发语义，不能让过期页面覆盖新配置状态。历史范围任务使用 request idempotency key、`range_expected_row_version` 和日任务唯一键控制创建、resume/cancel 并发。两类字段都是程序并发控制，不是审批或模型晋级门禁，不得混用。

### 17.3 UI 契约

页面按 Program/策略包独立展示：

- 历史范围研究任务创建、进度、逐日时间线、每日荐股列表、ENTER/HOLD/EXIT/WATCH、收益成熟状态和策略上线前验证汇总。
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
ADVISORY_RESEARCH_EVIDENCE_UNAVAILABLE
ADVISORY_HISTORICAL_RANGE_INVALID
ADVISORY_HISTORICAL_RANGE_DATE_NOT_COMPLETED
ADVISORY_HISTORICAL_RANGE_PAYLOAD_CONFLICT
ADVISORY_HISTORICAL_RANGE_PREVIOUS_DAY_PENDING
ADVISORY_HISTORICAL_RANGE_DAY_RETRYABLE
ADVISORY_HISTORICAL_RANGE_DAY_FAILED
ADVISORY_HISTORICAL_RANGE_OUTCOME_MATURING
ADVISORY_HISTORICAL_RANGE_CURRENT_SEMANTICS_ONLY
```

## 18. Design Acceptance Index / 设计验收索引

本索引验收的是蓝图覆盖范围，不代表代码实施完成。后续详细设计和实现 PR 必须引用这些稳定编号，并生成真实 implementation/test evidence。

| ID | 蓝图验收项 |
|---|---|
| F-001 | Advisory 模型能力与 Selection、StrategyPackage、QE、Paper 严格隔离 |
| F-002 | 多个策略包通过独立 Program 并行荐股，禁止跨包候选融合 |
| F-003 | 建立 stable canonical signal、versioned evidence/label scope、Program deployment lineage、Advisory 风格画像和层级校准 |
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
| F-016 | 历史观察满足无存活偏差 PIT universe、最晚研究决策时点、模型 vintage、cutoff、embargo、T/E/S/X_h 和 terminal/删失处理 |
| F-017 | DB 到不可变 Parquet 采用 capture/build-attempt/final SEALED 分层、manifest-content identity、完整 hashes 和 invalidation |
| F-018 | capability-closed 不可变模型 bundle、Program 级部署、校准、漂移和版本匹配可治理可审计 |
| F-019 | 数据/模型不足时模型通道 fail-closed，基线连续性策略明确且返回稳定 reason code |
| F-020 | UI 只展示决策证据，最终人工买入且不增加订单/逐股编辑 |
| F-021 | 模型 capability 与 immutable bundle 独立；只有用户明确配置后才影响指定 Advisory Program，不建立 canary/champion 审批状态机 |
| F-022 | 分阶段详细设计、开发、验证、发布和回滚边界明确 |
| F-023 | 验证覆盖排名、概率、收益、风险、价格、生命周期、业务隔离和完整命令链正向可达性 |
| F-024 | 生产控制收敛为 3 项开发/发布工程检查和 4 项运行不变量；零人工审批、零运行时 DDL、零策略包二次验证，合法数据具有可满足正向路径 |
| F-025 | Phase 0A 审计制品直接确定性生成 handoff/scope，可用范围由冻结规则自动判定；不引入 GLOBAL/scope approval、角色、approval bundle 或 action authorization |
| F-026 | stable signal、versioned evidence/lineage 与 snapshot 单版本选择阻止重复样本 |
| F-027 | Selection trace capture 开关和所有失败状态不改变现有业务结果 |
| F-028 | 原生多 Alpha 父包具有版本化 component/weight/combine provenance，禁止手工跨包融合 |
| F-029 | outcome 时间轴、成本、benchmark、terminal/censor、universe raw outcome 和计算证据可复算 |
| F-030 | append-only source revision、attempt fencing、程序化 generation termination、durable CAS、base/invalidation/blob refs/GC cancel 闭合 |
| F-031 | 当前单 Alpha target 按 current manifest 执行只读 research preflight 并与旧 manifest evidence 隔离；归档包不被复活 |
| F-032 | 现有原生多 Alpha 父包按历史日期解析已有 dated binding，并与其他 Program 独立执行；研究路径不创建 binding |
| F-033 | 所有新 binding 使用显式、无重叠的 `[effective_from_trade_date,effective_to_trade_date)`，legacy null 区间不被反推 |
| F-034 | 正式 Phase 0A policy registry 在首次 signal 前冻结 hash/effective range，且不含审批、角色或授权字段 |
| F-035 | Selection evidence producer 保存 decision clock、effective config、runtime/HMM、PIT universe、risk、asset/source 和完整 lineage，供历史研究只读消费 |
| F-036 | 合法空候选具有不可变权威 evidence；真实 L4 等待自然样本，不人为制造候选结果 |
| F-037 | Phase 0A.2 复用 Phase 1 source ledger；历史修复只接受 exact source，缺证据保持 retrospective/unavailable |
| F-038 | 正确历史输入先自动达到 `PARTIAL -> HANDOFF_EMITTED`，exact source/label closure 后达到 `RESEARCH_READY`，且不影响 Selection/模拟盘/Paper |
| F-039 | 手工历史 runner 对显式 Program/date 独立运行，Program/date/research-scope key 唯一、持久化可恢复、失败隔离且回执确定 |
| F-040 | 历史锚点只能是显式已完成交易日；DB_HISTORICAL、manual origin、research scope 与 PREVIEW/REPLAY/PUBLISHED/实时/交易语义永久隔离 |
| F-041 | 历史范围研究是可持久化、可恢复、用户可见的正式 Advisory 业务功能，不是诊断脚本、测试 helper 或自动 scheduler |
| F-042 | 范围执行器冻结已准入单 Alpha/原生多 Alpha 父包和 Program/config/code identity，按权威交易日历复用同一 Selection 推理语义逐日生成候选，不做策略包二次准入或第二套选股实现 |
| F-043 | 每个 Program/日期幂等、失败隔离且按前日 list hash 串行演进；ENTER/HOLD/EXIT/WATCH、替换配对和有界 active list 完整，禁止每日候选无界并集 |
| F-044 | 新策略包可用当前冻结语义从历史起点执行上线前研究并计算成熟收益，但结果固定为 RETROSPECTIVE_RESEARCH_ONLY，不伪造历史 binding、真实运行或正式前瞻 OOS |
| F-045 | 范围研究 observation/outcome/SEALED partition 可供 Phase 0B 与内部模型 bootstrap，且与 formal OOS、package calibration、当前荐股 list、Selection、模拟盘、Paper 和 QE 数据永久隔离 |

## 19. Implementation Plan / 分阶段实施方案

所有阶段都遵循：详细设计先确认，代码后实施；数据不足或模型未达标时准确记录当前 capability 状态并继续补采/迭代，不得淘汰研究方向或阻断现有荐股基线。工程检查和运行不变量必须同时具备正向和反向验证，禁止只设计拒绝路径。

### Phase 0A：口径冻结与数据可用性审计

- 目标：在计算任何表现指标前锁定候选权威源、五层 rank、全部 Alpha leg/model/HMM vintage、Selection runtime/adapter/query semantics、effective package OOS cutoff、决策时钟、embargo、benchmark、成本、价格单位、标签/barrier 优先级和数据可用时间。
- 进入条件：本蓝图获用户确认；当前 StrategyPackage、Selection 和 Advisory 契约可核对。
- 交付物：指标/标签口径字典、候选深度与 eligible universe 规则、数据可用性审计、研究与正式 OOS 分类规则。
- 完成判定：每个目标包都能判定合法历史起点；未知 vintage/决策时点有明确 `RETROSPECTIVE_RESEARCH_ONLY` 或 `RESEARCH_EVIDENCE_UNAVAILABLE` 处置。
- 发布/回滚：只读分析，无 DDL、DML 或 runtime 变化。
- 详细设计等级：F1。
- 已形成详细设计：`docs/architecture/advisory_phase0a_candidate_authority_oos_data_availability_f1_design_20260710.md`；只读审计框架已由 PR `#1958` 合入。2026-07-11 已对当前原生多 Alpha target 执行真实生产 DB 只读审计，结果因 policy、dated binding、clock/runtime/source evidence 缺失为 `BLOCKED`；该结果不能通过放宽规则提升。

### Phase 0A.1：确定性 Handoff normalization

- 目标：把 Phase 0A 不可变 audit receipt 按冻结规则直接转换为 handoff bundle、admission scope set 和稳定 signal semantics，不引入人工决策。
- 进入条件：Phase 0A audit outputs/hash contract 完整；目标策略包已通过现有 StrategyPackage 可执行性检查。
- 交付物：handoff v2、逐 admission scope 自动可用性分类、稳定语义 hash 和确定性校验 CLI；不创建审批表、角色、decision chain、approval bundle 或 operation authorization。
- 完成判定：合法资产、完整行情、匹配的 runtime/calendar/policy 输入必须稳定得到可消费 scope；缺失或冲突输入得到明确 reason code。相同输入重复执行 hash 相同，多 Program lineage 不混淆，正向 golden 与反向 fixture 全部通过。
- 发布/回滚：纯函数和不可变 handoff 制品，无 DDL、DML 或 runtime 变化。
- 详细设计等级：纳入 Phase 1 F2 详细设计 §6、§18、§22.2；实现候选尚未合入主线或激活生产运行。

### Phase 0A.2：历史研究证据就绪与手工多 Program 验证

- 目标：补齐 policy、历史 binding 只读解析、不可变 Selection evidence 和 manual historical runner，为单 Alpha current manifest 和原生多 Alpha父包建立独立、同体验、可复算的历史研究路径。
- 进入条件：Phase 0A.1 deterministic readiness/handoff 契约稳定；StrategyPackage、Advisory Program 和 Selection evidence producer 可按现状核对。
- 交付物：官方 immutable policy registry、统一 `[from,to)` binding 生命周期、decision/runtime/HMM/PIT/source/stage evidence、合法空候选契约、manual historical runner exactly-once/失败隔离/恢复回执和双轨 L4 计划。
- 当前 single：先做 current-manifest smoke；通过即复用现有 package/Program，失败时才标准发布新 identity。旧 manifest DSE 不继承。
- 历史边界：不复活两个归档单 Alpha 包，不回填猜测 binding/manifest/available-at；双轨近期仅一个精确共同 cutoff，不把 replay 或集中回补变成 formal。
- 完成判定：正确单 Alpha和原生多 Alpha历史输入均至少达到 `PARTIAL -> HANDOFF_EMITTED`；exact source/label closure 后复验 `RESEARCH_READY`。空候选和 binding switch 不作为其他合法研究日期的永久阻塞。
- 发布/回滚：代码、DEV migration、只读 L4 和生产 DDL 分离；不新增审批、角色、scheduler 或运行时 DDL，不影响 Selection、模拟盘和 Paper 结果。
- 详细设计等级：F2。
- 已形成详细设计：`docs/architecture/advisory_phase0a2_evidence_readiness_bootstrap_f2_design_20260711.md`；manual historical runner 已实现，未创建 scheduler、实时荐股或生产 research batch。

### Phase 1：最小 PIT 数据底座与不可变快照

- 目标：先建立可供基线审计的最小 historical observation、全候选 outcome label、dataset snapshot 和 Parquet 流水线。
- 进入条件：Phase 0A.1 handoff/scope set hash 完整，且至少一个 `READY` 或 `PARTIAL` admission scope 已 `HANDOFF_EMITTED`；`PARTIAL` 只允许建设其缺失的 source/capture capability，不能进入正式 Phase 0B 指标。
- 交付物：stable signal/versioned evidence、五层 rank、多 Alpha component provenance、全候选 labels、source revision/capture、build attempt/final snapshot、invalidation/GC 和 deterministic Parquet。
- 数据要求：数据库权威、append-only available-at/revision、effective runtime/review scope、查询模板、benchmark/cost/terminal hashes、calculation evidence、file SHA 和全量 partition reconcile。
- 快照状态：capture 与 build/attempt 承担进行中/失败状态；`advisory_dataset_snapshot` 只保存 manifest-content-addressed SEALED rows。只有未 invalidated SEALED 可供 Phase 0B 使用。
- 完成判定：同一 request/source/capture 命中同 logical build key，坏 checkpoint 由程序状态机 CAS 终止后才能增加受控 generation；final manifest content 命中同 snapshot；DB/Parquet 当前 snapshot 全部 partition hashes 一致；所有 deep-pool projection 有明确 maturity/event；T+1 信息未进入 T feature；lease/fencing/durable publish/base/invalidation/GC cancel/new-epoch tests通过。该校验只针对当前 Advisory snapshot，不扫描历史、QE/Qlib/backtest/Paper文件。
- 正向要求：合法数据必须贯通 capture -> label -> build -> publish golden E2E。
- 发布/回滚：DDL 只在开发/发布阶段执行；运行时 source/capture/label/build/snapshot/GC 全部由已设计程序、事务和状态机自动约束，无人工审批。
- 详细设计等级：F2。
- 已形成父级详细设计：`docs/architecture/advisory_phase1_pit_observation_labels_sealed_snapshot_f2_design_20260711.md`。Phase 1C-3 Batch D 已由 PR `#2056` 合入，并在 DEV PostgreSQL 与 repo-external filesystem 形成真实 SEALED golden；这只证明 fixture/development 离线数据集闭环，不表示生产历史数据积累、模型训练或 runtime consumer 已启用。Phase 1D 已按 `docs/architecture/advisory_phase1d_source_availability_observer_capacity_f2_design_20260714.md` 由 PR `#2067` 合入并完成 DEV E2E，生产 DDL 仍为 `pending`、observer 未激活，当前 capacity receipt 因缺少非空 `universe_outcomes` SEALED Parquet 测量保持 `PARTIAL`。
- Phase 1E 已按 `docs/architecture/advisory_phase1e_dual_track_readiness_execution_plan_f2_design_20260714.md` 由 PR `#2094` 合入；O4 authoritative CLI、逐 Program compiler dependency、source mapping、capacity v2 和 compile receipt 又由 PR `#2444` 合入：以 batch-independent `ProgramDateRequest` 编译多 Program 独立历史研究计划，无 scope 时保留 target diagnostic；通过 Advisory read-only projection 单向读取 immutable Selection/package evidence，不调用或修改 Selection、策略推理、模拟盘、Paper、QE/RD-Agent/Qlib runtime；策略包不做二次资产验证。BUG-764 PR `#2449` 已补齐 typed capacity policy producer；BUG-766 PR `#2458` 已保证 pending historical execution 同时返回可读回的 authoritative request ref。Phase 1E 当前状态为 `code_complete_real_dev_execution_pending`：persistent dual-track L4 仍等待所选决策日真实 DEV immutable DSE/receipt 与 O4 plan，不代表 runtime activation，也不阻塞 Phase 1R 使用已完成历史日期。
- Phase 1F 已按v1详细设计 `docs/architecture/advisory_phase1f_release_schema_verification_f2_design_20260714.md` 由PR `#2114`合入，并于2026-07-14完成DEV persistent L3 plan/apply/verify/exact-reapply：v1 managed/prerequisite均为`COMPATIBLE`、receipt中`downstream_ready=true`，最终catalog fingerprint一致；只应用order 50 schema migration，零业务DML、零runtime activation。Phase 1G复核发现v1 contract仍有父级偏差：stage/candidate局部hash被错误设为全局UNIQUE，lineage/candidate未按月分区，因此v1 receipt不冒充Phase 1G最终schema。
- Phase 1F.1 forward修正代码已由PR `#2129`合入并在 disposable PostgreSQL L2通过；2026-07-15依次完成DEV与production plan/apply/new-verify/new-exact-reapply。两个目标均为`COMPATIBLE/COMPATIBLE/downstream_ready=true`，最终catalog fingerprint一致为`106af55734c6ec7bb0b0dd4e438bcb780d672be95220aead686ec6f4b6c3e627`。它采用全局identity、月分区payload和只读compatibility view，保持snapshot读取结果不变；执行中零业务DML、零runtime activation，也未新增审批、角色或备份要求。
- Phase 1G开工一致性复核发现Phase 1F.1 outbox唯一键及capture-gap identity缺少scope，同一Selection证据无法被多个独立Program合法消费或形成独立失败证据。Phase 1F.2唯一实施级设计`docs/architecture/advisory_phase1f2_scope_aware_trace_identity_forward_migration_f2_design_20260715.md`已完成实现，并通过PR #2144及BUG修复PR #2146/#2150合入；2026-07-15 DEV与production均已plan/apply/new-verify/new-exact-reapply，exact v3 `COMPATIBLE/downstream_ready=true`，最终catalog fingerprint为`95600e18fbe4a4026f24a374e66289b7e530c874a95a203db2b738855a6a580a`。该状态是schema技术事实，不是审批或人工门禁。
- Phase 1G唯一父级实施设计为`docs/architecture/advisory_phase1g_source_observation_capture_dml_f2_design_20260714.md`。G1-G4代码已分别由PR #2158、#2167、#2178、#2191合入；G5唯一实施级设计为`docs/architecture/advisory_phase1g_g5_dev_evidence_f2_design_20260716.md`，代码已由PR #2217合入并完成L0-L2。2026-07-18已从合入主线对O3真实DEV双轨执行首次persistent和exact rerun：两个Program及两个`2026-07-20` future-effective ACTIVE binding已形成，初始request hash为`ba3f0c230b2f4efe3f1d85f15b1de2268f0ad2a3622661f83cfe79deaf8eed6f`。BUG-766合入后又在冻结代码`d230a33c0d4fe1ab9030401fed5e0cd35b247639`上生成request hash `479b9fa804f7e25d48c607216741a72b914ea7122d831fbb6524e09a6f76df5f`，真实DEV预日期执行准确返回两个Program `WAITING_INPUT`和可完整读回的request ref，未生成DSE v2或formal receipt。O4前瞻验证仍等待所选决策日真实ingestion后按既定顺序执行，G5 L3/L4继续等待exact Phase 1E plan。该等待只属于这次前瞻验收，不是历史范围研究、Phase 0B准备或模型研发必须等待最新交易日的门禁。Phase 1G在相同PIT cutoff重放并冻结source revision，只读投影immutable DSE/artifact/package evidence，用exact target连接和caller-owned PostgreSQL事务写observation capture，并把稳定result与逐次attempt receipt分离；不补造source event、不重复验证策略包、不调用Selection/推理，不读取回测、Paper或模拟盘数据，也不引入角色、审批、授权或运行时DDL。Phase 1G整体仍需真实DEV evidence，不得越级声明完成。

### Phase 1R：历史范围研究与新策略上线前验证

- 目标：把单日权威历史研究能力扩展为正式、有限、可恢复的日期范围业务功能，支持已准入单 Alpha/原生多 Alpha 父包从历史起点逐交易日荐股、演进独立研究列表、成熟 outcome 并形成上线前验证报告。
- 进入条件：单日 StrategyPackage/Selection 推理语义、历史 PIT 数据访问、Advisory Program/config projection 和列表生命周期契约稳定；不要求等待最新交易日或 O4 所选前瞻日期完成。
- 交付物：`HistoricalRangeResearchRequest/Run/DayRun/Receipt`、交易日展开、逐日 orchestration、幂等/resume/cancel、前日 list hash chain、研究 list/outcome/summary、API/UI、repo-external artifacts 和 retrospective SEALED dataset bridge。
- 正确性要求：范围首日无未来 active state；每日日任务只读当日 cutoff 前数据；未来行情只作为成熟 outcome；同一日重试收敛；跨日 list transition 顺序唯一；多 Program 独立；合法数据可从首日贯通到最终报告。
- 日级输入身份：R3 使用与 Alpha component/lookback 解耦、且不按当日 positive PIT universe过滤的 canonical `DECISION_MARK_SET`，将 candidate ref、mark-set ref、前日 exact receipt 和 canonical list semantics 一起闭合为 `day_input_hash v3`。准确的停牌、terminal no-quote 或退出 universe 形成显式 mark/rank evidence，不转永久 waiting。
- 新策略语义：包完成正常准入后即可创建范围任务，不重复验证 package 资产。历史日期早于 package/manifest/code vintage 时仍可运行 current-semantics research，但固定标记 `RETROSPECTIVE_RESEARCH_ONLY`，不得发布 package calibration 或用户可见 READY capability。
- 模型衔接：范围结果可以立即进入 Phase 0B 和内部 research bootstrap；正式 OOS、概率/区间校准和 Program 模型启用仍由后续阶段独立验收。
- 隔离：不写普通 Selection run、当前 Advisory list/episode、模拟盘、Paper、QE/Qlib/backtest 或交易表；不创建 scheduler、审批、角色、package re-approval 或运行时 DDL。
- 共享计算所有权：R2 的公共候选计算契约位于中立的 StrategyPackage 计算模块，由现有 Selection wrapper 和 Phase 1R adapter 分别调用；Advisory 不 import `simulation_runtime`、Paper 或模拟盘模块，公共计算也不持有 repository、sink 或默认生产依赖。
- 发布/回滚：范围任务是显式用户命令/API；关闭该能力只停止新任务和未开始日期，保留已完成研究事实，不影响现有单日 runner 和当前荐股基线。
- 详细设计等级：F2。父设计为 `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md`；R3 子设计为 `docs/architecture/advisory_phase1r_r3_ordered_day_executor_f2_design_20260722.md`，R4 子设计为 `docs/architecture/advisory_phase1r_r4_outcome_summary_phase1_bridge_f2_design_20260723.md`。R1-R4 均已合入并完成分层验收；当前下一实施批次为 R5 API、UI 与 legacy cutover，唯一实施级设计为 `docs/architecture/advisory_phase1r_r5_api_ui_legacy_cutover_f2_design_20260727.md`。R5 源码、真实 API/UI E2E 和 runtime activation 分别保持未完成，不得互相冒充完成。

### Phase 0B：基线质量与可建模性审计

- 目标：基于 Phase 1 的最小 `SEALED` snapshot，确认候选整体 Alpha、内部排名单调性、HMM 边际增益、硬过滤影响和长期赢家 Recall@K。
- 进入条件：Phase 1 数据最小闭环通过；至少存在一个内容闭合的 formal observation snapshot 或 Phase 1R retrospective snapshot，并明确其 evidence level。只有 retrospective 数据时可以执行候选质量审计和内部 bootstrap 判断，但不得输出已校准用户能力。
- 交付物：基线审计报告、strategy/conditional Recall 分母与报告、模型价值判断、建议候选深度和目标期限。
- 关键对照：`alpha_raw`、`hmm_adjusted`、`risk_policy_adjusted`、`selection_effective`、候选等权、候选内随机 5、HMM/risk overlay 启停和行业黑名单消融。
- 结果分类：明确每个包当前证据是否支持继续重排研究及建议候选深度；逐包输出 `RESEARCH_EVIDENCE_AVAILABLE` 或 `RESEARCH_EVIDENCE_UNAVAILABLE`。该分类不淘汰策略包、不修改荐股基线，也不停止后续数据补采、代理验证或模型迭代。
- 能力边界：没有合法 vintage/OOS 或兼容合法 prior 时，只保留研究报告和 `RETROSPECTIVE_RESEARCH_ONLY` 内部 bootstrap；不得向用户展示伪校准数字。证据补齐后可重新评估，不设置永久停止门禁。
- 发布/回滚：只读分析，无 runtime 变化。
- 详细设计等级：F1 数据分析设计。

### Phase 2：策略风格与模型平台

- 目标：建立 style profile、feature/label registry、模型版本、部署状态和层级收缩。
- 输入条件：Phase 1 数据契约稳定且 Phase 0B 已形成目标包/风格分类；没有正式 OOS 时，只有兼容合法 prior 才能建立 `STYLE_PRIOR + SHADOW`，否则仅允许内部 research bootstrap 且用户侧明确 `MODEL_UNAVAILABLE`。
- 交付物：风格路由、模型制品、校准层级、包版本迁移和 reason code 详细设计。
- 非目标：不训练正式交易模型，不影响当前排名。
- 完成判定：新包可解析到明确 style；prior 训练来源、版本和 OOS 合法性可审计；版本不匹配显式失败；`STYLE_PRIOR` 与 `PACKAGE_CALIBRATED` 不得混淆。失败只关闭对应模型能力，不阻断现有荐股。
- 发布/回滚：平台元数据默认不启用推理。
- 详细设计等级：F2。

### Phase 3：超跌反弹候选重排影子模型

- 目标：为当前多 Alpha 反弹包训练 LambdaRank 和风险感知 relevance 目标；用户可见收益胜率与路径分布仍由后续唯一权威 Outcome bundle 提供。
- 输入条件：Phase 1/2 完成并具有 SEALED 短周期观察；缺少正式 OOS/合法 prior 时允许训练 research bootstrap，但只能是 `RETROSPECTIVE_RESEARCH_ONLY + NONE`，不得声明 `RERANK_READY` 或产生用户可见模型排名。
- 交付物：短反弹 ranking 标签、HMM/risk overlay 消融、rank score normalization、原子 bundle、`RERANK_READY(SHORT_REBOUND)` 能力和影子推理详细设计。
- 验证对照：原始前5、HMM前5、无 HMM 模型、含 HMM 模型、随机5、候选20等权。
- 结果判定：详细设计预先锁定数值阈值、最小 OOS 交易日/有效样本、regime 覆盖和置信区间；时间样本外 NDCG@5、Precision@5、净收益、MAE、换手和 rank score 稳定性决定该模型版本是 `RERANK_READY` 还是 `MODEL_UNAVAILABLE`。未达标版本保留证据并继续迭代，不淘汰研究方向，也不影响荐股基线。
- 训练窗口：至少比较 2/3/5 年滚动窗口，不预先假定年限越长越好。
- 发布/回滚：仅 `SHADOW`，不覆盖 `selection_effective_rank`。
- 详细设计等级：F2。

### Phase 4：预期收益与持股周期模型

- 目标：输出多期限净收益分位数、正收益概率、信号存活和持股周期范围。
- 输入条件：对应 style 的 reranker 候选、标签和特征链稳定；SHORT_REBOUND 首次由 Phase 3 进入，LONG_TREND 在 Phase 8B 按本阶段同一契约执行。
- 交付物：唯一权威 outcome bundle、分位数、存活模型、conformal/概率校准、`RETURN_HORIZON_READY` 和拒绝预测详细设计。
- 结果判定：预设的数值阈值、最小 OOS 有效样本/regime/置信区间，以及概率 Brier/可靠性、分位数覆盖率、期限单调性、成本后结果和漂移分桶决定该模型版本是 `RETURN_HORIZON_READY` 还是 `MODEL_UNAVAILABLE`；未达标只关闭该能力。
- 发布/回滚：Advisory 预测写入必须满足既定 schema、事务、幂等、容量预算和保留规则，这些是程序正确性约束，不是审批或独立运行门禁；失败显式停写该能力并保留现有列表动作。
- 详细设计等级：F1 或 F2，由是否新增 DDL/API 决定。

### Phase 5：分钟路径与价格区间模型

- 目标：使用分钟行情完善同一 Outcome bundle 的 `EXECUTABLE_MFE/MAE` 与 `PATH_MFE/MAE` 标签/版本，并由 PricePath bundle 训练跳空、成交和事件先后，生成买入、止盈和止损范围；不得发布第二套同 projection prediction head。
- 输入条件：对应 style 的 Phase 4 结果模型稳定且分钟数据覆盖率通过审计；LONG_TREND 在 Phase 8C 按本阶段同一契约执行。
- 交付物：分钟标签、新的 immutable Outcome/model bundle version、兼容 Outcome bundle version/hash、`raw + CNY + yuan` 单位转换、T+1/涨跌停/停牌/tick、硬风险上限、`PRICE_RANGE_READY` 详细设计。
- 结果判定：任一 EXECUTABLE/PATH MFE/MAE 标签或头升级后必须产生新 bundle，重新计算 Phase 4 数值/OOS/校准证据和下游 compatibility；成交概率校准、分钟事件顺序、价格基准、除权和可交易性决定新版本是 `PRICE_RANGE_READY` 还是 `MODEL_UNAVAILABLE`。旧 READY bundle 保持不可变，不自动继承新能力。
- 发布/回滚：`rule_default` 继续作为明确基线；模型区间可独立关闭。
- 详细设计等级：F2。

### Phase 6：荐股页面按能力影子展示

- Phase 6A `RERANK_READY`：展示五层排名、Top5 shortlist、合格 N/5 和解释。
- Phase 6B `RETURN_HORIZON_READY`：增加收益分位数、概率、持股周期和校准状态。
- Phase 6C `PRICE_RANGE_READY`：增加买入、止盈/移动保护、软/硬止损和可执行性。
- Phase 6D `LONG_TREND_READY`：增加有序目标、time-to-hit、趋势存活和捕获率。
- 展示条件：只展示具有完整 immutable bundle、数据版本、模型版本和对应指标证据的 capability；不得因任意一个 bundle 可用就展示其他能力。缺失能力明确显示 unavailable，不阻断荐股页面其它区域。
- 交付物：API capability manifest、前端状态、解释、空态、错误态和可访问性详细设计。
- 非目标：不增加逐股人工编辑、订单或买入记录。
- 完成判定：每个开放区块分别通过真实 API E2E、移动/桌面 UI、长文本、状态不重叠、模型不可用和历史 list version 回看；各区块独立交付，不设置“全部能力同时就绪”才允许页面运行的总门禁。
- 发布/回滚：显示开关按 Program/capability 控制，缺失区块明确 unavailable，关闭后现有荐股页面不变。
- 详细设计等级：F2。

### Phase 7：Advisory Top5 模型排名启用

- 目标：用户明确启用某个 Program 的已验证模型版本后，模型可以影响该 Program 的 Advisory 排名和 Top5，但不影响 Selection/Paper。
- 输入条件：目标 Program、immutable model bundle/version、对应 capability 证据和回滚基线完整；不要求 `SHADOW/CHALLENGER/CHAMPION`、canary、ModelOps 前置审批或人工角色。
- 交付物：`selection_effective_rank` 到 model rank 的动作接入、生命周期滞回、Program 级版本化配置、Top5 shortlist/active target 分离及缩容迁移详细设计。
- 正确性要求：业务 E2E、稳态/迁移态列表约束、退出补位、重跑幂等、跨日连续性和隔离 oracle 必须通过；这些是代码合入验收，不形成运行时逐次审批链。
- 发布/回滚：Program 配置可在 `selection_effective_rank` 与指定 immutable model bundle 之间原子切换；价格能力独立回退 `rule_default`；不复活已退出 episode，保留全部决策与预测审计。
- 详细设计等级：F2，实际实现前需由用户确认该阶段范围。

### Phase 8：长期趋势专家

- 目标：建立 20 至 180 日趋势重排、有序目标、生存、峰前回撤和捕获率模型。
- 输入条件：Phase 1/2 完成；新策略包具有冻结版本和 SEALED 历史/在线观察。没有正式 OOS 时，仅在兼容合法 prior 存在时允许 `STYLE_PRIOR + SHADOW`；否则只做 `RETROSPECTIVE_RESEARCH_ONLY` 内部 bootstrap，不产生用户可见数字预测。
- 子阶段：8A 长期 reranker；8B 按 Phase 4 契约训练长期 outcome/horizon；8C 按 Phase 5 契约训练长期 price path；8D 组合 capability-closed shadow bundle。
- 交付物：长期候选 Recall@K、`RERANK_READY(LONG_TREND)`、`RETURN_HORIZON_READY(LONG_TREND)`、按覆盖率决定的 `PRICE_RANGE_READY(LONG_TREND)`、分层目标、趋势失效、HMM 持续性、长期生命周期和 `LONG_TREND_READY` 详细设计。
- 数据要求：优先比较 5/7/10 年严格 PIT 窗口；最新标签允许右删失。
- 结果判定：详细设计预设的数值阈值、最小 OOS 有效样本/regime/置信区间，以及 Recall@20/50（仅在独立深池契约完成后才含 @100）、barrier AUCPR/Brier、time-to-hit、趋势捕获率、回撤和假退出率决定当前模型版本的 capability 状态；未达标继续补采和迭代，不淘汰长期趋势方向。
- 发布/回滚：用户可见 shadow 只允许兼容合法 `STYLE_PRIOR`；research bootstrap 仅内部可见。合法 walk-forward 研究达标后可在研究页标记 `PACKAGE_CALIBRATED`，但不得产生实时建议、正式交易列表或执行输入。
- 详细设计等级：F2。

### Phase 9：可选 ModelOps 与持续治理

- 本阶段不是 Phase 7 的前置条件，也不是当前已批准开发范围。
- 当用户另行确认需要自动重训、漂移告警、模型健康监控或灾备时，再建立对应详细设计；不得预先实现 champion/challenger、自动晋级、自动停用或审批状态机。
- 已启用模型只需要保持 immutable bundle 可读、失败显式、Program 可切回基线和历史预测可追溯；这些能力由各自实现阶段直接验收。
- 自动训练如未来启用，也只产生新的候选 bundle，不自动改变 Program 当前模型配置。
- 详细设计等级：F2，开工前必须取得用户明确范围确认。

Phase 8 在 Phase 2 后可以并行准备，但不得绕过 PIT、OOS、immutable bundle 和用户可见能力证据。Phase 3 与 Phase 8 严禁共用同一个收益标签头。

## 20. 后续详细设计文档清单

建议按以下顺序新建，每份文档必须声明依赖的蓝图编号、Feature tier、验收索引、验证矩阵和生产影响状态：

1. 候选权威源、决策时钟、OOS/vintage、benchmark/cost 和数据可用性口径设计：已形成 `advisory_phase0a_candidate_authority_oos_data_availability_f1_design_20260710.md`。
2. research policy、历史 dated binding、manual multi-Program runner、immutable Selection evidence 和单/多 Alpha双轨验证设计：已形成 `advisory_phase0a2_evidence_readiness_bootstrap_f2_design_20260711.md`。
3. Advisory PIT 历史观察、全候选标签和原子 SEALED Parquet 快照设计：已与原第 3 项统一形成 `advisory_phase1_pit_observation_labels_sealed_snapshot_f2_design_20260711.md`。
4. Advisory 模型数据表、DDL、保留周期、回填和迁移设计：Phase 1 observation/label/snapshot 部分已并入第 3 项；模型表与部署表仍由 Phase 2 专项设计闭合。
5. 历史范围研究执行器与新策略上线前验证 F2 设计：父设计 `advisory_phase1r_historical_range_research_f2_design_20260719.md` 覆盖日期范围、当前语义 projection、逐日 orchestration、列表 hash chain、收益成熟、恢复、API/UI、retrospective dataset bridge 和跨模块隔离；R3 子设计 `advisory_phase1r_r3_ordered_day_executor_f2_design_20260722.md` 已完成 shared list transition、无 candidate-count gate 的 active rank observation、独立 decision-mark set/day-input v3、deterministic projection、durable day lease/takeover、execute-until-blocked、terminal PARTIAL、exact receipt upstream、原子 day commit、恢复和完整历史库 15 日验收；R4 子设计 `advisory_phase1r_r4_outcome_summary_phase1_bridge_f2_design_20260723.md` 已完成 outcome maturity、summary version、retrospective SEALED bridge、source correction 和 exact retry并由 PR `#2792` 合入。R1-R4 已闭合；下一任务为按 `advisory_phase1r_r5_api_ui_legacy_cutover_f2_design_20260727.md` 实现产品 API、历史验证 UI 和 legacy replay 主流程 cutover。
6. 荐股候选质量、HMM 消融和长期赢家双口径 Recall@K 基线审计设计。
7. 策略风格画像、特征/标签注册、原子 bundle 和 Program 部署治理设计。
8. HMM、行业黑名单和风格化行业优先级设计。
9. 超跌反弹候选重排与 Top5 shortlist 约束设计。
10. 收益分位数、信号存活和持股周期设计。
11. 分钟路径、成交概率、raw/CNY/yuan 转换和价格区间设计。
12. 长期趋势、有序目标、生存和捕获率设计。
13. WSL 训练、项目外 content-addressed artifact store、模型制品、数据快照和调度设计。
14. Advisory 推理服务、API、缓存、幂等和 reason code 设计。
15. 荐股页面 capability 影子展示与解释性设计。
16. Advisory 模型排名启用、Top5 active target 迁移、生命周期、发布和回滚设计；不包含 canary/champion 或审批状态机。
17. 可选 ModelOps、自动重训、漂移告警和灾备设计；仅在用户另行确认范围后启动。

## 21. Verification Plan / 验证方案

### 21.1 数据验证

- DB 与 Parquet 按日期、股票、字段和聚合值抽样一致。
- PIT 字段不得晚于决策时点；财务和行业数据使用真实可用时间。
- 日期身份满足 `decision/selection_as_of=T`、`target/review/SelectionRun/legacy episode.signal_date=T+1`，新观察不得把 legacy 字段误作 T 日 cutoff。
- 父包及全部 leg/model/HMM 的 effective OOS cutoff、最晚研究/冻结决策时点、embargo、manifest 和 vintage 完整。
- 非 COMPLETE capture、非 SEALED build checkpoint 和 ACTIVE/FAILED/EXPIRED attempt 不可消费；final snapshot 表只存在未 invalidated SEALED rows。
- observation/label selector 先解析 cutoff/as-of terminal revision，再检查 capability；最新失效 revision 不得回退旧 MATURED/COMPLETE。
- checkpoint 损坏只能经 authorized build termination 切换 generation；GC 新引用必须取消当前 epoch，logical quarantine 不移动 blob。
- 权威深池的所有候选均有固定期限成熟/删失标签，ENTER 与否不影响打标。
- T+1 交易状态只进入 outcome/price-quality label，不进入 T 日候选特征。
- 长周期标签的 censor、停牌、涨跌停和企业行动处理可复算。
- 数据缺失不允许静默填充为中性成功。
- 历史范围任务的交易日集合必须与权威日历一致；首日不得读取未来 active state，日任务特征最大可用时间不得晚于当日 cutoff，outcome 最小日期不得早于对应 entry/holding 时间轴。
- `HISTORICAL_RANGE_RESEARCH` 与 formal observation 使用不同 lineage/evidence level/dataset partition；selector 不得把 retrospective partition 自动提升为 formal OOS。

### 21.2 模型验证

共同指标：

```text
NDCG@5, Precision@5
Top5 cost-after excess return by benchmark_policy_hash/cost_policy_hash
win rate, payoff ratio, EXECUTABLE_MAE/MFE, PATH_MAE/MFE diagnostics, max drawdown, turnover
Brier score, reliability curve, quantile coverage
industry/regime/package/version stability
```

长期趋势额外指标：

```text
Recall@20/50 for EXECUTABLE_MFE thresholds; @100 only after an approved deep-pool contract
conditional +30/+50/+70 calibration
time-to-hit calibration
trend capture ratio
false early-exit rate
peak-before-stop path correctness
```

模型研究评价必须使用滚动时间样本外、purge/embargo 和按日期聚类的统计区间。行数多不等于独立样本多。

多期限、多模型、特征族和 regime 选择空间必须在训练前登记。详细设计至少选择并锁定一种适合该实验结构的多重检验/过拟合控制方法，例如 FDR、SPA/Reality Check、Deflated Sharpe 或 PBO，同时评价扣费后经济显著性；只挑最优一次结果不得声明该模型版本 ready。

### 21.3 业务与隔离 oracle

- 相同 SelectionRun 在模型关闭时保持当前行为。
- 五层 rank stage/hash 可追溯；影子模型不能改变 `selection_effective_rank`、Selection 结果或 Paper 意图。
- 多个 Program/策略包独立运行，互不覆盖模型、列表和生命周期。
- 历史范围任务可从任意显式已完成历史交易日起步；相同 request exact rerun 结果一致，失败日期恢复后继续按序推进，已完成日期不重复写入。
- 新策略包历史验证不触发 StrategyPackage 二次准入，不修改普通 Selection、当前 Advisory、模拟盘、Paper 或 QE；范围结果只能进入其独立研究页面和 retrospective dataset。
- 连续日期中候选变化较大时 active list 仍保持有界，每个退出与进入都有原因和替换关系，不发生候选名单无界膨胀。
- 行业黑名单股票不会被模型恢复。
- 稳态 active list 有界；DRAIN_TO_TARGET 迁移可解释收敛；退出股票有记录但不继续占 active count，也不会因回滚复活。
- 模型缺失、版本不匹配和数据过期返回明确 reason code。
- 每个 UI 模型区块只在对应 capability ready 时显示数值。
- UI 不把模型区间显示为确定收益或订单。

### 21.4 测试分层

- L0：schema、纯函数、哈希、标签、价格取整。
- L1：repository/service/model adapter、范围任务状态机、交易日展开和 list hash chain 契约。
- L2：DB 数据快照、真实历史多日 runner、恢复/冲突、训练 smoke、模型加载和推理幂等。
- L3：Advisory API、范围任务创建/进度/恢复/结果和现有业务流程。
- L4：真实页面 E2E、历史逐日时间线、收益成熟展示和影子运行。
- L5/nightly：长窗口 OOS、跨模块回归、漂移和长期标签成熟。

长训练、广泛市场阶段验证和 UI/API 业务流交由 Validation Center/CI/nightly；交互开发窗口只保留最小充分本地门禁。

## 22. Design Acceptance Matrix / 设计验收矩阵

本矩阵表示蓝图条款已经在文档中定义。`design_ready` 不表示代码已实现；代码 PR 必须替换为真实实现引用和测试证据。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §3、§6.2、§17 | 隔离架构和业务 oracle；artifact: `docs/architecture/advisory_real_dev_dual_track_input_onboarding_f2_design_20260716.md` | design_ready | none |
| F-002 | §2、§6.4 | 独立 Program 和禁止跨包融合；`backend/tests/advisory_dev_input_onboarding/test_o3_historical_onboarding.py` | design_ready | none |
| F-003 | §4.1、§7、§9.1、Phase 1、Phase 2 | stable signal/versioned evidence；artifact: `docs/architecture/advisory_phase1_pit_observation_labels_sealed_snapshot_f2_design_20260711.md` | design_ready | none |
| F-004 | §6.2、§8.1、§8.4、§16.2、Phase 1 | 五层 rank/score 和补采边界；`backend/tests/advisory_dev_input_onboarding/test_o4_orchestration.py` | design_ready | none |
| F-005 | §6.3、§9、Phase 0B、Phase 3、Phase 8 | 权威深池与 Recall@K；artifact: `docs/architecture/advisory_phase0a_candidate_authority_oos_data_availability_f1_design_20260710.md` | design_ready | none |
| F-006 | §8.1、§8.2 | HMM 边际特征和禁止二次乘权；`backend/tests/selection_center/test_hmm_runtime.py` | design_ready | none |
| F-007 | §8.3、§9.2 | 黑名单硬过滤；`backend/tests/selection_center/test_risk_policy.py` | design_ready | none |
| F-008 | §10、Phase 3 | 短反弹期限和标签；artifact: `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` | design_ready | none |
| F-009 | §10、§13、Phase 8 | 长趋势多状态和捕获率；artifact: `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` | design_ready | none |
| F-010 | §11.1、Phase 4 | 扣费后概率、分位数和校准；artifact: `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` | design_ready | none |
| F-011 | §11.2、Phase 4 | 存活与收益下分位数周期；artifact: `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` | design_ready | none |
| F-012 | §12.2、Phase 5 | 跳空、成交和净收益买入区间；artifact: `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` | design_ready | none |
| F-013 | §12.3、Phase 5 | 模型软区间、独立硬风险和 A 股约束；artifact: `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` | design_ready | none |
| F-014 | §9.2、§13.3、§16.1、Phase 7 | shortlist/target 分离和有界列表；artifact: `docs/analysis/advisory_recommendation_list_lifecycle_design_20260608.md` | design_ready | none |
| F-015 | §4.1、§14.1 | DB 权威及禁止回测/Paper 污染；`backend/tests/advisory_dev_input_onboarding/test_cli_and_isolation.py` | design_ready | none |
| F-016 | §14.1-14.2、Phase 0A、Phase 1、§21.1 | PIT universe、时钟、vintage 和 embargo；artifact: `docs/architecture/advisory_phase0a_candidate_authority_oos_data_availability_f1_design_20260710.md` | design_ready | none |
| F-017 | §14.3、Phase 1 | capture/build-attempt/SEALED/CAS；artifact: `docs/architecture/advisory_phase1_pit_observation_labels_sealed_snapshot_f2_design_20260711.md` | design_ready | none |
| F-018 | §15.2、§15.3、§17.1、Phase 9 | capability-closed immutable bundle；artifact: `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` | design_ready | none |
| F-019 | §4.2、§16.3、§17.4、§21.3 | 模型通道拒绝、基线连续性和 reason code；`backend/tests/advisory_dev_input_onboarding/test_o4_program_readiness.py` | design_ready | none |
| F-020 | §3、§17.3、Phase 6 | 仅决策展示、无逐股编辑和订单；artifact: `docs/analysis/advisory_native_multialpha_only_f2_design_20260710.md` | design_ready | none |
| F-021 | §15.3、Phase 3、Phase 6、Phase 7 | 用户确认后的 Program exact model configuration；artifact: `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` | design_ready | none |
| F-022 | §19、§20 | 分阶段路线和详细设计输出；artifact: `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` | design_ready | none |
| F-023 | §21、Phase 1 F2 §21 | 数据、模型、版本、业务和故障隔离；`backend/tests/advisory_dev_input_onboarding/test_o4_static_and_cli.py` | design_ready | none |
| F-024 | §19、§23、§25、Phase 1 F2 §21/§26 | 3 项工程检查、4 项运行不变量、零审批和零二次 package 验证；artifact: `docs/architecture/advisory_real_dev_dual_track_input_onboarding_f2_design_20260716.md` | design_ready | none |
| F-025 | Phase 0A.1、Phase 1 F2 §6/§18 | audit receipt 到 handoff/scope 的确定性直通；`backend/tests/advisory_phase0a/test_audit_service.py` | design_ready | none |
| F-026 | §14.3、Phase 1 F2 §7/§8/§13.5 | stable signal、version chain 和 snapshot selector；`backend/tests/advisory_phase1/test_phase1c3_batch_d_integrity.py` | design_ready | none |
| F-027 | §6.2、Phase 1 F2 §9.4/§21 | trace no-op/no-throw/budget/outbox；`backend/tests/advisory_phase1/test_trace_outbox_dev_db.py` | design_ready | none |
| F-028 | §6.4、Phase 1 F2 §9.1-9.3 | native parent/component evidence parity；`backend/tests/strategy_package/test_advisory_input_projection.py` | design_ready | none |
| F-029 | §11-14、Phase 1 F2 §10/§11 | executable timeline/cashflow/benchmark/terminal evidence；`backend/tests/advisory_phase1/test_outcome_engine.py` | design_ready | none |
| F-030 | §14.3、Phase 1 F2 §12-§16/§20 | source revision、attempt fencing、CAS 和 invalidation；`backend/tests/advisory_phase1/test_dataset_build.py` | design_ready | none |
| F-031 | Phase 0A.2 §1.2-1.4/§6.1/§11 | current-manifest research identity and archive isolation；`backend/tests/advisory_dev_input_onboarding/test_o2_exporter.py` | design_ready | none |
| F-032 | Phase 0A.2 §5.2/§6.2-6.3 | native parent、dated binding 和多 Program；`backend/tests/advisory_dev_input_onboarding/test_o3_historical_onboarding.py` | design_ready | none |
| F-033 | Phase 0A.2 §7/§12/§16 | `[from,to)`、future-effective 和 legacy null；`backend/tests/advisory_dev_input_onboarding/test_o3_postgres_integration.py` | design_ready | none |
| F-034 | Phase 0A.2 §8/§14/§15.1 | immutable policy registry 和零审批字段；`backend/tests/advisory_phase0a/test_policy_registry.py` | design_ready | none |
| F-035 | Phase 0A.2 §9/§12/§15.3 | immutable clock/config/runtime/HMM/universe evidence；`backend/tests/advisory_dev_input_onboarding/test_o3_historical_onboarding.py` | design_ready | none |
| F-036 | Phase 0A.2 §10/§16 | 合法空候选和自然样本边界；`backend/tests/advisory_dev_input_onboarding/test_o3_postgres_integration.py` | design_ready | none |
| F-037 | Phase 0A.2 §5.3/§9.6/§11 | source ledger 复用和 no-guess remediation；`backend/tests/advisory_dev_input_onboarding/test_o4_source_observer.py` | design_ready | none |
| F-038 | Phase 0A.2 §14-17/§19 | PARTIAL/HANDOFF/RESEARCH_READY 状态和运行不变量；`backend/tests/advisory_dev_input_onboarding/test_o4_program_readiness.py` | design_ready | none |
| F-039 | §6.5、Phase 0A.2 §6.4/§12-16 | manual historical runner、事务恢复和 batch receipt；`backend/tests/advisory_dev_input_onboarding/test_o3_historical_onboarding.py` | design_ready | none |
| F-040 | §6.5、Phase 0A.2 §1.3/§7.2/§11.3 | explicit completed trade date and research isolation；`backend/tests/advisory_dev_input_onboarding/test_cli_and_isolation.py` | design_ready | none |
| F-041 | §1.2、§2、§6.6、Phase 1R | 正式范围研究产品边界、持久化任务和用户可见研究结果；artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-042 | §6.2-6.3、§6.6、§25.1、Phase 1R | 冻结 package/config/code、复用权威推理语义、零 package 二次准入和零共享模块副作用；artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-043 | §6.3、§6.6、§16.1-16.2、§21.3 | 日任务幂等/恢复、decision-mark-set/day-input v3、前日 exact receipt chain、无 candidate-count gate 的 ENTER/HOLD/EXIT/WATCH 和有界 active list；artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-044 | §4.1、§6.6、§14.2、Phase 1R | 新策略 current-semantics 历史验证、收益成熟和 RETROSPECTIVE_RESEARCH_ONLY 语义；artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-045 | §3、§14.2、§17、Phase 1R/0B | retrospective dataset bridge、formal OOS 隔离及 Selection/模拟盘/Paper/QE 隔离；artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |

## 23. Rollout / Rollback / 发布与回滚

### 23.1 发布顺序

1. 合入并部署 Phase 0A.1/0A.2 policy、dated binding、immutable evidence 与 manual historical runner 代码；不创建 scheduler。
2. 对现有 single current manifest 和 native multi parent 执行只读 research preflight；手工选择已有历史 binding 可解析的 Program/date，不创建 successor binding 或正式 `T0`。
3. 在开发/发布流程完成 Phase 1 migration 验证并部署 schema；运行任务不执行 DDL。
4. Phase 1R R1-R4 已合入；R3 单/原生多 Alpha 15 日完整历史库执行，以及 R4 outcome maturity、summary version、retrospective SEALED bridge、source correction、exact retry 与隔离回执均已通过。不得把已完成的 R3/R4 重新列为 R5 前置实现任务。
5. 按 R5 F2 详细设计完成 typed API、历史验证 UI、legacy replay 主流程 cutover 和真实 API/UI E2E 后发布历史范围研究功能；使用显式历史区间执行单/多 Alpha 独立逐日研究、恢复和收益成熟查询，不等待最新交易日，也不改变当前荐股 list。R5 当前不新增 migration；若实现复核发现必须变更 schema，必须先更新详细设计并单独报告，不得静默加入 DDL。
6. 按版本化配置启用只记录数据库 ingestion completion 的 source observer，并将 formal 与 retrospective source 分区构建为各自 SEALED snapshot；observer 不触发荐股，范围研究不改变 evidence level。
7. 执行 Phase 0B、模型训练和制品提升；配置启用仅服务 Advisory 的模型预测 writer，无需审批事件或授权角色，模型不可用时现有荐股基线继续运行。
8. 再按 capability 发布 UI 影子展示。
9. 用户明确确认 Phase 7 范围后，为指定 Program 配置一个 exact immutable model bundle；该配置只影响学术研究 shortlist 展示，不产生实时建议、正式交易列表或执行输入。
10. 如未来需要 ModelOps、自动重训或漂移治理，另行完成专项设计和用户确认；它们不作为当前模型排名启用的前置门禁。

每一步都可独立停止。训练任务不得自动修改 Program 当前模型配置。

### 23.2 回滚

- 首先通过 Program 配置的正常乐观并发版本关闭受影响 model bundle，排名恢复 `selection_effective_rank`；若价格能力已启用，再独立恢复当前 `rule_default`，优先恢复用户侧基线。
- 随后停止受影响的 online/shadow prediction writer 和训练调度，阻止继续产生新副作用。
- 已发生的 target migration 不随模型回滚自动反向执行；不得复活已退出 episode，后续 list 继续 append-only。
- 不删除历史 prediction、dataset、model 和 monitor 证据。
- 不修改 StrategyPackage binding，不回滚 Selection/Paper 数据。
- DDL 仅允许向后兼容 expand-only 新增表/列；Program 模型配置回滚不删除表、历史或制品，代码回滚后旧 Advisory 路径仍可读取。
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
| current single manifest 继承旧 manifest DSE | 两个 signal identity 被错误合并 | current-manifest smoke 与 manifest-scoped evidence；旧 DSE 仅 retrospective |
| `review_schedule` 被误当成实际调度 | 历史研究被错误实时化 | 不实现 Advisory scheduler；只接受显式单日 request 或有限日期范围 request |
| 一个 Program 失败中断批次 | 多策略包独立性失效 | 稳定 Program 快照、逐 Program 事务/恢复和失败隔离 |
| replay 或集中回补冒充手工历史研究 | 研究证据混淆 | MANUAL_HISTORICAL_RESEARCH 与 REPLAY/PREVIEW/PUBLISHED 强隔离 |
| 范围研究被实现成一次性诊断脚本 | 新策略无法重复使用、恢复或比较 | 正式 HistoricalRangeResearchRun/DayRun/List/Outcome/API/UI 契约和确定性 receipt |
| 当前 active list 被用作历史首日 seed | 未来状态泄漏并污染收益 | Phase 1R v1 只允许空 seed；非空 seed 不在当前范围 |
| 范围日期并行提交导致列表乱序 | ENTER/HOLD/EXIT 状态分叉 | 候选可预取，list transition 按 Program/日期和前日 list hash 串行提交 |
| 当前策略回看被误报为当时真实运行 | 虚假 OOS 和错误上线判断 | `HISTORICAL_RANGE_RESEARCH + RETROSPECTIVE_RESEARCH_ONLY` 固定 lineage，禁止伪造历史 binding/vintage |
| 范围任务重新实现选股算法 | 与 Selection 语义漂移 | 仅通过 Advisory orchestration adapter 调用同一权威 StrategyPackage/Selection 推理组件并做隔离 oracle |
| 模型和数据版本错配 | 不可复现或错误预测 | 版本/hash 校验和 fail-closed |
| 模型区间被理解为保证 | 用户决策风险 | 概率、区间、置信度和明确风险说明 |
| 每日列表简单并集 | active list 膨胀 | 显式 ENTER/HOLD/EXIT 和有界 active count |
| 趋势股短期整理 | 每日重排导致过早退出 | 入选/持有/退出阈值分离和确认期 |
| 规则 fallback 冒充模型 | 错误的可信度展示 | `rule_default` 与 `model_predicted` 强区分 |

## 25. Production Gates / Engineering Checks And Runtime Invariants / 生产影响、工程检查与运行不变量

本设计不引入人工审批、审批角色、approval registry、operation authorization 或荐股运行时 package preflight。策略包在进入系统时已经完成准入和资产完整性验证；Advisory 只消费 dated binding、frozen manifest identity 和既有推理/Selection 证据，不再次检查模型、因子、组件或资产闭包，也不回写 package 状态。

开发和发布只保留三项常规工程检查，它们不进入荐股业务流程：

| ID | 阶段 | 工程检查 | 通过条件 |
|---|---|---|---|
| E-DEV-01 | 开发 | 代码与测试 | lint、契约测试、Feature Workflow、CI 和隔离 oracle 通过 |
| E-DEV-02 | 开发/发布 | Schema migration | 仅在存在 migration 时于开发/测试库验证可重复应用和 schema/code 匹配；运行进程无 DDL 入口 |
| E-DEV-03 | 发布 | 发布健康 | 本次变更涉及的依赖、配置、schema version、服务 health 和回滚 smoke 正常 |

运行阶段只保留程序必须满足的业务不变量，不建独立 gate/approval 状态机：

| ID | 运行不变量 | 正常语义 |
|---|---|---|
| R-DATA-01 | PIT 行情与输入可用性 | 交易日、行情、calendar 和已配置 HMM/risk 输入按 data-as-of 读取；缺失时对应 Program 明确 pending/error，数据正常时自动通过 |
| R-IDEMPOTENCY-01 | 幂等与并发 | 唯一业务键、事务、lease/fencing/CAS 和重试语义防止重复或分叉，不要求人工放行 |
| R-CONSISTENCY-01 | DML 一致性 | 程序化写入满足版本、hash、行数、状态转换和回滚约束，不产生半成品 |
| R-ARTIFACT-01 | Advisory 制品完整性 | 只验证本次新生成的 Advisory snapshot/model manifest、schema、hash 和 readback；不得重读或全量复算历史 Parquet，不得读取 QE/Qlib/backtest/Paper 文件 |

调度器、capture、label、build、prediction writer 和 Program model configuration 均由版本化配置运行；配置变化保留操作日志和 content hash，但不生成审批事件。

### 25.1 Positive-path satisfiability / 正向可达性

每个运行不变量必须在详细设计和实现中同时提供：

- 唯一上游 producer 和字段来源，禁止消费者猜值或要求上游永不产生的字段。
- 明确 pass predicate、失败 reason code 和可重试/不可重试分类。
- 至少一个合法正向 contract fixture 和一个针对每个关键拒绝分支的反向 fixture；最终业务可达性不得只靠 mock/fixture 声明完成。
- 从合法策略包、完整行情和正确配置出发的全链路 golden E2E；不得用 mock-only 证明业务可达。
- 状态机 reachability 检查，证明每个非终态都有合法后继，且合法输入不存在“所有分支均拒绝”的死门禁。
- 生产同构只读或隔离 smoke，验证真实 schema/type/timezone/hash 与 fixture 一致。

单日精确历史研究正向路径固定为：

```text
admitted enabled StrategyPackage identity + dated binding
  -> read frozen package identity without asset/model/factor revalidation
  -> R-DATA-01 market/input readiness is available
  -> R-IDEMPOTENCY-01 idempotent manual research acquisition
  -> historical runner executes each explicitly requested Program independently
  -> read-only persisted Selection evidence resolution with no candidates bypass
  -> R-CONSISTENCY-01 atomic research-result persistence
  -> per-Program result + batch receipt
  -> historical research list available
```

在已准入策略包、行情准确且配置匹配时，上述路径必须通过。模型、训练数据或 snapshot 子系统不可阻塞现有荐股基线；其不可用只能关闭对应模型能力并输出 reason code。

历史范围研究正向路径固定为：

```text
admitted StrategyPackage + explicit Program/config snapshot + completed date range
  -> freeze manifest/runtime/review/style/code/calendar identities without package revalidation
  -> expand ordered trading dates
  -> for each Program/date read DB_HISTORICAL PIT inputs at exact cutoff
  -> invoke the same authoritative StrategyPackage/Selection inference semantics through isolated Advisory adapter
  -> persist candidate set and day receipt idempotently
  -> apply ordered ENTER/HOLD/EXIT/WATCH transition from previous list hash
  -> append mature outcomes when their horizon data becomes available
  -> deterministic range summary + retrospective SEALED dataset refs
```

完整历史数据和合法当前策略语义必须能够自动贯通该链路。模型不可用时范围任务继续保存基线候选、列表和规则结果；不得用模型能力作为范围研究总门禁。

## 26. 开放决策与后续评审点

以下决策必须在对应详细设计中基于 Phase 0A/1/0B 证据确定，不阻断本蓝图：

- 每种风格在 v1 的内部候选池是 20 还是 50；是否需要 Top100 及其独立深池契约。
- 包级校准的最小有效 OOS 样本、市场阶段和时间跨度。
- 长期趋势的风险屏障、time stop 和移动保护口径。
- 行业集中度和相关性簇约束的默认值。
- 用户另行确认 ModelOps 后的自动重训频率、配置变更和漂移停用阈值。
- 分钟数据覆盖不足时允许展示的最粗价格区间等级。
- 模型预测表按候选、期限展开还是 JSONB 混合存储。
- Phase 1R 详细设计已决定不设置业务性的最大 Program 数或日期跨度；合法范围通过排队、分块和有界并发推进。v1 吞吐默认值为每进程 2 个 Program、每 Program 预取 2 个候选日期、每个 day-plan 物化事务 500 行、每个 outcome 短事务 500 个稳定 key，后续只可按测量结果调优，不得变成请求审批或业务门禁。
- Phase 1R 详细设计已决定在现有 Advisory 页面内使用独立“历史验证”tab；它与当前荐股视图共享导航但保持 API、list identity、表和 artifact root 隔离。

这些参数不得在代码阶段临时拍脑袋确定，必须回到相应 F1/F2 详细设计和验收矩阵。
