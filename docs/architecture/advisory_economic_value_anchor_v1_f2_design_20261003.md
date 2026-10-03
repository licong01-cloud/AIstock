# Advisory D日价值锚与买入价格集合 H-VALUE-ANCHOR-1（F2）

> 日期：2026-10-03；版本：v0.2；当前接续状态：NAVIGATION_COMPLETED_CANDIDATE_STOPPED。原设计经#5344合入，内核#5346已合入，研究源码#5347与一次真实研究接续见§13。本文设计矩阵仍只验收设计，不是模型收益确认、每日API接入或生产启用。
> 父级：[蓝图§16](advisory_strategy_conditioned_model_blueprint_v1_20260710.md)。唯一目标是收益与风险驱动的日频买入价格建议；不预测开盘落点或最佳分钟，不重复QE选股研究。

## 1. Background / 当前事实与新问题

收益型v1/v3/v4及H-TIMING-1均未获得经济确认，合格ENTRY_VALUE角色数量仍为0。H-TIMING-1已完成一次探索并停止；其15字段相对原基线日增量为-15.0261bps，不再追加窗口、种子、阈值或消费者家族。历史已消费数据只用于新假设的导航，不能转称独立OOS。

原经济模型以真实T开盘gap作为输入，拟合原退出政策下的收益。这里混合了两个不同问题：开盘价反映的市场信息，以及假想买价改变后的净价值。原政策按买价触发止损/止盈；改变买价后沿用原退出日期，不是合法的反事实政策评价。简单将旧收益乘回价格改名“未来价值”，也不构成实质新假设。

本轮先定义独立、价格无关的复评退出场景，预测相对D可见参考价的未来价值及路径最低估值。退出场景是监督目标的改变，不是生产政策变更。只比较同一新场景的对照，不能与旧19.1729%收益拼接判胜。

## 2. Scope / 可证伪假设及精确写入范围

**H-VALUE-ANCHOR-1：在固定原Top20、12D信息、价格无关复评场景及成本下，D-only未来价值/路径下沿预测导出的价格集合，在真实T开盘观察点上，能同时优于同场景无保护Top5和train-only常数价值锚。**

本假设不宣称增加外部信息、不宣称因果识别任意限价成交。它检验新的、可从既存快照识别的估值目标及价格应用语义；模型仍可能没有增量。D估计不消费T价格，T真实观察仅用于支持检查与实际动作评价。先核验新场景的退出与标签确实独立于假想买价；否则在训练前停止。

本设计阶段只改本文及蓝图当前队列。实施阶段分别新建以下精确Advisory叶文件及同名测试，实施前另交付F1验收切片，不修改既有共享模块：

实施验收文档范围另明确为`docs/architecture/advisory_economic_value_anchor_kernel_v1_f1_20261003.md`（前三叶/标签与内核）和`docs/architecture/advisory_economic_value_anchor_study_v1_f2_20261003.md`（后三叶/唯一研究）。不得为了通过索引把未实现叶项纳入已完成切片。

- `backend/services/advisory_model_first/economic_value_anchor_contracts_v1.py`
- `backend/services/advisory_model_first/economic_value_anchor_labels_v1.py`
- `backend/services/advisory_model_first/economic_value_anchor_inference_v1.py`
- `backend/services/advisory_model_first/economic_value_anchor_training_v1.py`
- `backend/services/advisory_model_first/economic_value_anchor_pipeline_v1.py`
- `backend/services/advisory_model_first/economic_value_anchor_evaluation_v1.py`
- `backend/tests/advisory_model_first/test_economic_value_anchor_contracts_v1.py`
- `backend/tests/advisory_model_first/test_economic_value_anchor_labels_v1.py`
- `backend/tests/advisory_model_first/test_economic_value_anchor_inference_v1.py`
- `backend/tests/advisory_model_first/test_economic_value_anchor_training_v1.py`
- `backend/tests/advisory_model_first/test_economic_value_anchor_pipeline_v1.py`
- `backend/tests/advisory_model_first/test_economic_value_anchor_evaluation_v1.py`

源码验收按真实切片分别交付；未实现的后续切片不在本设计矩阵冒充PASS。没有值得继续的模型时，不建立新family API/UI或通用平台。任务自有ignored计划位于`tmp/handoff/advisory-value-anchor-longtask-20261003/`。

## 3. Non-goals / 模块与授权边界

不修改QE、Selection、StrategyPackage、HMM、数据准备、Paper、Execution或公共流水线；不重选股票或提交QE实验。新候选输出仍按原rank优先保留最多5个固定空槽，不开展Top20重排序、动态资金配置或分钟执行。

不写数据库，不补数据/receipt，不激活profile或binding，不安装依赖，不启动/停止用户进程。实际fit不得与QE训练并行；准备和只读回放不要求QE停止。资源状态未知只暂停fit。临时全部X，持久全部F，不回落C；后端重启用户负责。

## 4. Architecture / 单线实施

既存冻结候选与价格快照 → 新场景独立episode标签 → 相同12D train-only监督 → 两头D预测 → 支持域内价格—价值集合 → 真实T开盘选择 → 三臂同场景shadow比较。

直接消费已交付`policy_episode_labels.build_policy_episode_labels`、`shadow_portfolio_policy.replay_shadow_portfolio`、原12D计算及原子stage发布/registry；不开发第二个模拟器、通用ModelOps或研究审批平台。旧训练请求、模型和产物不改写、不补侧车。

## 5. Contracts / 场景、标签、价格与支持

### 5.1 独立场景 VALUE_REVIEW_5_V1

新`AdvisoryTransitionPolicyV1`仅供离线研究：target_count=5，rank_enter=5，rank_exit=40，rank_exit_confirm_days=2，daily_replacement_budget=5，stop_loss=0，take_profit=0，trailing_stop=0，time_stop_days=5，take_profit_mode=trailing。完整序列化配置绑定新policy hash。

0是现有引擎明确的关闭语义，不是新增兜底。保留原Top40复评及跌出名单的原生rank_depth+1处理；不伪造排名、不用极大阈值关闭rank退出。5遵循现有引擎的**有效复评计数**，非保证五个日历/交易日成交：labeler对停牌跳过复评，无法卖出的涨跌停继续持有，episode实际结束可能延迟。复评退出与price-trigger退出明确分离，A股T+1仍禁止T日买卖闭环。

单候选label与组合回放同引擎/费用/可执行性/时钟。rank replacement预算=5覆盖全部固定槽，无不同持仓竞争导致的预算截断；每次首次买入后，未来rank和可执行bar不依赖假想买价。必须测试改entry price不改变rank/time退出时点，并核对labeler/portfolio的停牌、跌停延期及有效计数一致；不一致则停止该场景拟合并报告，不能改共享引擎或忽略不一致。

共享模拟器的一字涨跌停判定读取完整日线，只是历史可执行性裁定，不能进入T开盘决策特征。三臂统一先以T开盘可见的实际价/法规界/停牌状态作保守准入：开盘达到涨停价不进入，不等盘后high/low来证明能买。共同基线仍是原Top5，无价值过滤，不补第6名。开盘价/停牌/法规身份未知时所有臂均不制造进入。跌停价开盘的实际卖出是否可执行若仅靠盘后日线才能判断，单候选标签标记执行未证明，组合实际episode须通过只读endpoint audit；有未证明端点时终止经济比较并报告具体阻断，不篡改bar或宣称P&L已证实。

### 5.2 新监督标签与坐标

每个原候选键为(D,T,instrument)，T必须是D的下一权威交易日。D参考价R采用父快照的`target_reference_raw_cny`，可见边界≤D；fT是该候选T原始元到policy价的已证明换算。新episode退出policy价E生成未来价值`Y=E/(R*fT)`。Y为不含买卖费用的gross value ratio，非旧policy return的重命名。

路径下沿标签`L=min(M_s/(R*fT))`：入场后T收盘、T之后持有期的真实开/收盘及最终退出开盘；退出开盘后的close不读。仅反映日级mark风险，不包含未来分钟最低价、滑点成交或最佳卖点。L不混入假想买价，不用峰值drawdown，不将真实T买价机械加入路径下沿。停牌使用可证明的同坐标carry mark及原等待语义，不合成可成交bar；无法证明的mark、坐标或停牌状态为UNKNOWN，原候选/日期保留。

T当日收盘风险虽不可T+0卖出仍须计入持有风险；最早退出为T之后。停牌carry从最后已证明policy mark继承（首个mark必须是已证明入场价），不因缺行取0或把未来复牌价反填。未来企业行动若policy坐标换算链不能证明，则UNKNOWN，而不是使用今日factor拼接。任何候选bar中的source/coordinate/hash矛盾全局fail closed；正常缺行只影响对应标签，不删名单/日期。

市场价格使用父快照policy OHLC及原始元换算，企业行动沿同一原坐标。标注ADJUSTED_SHADOW_VALUATION，不冒称实际现金分红账本或实盘P&L。每个新标签绑定新policy/cost/source/reference/coordinate hash及实际label_information_end；原生成员未证明、RECOVERED_LIMITED/NON_VINTAGE限制不升级。标签成熟状态与未来路径不得进入预测mask。

### 5.3 价格—净价值内核

模型只读12D字段，无query、T行情或成熟状态；输出`mean(Y)`和`q0.10(L)`。对于价格p/R=x>0，买费b=0.95bps、卖费s=5.95bps仅扣一次：

```text
expected_net_bps(x) = 10000 * (mean(Y)*(1-s/10000)/(x*(1+b/10000))-1)
downside_q90_bps(x) = max(0, 10000*(1-q10(L)*(1-s/10000)/(x*(1+b/10000))))
```

两者都不等于盈利概率或均值置信下界。q10只估计路径下沿分布；800bps沿用研究风险参考，不是用户资金预算。非有限/非正gross估计、越界费用、坐标矛盾fail closed；真实未知状态输出UNKNOWN，不伪成功。

这里的“期望”只指D信息下的预测值除以给定价的代数估计，不宣称是`E[收益|D,T价=p]`：真实价格可能携带D未知的消息。支持筛选只限制外推，不能补足这一条件信息。因此actual-open价值必须经三臂验证，正式产品语义需要后续确认/每日观察合同，不能仅因该公式单调就宣称低开安全。

价格集合要求合法tick/法规域、已证明支持、expected_net>0及downside_q90≤800。根为`x < mean(Y)*(1-s)/(1+b)`及`x ≤ q10(L)*(1-s)/((1+b)*0.92)`（公式中s/b为费率非bps）。严格收益根使用tick向内，不能将零收益边界取成可买；支持桶之间的洞不得连成大区间。这里只是条件式研究估值集合，日线触价不证明限价成交。法规模型或D目标价坐标未证明时不得产出正式价格建议。

### 5.4 观察支持与拒绝极端外推

支持仅由训练集12D完整输入及真实观察gap确定，不用盈利/标签成熟筛价格。gap桶100bps，每桶≥30观察且覆盖≥5个D，并限制在该桶真实min/max及train gap的固定2.5%～97.5%分位范围（线性分位法）。这是一项事前的尾部外推保护，不能看到TAKE后改变分位/门槛。

它不保证支持内的低价就是安全、没有新闻冲击或具备个股反事实重叠；actual-open分布支持不是因果positivity证明。T真实观察超支持/未知时为UNKNOWN_OUT_OF_SUPPORT，需要重估而非“越跌越值得买”。价格更低时公式机械变好也不授权越界购买。生产UNKNOWN不得按研究控制买入。

## 6. 唯一研究合同 / 输入、预算与不可重选

元数据已确认：原prepared快照有406日历日、386D/7720候选；train=2024-07-04～2025-05-30，validation=2025-06-03～2025-09-30，已消费test=2025-10-09～2026-02-02，label cutoff=2026-03-10。准确切分仍读取父请求绑定，不按本文近似推导。只消费旧开发窗口，不读取sealed、新holdout或未来新增结果。

价格/候选/政策上下文源：`F:/Dev/AIstock_model_artifacts/advisory_economic_entry_value_v1_20261002/adveconomic_4962fd2ec68948033628954a/prepared/`。新12D投影源：`F:/Dev/AIstock_model_artifacts/advisory_entry_timing_v1_20261003/inputs/advtiminginput_9a929b2d2dafd1c4cf18f42f/prepared/manifest.json`。前者prepared manifest SHA256为`65b095bf40974f9a4edaed420b5770bbedb94bb6b5a123ff3ec524d224dd25df`；实际所有artifact SHA与父身份须在prereg前再核验。不读库重建这些既存输入。

仅新12D投影，显式忽略两个timing字段，不能交给旧权重。train/validation eligible由本场景Y/L AVAILABLE、12D完整和label-end purge的同交集决定；支持用无收益的训练观察，不能偷用test分布。validation只报告诊断，不early stop/校准/选阈值；test只作本新lineage一次导航，不选点或改参数。所有原人口完整保留；KNOWN/UNKNOWN及train purge计数逐层报告。

一个candidate、一个模型配置、两头fit：LightGBM 200轮/lr0.05/depth3/leaves7/minleaf30/seed20261002/2线程，mean为L2、路径下沿quantile alpha=.10，固定LightGBM4.6.0；不得扫seed、loss或horizon。常数控制仅使用同train eligible的meanY及经验q10L，不额外搜索/拟合模型。registry记录真实2头、一个模型candidate、常数规则控制及累计研究族背景；study_type=EXPLORATORY_SCREEN，objective_contract=RISK_MANAGED_ADVISORY，decision_use=NAVIGATION_ONLY。

拟合前发布完整输入/label recipe/scenario/support/model/cost/source/code身份与预算；新输出根`F:/Dev/AIstock_model_artifacts/advisory_value_anchor_v1_20261003`，temp=`X:/AIstock_temp/advisory/value-anchor-next`。沿既有不可变stage及fit-started标记，中断不得隐式重fit；exact retry只验证读取已发布工件，不再增加拟合/结果访问。源hash按既有UTF8_LF_BYTES_V1仅规范CRLF，同时记录Git blob；真实文件hash不规范。

## 7. Evaluation / 经济价值与证据等级

三臂仅为同VALUE_REVIEW_5_V1下的原无保护Top5、常数锚、D模型锚。两锚使用同支持mask及真实T开盘决策；只过滤原Top5，不用第6～20名补位，UNKNOWN研究控制分列不成为部署fallback。原Top20仍用于监督覆盖。所有臂同成本/可执行性/原rank及共同日历，固定5槽，空槽留现金但不形成动态资金分配。

UNKNOWN控制的唯一事前规则：若市场准入可证明而模型特征/支持未知，锚臂在研究中沿原Top5顺序做共同基线控制并单列贡献；若市场/法规/停牌未知，三臂都不买。控制不是模型TAKE，不能用其盈利提升模型胜率。两头非法预测不得降为控制，必须报错停止。

报告D模型相对常数和相对同场景基线的成对日净收益增量、各臂名义净收益、MDD、尾损、实际进入差异日、模型TAKE/SKIP/UNKNOWN、真实episode及UNKNOWN控制贡献。共同估值终端由原冻结日历与实际已进入episode是否完成核定，不强行套旧100日结果；持仓未完成保留标记，不只选盈利已结束episode。时钟/标签/回放不一致或held price不明为明确数据阻断，不偷偷carry未知价格凑终值。

预测诊断用mean的平方误差及q10的pinball loss/经验分位覆盖；只诊断，不替代经济价值。moving-block bootstrap block5/reps2000/seed20261002，95%区间/MDE只属开发描述，不是多轮适应搜索后的确认性显著性。regime未预定义则UNPROVEN，不事后按行情分组。

两个增量均正且非零真实干预，只能建议另立确认合同；零/负增量、无支持或零干预即STOP_CURRENT_CANDIDATE，不反调风险、窗口、seed或支持分位，也不为其收集更多证据。新方向若不值得继续允许提前结束约10小时任务。导航不得支持激活或关闭全局方向，正式confirmation必须另预注册经济MDE、最低干预次数/日占比/regime及一次选点规则；本轮不读确认窗口。

## 8. Implementation Plan / 约10小时有效预算

1. 设计及三视角审核约2小时：先场景/因果语义，再PIT/缺失，最后投入/授权；结构validator及diff check通过后交付设计PR。
2. labels/contracts/纯价格内核F1约2小时：先做price-independent exit及label/portfolio一致性最小例子，再实现Y/L与成本一次、支持洞、合法tick的内核。不符即停止，不另造模拟器。
3. 两头/原子研究切片约2小时：严格12D/order、purge、train-only support、真实trial/fit-started、旧family拒绝新scope；实现完成才预登记。
4. 公开QE资源空闲且输入正确后一次导航约1～2小时；不提交QE实验，不等待实盘日更，不查询新holdout。没有ready资源不控制其它实验。
5. 多轮审核、CI/合入、真实蓝图进度及本任务精确官方清理约2小时。CI等待另报，不为凑工时重训。可先合入已通过的独立切片，不能冒称整个F2已经实现。

## 9. Verification Plan / Design Acceptance Index

| ID | 设计验收要求 |
|---|---|
| F-631 | 实质新场景，不反事实复用原entry-dependent退出或旧收益改名 |
| F-632 | 现有引擎零触发/有效复评/停牌延期与单候选组合一致性 |
| F-633 | D参考/policy坐标、Y/L、成本一次、支持洞及严格tick根 |
| F-634 | D-only 12维、purge、未知人口和旧source限制不升级 |
| F-635 | 一candidate两头及常数控制、train-only支持、预登记不可重fit |
| F-636 | 三臂同场景经济/实际干预、导航/确认/启用隔离 |
| F-637 | 精确叶范围、阶段验收、授权/QE/临时目录及提前停止 |
| F-638 | 观察估值与因果成交、日级风险与收益概率的边界 |

最小测试只保留业务合同：相同未来rank/bar改变买价不改退出；停牌计数/跌停延期同labeler与portfolio；Y/L坐标尺度不变、退出后价格毒化无影响；真缺失UNKNOWN不删候选；费用一次、有限/正值、strict零收益tick与支持洞；test污染不能改fit/支持；非法旧scope/跨包/hash矛盾fail closed；fit中断不得再训练。避免实现快照、重复fixture和仅证明字段拼装的测试。单D/批量同输入同输出，不新加日常provider查询。

## 10. Design Acceptance Matrix

下表仅验收本文设计；源码/真实输入/模型/经济/激活均独立报告。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-631 | 本文§1/2/5 | artifact: docs/architecture/advisory_economic_value_anchor_v1_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-632 | 本文§5.1/8/9 | artifact: docs/architecture/advisory_economic_value_anchor_v1_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-633 | 本文§5.2/5.3/5.4 | artifact: docs/architecture/advisory_economic_value_anchor_v1_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-634 | 本文§3/5/6 | artifact: docs/architecture/advisory_economic_value_anchor_v1_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-635 | 本文§6 | artifact: docs/architecture/advisory_economic_value_anchor_v1_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-636 | 本文§7 | artifact: docs/architecture/advisory_economic_value_anchor_v1_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-637 | 本文§2/3/8/12 | artifact: docs/architecture/advisory_economic_value_anchor_v1_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-638 | 本文§5/11 | artifact: docs/architecture/advisory_economic_value_anchor_v1_f2_design_20261003.md | DESIGN_VERIFIED | none |

## 11. Risks / 理论边界与审核

[Athey/Wager：Policy Learning with Observational Data](https://arxiv.org/abs/1702.02896)提醒政策价值估计依赖识别条件；本项目没有任意价格干预的成交数据，因此不声称限价动作的因果效应。[Gneiting/Raftery：Strictly Proper Scoring Rules, Prediction, and Estimation](https://stat.uw.edu/research/preprints/tech-report/strictly-proper-scoring-rules-prediction-and-estimation-0)提供概率/分位预测评价的理论背景；它不是本场景alpha、800bps或5次复评的证据。本文价格公式为成本账本代数，真正业务价值仍需同场景经济比较。

主要风险：同一12D信息仍可能无收益信号；平均价值/q10不能识别个股坏消息；固定短场景可能缩减样本且偏离未来产品持有需求；有效复评不是固定自然期限；复权shadow账本不是现金交易。任何正导航都不能直接替换生产退出或正式价格建议。

自审1（业务/可识别性）：核对共享引擎与episode标签，修订“固定五交易日”为五次有效复评并保留rank退出；新增整日日线可执行性与开盘可见性边界，未证明端点阻断经济比较，不改公共模拟器。

自审2（PIT/价格/未知）：补充T收盘不可卖但计风险、停牌carry首mark与企业行动未知；明确D-only估值不是条件于真实T价的收益期望，保留极端支持拒绝、源/hash fail closed及正常缺失人口。

自审3（投入/范围/验收）：精确列出两个阶段验收文档及十二叶路径；区分常数控制、两头fit、UNKNOWN基线贡献与真实TAKE，验证不为负candidate补证、不建新模拟器/平台、不提前接API/UI。三轮为本窗口不同视角自审，不冒称外部独立审核。

## 12. Rollout / Rollback / Production Gates

仅离线独立family，旧生产API/UI/scheduler/policy默认行为全部不变。无需后端重启来运行纯离线代码，不声称运行时已加载。若未来值得继续，再单独设计同scope每日接入和正式资格；没有值得继续的模型则停止。本轮无binding/profile/数据激活、DDL/DML、依赖或进程操作，production_ddl_gate=noop，runtime_activation=noop。

DESIGN-COMPLIANCE-001逐项：设计不冒充已实现；UNKNOWN/无支持不伪成功；新场景显式命名且不偷偷改原政策；不增加未来日期/治理平台门禁。重复审核必须记录真实发现和修订，不以validator通过代替业务审查。

## 13. 真实接续结果 / 不改变事前合同

已按固定方案完成run `advvalue_a4bc66a30cfa7d9d5078850c`，源HEAD1c8545ab870f5e12b9e128e09b1b13a4bdf0ae2f。386D/7720候选保留，新场景标签7699可用/21 UNKNOWN，12D未知10；共同train4250/validation1622，purge296。一配置两头一次fit，81D/1620实际评价候选与100共同估值日，三臂端点/held mark审计问题0；prepare15.163秒、fit0.880秒（含源核验）、fit＋评价12.169秒；前后QE7项状态均0，无DB/sealed/服务操作。

新场景baseline/constant/model名义净收益21.3220%/25.4597%/17.4642%，MDD -10.3314%/-10.7283%/-8.0746%。model减constant日增量-6.9022bps，95%[-24.5301,9.9157]；减baseline-3.4196bps，95%[-18.9390,12.2020]。模型81真实TAKE＋3 UNKNOWN控制episode，相对常数实际进入不同58日。虽然模型胜率61.90%及MDD改善，两个收益条件仍未满足，`STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION/NAVIGATION_ONLY`。不事后把常数控制升级为选中的candidate，不改阈值/seed/期限/信息集补救本run；经济确认、binding和正式启用0。源码与内核能力保留，但不为该模型接daily family或收集更多证据。

本段仅更新真实进度，原§2～7事前假设、目标、支持及终止条件不因结果调整。完整结果路径与主线优先级见蓝图§1.3/§16，旧19.17%不参与此场景判胜。
