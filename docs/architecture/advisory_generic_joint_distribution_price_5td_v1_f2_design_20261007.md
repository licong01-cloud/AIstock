# Advisory GP5-JOINT-DISTRIBUTION-1：固定5交易日联合收益/风险分布 F2详细设计

2026-10-07，SOURCE_READY_NOT_RESEARCHED。当前量价路径run advgp5volume_45dbd44d9afa27fa1889191c一次负：candidate−base/matched为−47.5435/−3.8856bps每5TD cohort，避免亏损39.8462、错过上涨89.4341，已知拒买净贡献−49.5879bps。其源码PR #5630当前CI37495032551 SUCCESS，合入524910031f7a12d1abb087a4c3d50275ab723cff；该候选结束，不重拟合、不救阈值。当前实际111研究fit+1历史index。本文只定义新的学习器结构，不宣称已训练或能产生超额收益。 设计#5632合入df43a81aa/自身官方清理完成。两份新源与两份测试已实现，未prepare或研究fit。

## Background / Goal

前三版GP5分别增加日频、D分钟、D量价输入后仍误拒盈利样本；新的三项量价全部可测量，不应再以缺失坐标为收益瓶颈。下一假设：在同19原始特征/39维价格条件化输入下，以同一份经验联合分布计算期望收益与路径下分位，替代两个独立GBDT回归头，是否减少无净价值的拒买？这是一个事前整体学习器假设（森林分区＋不复用拟合标签的估计池），不是新loss/seed/阈值搜索。

方法依据：[Quantile Regression Forests，JMLR原论文](https://www.jmlr.org/papers/v7/meinshausen06a.html)支持森林邻域权重估计条件分布；本方案采用时间分离、已成熟监督的估计池，是明确的本地改造而非声称原论文原样实现或因果识别。[RandomForestRegressor官方文档](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestRegressor.html)仅作为参数/leaf API依据；实际环境版本记录在新产物，不安装或更换依赖。

固定5TD、原Top5五槽、原合法价格与成本不变，目标仍是相对原Top5的成本后净增量。胜率、开盘覆盖、留现金或风险下降单独不能替代目标；price集合可为空/未知。UI后置，M1 NOT_CONFIGURED辅线不阻此离线主线。只研究价格预测，不研发分钟执行或QE Alpha。

## Scope / Non-goals

设计阶段只本文件及蓝图。#5630合入后从最新main建立独立源码树，严格六文件：

- backend/services/advisory_model_first/generic_joint_distribution_price_5td_model_v1.py
- backend/services/advisory_model_first/generic_joint_distribution_price_5td_pipeline_v1.py
- backend/tests/advisory_model_first/test_generic_joint_distribution_price_5td_model_v1.py
- backend/tests/advisory_model_first/test_generic_joint_distribution_price_5td_pipeline_v1.py
- 本文件与advisory_strategy_conditioned_model_blueprint_v1_20260710.md。

只读复用现有volume-path prepared/源身份、frozen trained GBDT控制包、原feature values/matrix/support、stage/registry、KEY及5TD政策。旧GP5/GP5-MINUTE/volume-path源文件、权重、阶段及研究结果不修改、不重跑。新family/run/schema/implementation SHA独立。禁止改QE/Selection/HMM/StrategyPackage/行业/公共数据/Execution/Paper/CI/workflow/AGENTS；不新建平台、UI或新的资格门。

所有临时/cache/pytest全X，正式新root F:/Dev/AIstock_model_artifacts/advisory_generic_joint_distribution_price_5td_v1_20261007。0DB/DDL/DML/安装/profile或模型激活/其它进程控制；后台重启user-owned。本离线切片不需要重启。只读取已消费开发窗，不读取sealed，不重选池/名单或补数据。

## Architecture / Contracts

### 1. 原监督与独立身份

新plan引用volume-path preregistered plan、prepared manifest、trained manifest的完整文件SHA与size，读回原子parent chain、configuration、政策hash、386原decision_dates、7720KEY及schema、训练键hash。prepared仍来自D与更早，T仅监督中观察开盘价、H=T+4收盘，正常停牌/未知不压缩或延长H；buy.95bps/sell5.95bps各一次，risk800bps与expected_net>0不变。新模型不读取活动profile或重新加载源bin，承接非vintage来源等级。

父包身份不重新资格审查。stock_universe/single_index/index_union只是元数据，价格计算不读父alpha、rank或腿分数；真实矛盾/损坏显式计算错误，正常未知逐行返回，不删除原人口。

### 2. 固定诚实时间分区

先按原train时钟筛，再解析数值，test/validation poison不影响fit、编码、支持或输入校验。沿用冻结控制包的19 train-only medians、missing flags和原gap支持，不结果后选择区间。成熟pool与旧volume-path相同，要求AVAILABLE且label_information_end<=train_end、T<=train_end和原可测日频/价格支持。

成熟D排序，第floor(N/2)个D为估计池首日：结构池D在其之前且label_information_end严格早于估计池首D；跨界未成熟/重叠的结构监督不使用，不能将同一个5TD标签同时放两池。估计池D在分割日及以后、H<=train_end；两池记录KEY/labelend/hash与行数/日期数，原prepared/test人口不删除。若没有可训练或可估计监督则无fit、明确不具备此模型输入，而非更换切点或过滤原候选；不规定新的策略包准入门。

结构池以gross_terminal_ratio训练一个RandomForestRegressor：n_estimators128/max_depth6/min_samples_leaf30/max_features1.0/bootstrapTrue/random_state20261007/n_jobs2，固定且不搜索。一次模型fit（128内部树另记，不与候选试验数混淆），估计池标签从不参与树分裂/目标拟合。所有池的监督都在原train_end前成熟；这不是独立OOS或sealed确认。

### 3. 联合经验分布

为每棵树保存非执行JSON节点（feature/threshold/left/right）及估计池leaf→原估计样本index映射；估计样本只保存成对gross_terminal_ratio/path_min_ratio与KEY，path_min<=terminal的已知矛盾显式报错，容差1e-6。JSONwalk将输入转float32后与sklearn.apply做精确leaf parity，避免阈值邻域与原生库坐标不一致；不输出可执行pickle。

新点x每树匹配一个leaf；有估计成员的树贡献该leaf内等权质量1/n_leaf，再对有成员树平均，最终对原估计样本求和权重。重复样本跨树累积权重而非当作128倍独立观测；有效树数、unique估计样本数和权重集中度分别输出。所有树都无成员则该点UNKNOWN_MODEL_DISTRIBUTION，不用全局均值填补；没有样本的树不伪造成零收益。

同一组样本权重计算terminal均值和path_min的0.1加权分位（CDF首次>=.1，浮点求和边界容差1e-12），并可报告同价净收益>0的概率。均值/分位保留成对样本信息，没有两个独立拟合头；概率仅解释，不新增概率阈值或换目标。这里是条件关联估计，不识别未成交挂单的因果fill/收益。 输出权重effective sample size只解释集中度，不能当时间独立样本量；概率仅因浮点舍入clip至[0,1]，不新增价格准入阈值。

### 4. 价格查询和集合

query_joint_price_nodes_v1接受D features及假设gap，以原39维编码计算联合分布。净收益=10000*(E[terminal]*(1−sell)/(entry*(1+buy))−1)，entry=1+gap/10000；downside_q90=10000*max(0,1−q10(path)/entry)。沿用原expected_net>0与risk<=800做ACCEPTABLE/AVOID；支持外、日频必需输入不足或无估计质量分别UNKNOWN，保留KEY/包/池元数据和逐字段未知。

joint_price_set_5td_v1扫描完整legal tick，精确Decimal边界、不将UNKNOWN洞并成区间；输出多段/全未知/空合法集合/无可买价。UNKNOWN_INPUT_OR_SUPPORT与UNKNOWN_MODEL_DISTRIBUTION都按UNKNOWN分类，不能将无估计质量当作已知AVOID或补全球均值。不是开盘价、最佳分钟买点或卖点预测。输入价格单位D_ANCHORED_CNY；不会因为假设低价更好无限外推。成本各一次，所有tick<=100000，分批最多128查询点×7720估计样本，权重内存受界；不按股票每日重建工作树。

### 5. 一次新研究与四臂

四原子stage preregistered→prepared→trained→evaluated、只追加registry EXPLORATORY_SCREEN/NAVIGATION_ONLY/RISK_MANAGED_ADVISORY，parent lineage引用旧volume-path与GP5，唯一变量HONEST_JOINT_DISTRIBUTION_LEARNER。准备只读校验并原子引用/保存父冻结监督与控制包；不会再次运行Selection或读取旧收益输出。

四臂：新joint candidate、既存39维GBDT candidate作为frozen matched、original Top5 baseline、固定±300bps rule。控制bundle由父trained manifest固定，不新fit、不选旧33维/其它seed，不根据本次结果回选control；同19信息/同成熟pool来源，诚实分区属于新学习器事前定义，不能声称训练角色完全相同。只有新candidate一次物理fit/1model trial，partial journal不隐式重试，完成stage只读回。

每个test原D/Top5独立五槽5TD cohort，SKIP留空、不补Top6，1正常未结算null，不把重叠cohort复利成NAV/MDD。两条配对增量candidate−frozen matched和candidate−base，四臂共同比较分母另列；TAKE胜率/幅度、KNOWN干预、UNKNOWN-only、不可执行、未结算与避免亏损−错过上涨逐项报告。沿用完整配对序列block5且有洞区间null；不能压缩洞作独立日。负只停止此候选，不调seed/参数/切点/概率阈值或救研究，源码完整可交付不依赖盈利。

## Implementation Plan

设计自审/修订/F2先合入→#5630已合入最新main独立六文件scope→联合权重/纯JSON叶子/查询价集→新父输入只读stage/一次fit journal/四臂→来源、数学/训练、收益/授权多轮修复，失败只定向节点→稳定一次小矩阵/Ruff/L0/F2→clean producer→fresh QE single/custom_evo/multi-alpha全0后一fit/完整回放、后读回→真实结果同步/当前CI合入/自身清理。不提交QE任务；QE忙则继续自身源码，不停止QE或用户服务。运行超过30min才按每半小时检查，不密集监控。

约3～6h工作量估计，不凑时长或隐式重复训练。若正也仅开发导航，不直接激活或宣称利润；下一步必要日频消费者单独scope且UI后置，配置/模型激活仍单独授权。现有M1的五有效review不是本5TD，不静默改变旧API。

## Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-741 | 原386D/7720、policy/profile来源及父prepared/trained身份只读继承、独立新schema |
| F-742 | test poison不影响、train-only编码/支持、结构标签成熟purge、两池不重合 |
| F-743 | 固定一fit、leaf JSON/apply parity、成对样本约束、budget与partial不隐式再fit |
| F-744 | 权重手算/归一、重复样本去伪独立、零质量UNKNOWN、weighted q10/mean/profit |
| F-745 | 成本一次/同800风险、完整legal tick与洞、元数据/不同包与指数池数学一致 |
| F-746 | frozen39维控制不重训、四臂/原槽与原D保留、未知/未结算/非NAV |
| F-747 | 避免亏损−错过上涨及UNKNOWN另列、共同分母与已知干预归因对账 |
| F-748 | 独立精确scope、多轮小矩阵/CI、真实计数/来源等级、QE串行与生产NOOP |

## Design Acceptance Matrix

当前源码切片已实现，矩阵只验收完整离线模型/编排；未prepare或研究fit/收益，单元fixture不计研究。API/UI/经济/运行不冒充已验收。
| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-741 | backend/services/advisory_model_first/generic_joint_distribution_price_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_generic_joint_distribution_price_5td_pipeline_v1.py | SOURCE_VERIFIED | none |
| F-742 | backend/services/advisory_model_first/generic_joint_distribution_price_5td_model_v1.py | backend/tests/advisory_model_first/test_generic_joint_distribution_price_5td_model_v1.py | SOURCE_VERIFIED | none |
| F-743 | backend/services/advisory_model_first/generic_joint_distribution_price_5td_model_v1.py | backend/tests/advisory_model_first/test_generic_joint_distribution_price_5td_model_v1.py | SOURCE_VERIFIED | none |
| F-744 | backend/services/advisory_model_first/generic_joint_distribution_price_5td_model_v1.py | backend/tests/advisory_model_first/test_generic_joint_distribution_price_5td_model_v1.py | SOURCE_VERIFIED | none |
| F-745 | backend/services/advisory_model_first/generic_joint_distribution_price_5td_model_v1.py | backend/tests/advisory_model_first/test_generic_joint_distribution_price_5td_model_v1.py | SOURCE_VERIFIED | none |
| F-746 | backend/services/advisory_model_first/generic_joint_distribution_price_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_generic_joint_distribution_price_5td_pipeline_v1.py | SOURCE_VERIFIED | none |
| F-747 | backend/services/advisory_model_first/generic_joint_distribution_price_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_generic_joint_distribution_price_5td_pipeline_v1.py | SOURCE_VERIFIED | none |
| F-748 | backend/services/advisory_model_first/generic_joint_distribution_price_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_generic_joint_distribution_price_5td_pipeline_v1.py | SOURCE_VERIFIED | none |

最小矩阵共享一个手工小森林/一份成熟训练fixture：精确leaf parity及界点、手算weights/q10、零成员树/未知质量、正路径矛盾、两池成熟/未来毒化、train medians/support不变、配对固定父bundle、完整tick洞/成本、UNKNOWN拒买归因、正常未结算、partial不能重fit。真实fit与prepare另记录，不把mock行为当业务结果；不跑整QE、UI或其它模块套件。

## Risks / Rollout / Rollback / Production Gates

诚实时间分区降低结构训练样本且可能有regime漂移；联合分布不保证更精准，partitions按terminal分裂不保证path分位最佳，不能结果后换partitions目标。现有来源非vintage、单父候选流、已消费窗、开放研究系列多重尝试都保留限制，不能拿探索性正值激活或关闭其它方向。leaf重复不是独立样本、赢率不等于期望、低价假设不是实际挂单fill。

完整离线切片backend_restart_required=false；数据库/依赖/profile/模型/角色启用/进程控制NOOP；回滚只停止新离线调用，旧源和产物不变。旧NOT_CONFIGURED不自动配置，运行身份与经济结果分开。DESIGN-COMPLIANCE-001：完整实装所有八项才能源码交付，不简化fake success；原目标/未知语义/模块边界符合最新用户要求，不添加QE资格门或UI阻塞。

## Review / 多轮自审

第一轮业务：控制不是回选旧33维模型，而是冻结父39维candidate，隔离整个新学习器；预算只有一个新forest fit、不复跑失败candidate，成本/风险/五TD不变，若研究负源码仍可交付。

第二轮PIT/数学：估计池标签不用于分裂，结构标签H严格早于估计池开始，处理5TD跨边界重叠；leaf walk float32 parity、同成对样本权重、q10的CDF定义与无质量未知固定；未把honesty声称独立OOS或causal uplift。

第三轮性能/授权：查询分块≤128、原7720上界和固定128树，完整tick但不重复读源/重建工作树；补齐新UNKNOWN_MODEL_DISTRIBUTION在价集与评估中的统一未知分类，避免复用旧单一状态判断导致伪已知拒绝。源码只六Advisory文件，旧已拟合implementation与产物身份不动、公共模块和生产操作NOOP。以上是本窗口自审，不冒称独立外审。

源码第一轮：模型5项定向通过，原生apply与非执行JSON leaf parity在真实fixture fit中校验；train/estimation标签成熟分离、未来test Inf及非法日期不解析、frozen control训练KEY一致。编排两个fixture错误分别是误把publish_stage返回Path当dict、误期望ValueError而正式合同抛AdvisoryModelFirstError；只修本测试，失败节点逐项复测通过，未改公共stage或错误合同。

源码第二轮：按模型数学/分布完整性修订持久化schema/quantile/正成对样本与median校验、CDF浮点边界、概率仅舍入clip及权重集中度解释，成本各一次与完整tick洞2项定向通过；无质量返回UNKNOWN、不计已知AVOID。第三轮：确认原始date/Top5/未结算null、拒买贡献与UNKNOWN贡献严格对账、不补Top6/不复利成NAV；stage只读父prepared/trained、partial journal不可再fit，QE忙不创建fit_attempt。最终小矩阵/Ruff/L0/F2与实际研究另报告。该完整离线切片不含API/UI挂载，不宣称经济激活；本轮无额外研究fit。
