# Advisory P25：跨包训练人口迁移的五交易日收益型价格模型 F2详细设计

> 版本：v1.5；日期：2026-10-08；状态：P25设计已合入，P26两步输入准备完成/PREPARED_IDENTIFIABLE_NO_FIT；PR #5778/#5780分别已合入850f83ac/10e86ed9并完成自身清理。P27独立19维模型内核已实现并定向验证，源码未交付；研究控制/真实两fit及P28～P29仍未完成。
> 上位依据：`advisory_strategy_conditioned_model_blueprint_v1_20260710.md` §6.3.4、§16.0 P25～P29。P25 PR #5771已合入268f2e0d并完成自身清理；P26完成共同九字段、原5TD标签、purge、共享编码及真实只读准备/读回，本轮仅开发P27模型内核。未读取候选score、旧收益或sealed金融值；此前开发窗的价格/监督标签已读取，本轮仅合成fixture拟合/查询测试，未评价真实新模型收益，新研究fit=0、盈利确认与生产启用=0。

## Background / Goal

当前没有经过确认、可替代原基线的Advisory盈利型Entry/Exit版本。最新GP5-MONEYFLOW-INFO-1已完成并停止：共同80个成熟5TD组的base/rule/GP5/candidate均值135.3458/132.8314/59.1348/44.9299bps；candidate对base−90.4159bps，对GP5另81配对−14.0295bps、CI跨0。累计131实际研究fit＋1旧INDEX_BUILD，不是131个独立假设。基线在该窗口为正，不支持“全部QE包没有Alpha”或“价格预测必然无效”的结论。本设计不复跑旧资金、joint或Exit候选，也不为其补证/归档。

唯一新假设为 **GP5-POPULATION-TRANSFER-1**：在相同日频信息、价格坐标、学习器、时钟、成本、买入规则和评价人口下，增加来自其他已交付包的既存冻结候选监督，是否改善五日价格价值预测，并减少无净价值的拒买？唯一处理变量是`TRAINING_POPULATION_BREADTH`，不是新增loss、资金/分钟字段、seed或阈值。两臂都重新训练同一个简单模型；旧39维joint和GBDT仅保留历史结论，不能当成公平控制或以不同旧窗口均值计算新增量。

目标合同固定`RISK_MANAGED_ADVISORY`。模型输出相对D参考价的五日终值/路径联合经验分布，转成合法价格情景下的成本后收益、盈利概率与尾部不利波动；不预测开盘落在某个区间的概率来替代经济目标。允许0～5只/空槽，不能把胜率、留现金或少买本身当收益成功。原Top5、固定简单规则是不可更换的业务对照；`ALPHA_RANKING`不是本次合同。

方法复用森林邻域条件分布思想：[Quantile Regression Forests原论文](https://www.jmlr.org/papers/v7/meinshausen06a.html)；参数/叶子接口依据[RandomForestRegressor官方文档](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestRegressor.html)。时间分离的结构/估计池及成对终值/路径估计是本地明确改造，不声称原论文原样实现、因果识别或盈利保证。不安装依赖，实际已有库版本随未来新产物记录。

## Scope / Non-goals

P25设计提交只改本文和上位蓝图，未修改源码/数据/权重/运行态。P26第一步范围为下列contracts、population、population测试及两文档；第二步扩展该范围内contracts/population，新增pipeline/CLI和pipeline测试来准备共同输入。本轮P27内核仅新增model、model测试及更新两文档；P27研究控制/pipeline/CLI扩展、evaluation仍是后续计划，未实现。独立树基于最新合入main `10e86ed9453b41aa4faedf60166960af81bec35b`，完整计划范围为：

- `backend/services/advisory_model_first/generic_population_price_5td_contracts_v1.py`
- `backend/services/advisory_model_first/generic_population_price_5td_population_v1.py`
- `backend/services/advisory_model_first/generic_population_price_5td_model_v1.py`
- `backend/services/advisory_model_first/generic_population_price_5td_evaluation_v1.py`
- `backend/services/advisory_model_first/generic_population_price_5td_pipeline_v1.py`
- `backend/services/advisory_model_first/generic_population_price_5td_cli_v1.py`
- `backend/tests/advisory_model_first/test_generic_population_price_5td_population_v1.py`
- `backend/tests/advisory_model_first/test_generic_population_price_5td_model_v1.py`
- `backend/tests/advisory_model_first/test_generic_population_price_5td_pipeline_v1.py`
- 本文与上位蓝图。

不改QE、Selection、StrategyPackage、HMM、公共数据、Paper/Execution、CI/workflow或AGENTS。合格QE包直接消费，不重新审核Alpha资格、不重跑Selection、不新组合、不提交QE训练。股票池支持stock_universe、单指数、指数并集的既有身份；股票池影响输入人口，不作为本次模型特征/包准入条件。没有现成冻结候选的包记录精确可产缺口，不以重选或新QE实验补齐。

首版仅买价价值，不同时实现卖价、Top20重排、动态仓位/资金权重或分钟执行。输出价格场景不等于当天最佳分钟买点、挂单成交或卖出择时；未来执行消费者属于其所有者。UI、未配置的旧API、旧Exit Draft与自然20日等待均不阻断离线主线。

所有临时/cache/pytest在`X:/AIstock_temp/advisory-p25-design-20261008`；正式root为`F:/Dev/AIstock_model_artifacts/advisory_population_transfer_price_5td_v1_20261008`。禁止C临时目录、DB/DDL/DML/安装、profile/模型激活及任何进程控制。未来fit在既有WSL/worker环境运行，不在Windows backend拟合，不修改或安装worker基础设施；backend_restart_required=false，用户仍拥有重启权。

## Phase 0：源码与文档发现 / Allowed APIs

Phase 0源码检查基线为main `7acdc7268338d2d8cb7c178db167ba6314ef5c8b`；交付前已同步更新main `9092dd4ed8753bec8150c3818357ff82b62450ec`，新增只有其它模块文档，不改变下列Advisory接口。下列是已存在、只读或纯计算接口；新population函数/CLI均在上面的新增文件内设计实现，不能伪称现有API已支持本计划。

| 现有接口及精确签名 | 源码位置 | 允许用途 / 禁止误用 |
|---|---|---|
| `list_strategy_packages(status=None, limit=100, view="full")`；HTTP `GET /api/v1/strategy-packages?view=summary&limit=500` | `backend/routers/strategy_packages.py` | P26只投影包/manifest/prediction/pool元数据，不按收益选包。summary仓储执行SELECT；不调用generate、retrain或model-state |
| `StrategyPackageSelectionArtifactService.list_artifacts(package_id, *, limit=100)`；HTTP `GET /api/v1/strategy-packages/{package_id}/selection-artifacts?limit=500` | `backend/services/strategy_package/selection_artifact.py` | 发现既存artifact，不能生成/改写。artifact存在不等于完整原候选名单；当前manifest筛选遗漏旧版本时明确使用已绑定历史文件，不回填当前身份 |
| `build_generic_daily_price_input_v1(*, candidates, calendar, panel, benchmark_daily, market_state, source_context)` | `backend/services/advisory_model_first/generic_daily_price_input_v1.py` | 单包单D、最多50原项、20原D会话＋立即下一T，纯计算九字段；正常缺失保留 |
| `validate_roster(candidates, decision_dates, calendar)`；`build_generic_price_5td_labels_v1(*, candidates, decision_dates, calendar, prices, references, source_context)` | `backend/services/advisory_model_first/generic_price_5td_labels_v1.py` | 同原5TD名义执行合同，每调用最多7720原项；按完整日期组分块，不能改旧上界或把全包KEY当原单包KEY |
| `feature_values(features)`；`matrix(features, medians, *, gaps=None)` | `backend/services/advisory_model_first/generic_price_5td_models_v1.py` | 9原值＋9缺失旗＋gap/100，纯19维编码；不调用其四GBDT头train，不能借它读取test或拟合校准 |
| `file_sha256(path)`；`read_stage(path, *, stage, plan_sha256, parent_sha256)`；`publish_stage(*, study_root, stage, plan_sha256, parent_sha256, artifacts)` | `backend/services/advisory_model_first/economic_entry_pipeline.py` | 既有最小原子stage和引用校验，不新建平台/归档系统 |
| `EvidenceReferenceV1(role, artifact_uri, sha256, size_bytes)`；`build_trial_record(**values)`；`AdvisoryResearchTrialRegistryV1(root).append_batch(records)` | `research_control_contracts.py` / `research_control.py`（均在Advisory目录） | 复用既有枚举、只追加JSONL；不能改objective或用ORACLE类型藏实际模型fit |
| `train_joint_price_5td_v1(*, rows, frozen_control, configuration, before_fit)` / `query_joint_price_nodes_v1(*, fitted, features, scenario_gap_bps)` | `generic_joint_distribution_price_5td_model_v1.py`（Advisory目录） | **仅作为算法参考，不调用**：现有JSON叶子/验证器硬编码39维且绑定旧控制；新19维模型独立身份，不修改旧模型或假装兼容 |

现有generic配置要求train/validation/test三段且旧pipeline绑定单父人口；本计划使用新的独立配置，不能拼一个假test窗口满足旧构造器。源码发现只证明接口与限制存在，不证明跨包历史候选、原生身份或新holdout已齐备。P25本轮未调用业务API、未解码金融parquet、未进行数据准备。

## Architecture / Contracts

### 1. 经济estimand、价格单位与三时钟

原候选D为决策as-of，T为D后立即下一原交易日；H=T+4，是包括入场日在内的第5原交易会话，绝不等于五次有效复评。D输入只能来自D及以前；T观察实际开盘仅用于T情景取值/历史监督，T..H路径和H终值仅用于已成熟标签/结算，不进入更早预测。

按现有`GENERIC_ENTRY_FIXED_5TD_V1`政策：reference为D原始收盘CNY，历史T..H价格乘`adj_factor(day)/adj_factor(D)`转D锚坐标。定义`g=10000*(T_open_D_anchor/reference−1)`、`Y=H_close_D_anchor/reference`、`L=min(T..H low_D_anchor)/reference`；有效标签满足`0<L<=Y`。买成本0.95bps、卖成本5.95bps各一次，固定下行风险800bps，不结果后调整。

给定价格坐标`a=1+g/10000>0`，单个估计样本的五日净收益为`R5=10000*(Y*(1−5.95/10000)/(a*(1+0.95/10000))−1)`；路径风险为`10000*max(0,1−L/a)`。对实际T开盘，`TAKE`相对原名义入场的增量为0，`SKIP`相对该动作的增量为`−R5`；现金槽收益为0，不形成动态资金仓位。候选−matched的增量按两者实际动作差计算，不以均方误差或胜率代替。

D发布的是一组带条件说明的假设价格价值，不预测未来g，也不能将任意低价视为可成交的干预。T只取实际开盘在冻结曲面上的节点，不重新fit/选阈值；一个stock/date只有一个observed g监督，扫上千tick不增加样本量。模型可对假设g作条件关联查询，但没有证明未成交limit订单、盘中碰价或最优成交点的反事实收益。

价集单位严格为D_ANCHORED_CNY。T实际原价转换需要T当时可见的复权桥；D若没有当时可见的T原价/合法tick桥，不能借未来复权因子伪造D时原价承诺。保留锚定价集及`UNKNOWN_PRICE_BASIS`原因，未来API消费者再明确可见桥。标签允许用未来实现值作监督，不能反流成D特征。来源非vintage的既有限制保持，不升级为原生捕获。

### 2. P26：有界既存人口与唯一监督簇

锚人口为GP5原冻结输入，父plan `advgp5_cca74d0498942676713c68ad`；父文件身份已可定位，但不是新跨包人口PASS。仅复用开发D：train=2024-07-04..2025-05-30，evaluation=2025-06-03..2025-09-30；标签只可消费至evaluation_end，边界未成熟保留。2025-10-09..2026-02-02原GP5 test已经消费，**不能**重命名成新sealed。

P26先读取现有包/冻结文件的身份、列结构和日期KEY，后按冻结日期投影开发金融列。一次研究最多8个package-manifest身份：锚包＋按stock_universe/single_index/index_union类别覆盖、再按package_id/manifest字典序选择的可产包；不按旧/new收益、IC或此次标签选择。完整选择规则及所见catalogue上限进入plan，不能把有界清单叫全库审计。只等待本次对照所需输入，不等所有包、所有池或多个生产bundle。

若有至少两个附加身份，按冻结排序最后一个附加身份保留为`HELD_PACKAGE_EVALUATION`，不进入任一臂训练；其余附加身份为扩展训练来源。只有一个附加身份时可检验人口增加，但没有留包验证，必须标注`UNSEEN_PACKAGE_TRANSFER_NOT_TESTED`，不能宣称跨包泛化完成。留包允许股票曾在训练其他包中出现，故不等于未见股票检验。

每包保留完整原日期、0候选日、至多50原候选、rank、run/list/package/manifest/pool/policy及来源等级。各包`source_policy_hash`可以不同，必须保留；本研究共同`5TD_shadow_policy_sha256`是另一个估值身份，不能覆盖源policy，也不声称复现各包原QE组合绩效。训练用`stock_date_cluster=(D,T,instrument,5TD_policy)`，预测/账本用`roster_key=(package,manifest,run,list,D,T,instrument)`；不得拿跨包重复KEY破坏旧单包唯一性。规范化到同一D价格/份额单位与共同市场宽度定义后，同簇特征/实际g/标签已知值应一致；冲突报告精确列及源身份，不平均、不选较好标签或用当前配置回填。未知项可在有证据的一致规范化源下计算一次，不能伪造缺失的原捕获信息。

探索允许继承既有冻结候选的legacy provenance，原pool/run/list等无法证明的项仍nullable/UNKNOWN并附原文件引用；不重建原生receipt、不填捕获时间、不用当前池成员补历史，也不把这些未知升级为新的包资格阻断。内部原名单键仍须由冻结来源命名空间加原D/T/股票唯一定位；未知身份不能导致跨来源别名合并。实际hash/名单/时钟矛盾是读取错误，明确报告，不以“所有QE包可用”为理由吞掉。留包验证若无法证明不同真实包身份，只能称来源迁移，不冒称跨包结果。

扩展训练按唯一stock/date监督的并集，每簇质量1；锚包内重复/跨包重叠只计一次，原名单和各包评价均不删除。该研究不另做包收益权重、过采样或package embedding；新增唯一样本数、重叠比例、池和日期覆盖必须报告。若附加包只复制锚簇或无成熟可测监督，则`NOT_TESTABLE_POPULATION_CONTRAST`、0fit，而非两次相同训练后声称实验成功；这是对照不可识别，不是QE包准入门，也不阻P25或纯源码开发。

正常停牌、缺D行情/宽度/映射、H未成熟或不可执行均留在原人口、逐字段原因可见。特征可测部分按训练编码处理；无监督行不参与损失，但不从覆盖分母/原名单删除。现有标签遇路径停牌/缺bar保持UNKNOWN，不补0、延长H或改成可成交。按完整单包D组分块调用旧7720标签上界，再在新population层合并；最多200000原名单行、100000唯一训练簇，超过资源上界只报告本study范围不可产，不偷偷采样或缩日期。该上界不是业务股票池容量限制。

金融投影先按允许日期过滤再解码，calendar可只读未来会话元数据但不是未来价格。新population额外保存`maturity_at_cutoff`：H超过声明cutoff为UNSETTLED_CUTOFF，H在cutoff内而缺bar才是正常UNKNOWN；旧标签helper没有cutoff参数，不能向它编造参数或把投影后缺未来bar说成真实数据缺口。边界原项不删除，新输入核明确IMMATURE及cutoff原因，并与成熟性分别携带；H在cutoff内的正常标签状态/原因不改写，评估先按成熟性判null。任何H>train_end的监督都不进入训练/medians的标签统计。

确认窗只登记元数据身份、日期、消费状态和隔离引用，在新模型输出前冻结；若已有一个附加包被留包且其evaluation窗口未消费，该窗口必须另标sealed并从本探索读/评估清单排除，不能借“留包”名义污染它。oracle/探索、模型prepare、预处理和阈值均不读sealed金融值；P25没有承诺一个尚未证明可用/未消费的新时间窗。未发现真实未消费历史窗时明确`CONFIRMATION_WINDOW_UNAVAILABLE`，不妨碍开发探索，也不假装开发正值支持激活。

### 3. 两个公平训练臂与诚实估计

`matched_anchor`只用锚包成熟唯一监督，`candidate_transfer`用锚包＋附加训练包的成熟唯一监督。两臂使用相同9原字段、9缺失旗、g/100（共19维）；包alpha/score/rank/pool身份不入x。九字段与现有generic输入完全相同，不追加资金、分钟或HMM头。rank仅绑定原Top5账本，不作为收益解释变量。

两臂共用锚训练段导出的9 medians和g支持，保证唯一变量不是编码/支持范围。缺失median数值是训练编码，原缺失旗/原因仍保留；某字段全未知用0编码＋缺失旗，不把0说成真实行情。沿用现有`STOCK_FEATURES`六个直接个股字段全未知的点仍UNKNOWN，不改变generic输入的可测语义。g支持复用现有固定算法：锚训练可见观察价的2.5%～97.5%范围、100bps桶、每桶至少30观察/5原D、上边界不包含且保留洞；不扩大支持以救候选。支持只定义查询可估计范围，不评审策略包/股票Alpha资格；两臂使用同一支持内训练监督和完整评价名单。

取锚成熟支持内D排序的中位D作为共同估计池首D。两臂结构池均为该D之前且`label_information_end < estimation_first_D`的各自训练簇；估计池均为其后至train_end、标签已成熟的各自训练簇。训练标签H还必须早于首evaluation D；跨界标签purge但原名单仍保留。附加包不能结果后移动分割点；没有任一臂的结构/估计监督则0fit、精确原因，不寻找另一切点。

两臂均固定RandomForestRegressor：128树、max_depth6、min_samples_leaf30、max_features1.0、bootstrap=True、random_state20261007、n_jobs2、squared_error目标Y；结构池一次fit，估计池标签不用于树分裂。每估计stock/date仅一个成对(Y,L)，每树叶子内等权，再对有成员树平均并归一成同一权重。零成员树不补全局均值，全树无质量为UNKNOWN_MODEL_DISTRIBUTION；重复跨树不是独立观测。权重effective sample size仅浓度诊断，不等于独立时间样本量。

导出独立19维非执行JSON树及估计样本/leaf映射，float32 walk与sklearn.apply逐叶一致；旧39维模型/验证器不修改、不调用。两模型的同方法均值、q10(L)、`P(R5>0)`与路径不利波动来自同一成对估计质量，不是两套边际头或任意price action因果样本。honest估计不保证更好，也不代表新独立OOS。

### 4. 价格节点、校准和动作

在共同支持内，`expected_net_bps>0 AND downside_q90_bps<=800`为ACCEPTABLE，否则已知AVOID；盈利概率仅解释，不新增0.6等强制概率阈值。支持外、全个股未知、无叶子估计质量和价格单位缺口分别UNKNOWN。完整Decimal合法tick网格保留多段、洞、空集合，最多100000节点、每批128点；不能为得到连续区间跨过UNKNOWN，也不能截取最高收益tick声称最佳入场点。

训练medians/support和经验估计池都是train-only。evaluation只报告均值误差、固定概率分桶[0,.2,.4,.6,.8,1]、Brier、q10路径覆盖及KNOWN/UNKNOWN比例；不fit isotonic/Platt、CQR或结果后校准/改阈值。q10应以其尾部事件解释，不设置旧“预测开盘coverage75%”目标。校准失配必须显示，但良好校准/更低误差不代替净收益。

历史实际T_OPEN的ACCEPTABLE→TAKE、AVOID→SKIP、UNKNOWN→独立未知空槽，不补Top6、不按新模型重排原Top5。整日0推荐合法；生产未确认时不使用此研究规则强制拒买，原基线/旧绑定行为不变。暂停/涨停入场不可执行、H跌停/正常缺失未结算按原政策逐行保留，不把模型未知贡献归已知避免亏损。

### 5. 一次研究、统计与终止

一次preregistered→prepared→trained→evaluated，复用既有原子stage/JSONL，不再建调度/档案平台。计划绑定候选清单、全部输入SHA/size、股票池/来源等级、日期/簇、共同编码/support、结构/估计purge、19维schema、policy、源码身份、库版本、资源和预算。`EXPLORATORY_SCREEN / NAVIGATION_ONLY / RISK_MANAGED_ADVISORY`预注册；唯一新假设、两臂**2 physical fits / 2 model trials**，128内部树/臂单列，不算256试验。test原窗口不消费，旧131fit＋1index计数不覆盖。

源码小fixture的fit是测试，不计真实研究；真实fit前写attempt journal，逐fit计账。partial不隐式重试，源码/计量NO_FIT修正保持权重、人口与动作且新输出另存，不覆盖原研究。WSL/worker执行复用现成计算能力，节点可见artifact URI显式记录，不猜worker私有路径或新增公共端点；每fit前后fresh QE single/custom_evo/multi-alpha查询，QE忙时只等待拟合、不停止QE，若查询发现他方新启动则暂停下一fit并记录资源冲突，不控制他方进程；纯只读prepare/回放可并行但内存不足不占满资源。process working set上限8GiB、n_jobs2，单次fit预算30分钟、价集分批；异常只停止自身调用/报告，不控制其他进程。长于30分钟的实验按每半小时检查，不持续轮询。

四臂为candidate_transfer、matched_anchor、original Top5、固定规则`−300<=actual_g_bps<=300`；后两者0fit。四臂用完全相同原名单、五槽、5TD label/cost和开发评价D。锚包是预注册主评价人口；其他训练包、留包人口单列，不能事后挑收益最高包作为主结论。全局摘要先按包/日期聚合再按同一D取包均值，推断以D簇为单位，不能把包×股票/日期复制成独立天数。

历史回放只对每个唯一stock/date查询一个实际开盘g，再映射回各包原名单；不为每个历史episode扫描整张价格网格，不重复读源/构建每日工作树。完整legal tick扫描保留为D价集接口的必要能力和定向测试，不能以历史T取值过程声称D已读到未来开盘。输入列缓存按明确冻结身份复用，不扫描latest模型或活动profile。

单组净收益为五原槽R5之和/5；SKIP及无原项槽为0，TAKE未结算使该臂该组null，未知空槽仍独立列。这是重叠5TD cohort回报，不是资金NAV/年化/MDD。每条candidate−matched和candidate−base使用该配对的共同成熟完整组，另列四臂共同分母；均值可对已知配对描述，但原日期序列内部有null时CI/MDE=null，不压缩洞重排。统计固定原会话block5、bootstrap2000、seed20261008、双侧CI95和MDE80，不结果后换block/seed。

报告TAKE胜率/平均盈利亏损/尾部q10、净组均值、KNOWN改变（全部与已结算分别计数）、干预日/比例、避免亏损−错过盈利，以及UNKNOWN/不可执行/未结算的独立贡献。归因与**同一配对分母**的增量逐槽对账。现有20成熟干预episode/12原D/10%成熟D仅沿用作探索支持提示，不能成为确认门槛或证实稳定；支持不足结果仍可导航。按D可见CSI300过去5日收益正/负/零/UNKNOWN分层，这是滞后趋势代理，不冒称覆盖全部经济regime或已交付HMM。

可继续条件为预先固定的两条成本后均值增量均>0、已知动作贡献>0；尾部风险、缺失与跨包结果同时报告，不能用UNKNOWN空槽改善替代模型价值。CI跨0/MDE不足仅EXPLORATORY_POSITIVE，不支持激活；任何一条不满足为NEGATIVE_OR_INSUFFICIENT，只停止精确candidate，不改loss/seed/support/概率阈值，不补旧证据、不宣布整个业务无效。后续确认还须在未消费窗口前依据开发方差/cluster MDE、实际容量折损与业务最小效应预注册数字，不借本探索提示或0bps符号检验冒充经济激活合同；成本已扣，不二次收费。当前设计不承诺自动进入confirmation/生产。

## Implementation Plan / 按P25～P29顺序

P27先交付完整模型内核，再交付研究控制/执行切片，不以其中一项冒称整个P27或收益完成。新接口限定`train_population_price_5td_v1(*, clusters, encoding, arm, input_plan_sha256, before_fit, after_fit)`、`query_population_price_nodes_v1(*, fitted, features, scenario_gap_bps)`、`population_price_set_5td_v1(*, fitted, d_features, reference_cny, legal_low_cny, legal_high_cny, tick_cny)`及显式非执行JSON读写。两个fit callback保留真实研究登记/QE互斥接口，本轮fixture拟合不是已注册研究。

内核的成对分布来自同一估计样本权重；均值净收益、P(net>0)、q10(L)及q90实际路径不利波动分别计算。离散经验分布使用经验CDF首次达到目标概率的加权分位点（inverted CDF，边界取第一个满足累计质量>=目标概率的样本，不作插值）：`q90(max(0,1-L/a))`直接从风险样本计算，不能在恰好10%原子质量边界上简单以`1-q10(L)/a`替代。此约定在研究fit前固定，两臂一致，不是结果后修改风险阈值。独立19维float32树walk、叶子成对样本索引及Decimal原tick全网格/支持洞都需定向验证；不修改旧39维实现、不调用旧train、不增加新信息或调参。

| 阶段 | 交付与源码依据 | 必须核对 / Anti-pattern |
|---|---|---|
| P25本轮 | 本详细设计＋蓝图链接/状态；上述已有标签/输入/19与39维接口核对 | 多轮设计审核、F2、scope/diff；不把设计PASS写成数据/源码/模型PASS |
| P26第一步 | population/contracts、最小键/日期/schema metadata inventory；复用只读catalogue/历史文件 | 冻结可产包和held身份、同stock/date规范化、原名单与缺失；不读取sealed、不补数据/重选名单、不要求所有包齐备 |
| P26第二步 | train/evaluation开发投影、9字段/原5TD标签、purge/簇/共享编码；只prepare不fit | 同包旧上界分块、H/PIT、物理唯一质量与可识别新簇、正常未知；不能复制同样本虚增训练量 |
| P27 | 新19维model、pipeline/CLI，2固定臂、JSON parity及journal | 同信息同政策/分割/算法、仅训练人口不同；不偷调用39维旧train或改QE，QE空闲才2fit |
| P28 | 新evaluation与同原名单四臂，完整配对归因/区间、跨包/滞后行情分层 | 原D/null/五槽、成本一次、block/cluster、不冒充NAV或激活；负即停止此候选，无默认第二模型 |
| P29条件任务 | 正且有独立确认价值才另设consumer/API/binding scope；同价集字段 | 不自动配置生产，不扩大为分钟执行/资金仓位；UI后置、用户重启独立 |

代码阶段按数据/统计、模型数学、业务/授权三个视角重复审核修复；失败只先重跑对应节点，行为稳定后一次小矩阵/Ruff/changed-file scope/F2/CI。P26可先实现只读prepare，不能把未有新人口的P27拟合当研究进展；可产性失败精确报告owner需求，不修改其他模块。约半日至两日工作量取决于既存候选可产性，不为凑长任务时长开无界模型搜索。

### P26第一步实际进度（2026-10-08历史检查点；当时仅metadata）

已实现`PopulationMetadataRequestV1` / `FrozenPopulationSourceV1`和`prepare_population_metadata_v1(*, request)`：读取前后校验明确文件SHA/size，按开发日期过滤且只解码原键/排名/身份列；追加包的arm/package/manifest与原request及bundle文件描述绑定。原GP5控制严格复用`is_candidate_decision=true AND rank<=20`，不能将父文件的40只/日当新控制；追加包保留其既存Top50。单/并指数等已知pool身份仅原样携带，未证明的run/list/pool/source policy继续nullable，未生成原生receipt。

只读catalogue观察21包/8未退役；当前公开Selection API未覆盖本次开发窗，但既存N2-B r3冻结文件覆盖，故无需重跑QE或Selection。只按身份/文件可产性取锚＋两附加来源，不按收益筛选，不声称全库或所有指数覆盖。三者均保留305个开发D，无缺名单日；实际键核对首次耗时13.286秒，原名单总数36600，跨所有来源唯一股票/日期键36093。统计如下：

| 原冻结来源 | 角色 | 原名单行 | 仅按H日期可能成熟的train行 | evaluation原名单行 | H超过cutoff原行 |
|---|---|---:|---:|---:|---:|
| `pkg_ma_8ec5e389fa2c5e484a1ac7e9` / GP5 Top20 | matched anchor | 6100 | 4280 | 1720 | 100 |
| `pkg_378eb9c91e104c64935404e257e932ee` / 既存Top50 | transfer train | 15250 | 10700 | 4300 | 250 |
| `pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27` / 既存Top50 | held package evaluation | 15250 | 10700（不进入训练） | 4300 | 250 |

锚与追加训练来源重叠18个潜在成熟簇，新增10682，潜在训练并集14962；held原名单完整保留而不计训练。这个数量**不是可训练标签数**：九字段、价格单位、实际可执行/缺bar和AVAILABLE标签尚未读取验证；原GP5的3684可训练数也不能用4280日期成熟数替代。`features_ready=false / labels_ready=false / physical_fit_count=0 / deployable=false`。

原H超过2025-09-30的边界保留`UNSETTLED_CUTOFF`；其余仅为`MATURE_BY_CALENDAR_ONLY`，不能伪称已有监督。原日程缺行区分有证据的EXPLICIT_EMPTY与UNKNOWN_ABSENT_FROZEN_DAY，不默认为0；簇质量1且supervision_ready=false，去重不删除任何包原名单。真实来源均legacy，当前pool/run/list/source-policy缺证项未升级；旧GP5 test及新的sealed金融值均未读取，确认窗未发现仍未承诺可用。

以上为第一步检查点，不代表本文当前断点。该时点下一步是共同特征/标签准备；第二步结果见下。未因metadata合入或日期成熟数自动宣布标签/盈利完成。

### P26第二步实现约定（实施前明确）

既存父daily冻结文件在开发窗口仅覆盖938只股票，不能直接当新增包完整行情来源。新增Advisory-owned `freeze_population_source_v1(*, metadata_request, output_root, connection_context_factory=None, progress=None)`使用既有DB连接合同，在一份REPEATABLE READ只读快照内按月读取所选包候选股票的日线原价/成交量、精确日复权因子、涨跌停、停牌及CSI300指数，SQL逐条30秒；日期限定原calendar首日至2025-09-30，不读旧test或sealed金融值。不修改公共pool、QE源码/profile、基础数据或服务。未知/缺bar留在名单；正常价格0占位仅显式转为未知并记录，不作为真实0收益或默认价，负值/重复键/单位矛盾仍明确报错。

两个臂、全部来源复用同一份明确SHA/size的日频快照，价格`li/1000=CNY`、`hand*100=shares`，同一D精确factor为锚；市场宽度延续原GP5未请求定义的UNKNOWN，不借人口实验新增宽度信号。原calendar不足20原会话的行仍保留冷启动UNKNOWN，D特征不使用T..H价格。不是重建Selection或旧模型收益补证。

新增`prepare_population_inputs_v1(*, metadata_request, source_root, output_root, progress=None)`及纯计算`build_population_inputs_v1`只按开发日期/列投影上述快照；逐原包D复用九字段核，按完整100D块调用原5TD标签核。原H超过cutoff不伪称真实缺bar。共享medians/g支持来自有个股输入且H<=train_end的锚训练时间段；medians不因held/evaluation、标签盈亏或未知状态选择改变，支持沿用固定100bps/30观察/5D算法。监督再取AVAILABLE、成熟、支持内的唯一簇；共同估计首D为锚监督原D中位，结构H严格早于估计首D，任一训练H早于首evaluation D。原名单/日期、缺失原因和各包来源身份仍完整；同簇已知值矛盾不平均或挑好标签。
原H超过cutoff的行明确`label_status=IMMATURE / label_reason=HORIZON_BEYOND_SOURCE_CUTOFF`并保留`UNSETTLED_CUTOFF`成熟状态，不将未到期未来bar算成普通缺数据。对照可识别性按purge后新增STRUCTURE/ESTIMATION质量判断；新增簇若全部被purge，必须0fit/NOT_TESTABLE，不能仅凭purge前新增量启动两次相同训练。

输出包括完整包级rows/days、唯一簇、训练/结构/估计membership、共享19维编码recipe和缺失/成熟/可识别新簇计数，不报告新策略收益或运行模型。复用既有`publish_stage/read_stage`保存在本研究F根下明确的source/inputs组件目录；它们是P27唯一study的输入准备件，不是第二个研究run，也不增加模型trial。每个组件先登记精确recipe再原子发布、已有件只按原manifest读回，不扫描latest/活动profile、不覆盖旧产物。实际P27仍须完整模型源码/参数预登记后才执行唯一2fit；本轮physical_fit_count=0、research_run_created=false，不伪称收益验证或激活。

### P26第二步实际结果（2026-10-08；输入准备，不是模型研究）

一份只读REPEATABLE READ快照共18条SQL/19.585秒查询时间：15个月批量日频、原calendar一致性、停牌、CSI300；不逐候选/逐日反复查询。共同source为`CURRENT_DATABASE_NON_VINTAGE`，零价占位及无原bar停牌记录合计1036条如实标注，不伪造历史vintage/native。原开发305D/36600名单及36093唯一簇全部保留，价格li/1000及成交量hand*100已在同核验证；九字段中市场宽度继续UNKNOWN，不新增信息变量。

| 原来源 | 原行/开发D | AVAILABLE标签 | 不可执行 | 未成熟 | UNKNOWN |
|---|---:|---:|---:|---:|---:|
| GP5_ANCHOR_TOP20 | 6100 / 305 | 5964 | 16 | 100 | 20 |
| PKG_378EB9 | 15250 / 305 | 14877 | 40 | 250 | 83 |
| PKG_5A5CCB（留包） | 15250 / 305 | 14906 | 50 | 250 | 44 |
| 合计 | 36600 / 305 | 35747 | 106 | 600 | 147 |

35747是全部来源/开发窗的可测标签数量，**不是训练量，也不是盈利/可买次数**。共同锚编码域3900簇；两臂均使用同一9原值＋9missing旗＋g/100、medians/support，held/evaluation毒化不改变训练recipe。共同估计首D=2024-12-24，结构池H严格早于该日，所有监督H早于2025-06-03：

| 训练臂 | STRUCTURE | ESTIMATION | PURGED_LABEL_OVERLAP | 实际训练质量 |
|---|---:|---:|---:|---:|
| matched_anchor | 1740 | 1848 | 98 | 3588 |
| candidate_transfer | 5734 | 6217 | 325 | 11951 |

purge前可测新增8590，purge后真正新增训练簇8363；held-only簇不训练，跨包同stock/date质量仍1。状态`PREPARED_IDENTIFIABLE_NO_FIT / features_ready=true / labels_ready=true / physical_fit_count=0 / research_run_created=false / deployable=false`，不证明跨包收益迁移或所有池完整。legacy run/list/pool/source-policy未知、现库非vintage和未有独立确认窗的限制不变。

精确输入root为`F:/Dev/AIstock_model_artifacts/advisory_population_transfer_price_5td_v1_20261008/advgp5popinputs_fc14302167a95eab9972cbdd`，其plan/manifest绑定source `advgp5popsource_f303102e103371a637a60c4d`和原metadata请求SHA `3dc19abedd104af820abe0f8aa50a09cd38e3214a67bfba48f99e98580fd0405`。同root缓存只读读回通过，不重查DB；审核期旧输入组件不覆盖，正式消费仅引用此精确root，不扫latest。多轮修正了发布前候选引用复核、跨cutoff未成熟语义及purge后实际新增质量；输入定向合同测试和真实读回均通过。该输入阶段没有模型fit/JSON leaf parity或经济四臂结果，后续P27内核验证见下，不回写为输入阶段完成证据。

P26两步源码PR #5778/#5780分别合入850f83ac/10e86ed9，后者同步最新main后以新HEAD运行CI通过再合入，两树/分支均经官方流程清理，F正式输入原样保留。下一步P27状态如下，不因输入ready自动配置生产；没有后端重启、DB/DDL/DML/profile或其它模块修改。

### P27模型内核进度（2026-10-08；非真实研究结果）

新增独立19维诚实联合森林：仅STRUCTURE标签参与128树分裂，ESTIMATION成对(Y,L)挂接同一叶子权重；两训练臂共用P26编码/支持/时间切点。原生sklearn.apply与非执行JSON的float32 walk在结构/估计输入逐叶一致。held、evaluation、purged行的特征/结果不解码，标签边界矛盾在fit前报错；两个callback仅是研究控制接入点，不能替代尚未实现的注册/QE互斥/journal。

查询计算同权重净均值、盈利概率、直接风险q90和q10(L)，成本各一次；无叶子质量为UNKNOWN而非全局均值填充。完整Decimal合法tick扫描保留多段价集、UNKNOWN洞和空集合；不改价格/风险阈值，不加入score/rank/包身份特征。JSON仅数据，不持久化可执行pickle。九项定向测试共用合成fixture，覆盖公平两臂、监督时间分离/毒化、叶子联合质量/手算成本、零质量、JSON读回及支持洞；这些单元拟合不属于研究trial，也不能证明经济收益。

当前仅模型内核完成，源码PR/CI/合入另报，真实study未注册、0真实新fit，累计仍131研究fit＋1旧INDEX_BUILD。下一切片为显式冻结当前模型源码/参数、接入既有registry/stage、逐臂fit attempt journal和公开QE互斥，确认WSL/worker可用路径后只执行两fit；随后P28四臂评价。P26组件创建时源码身份保持原值，本轮模型源码另绑定，不以Git合入的换行差异重做输入或覆盖旧manifest。未改旧生产绑定，无需后端重启。

## Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-841 | 单一人口迁移假设；19维两公平臂与原Top5/规则；目标合同不改判 |
| F-842 | 完整跨包名单、单/并指数身份、同stock/date规范化/去重；正常未知及精确不可产原因 |
| F-843 | D/T/H、成本/单位/复权桥、原5TD标签、不可执行/未成熟；价格场景非因果fill |
| F-844 | 共享train-only medians/support，共同分割与实际label end purge，held/test/sealed隔离 |
| F-845 | 同固定forest、独立19维JSON/float32 parity、同成对估计质量、零质量UNKNOWN及预算 |
| F-846 | 完整Decimal tick/支持洞/空集合；概率仅解释、校准只测不选，不预测开盘coverage |
| F-847 | 四臂原五槽/原D、不同配对分母/内生洞null、时间和跨包簇、贡献对账/非NAV |
| F-848 | 2实际fit登记、partial不隐式重试、探索支持/结论边界、exact candidate停止/确认另注册 |
| F-849 | Advisory精确范围、X/F、QE互斥、0数据/DB/进程操作；设计与交付/经济激活分态 |

## Design Acceptance Matrix

设计规格、P26两步输入及P27模型内核分别记录；本轮内核交付不冒充真实研究/经济验收。`DESIGN_SPECIFIED_NOT_IMPLEMENTED`的gap仍为P27研究控制～P29计划内交付。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-841 | 本文Goal/§3/§5；input contracts/独立19维model | target: backend/tests/advisory_model_first/test_generic_population_price_5td_model_v1.py | MODEL_KERNEL_VERIFIED_NO_RESEARCH_FIT | approved_by_user: 按顺序分步开发；两公平臂接口/编码经fixture验证，真实两fit及四臂收益尚未执行 |
| F-842 | generic_population_price_5td_contracts_v1.py / generic_population_price_5td_population_v1.py；本文P26结果 | target: backend/tests/advisory_model_first/test_generic_population_price_5td_population_v1.py；target: backend/tests/advisory_model_first/test_generic_population_price_5td_pipeline_v1.py；真实3来源305D/36600保留 | POPULATION_INPUTS_PREPARED_NO_FIT | approved_by_user: 已批准分步实施；九字段/标签已规范化，legacy pool/run/list未知不伪补，不宣称全库/所有池或收益泛化PASS |
| F-843 | 本文§1～2；population/原5TD标签核 | target: backend/tests/advisory_model_first/test_generic_population_price_5td_pipeline_v1.py；真实cutoff/原H及单位读回 | INPUT_LABEL_CLOCK_VERIFIED_NO_FIT | approved_by_user: 600未成熟保留、106不可执行/147未知不删除；不是native或成交证明 |
| F-844 | 本文§2～3；population共享编码/结构估计池；model监督池选择 | target: backend/tests/advisory_model_first/test_generic_population_price_5td_pipeline_v1.py；target: backend/tests/advisory_model_first/test_generic_population_price_5td_model_v1.py；毒化/purge | TRAIN_ISOLATION_KERNEL_VERIFIED_NO_RESEARCH_FIT | approved_by_user: 共同19维/首D2024-12-24/实际新增8363；旧test已消费不可sealed，新确认窗未证明可用，真实study仍待执行 |
| F-845 | 本文§3；generic_population_price_5td_model_v1.py，旧joint仅参考 | target: backend/tests/advisory_model_first/test_generic_population_price_5td_model_v1.py；19维原生/JSON parity和手算成对质量 | MODEL_KERNEL_VERIFIED_NO_RESEARCH_FIT | approved_by_user: 新19维内核已实现/验证；真实2fit、跨节点运行预算和stage验证仍属下一切片 |
| F-846 | 本文§4；generic_population_price_5td_model_v1.py | target: backend/tests/advisory_model_first/test_generic_population_price_5td_model_v1.py；Decimal全网格/支持洞/空集/概率解释 | PRICE_SET_KERNEL_VERIFIED_ONLY | approved_by_user: 完整价集/概率接口已实现；真实校准与收益P28未执行，不是执行/盈利承诺 |
| F-847 | 本文§5；拟新增evaluation/pipeline | artifact: docs/architecture/advisory_population_transfer_price_5td_v1_f2_design_20261008.md；target: backend/tests/advisory_model_first/test_generic_population_price_5td_pipeline_v1.py | DESIGN_SPECIFIED_NOT_IMPLEMENTED | approved_by_user: 真实回放未运行，不将旧结果当新matched |
| F-848 | 本文§5；既有registry/stage＋拟新增pipeline | artifact: docs/architecture/advisory_population_transfer_price_5td_v1_f2_design_20261008.md；target: backend/tests/advisory_model_first/test_generic_population_price_5td_pipeline_v1.py | DESIGN_SPECIFIED_NOT_IMPLEMENTED | approved_by_user: 新study未注册/拟合，131实际研究fit＋1旧index不变 |
| F-849 | 本文Scope/实施/生产边界；population/pipeline/CLI及本轮纯模型内核 | target: backend/tests/advisory_model_first/test_generic_population_price_5td_model_v1.py；本轮一源码/一测试/两文档scope | MODEL_KERNEL_BOUNDARIES_VERIFIED_ONLY | approved_by_user: X临时/F旧输入保留、0新研究fit/0其它模块/DB/服务变更；研究fit互斥及模型stage仍计划，不宣称生产或模型经济验收 |

最小测试按合同共享少量fixture，不做实现快照/重复场景：两包重叠stock/date只一份质量且原名单完整；不同池/manifest/价格单位冲突与历史未知；5原会话正常停牌、H跨界purge、validation/test poison不改变训练；新19维JSON与原生apply parity及手算叶子联合权重/无质量未知；Decimal合法边界与支持洞/成本一次；四臂各配对不同分母、UNKNOWN贡献和未结算null；QE忙/partial禁止隐式fit与stage输入hash变化。真实prepare/fit/经济读回另报，不用fixture取代业务结果。

## Risks / Rollout / Rollback / Production Gates

跨包共享大量股票/日期可能几乎没有新监督；不同冻结来源、复权/宽度定义可能无法一致规范化；更多人口也可能引入有害domain shift。共同锚support固定，故本研究不检验新价支持/极端gap能力；只检验选定既存人口，不证明所有策略包/指数或未来行情泛化。九字段可能仍不足，诚实分割减少样本，森林以Y分裂不保证L尾部最优，探索窗已消费且研究族多重尝试不消失。阴性只否定精确候选，不回到同数据换loss救结果。

本离线代码后续可交付但不依赖经济为正；运行或数据不可产时不伪装整体完成。设计/源码merge、prepare、fit、探索评价、独立确认、DB/API消费与生产启用分别报告。production/database/runtime activation/依赖/profile/服务操作均NOOP；回滚仅停止新离线调用，旧生产绑定与全部旧模型/实验原样保留。未消费确认不存在时不等待自然20日来阻开发，但不虚构激活证据。

## Review / 多轮审核修订

本节记录同一窗口的不同视角自审，不冒称独立外审。

1. 业务/可实现轮：修订各包source policy与共同5TD shadow policy的双身份；补旧标签helper没有cutoff参数、成熟性单列，避免边界投影缺bar冒充真实数据缺失；历史回放仅唯一stock/date实际g查询，不逐项全网格扫描。
2. 科学/公平性轮：固定锚主评价、19维同算法两臂/同support/同时间切点，旧39维模型不能当matched；去重只影响训练质量不删原名单，未见包与未见股票不混淆；补留包窗口可能同时sealed时排除探索读取，已消费GP5 test不能改名sealed，原始内生洞CI=null不压缩。
3. 交付/边界轮：独立九项索引、两文档scope、2fit只是未来预算；新配置不伪造旧test、39维API不假调用、节点URI不得猜；0QE/数据/DB/服务修改、UI/旧失败证据不前置。新F2初检9/9、0warnings通过，蓝图初检将外部索引末项误作本表验收项，已改为九项独立索引说明，不复制制造完成证据。最终独立设计9/9、蓝图148/148、均0warnings及两文档scope/`git diff --check`通过；PR/CI/merge状态以本次实际交付读回为准。

DESIGN-COMPLIANCE-001逐项：P25完整设计及已批准P26两步输入准备单独交付，不宣称P27模型/经济完成；UNKNOWN/身份不可证/未成熟/统计null及缺口可见，不伪成功；原基线/价格政策/名单/旧实验与模块边界不改；没有新增包资格审批、自然等待或平台工程。确认数字仍须真实开发方差/容量合同，不以探索性支持提示当激活门禁。

P27内核多轮同窗口自审（非独立外审）：第一轮实现/回读修复DatetimeIndex与Series比较接口不一致，失败节点先定向复验；第二轮数学/业务核对修正经验分位点零质量端点，固定直接风险q90而非变换q10(L)，增加手算成本/成对权重/整森林零质量及全价集已知AVOID验证；第三轮JSON/时钟/交付核对明确数值类型、原日期、完整叶子样本质量和只读非执行身份，保持P27研究控制/P28经济条款未完成，不用单元fit夸大研究计数。最终内核9项定向测试/0 warnings、Ruff、四文件ownership与scope/diff均通过，独立F2 9/9、蓝图148/148、均0 warnings。内核接口完整交付，整个P27仍未完成；不会把未接入研究控制的函数直接当作正式实验入口。固定日期只属于这一Advisory研究版本，既不设置QE全局历史窗，也不影响其它实验。

P26第一步多轮自审：第一轮业务/键身份检查保持原Top20、留包不训练、去重只改监督质量；7项直接合同测试通过，Ruff发现lambda写法已修。第二轮时钟/缺失检查补五原会话H/cutoff测试及日期成熟不等于标签可用，指数身份原样携带，不把held来源错误称未见股票或原生身份。第三轮范围/状态检查添加request/policy/窗口身份，明确metadata不是训练plan或原生receipt；仅本阶段八项合同测试和真实只读名单核对，不声称独立外审/经济验收。最终验证与源码PR状态以实际交付读回为准；后续未实施条款仍保留明确gap。
