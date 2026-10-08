# Advisory P25：跨包训练人口迁移的五交易日收益型价格模型 F2详细设计

> 版本：v1.0；日期：2026-10-08；设计状态：DESIGN_SPECIFIED_PLAN_ONLY，源码与数据实施待P26～P29。
> 上位依据：`advisory_strategy_conditioned_model_blueprint_v1_20260710.md` §6.3.4、§16.0 P25～P29。本文只交付详细设计；P26人口尚未准备、源码未实现、新研究fit=0、盈利确认与生产启用=0。

## Background / Goal

当前没有经过确认、可替代原基线的Advisory盈利型Entry/Exit版本。最新GP5-MONEYFLOW-INFO-1已完成并停止：共同80个成熟5TD组的base/rule/GP5/candidate均值135.3458/132.8314/59.1348/44.9299bps；candidate对base−90.4159bps，对GP5另81配对−14.0295bps、CI跨0。累计131实际研究fit＋1旧INDEX_BUILD，不是131个独立假设。基线在该窗口为正，不支持“全部QE包没有Alpha”或“价格预测必然无效”的结论。本设计不复跑旧资金、joint或Exit候选，也不为其补证/归档。

唯一新假设为 **GP5-POPULATION-TRANSFER-1**：在相同日频信息、价格坐标、学习器、时钟、成本、买入规则和评价人口下，增加来自其他已交付包的既存冻结候选监督，是否改善五日价格价值预测，并减少无净价值的拒买？唯一处理变量是`TRAINING_POPULATION_BREADTH`，不是新增loss、资金/分钟字段、seed或阈值。两臂都重新训练同一个简单模型；旧39维joint和GBDT仅保留历史结论，不能当成公平控制或以不同旧窗口均值计算新增量。

目标合同固定`RISK_MANAGED_ADVISORY`。模型输出相对D参考价的五日终值/路径联合经验分布，转成合法价格情景下的成本后收益、盈利概率与尾部不利波动；不预测开盘落在某个区间的概率来替代经济目标。允许0～5只/空槽，不能把胜率、留现金或少买本身当收益成功。原Top5、固定简单规则是不可更换的业务对照；`ALPHA_RANKING`不是本次合同。

方法复用森林邻域条件分布思想：[Quantile Regression Forests原论文](https://www.jmlr.org/papers/v7/meinshausen06a.html)；参数/叶子接口依据[RandomForestRegressor官方文档](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestRegressor.html)。时间分离的结构/估计池及成对终值/路径估计是本地明确改造，不声称原论文原样实现、因果识别或盈利保证。不安装依赖，实际已有库版本随未来新产物记录。

## Scope / Non-goals

本设计提交只改本文和上位蓝图；禁止任何业务源码、数据、权重或运行态修改。后续源码必须在最新main独立树内，精确范围为：

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

所有临时/cache/pytest在`X:/AIstock_temp/advisory-p25-design-20261008`；未来正式root为`F:/Dev/AIstock_model_artifacts/advisory_population_transfer_price_5td_v1_20261008`。禁止C临时目录、DB/DDL/DML/安装、profile/模型激活及任何进程控制。未来fit在既有WSL/worker环境运行，不在Windows backend拟合，不修改或安装worker基础设施；backend_restart_required=false，用户仍拥有重启权。

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

金融投影先按允许日期过滤再解码，calendar可只读未来会话元数据但不是未来价格。新population额外保存`maturity_at_cutoff`：H超过声明cutoff为UNSETTLED_CUTOFF，H在cutoff内而缺bar才是正常UNKNOWN；旧标签helper没有cutoff参数，不能向它编造参数或把投影后缺未来bar说成真实数据缺口。边界原项不删除，原label状态/原因与新成熟性分别保留，评估先用该成熟性判null。任何H>train_end的监督都不进入训练/medians的标签统计。

确认窗只登记元数据身份、日期、消费状态和隔离引用，在新模型输出前冻结；若已有一个附加包被留包且其evaluation窗口未消费，该窗口必须另标sealed并从本探索读/评估清单排除，不能借“留包”名义污染它。oracle/探索、模型prepare、预处理和阈值均不读sealed金融值；P25没有承诺一个尚未证明可用/未消费的新时间窗。未发现真实未消费历史窗时明确`CONFIRMATION_WINDOW_UNAVAILABLE`，不妨碍开发探索，也不假装开发正值支持激活。

### 3. 两个公平训练臂与诚实估计

`matched_anchor`只用锚包成熟唯一监督，`candidate_transfer`用锚包＋附加训练包的成熟唯一监督。两臂使用相同9原字段、9缺失旗、g/100（共19维）；包alpha/score/rank/pool身份不入x。九字段与现有generic输入完全相同，不追加资金、分钟或HMM头。rank仅绑定原Top5账本，不作为收益解释变量。

两臂共用锚训练段导出的9 medians和g支持，保证唯一变量不是编码/支持范围。缺失median数值是训练编码，原缺失旗/原因仍保留；某字段全未知用0编码＋缺失旗，不把0说成真实行情。个股七字段全未知的点仍UNKNOWN。g支持复用现有固定算法：锚训练可见观察价的2.5%～97.5%范围、100bps桶、每桶至少30观察/5原D、上边界不包含且保留洞；不扩大支持以救候选。支持只定义查询可估计范围，不评审策略包/股票Alpha资格；两臂使用同一支持内训练监督和完整评价名单。

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

| 阶段 | 交付与源码依据 | 必须核对 / Anti-pattern |
|---|---|---|
| P25本轮 | 本详细设计＋蓝图链接/状态；上述已有标签/输入/19与39维接口核对 | 多轮设计审核、F2、scope/diff；不把设计PASS写成数据/源码/模型PASS |
| P26第一步 | population/contracts、最小键/日期/schema metadata inventory；复用只读catalogue/历史文件 | 冻结可产包和held身份、同stock/date规范化、原名单与缺失；不读取sealed、不补数据/重选名单、不要求所有包齐备 |
| P26第二步 | train/evaluation开发投影、9字段/原5TD标签、purge/簇/共享编码；只prepare不fit | 同包旧上界分块、H/PIT、物理唯一质量与可识别新簇、正常未知；不能复制同样本虚增训练量 |
| P27 | 新19维model、pipeline/CLI，2固定臂、JSON parity及journal | 同信息同政策/分割/算法、仅训练人口不同；不偷调用39维旧train或改QE，QE空闲才2fit |
| P28 | 新evaluation与同原名单四臂，完整配对归因/区间、跨包/滞后行情分层 | 原D/null/五槽、成本一次、block/cluster、不冒充NAV或激活；负即停止此候选，无默认第二模型 |
| P29条件任务 | 正且有独立确认价值才另设consumer/API/binding scope；同价集字段 | 不自动配置生产，不扩大为分钟执行/资金仓位；UI后置、用户重启独立 |

代码阶段按数据/统计、模型数学、业务/授权三个视角重复审核修复；失败只先重跑对应节点，行为稳定后一次小矩阵/Ruff/changed-file scope/F2/CI。P26可先实现只读prepare，不能把未有新人口的P27拟合当研究进展；可产性失败精确报告owner需求，不修改其他模块。约半日至两日工作量取决于既存候选可产性，不为凑长任务时长开无界模型搜索。

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

当前行验收的是设计规格及源码发现，不是尚不存在的源码或真实实验。`DESIGN_SPECIFIED_NOT_IMPLEMENTED`的gap是P26～P29计划内交付；不得对外声称这些功能已完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-841 | 本文Goal/§3/§5；拟新增contracts/model | artifact: docs/architecture/advisory_population_transfer_price_5td_v1_f2_design_20261008.md；target: backend/tests/advisory_model_first/test_generic_population_price_5td_model_v1.py | DESIGN_SPECIFIED_NOT_IMPLEMENTED | approved_by_user: 本轮仅P25详细设计，P27代码/2fit未执行 |
| F-842 | 本文§2；拟新增population | artifact: docs/architecture/advisory_population_transfer_price_5td_v1_f2_design_20261008.md；target: backend/tests/advisory_model_first/test_generic_population_price_5td_population_v1.py | DESIGN_SPECIFIED_NOT_IMPLEMENTED | approved_by_user: 可产清单/索引池身份/留包仍待P26，未宣称新人口PASS |
| F-843 | 本文§1～2；既有generic_price_5td_labels_v1.py | artifact: docs/architecture/advisory_population_transfer_price_5td_v1_f2_design_20261008.md；target: backend/tests/advisory_model_first/test_generic_population_price_5td_population_v1.py | DESIGN_SPECIFIED_NOT_IMPLEMENTED | approved_by_user: 已有标签接口可复用，新跨包投影和单位读回未实施 |
| F-844 | 本文§2～3；拟新增contracts/model/population | artifact: docs/architecture/advisory_population_transfer_price_5td_v1_f2_design_20261008.md；target: backend/tests/advisory_model_first/test_generic_population_price_5td_model_v1.py | DESIGN_SPECIFIED_NOT_IMPLEMENTED | approved_by_user: 原test已消费不可sealed，未证明新确认窗可用 |
| F-845 | 本文§3；拟新增model，旧joint仅参考 | artifact: docs/architecture/advisory_population_transfer_price_5td_v1_f2_design_20261008.md；target: backend/tests/advisory_model_first/test_generic_population_price_5td_model_v1.py | DESIGN_SPECIFIED_NOT_IMPLEMENTED | approved_by_user: 新19维实现/叶子parity/2fit尚未完成 |
| F-846 | 本文§4；拟新增model | artifact: docs/architecture/advisory_population_transfer_price_5td_v1_f2_design_20261008.md；target: backend/tests/advisory_model_first/test_generic_population_price_5td_model_v1.py | DESIGN_SPECIFIED_NOT_IMPLEMENTED | approved_by_user: 完整曲面与校准是计划，不是模型收益/执行承诺 |
| F-847 | 本文§5；拟新增evaluation/pipeline | artifact: docs/architecture/advisory_population_transfer_price_5td_v1_f2_design_20261008.md；target: backend/tests/advisory_model_first/test_generic_population_price_5td_pipeline_v1.py | DESIGN_SPECIFIED_NOT_IMPLEMENTED | approved_by_user: 真实回放未运行，不将旧结果当新matched |
| F-848 | 本文§5；既有registry/stage＋拟新增pipeline | artifact: docs/architecture/advisory_population_transfer_price_5td_v1_f2_design_20261008.md；target: backend/tests/advisory_model_first/test_generic_population_price_5td_pipeline_v1.py | DESIGN_SPECIFIED_NOT_IMPLEMENTED | approved_by_user: 新study未注册/拟合，131实际研究fit＋1旧index不变 |
| F-849 | 本文Scope/实施/生产边界 | artifact: docs/architecture/advisory_population_transfer_price_5td_v1_f2_design_20261008.md；本轮changed-file只两文档 | DESIGN_SPECIFIED_NOT_IMPLEMENTED | approved_by_user: 文档交付不授权其它模块/服务/数据库操作 |

最小测试按合同共享少量fixture，不做实现快照/重复场景：两包重叠stock/date只一份质量且原名单完整；不同池/manifest/价格单位冲突与历史未知；5原会话正常停牌、H跨界purge、validation/test poison不改变训练；新19维JSON与原生apply parity及手算叶子联合权重/无质量未知；Decimal合法边界与支持洞/成本一次；四臂各配对不同分母、UNKNOWN贡献和未结算null；QE忙/partial禁止隐式fit与stage输入hash变化。真实prepare/fit/经济读回另报，不用fixture取代业务结果。

## Risks / Rollout / Rollback / Production Gates

跨包共享大量股票/日期可能几乎没有新监督；不同冻结来源、复权/宽度定义可能无法一致规范化；更多人口也可能引入有害domain shift。共同锚support固定，故本研究不检验新价支持/极端gap能力；只检验选定既存人口，不证明所有策略包/指数或未来行情泛化。九字段可能仍不足，诚实分割减少样本，森林以Y分裂不保证L尾部最优，探索窗已消费且研究族多重尝试不消失。阴性只否定精确候选，不回到同数据换loss救结果。

本离线代码后续可交付但不依赖经济为正；运行或数据不可产时不伪装整体完成。设计/源码merge、prepare、fit、探索评价、独立确认、DB/API消费与生产启用分别报告。production/database/runtime activation/依赖/profile/服务操作均NOOP；回滚仅停止新离线调用，旧生产绑定与全部旧模型/实验原样保留。未消费确认不存在时不等待自然20日来阻开发，但不虚构激活证据。

## Review / 多轮审核修订

本节记录同一窗口的不同视角自审，不冒称独立外审。

1. 业务/可实现轮：修订各包source policy与共同5TD shadow policy的双身份；补旧标签helper没有cutoff参数、成熟性单列，避免边界投影缺bar冒充真实数据缺失；历史回放仅唯一stock/date实际g查询，不逐项全网格扫描。
2. 科学/公平性轮：固定锚主评价、19维同算法两臂/同support/同时间切点，旧39维模型不能当matched；去重只影响训练质量不删原名单，未见包与未见股票不混淆；补留包窗口可能同时sealed时排除探索读取，已消费GP5 test不能改名sealed，原始内生洞CI=null不压缩。
3. 交付/边界轮：独立九项索引、两文档scope、2fit只是未来预算；新配置不伪造旧test、39维API不假调用、节点URI不得猜；0QE/数据/DB/服务修改、UI/旧失败证据不前置。新F2初检9/9、0warnings通过，蓝图初检将外部索引末项误作本表验收项，已改为九项独立索引说明，不复制制造完成证据。最终独立设计9/9、蓝图148/148、均0warnings及两文档scope/`git diff --check`通过；PR/CI/merge状态以本次实际交付读回为准。

DESIGN-COMPLIANCE-001逐项：仅完整P25设计交付，不宣称源码/模型完成；UNKNOWN/身份不可证/未成熟/统计null及缺口可见，不伪成功；原基线/价格政策/名单/旧实验与模块边界不改；没有新增包资格审批、自然等待或平台工程。确认数字仍须真实开发方差/容量合同，不以探索性支持提示当激活门禁。
