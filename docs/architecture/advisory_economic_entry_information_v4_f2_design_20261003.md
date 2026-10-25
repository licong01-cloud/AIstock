# Advisory 收益型进入价格：固定新增日频信息 v4 F2详细设计

> 2026-10-03；OFFLINE_SOURCE_IMPLEMENTED_EXPLORATORY_NOT_CONFIRMED。§7批准的六个离线叶模块已实现并完成一次真实固定研究；新family每日消费者、浏览器验收、原生资格与角色启用仍未完成，不能称整个v4业务已交付。新增信息计算PR #5301、详细设计PR #5303已合入；旧每日消费者六项浏览器验收仍待专用runner。各状态不互相替代。

## 1. Background / 当前事实和目标

aligned v3 study `advaligned_00089672bee4c91dd2261cf1`已完成一个真实候选：模型/基线100日名义净收益9.6268%/19.1729%，日配对增量-8.8880bps，95%区间跨零；模型实际TAKE16笔，另9笔为UNKNOWN基线控制。candidate停止，不激活。81日D冻结网格是功能验证，不是经济确认。

本次唯一新假设：D可见个股历史量价信息，能改善同一实际T观察价条件下的成本后进入价值与entry-loss估计。目标是给出可能具有净收益及可接受下行风险的价格条件集合，不是命中T开盘价，不是改变成交价的因果最优点，也不是日内最佳买卖时点。

固定加入`ret_10`、`relative_ret_5_vs_csi300`、`close_location_in_day`、`volume_ratio_5_to_20`；第二项是已有收益/指数信息的派生，不能单独算独立alpha。原九字段、旧模型、标签、风险参考和成本/policy不改。

## 2. Scope / Non-goals / 范围和权限

仅Advisory自己实现独立schema、训练、推理、pipeline及消费适配，不修改QE、Selection、数据准备、公共验证、Paper或Execution；不运行QE研究、重选股票、补行情、写数据库、激活PIT/profile、安装依赖或控制服务。临时X、持久工件F，重启用户执行。动态资金仓位、分钟择时、卖出模型及确认producer不捆绑进本切片。

详细设计合入不授权读取sealed；新研究只在已消费开发窗口一次导航。未来正式日常使用、独立确认和经济激活分别保留严格原生与证据边界。六层目标栈不是六条新项目：本轮一个价格信息主线，Exit只保留既有方案，不重复旧learnability。

## 3. Architecture / 单一信息语义和双版本

`economic_daily_information_v1.build_economic_daily_information_v1`为四字段唯一纯计算内核。原八D字段和query_gap_bps继续取原冻结值；新版本明确十三字段顺序为原九字段完整顺序，再追加上述四字段，query_gap_bps的位置仍为第九。所有训练、单条件估值和D价格网格显式引用该顺序/语义hash，不按dict排序猜矩阵顺序。

新scope为`economic_entry_information_scope_v4`，绑定原package/manifest/universe/policy/cost、九字段语义、新信息语义、训练来源及窗口资格、模型和双头身份。新model family=`ALIGNED_ENTRY_INFORMATION_V4`，新reader/dispatcher只按显式family/schema/维数分派；旧V2 scope与v3九维reader继续严格拒绝十三维输入。不修改旧validator、weights或旧manifest，不用新特征硬塞旧权重。

训练与每日原始source可分批读，但统一D计算合同：恰好20个权威交易日，末日D；候选最多20、顺序/rank/键唯一；最多400条候选原始bar。daily与batch只是读取方式差异，逐D的特征、未知原因和内容hash必须相同。批量按有界窗口/候选键联合读，逐D截断计算；禁止全universe×date扩表或逐候选查询。

## 4. Contracts / 来源、时钟、未知和身份

四字段公式和未知语义直接引用F1语义hash：10日收益需11个close；相对5日收益需6个close及同D沪深300收益；收盘位置需合法D高低收；量比需20个原始同单位volume。ret类小数、量比/位置无量纲。股份拆分/复权坐标由真实adj_factor(day)/adj_factor(D)统一D价；不能用今天最新因子回填历史D。raw volume_hand统一换算raw shares，不复用既有`_market_frame`的复权volume来冒充RAW_SAME_UNIT。若以后希望改成交量口径，必须另立信息schema，不能更改本次公式。

仅允许D及以前价格/复权/指数/suspend输入；T实际观察价只生成真实监督条件query_gap，不进入D字段。T法规价边界由D可见参考和既有Advisory合同解析。D输入中禁止收益标签、未来OHLC、退出执行状态、test成熟状态和事后调仓信息。索引日期规范后再次检查唯一性，源键漂移/交叉包/未来日期/hash矛盾fail closed。

正常缺行、缺字段、停牌、退化分母保留原候选和原日期，按字段UNKNOWN，不补0、不删股票、不压缩交易日、不补第6名。停牌bar仅由已有获证实的停牌归一化合同提供，必须保留真实raw与归一化hash；S/R歧义维持未知。未提供native证明的历史读回只能CURRENT_DB_HISTORICAL_NON_VINTAGE/RECOVERED_LIMITED；COMPUTATION_ONLY hash不是receipt恢复。

不可变source manifest绑定原始交易日历、原候选、所有实际使用源值、缺字段存在性、D复权基准和benchmark。相同数值但不同缺字段来源不合并身份；原数据/旧实验不覆盖。训练与daily的前八字段temporal parity目前UNPROVEN，必须单列验证，不能因新增四字段一致就宣称十三字段全链路native一致。

## 5. 固定比较：缺失集合不得混淆信息增量

原v3共同监督集train3,117/validation1,507仅作父集合。新增缺失可能缩小可拟合集，但原7,720候选仍完整保存。新共同fit集合严格为原v3 eligible与四新字段均有限的交集，沿用原label-information-end purge。新集合hash在训练前登记，不按照收益/预测结果筛选。

为消除训练人口变化的混淆，**始终**固定拟合九字段matched control与十三字段candidate，两者使用同一新集合、同一原return/v2 entry-loss标签、同参数/seed与price-bin支持。经济candidate只有1个，但必须如实登记2个模型配置、4个头拟合；control重训不是零trial/exact retry。旧v3只作既有结果参照，不回选旧frontier。若缺失未减少集合，也不临时省掉control或改变预登记试验数。

LightGBM沿用父有效参数：200轮/lr0.05/depth3/leaves7/minleaf30/seed20261002/2线程；mean与q0.9 entry-loss各一头，无early stop/搜索/额外seed。entry-loss继续`entry_net_max_loss_open_close_bps`，固定800bps只是原研究stop参考，不是用户资金预算。gap桶宽100bps、至少30观察/5日期；支持界限仅train计算，两臂同共同集合gap支持。

缺失新信息时，两臂研究策略均走明确UNKNOWN基线控制，不归模型TAKE；两臂另报告缺失遮罩及控制贡献，避免control偷偷拥有更多可决策人口。正常缺失不阻断整个批次；没有足够train/validation监督则本study停止为INSUFFICIENT_SUPPORT，不以填值或删原日期抢救。

## 6. Evaluation / 探索与确认分开

四臂固定：原基线、固定gap±300bps规则、matched九字段、十三字段。原shadow simulator、Top5固定等权槽位、政策/成本/共同估值期不变；非支持/UNKNOWN控制归因与TAKE真实episode分开。单条条件与D价格网格共用新唯一节点内核，ACCEPTABLE要求支持内、预测期望净收益>0及entry-loss q90≤800；不将风险coverage当胜率或均值置信界，不把非空网格等同于盈利。

主要信息增量是十三减matched九字段的全共同日净收益差；同时报告十三减原基线的收益/MDD、TAKE/SKIP/UNKNOWN、动作不同日、入场episode数量、错失盈利/避免亏损、训练覆盖变化与regime分布。moving-block bootstrap固定block5/reps2000/seed20261002，95%描述性区间；不只挑干预获利日，不改变block或800寻找通过。

登记时绑定study_type=EXPLORATORY_SCREEN、objective_contract=RISK_MANAGED_ADVISORY、decision_use=NAVIGATION_ONLY、累计研究族trial及已消费窗口。拟合前用父开发日序列固定重算block噪声/MDE，报告新增动作支持最低值的推导；新模型动作统计只能在运行后报告，不能把预期支持说成已达成。欠功效可导航，不支持关闭整个方向或激活。

未来confirmation另需未消费合法窗口、原生training/daily身份、预注册MDE/最低干预数和日占比/regime、一次选点及真实授权；仅主线冻结后访问。开发诊断绝不接触sealed，失败不得在同frontier重新挑点。没有确认输入不等待固定未来实盘日期，也不拿已消费开发窗口冒充确认。

## 7. Implementation Plan / 精确文件清单与顺序

设计PR #5303只增加本文并精确更新蓝图§16，未动业务源码。本次实施按该已批准分阶段范围从设计已合入main创建独立Advisory树；六叶模块离线源码与后续每日消费分别交付，精确范围：

- 新增`backend/services/advisory_model_first/economic_entry_information_contracts.py`：十三字段scope/request/plan。
- 新增`economic_entry_information_source.py`、`economic_entry_information_training.py`、`economic_entry_information_inference.py`、`economic_entry_information_pipeline.py`、`economic_entry_information_evaluation.py`（均在同Advisory目录）。
- 新增上述叶模块对应最小测试；更新本设计实现矩阵及蓝图进展。模型source/训练同schema完成后，另登记`economic_entry_serving_bundle.py`、`economic_entry_daily_inference.py`、`economic_entry_daily_source.py`的**显式新family消费分派**，不得提前修改未合入消费者分支或让旧schema接受新维度。

先四字段来源可产性→独立合同和有界source→同集合控制/新头→单候选预登记→一次探索导航→新family消费→完整工程/浏览器验收。并非实现前自动训练；必须先核对父工件SHA、来源/窗口/PIT边界、四字段覆盖和purge集合，preflight失败仅提具体owner缺口。所有源代码多轮审核，失败先直接节点再一次稳定最小矩阵；广回归交CI/专用Validation Center，不借其它模块修复。

## 8. Verification Plan / 可产性与设计复核

2026-10-03固定首/中/末已消费D=`2024-07-04/2025-04-22/2026-02-02`：每D原Top20及400条raw bar、20个权威交易日，四字段每项20/20可计算。只读repeatable-read事务10个SELECT耗时0.562秒，最后rollback；模型fit0、收益/sealed读取0、DB写入0。原冻结raw_daily无volume，因此不能直接复用该旧投影完成新信息源；未覆盖旧工件。当前DB历史值不证明native capture、三日覆盖不等于386日全覆盖。

可产性观察时间`2026-10-02T20:44:20.694357+00:00`，三D的information SHA依次为`1e39e676a95ec716d72def63aabfb45be977d61f8012133afac40b3e8cdd356d`、`0af570649cb592a5a2a64ff794b3bcbedc2574cd17fd2d91284e88a9df334a30`、`55ccd50475a33fc5c082d9af5081df57f53f391fd55f3b97f9d05d8b1ab07828`。这些hash绑定实际计算输入而非原生采集资格；首次脚本报告使用错误输出键`features`在事务内报错，已rollback，修正为实际`values`后成功，不是数据库连接故障或业务代码修复。

全开发窗口来源预检随后完成（观察时间`2026-10-02T20:58:32.263175+00:00`）：386D/7,720原候选、938股、379,307 raw bar，四字段全部已知7,710候选；ret_10/相对5日/收盘位置/量比已知数7,716/7,720/7,720/7,710。4条close窗口和10条volume窗口缺口按UNKNOWN保留，均位于train输入；train/validation/已消费test原候选4,380/1,720/1,620，四项全知4,370/1,720/1,620。四个SELECT，一次只读repeatable-read snapshot并rollback，来源6.438秒/总20.422秒；逐D内核只收到原20股≤D的20日子集。逐D信息hash清单SHA=`201a1ef677f74898e5043f509077992856f92bee337288daabce1398f994aebd`。未读取labels/eligible/经济结果、未fit；7,710是特征来源已知数，不是监督样本、胜率或收益支持度，正常缺失不触发填补/删候选。当前历史值仍NON_VINTAGE，原生训练/daily资格未因此闭合。

正式实施测试重点：原候选/日期完整、raw-vs-adjusted volume语义、D adjustment/benchmark/warmup、停牌UNKNOWN、sourcehash/规范重复键、train-only边界及test毒化不影响拟合、两臂同集合/price support、标签/成本政策不变、十三与九reader明确分派、单/批一致及预算，不再重复整套旧研究。真实验证前不将合成测试当收益、native或浏览器通过。

## 9. Design Acceptance Index

| ID | 验收条款 |
|---|---|
| F-587 | 单一新信息假设，原负结果/权重/合同不改 |
| F-588 | 四字段统一D/raw-volume语义、正常未知保留 |
| F-589 | 十三字段独立scope、离线双臂严格readback；每日serving按§7另登记交付 |
| F-590 | 同监督集合双臂控制，全部配置/头如实计trial |
| F-591 | 原政策/成本/支持域，固定全日增量及功效 |
| F-592 | NAV与native/confirmation/activation严格分开 |
| F-593 | Advisory精确范围、零其它模块/DB/服务变更 |

## 10. Design Acceptance Matrix

本表验收§7明确分开的六叶模块离线源码及探索流程，不替代后续每日serving/API/UI、native或确认。34项最终小矩阵通过（5.64秒）；随后配置投影修订仅复跑pipeline节点，通过2.80秒。完整四臂交易回放采用真实原shadow simulator；合成fixture只证明工程合同，真实研究见§13。相同测试/CI不反复全量运行。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-587 | backend/services/advisory_model_first/economic_entry_information_contracts.py | test: backend/tests/advisory_model_first/test_economic_entry_information_contracts.py::test_separate_schema_keeps_old_nine_strict_and_counts_control | VERIFIED_OFFLINE | none |
| F-588 | backend/services/advisory_model_first/economic_entry_information_source.py | test: backend/tests/advisory_model_first/test_economic_entry_information_source.py；artifact: §13 source_receipt.json | VERIFIED_OFFLINE | none |
| F-589 | backend/services/advisory_model_first/economic_entry_information_contracts.py；economic_entry_information_pipeline.py；economic_entry_information_inference.py | test: backend/tests/advisory_model_first/test_economic_entry_information_contracts.py；test_economic_entry_information_pipeline.py | VERIFIED_OFFLINE_SCOPE_AND_READBACK | none |
| F-590 | backend/services/advisory_model_first/economic_entry_information_training.py | test: backend/tests/advisory_model_first/test_economic_entry_information_training.py::test_common_cohort_four_heads_and_test_poison_never_fits；artifact: §13 trained metadata / registry | VERIFIED_OFFLINE | none |
| F-591 | backend/services/advisory_model_first/economic_entry_information_evaluation.py | test: backend/tests/advisory_model_first/test_economic_entry_information_pipeline.py::test_atomic_two_configuration_registration_fit_readback_and_exact_retry；artifact: §13 prefit power / evaluation | VERIFIED_EXPLORATORY_ONLY | none |
| F-592 | backend/services/advisory_model_first/economic_entry_information_pipeline.py；economic_entry_information_evaluation.py | test: backend/tests/advisory_model_first/test_economic_entry_information_evaluation.py；artifact: §13 immutable plan / exact retry | VERIFIED_OFFLINE | none |
| F-593 | 本文§7全部精确路径；本次Git范围 | artifact: 仅六Advisory叶模块、六对应测试及两文档；Ruff / diff / scope review通过 | VERIFIED_SCOPE | none |

## 11. Risks / 审核记录

量价信息可能仍无增量。三日读回仅证明产出，原前八字段train/daily temporal parity及原生来源未闭合；扩大矩阵会损失支持，不能以新增字段宣称跨策略包通用或稳定收益。raw成交量受拆分影响是冻结定义的已知风险，不结果后替换口径。两个头分别学习均值/风险，不保证统计置信；日频价格集合不是fill或最佳点证明。

审核1（业务/信息/缺失）：明确raw volume而非既有复权volume；三日可产性与原生资格分开；原候选保留。审核2（因果归因/方法）：发现新增字段缺失使监督人口改变，增加强制matched九字段control，不复用不同人口旧权重声称“信息增量”；登记2配置/4头，不能假报单一fit。审核3（旧兼容/权限/交付）：十三scope独立、旧validator不改；source语义与未来daily parity分离；本PR仅设计、不读sealed、不启动训练或借其它模块补缺。

DESIGN-COMPLIANCE-001逐项：完整新设计范围而非旧模型自动升级；未知/不足/负结论显式；不更改成本、策略或动作边界；不捆绑通用平台、分钟执行或治理项目。结构validator只证明文档结构，功能未实现状态不升级。

## 12. Production Gates / Rollout / Rollback

production_ddl_gate=noop；production_backend_dependency_gate=noop；production_frontend_dependency_gate=noop；model_configurations_generated=2；economic_candidates=1；heads_fitted=4；model_confirmed=false；binding_active=false；backend_restart_owner=user。本次离线源码无需重启，不部署新reader或绑定。旧artifact不可变；不改运行时、DB、profile、资金仓位或服务。后续每日新family消费者、浏览器验收、经济确认和runtime activation分别报告；涉及重启由用户执行。

## 13. 实施复核与真实探索结果（2026-10-03）

离线源码检查点`d656f0a376109555e7eddac693745aa22a633616`，study=`advinfo_153e00c8509484155d3c8ae0`；持久根`F:/Dev/AIstock_model_artifacts/advisory_entry_information_v4_20261003/`。`preregistered/plan.json`绑定输入`inputs/advinfoinput_26aed90e74c80316334a5cdb/prepared/manifest.json`及source_receipt、模型scope、实现、原policy/成本/两配置；`trained/model_metadata.json`绑定四头，`evaluated/evaluation.json`与matched_daily/各臂episodes是收益来源。全部为已消费开发窗口的NAV，不是独立OOS。

三轮源码审核：①PIT/原候选/正常缺失/哈希与有界批量；②固定同eligible、test毒化、只读原值Decimal精度及scope/readback；③四臂全流程、all-ACTIVE无settled-return列时保留NaN未知、精确重试及跨模块边界。修正了source配置投影（配置与带身份request分开验证），未改变旧9维validator、标签、policy、成本或权重。四臂fixture最初缺combined_score及Top40持有上下文，只修测试fixture，不削弱真实模拟器的Top40合同。

原父实现登记字节为LF，而新Windows树自动CRLF会触发实施身份拒绝。只在本任务树对明确列出的Advisory父文件恢复父研究原字节（含原simulator自身CRLF），核对仅换行差异、旧hash及Git无业务diff；未修改旧plan/receipt，也未放宽任何hash校验。新树先前未加载凭据位置导致一次无密码连接失败，改为读取既存`F:/Dev/AIstock/.env`后成功，未输出密钥、安装依赖或写库。

真实来源准备/plan登记25.000秒，模型四头拟合5.234秒、拟合至四臂评估共21.922秒。仅一次批量只读数据库来源准备，后续训练/评估全部消费不可变工件；保留7,720原候选。新共同train/validation=3,117/1,507，与父v3相同（10条新增量价未知原本已不eligible），matched九字段不能省略，仍登记2配置/4头/1candidate。登记前QE公开只读列表running/queued/finalizing/reconciling均0，不提交QE训练。

| 固定臂 | 100共同估值日名义净收益 | 共同期最大回撤 | 全episode数 | 实际模型TAKE / 正收益笔数 | UNKNOWN基线控制 |
|---|---|---|---|---|---|
| baseline | 19.1729% | -10.2278% | 49 | 不适用 | 不适用 |
| rule ±300bps | 19.1729% | -10.2278% | 49 | 不适用 | 不适用 |
| matched九字段 | 9.6268% | -5.2608% | 25 | 16 / 10（62.50%） | 9 |
| 十三字段 | 19.0748% | -8.8228% | 35 | 27 / 16（59.26%） | 8 |

十三减matched九字段日增量+8.7336bps，固定block95%区间[-8.6091,25.5629]；十三减baseline为-0.1545bps，区间[-16.4598,17.0273]。Prefit父开发噪声代理MDE=29.6721bps（不是新模型功效保证）。两模型实际入场不同22/100估值日、持仓不同77/100；81原候选D与100估值日不可混用分母。没有预注册/获证实的市场regime分区，跨regime支持仍未证明，不按结果临时分桶宣称稳定。净收益是原shadow政策名义值、零收益cash参照，非沪深300超额或真实成交。

原test episode执行UNKNOWN=1仍单列在execution_limitations.json；四臂实际模拟episode endpoint audit当前限制数均0，仅表明这些名义端点未发现限制，不证明真实fill/native完整。UNKNOWN控制利润不归模型TAKE；16/27是重复入场episode而非唯一股票数，59.26%不构成经济有效性判定。

实际exact retry8.156秒，profile计数新fit=0/shadow replay=0/DB配置调用=0，trained/evaluated manifest hash不变，registry仍三阶段三记录。新增信息改善旧模型但未显示可靠基线超额：EXPLORATORY_NOT_CONFIRMED、NAVIGATION_ONLY、deployable=false；不搜索阈值/seed、不再次消费sealed、不激活。下一步先交付离线源码及按§7登记每日family消费；旧消费者六真实浏览器场景仍只暂停对应合入，不能用本研究替代UI。经济确认须另具原生训练/daily资格与合法未消费窗口。
