# Advisory 收益型进入价格：固定新增日频信息 v4 F2详细设计

> 2026-10-03；DESIGN_REVIEWED_SOURCE_NOT_IMPLEMENTED。本文交付详细设计，不宣称实现、训练、确认或角色启用。新增信息计算PR #5301已合入`9c01032272192fcfc93329d545f1b8c3f8425fab`（34定向测试、required CI通过），旧每日消费者六项浏览器验收仍待专用runner；这些状态不互相替代。

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

本设计PR只增加本文并精确更新蓝图§16当前进展/新主线，不动业务源码。实施必须从设计已合入最新main新建独立Advisory树，精确范围：

- 新增`backend/services/advisory_model_first/economic_entry_information_contracts.py`：十三字段scope/request/plan。
- 新增`economic_entry_information_source.py`、`economic_entry_information_training.py`、`economic_entry_information_inference.py`、`economic_entry_information_pipeline.py`、`economic_entry_information_evaluation.py`（均在同Advisory目录）。
- 新增上述叶模块对应最小测试；更新本设计实现矩阵及蓝图进展。模型source/训练同schema完成后，另登记`economic_entry_serving_bundle.py`、`economic_entry_daily_inference.py`、`economic_entry_daily_source.py`的**显式新family消费分派**，不得提前修改未合入消费者分支或让旧schema接受新维度。

先四字段来源可产性→独立合同和有界source→同集合控制/新头→单候选预登记→一次探索导航→新family消费→完整工程/浏览器验收。并非实现前自动训练；必须先核对父工件SHA、来源/窗口/PIT边界、四字段覆盖和purge集合，preflight失败仅提具体owner缺口。所有源代码多轮审核，失败先直接节点再一次稳定最小矩阵；广回归交CI/专用Validation Center，不借其它模块修复。

## 8. Verification Plan / 可产性与设计复核

2026-10-03固定首/中/末已消费D=`2024-07-04/2025-04-22/2026-02-02`：每D原Top20及400条raw bar、20个权威交易日，四字段每项20/20可计算。只读repeatable-read事务10个SELECT耗时0.562秒，最后rollback；模型fit0、收益/sealed读取0、DB写入0。原冻结raw_daily无volume，因此不能直接复用该旧投影完成新信息源；未覆盖旧工件。当前DB历史值不证明native capture、三日覆盖不等于386日全覆盖。

可产性观察时间`2026-10-02T20:44:20.694357+00:00`，三D的information SHA依次为`1e39e676a95ec716d72def63aabfb45be977d61f8012133afac40b3e8cdd356d`、`0af570649cb592a5a2a64ff794b3bcbedc2574cd17fd2d91284e88a9df334a30`、`55ccd50475a33fc5c082d9af5081df57f53f391fd55f3b97f9d05d8b1ab07828`。这些hash绑定实际计算输入而非原生采集资格；首次脚本报告使用错误输出键`features`在事务内报错，已rollback，修正为实际`values`后成功，不是数据库连接故障或业务代码修复。

正式实施测试重点：原候选/日期完整、raw-vs-adjusted volume语义、D adjustment/benchmark/warmup、停牌UNKNOWN、sourcehash/规范重复键、train-only边界及test毒化不影响拟合、两臂同集合/price support、标签/成本政策不变、十三与九reader明确分派、单/批一致及预算，不再重复整套旧研究。真实验证前不将合成测试当收益、native或浏览器通过。

## 9. Design Acceptance Index

| ID | 验收条款 |
|---|---|
| F-587 | 单一新信息假设，原负结果/权重/合同不改 |
| F-588 | 四字段统一D/raw-volume语义、正常未知保留 |
| F-589 | 十三字段独立scope及双版本显式分派 |
| F-590 | 同监督集合双臂控制，全部配置/头如实计trial |
| F-591 | 原政策/成本/支持域，固定全日增量及功效 |
| F-592 | NAV与native/confirmation/activation严格分开 |
| F-593 | Advisory精确范围、零其它模块/DB/服务变更 |

## 10. Design Acceptance Matrix

以下仅为设计条款核查，不是未来功能实现验收。实现后必须替换为实际源码/nodeid/研究receipt；不可凭此表提交“v4模型已完成”。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-587 | 本文§1/§5 | artifact: 本文§1及aligned v3设计§13 | DESIGN_VERIFIED | none |
| F-588 | 本文§3/§4 | artifact: 本文§8三日D-only可产性与F1合同 | DESIGN_VERIFIED | none |
| F-589 | 本文§3/§7 | artifact: 本文§3旧validator不变条款 | DESIGN_VERIFIED | none |
| F-590 | 本文§5 | artifact: 本文§5两配置四头/同集合定义 | DESIGN_VERIFIED | none |
| F-591 | 本文§5/§6 | artifact: 本文§6固定统计和归因 | DESIGN_VERIFIED | none |
| F-592 | 本文§4/§6/§12 | artifact: 本文§6sealed硬边界 | DESIGN_VERIFIED | none |
| F-593 | 本文§2/§7/§12 | artifact: 本文§7精确允许范围 | DESIGN_VERIFIED | none |

## 11. Risks / 审核记录

量价信息可能仍无增量。三日读回仅证明产出，原前八字段train/daily temporal parity及原生来源未闭合；扩大矩阵会损失支持，不能以新增字段宣称跨策略包通用或稳定收益。raw成交量受拆分影响是冻结定义的已知风险，不结果后替换口径。两个头分别学习均值/风险，不保证统计置信；日频价格集合不是fill或最佳点证明。

审核1（业务/信息/缺失）：明确raw volume而非既有复权volume；三日可产性与原生资格分开；原候选保留。审核2（因果归因/方法）：发现新增字段缺失使监督人口改变，增加强制matched九字段control，不复用不同人口旧权重声称“信息增量”；登记2配置/4头，不能假报单一fit。审核3（旧兼容/权限/交付）：十三scope独立、旧validator不改；source语义与未来daily parity分离；本PR仅设计、不读sealed、不启动训练或借其它模块补缺。

DESIGN-COMPLIANCE-001逐项：完整新设计范围而非旧模型自动升级；未知/不足/负结论显式；不更改成本、策略或动作边界；不捆绑通用平台、分钟执行或治理项目。结构validator只证明文档结构，功能未实现状态不升级。

## 12. Production Gates / Rollout / Rollback

production_ddl_gate=noop；production_backend_dependency_gate=noop；production_frontend_dependency_gate=noop；model_trials_generated=0；model_confirmed=false；binding_active=false；backend_restart_owner=user。新family保留旧reader可回退，旧artifact不可变；本设计不改运行时、DB、profile、资金仓位或服务。未来新功能源合入、实际推理、浏览器验收、经济确认和runtime activation分别报告；涉及重启由用户执行。
