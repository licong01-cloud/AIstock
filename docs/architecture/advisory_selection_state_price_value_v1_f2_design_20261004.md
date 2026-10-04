# Advisory D可见选股状态持续性条件价格价值 F2详细设计 v1

2026-10-04；M5研究类型EXPLORATORY_SCREEN、RISK_MANAGED_ADVISORY、NAVIGATION_ONLY。按用户最新[QE包直接消费合同](advisory_qe_package_direct_consumer_v1_f2_design_20261004.md)，不再以父训练时钟、历史native或M1正导航阻止本独立新信息研究。M1已终态并完成#5414交付，后续同步源码/登记后可执行；原排名、权重、指标和M1结果不改。未确认的机械持有期可预测性不能冒称盈利。

## 1. Background / 当前事实与研究问题

R2的M2/M3/M4源码#5404已合入、11物理fit及1索引完成，三个精确candidate均负向停止；M1设计#5411及源码#5414已合入，已完成7720键prepare、四fit与完整四臂，开发导航为正但两增量区间跨零。早前QE multi-alpha running2导致的拟合暂停是历史检查点，不是当前状态；新fit前按实际资源状态检查。当前48h截止2026-10-06 02:36不变。

一次只读源核定：父frozen_rankings.parquet包含405个D/16200行，逐D恰好40个唯一证券、rank1～40完整，无重复证券；386个原候选D。这里只读取排名/键/来源，不读收益、不生成模型结果。历史原生身份和PIT恢复限制照旧，并不因完整40名就升级证据等级。

H-SELECTION-STATE-VALUE-1：某股票连续进入当前包前5/前20及D前排名变化，是否提供现有12D与价格g之外的收益延续/拥挤/反转信息，形成成本后相对匹配模型和原Top5的增量。其价值未被证明，利弊可能随市场阶段变化；不声称这些字段天然正交alpha。

## 2. Scope / 精确文件及依赖

初版设计#5415及准备修订#5416已合入、原自身树已清理。本次方向修订使用`advisory-qe-package-direct-use-design-20261004`独立树，精确五文档范围见直接消费合同§2。源码合入前同步蓝图当前队列，不把准备或设计当已完成研究。

仅在自己的M5源码树实现/测试固定三字段、模型和薄编排，保留M1旧研究和其原implementation receipt。M1已终态/#5414已交付，正导航不再禁止M5：更新源码合同、冻结清洁HEAD并独立登记后可读原开发收益、一次fit/四臂；不称为新独立确认。正式结果前不调seed/窗口/支持/风险或回选控制。允许源码范围仅：

- backend/services/advisory_model_first/economic_selection_state_price_v1.py（纯D历史信息、plan与固定模型适配）
- backend/services/advisory_model_first/economic_selection_state_pipeline_v1.py（来源、登记、prepare/fit/evaluate的薄适配）
- backend/services/advisory_model_first/economic_sector_price_value_v1.py（抽取冻结任意信息块的同核纯GBDT/JSON数学；M1包装语义不变）
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py（仅抽取共享原子登记/阶段/fit journal/预算纯编排；M1入口和合同不变）
- backend/tests/advisory_model_first/test_economic_selection_state_price_v1.py
- backend/tests/advisory_model_first/test_economic_selection_state_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py（仅共核提取的M1精确回归）
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py（仅共核提取的M1阶段回归）
- 本设计及docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

不复制第二套four-arm simulator、label平台或另造通用审批/数据平台。复用既存JSON树核、全局价格支持、完整四臂公共helper、不可变stage和registry；M5不借M1行业模型或旧失败评价，只借冻结父数据。

## 3. Non-goals / 权限与证据边界

不改QE/Selection/HMM/StrategyPackage/公共数据/Execution/Paper/CI源码；不提交QE、不写数据库/DDL/DML、不重建池/候选/激活profile、不安装依赖、不控制backend/worker/其它进程。无需后端重启，若未来需要由用户执行。临时X任务目录，正式F独立hash目录，不覆盖旧plan/输入/结果。

不读sealed或新holdout，不复跑旧负候选，不通过换窗口/seed/风险/门槛救活M1。M5独立树可预备重构，但M1冻结树保持原始实现及hash不变；合入重构必须在M1终态及源码交付后，旧M1精确code receipt仍保留历史有效，不能声称用新共核重新验证旧研究。

## 4. Architecture / 目标与时钟

本模块只输出D及以前信息条件下的合法价格建议函数，接受假设raw价格g；真实T open只代入冻结D函数。买价低于参考不必更安全，高开不必买入；输出可多段、空集或UNKNOWN。收益型判断与开盘价coverage分开，不研发分钟触价/成交或资金仓位。

两个新增时间持续性信息与一个变化信息来自同一个既存包的405D frozen Top40，保持package/manifest/Selection/policy/D/T/candidate hash。不同包不能直接复用本模型，无bundle仍typed unavailable；不对其它包或当前实盘收益作承诺。

## 5. Contracts / 完整名单与可识别性

先复用既存只读父授权器核定原已消费开发窗口、prepared manifest链/原candidate roster/12D/hash/profile/pool/policy，再读取排名和value标签。禁止从current Selection重新计算名单、从当前池删旧候选、缩短日期或把排名证据当行业/PIT COMPLETE。

历史排名必须为每D恰好40个唯一证券和唯一rank1～40，同D唯一T且D<T，原rank为整数非bool；每个D应按父完整交易日calendar映射到下一T。重复rank/证券、非法rank、交叉D/T、源hash漂移或当前候选与该D Top20不一致一律fail closed。

正常历史名单缺日或不足40行不能据此推断证券“不在名单”，该窗口/该候选新增字段UNKNOWN，保持原候选和日期；与来源声明完整相矛盾或有重复/非法身份则硬失败。必须沿完整calendar取最近5/20个D（包含当前D），不能把20个稀疏名单当20个交易日。只处理D及以前，后续D排名毒化或收益字段不可影响此D输出。

只有已证明完整40名的历史D，某证券不在该40名内才可记为“完整Top40外”。这不是行情补零，不是证券删除，也不是其全市场真实rank=41。

## 6. Frozen information block / 本轮只三字段

| 字段 | 精确定义与单位 | 支持/缺失 |
|---|---|---|
| selection_top5_rate5 | 包含D最近5个完整交易日名单中rank≤5的次数/5，比例 | 任一天名单不完整或不足5日→UNKNOWN |
| selection_top20_rate20 | 包含D最近20个完整交易日名单中rank≤20的次数/20，比例 | 任一天不完整或不足20日→UNKNOWN |
| selection_censored_rank_improvement1 | 前一完整D的censored rank减当前D rank；Top40外记固定右删失边界41，仅表示超出前40 | 必须前一D及当前D完整，当前候选rank≤20；不声称全市场精确rank |

正的improvement表示向前改善。原parent_rank_pct已有当前rank，新增的是过去持续性/变化；不能把当前rank复制三份充当新信息。三字段同源可能高相关；本轮不再拆分/增删字段、调窗口5/20、将41改为其它值或择优报告。

先按D原名单推导，只读字段不依赖label_status/maturity/收益/实际T HLC，所有原键left join保留。共同mask需要12D及3字段完整、D时钟正确；缺新增字段时matched和candidate共同UNKNOWN，不只禁candidate。

## 7. Model / 同核四fit与全局支持

固定matched=13D（12D+g）、candidate=16D（12D+3新增+g），均GradientBoostingRegressor200 trees/lr.05/depth3/min_leaf30/subsample1/seed20261004，gross Y均值和path L q.1各两头，共4物理fit，一candidate。与M1同核不是重跑M1，唯一变量为新的可识别信息块；不与不同监督人口的旧R2/M1run跨实验判胜。

监督仍同VALUE_REVIEW_5_V1标签、同共同cohort：mature Y/L、实际information_end≤train_end、T≤train_end、原12D完整、3新字段AVAILABLE、合法gap且位于global支持；最低100行/20D。匹配和候选共享全部train keys。原global支持仍从无标签train完整12D及原价格观察构建，100bps桶/30行/5D、2.5～97.5%及洞；不按新增信息成功或收益筛支持。

固定父train2024-07-04～2025-05-30，validation2025-06-03～2025-09-30，已消费test2025-10-09～2026-02-02，label cutoff2026-03-10；属于显式plan，不修改公共默认窗口。validation只诊断不反调；test不训练/选点/校准。JSON公开树float32分裂语义与sklearn parity，不pickle/joblib，扣buy.95/sell5.95bps一次，expected net>0且下行参考≤800bps。

## 8. Trial / 新预算但不清零研究族

M5为新的显式lineage/campaign`advisory_price_selection_state_v1_20261004`，在源码和本合同冻结后先预登记四fit/一candidate/全部窗/implementation和source hash，再看结果。依旧同经济研究族、同已消费开发窗，绝不宣称R3是新的独立OOS。持久产物沿同48h输出根`F:/Dev/AIstock_model_artifacts/advisory_price_research_campaign_r2_20261004`的新experiment_id目录，trial registry及campaign_fit_journal.jsonl沿原位置只追加，不换根/删除/清空来重置累计预算。

plan携带`budget_anchor_ref`（role=`price_campaign_budget_anchor`），指向原R2已登记研究的`<campaign_root>/<experiment_id>/preregistered/manifest.json`。纯plan校验绝对非C盘路径/manifest层级，根只能从锚点推导；登记/拟合编排再只读验证锚点hash、原manifest/plan、同父数据/政策、registry及累计journal。只有目录结构不够；清空journal、外来锚点、源hash或输出根不符均fail closed，不借新lineage建立空预算账本。

M2/3/4已11fit+1索引，M1已4fit，本设计M5最多4fit，累计19物理fit+1索引为计算预算，旧计数不清零。单测fit和研究trial分账，partial不隐式retry。M1无论正/负/执行阻断都不形成M5收益准入；仅核对该真实前序身份及实际累计预算。fit与QE训练互斥沿用户既有资源边界，running/unknown只暂停fit，不停止其它研发；不控制QE，不读holdout。

薄编排核对原M1真实stage链、registry及累计四fit，接受包括正导航在内的真实终态；不按收益门真假或缺父训练时钟拒绝。身份链或实际预算矛盾属于输入/编排错误。正导航拒绝已在本源码候选移除，正/负/执行阻断均有直接测试；源码尚未合入，不宣称运行后端已加载。

## 9. Evaluation / 同冻结四臂与分流

完整原81D/1620候选、共同100估值日的Top5 baseline/±300bps规则/matched/candidate；Top40复评、VALUE_REVIEW_5_V1、五槽、现金0、无Top6补位。UNKNOWN但市场证明可执行仅为原动作研究控制，分列贡献、不计模型TAKE；市场不证明四臂均不进入。任一endpoint/held-mark/unsettled不证则经济BLOCKED、不输出netmetrics。

候选减baseline及matched日均net都≥5bps，两个实际进入差异≥12D且≥15%原D，真实模型TAKE≥30episode；MDD恶化≤200bps、最差5%日均恶化≤20bps，block5/reps2000/seed20261004区间仅NAV。只在相同新共同cohort内判断信息增量，胜率不能替代净收益，机械持有期相关不能替代净价值标签。

本次M5结果无论正负均如实报告，不结果后调参或回选matched；可继续不同信息假设和正常消费者功能研发，不因正结果全项目暂停等待确认。可另行安排独立效果研究，当前不读取sealed、绑定或激活。开发多模型选择偏差累计披露，不以各次成功测试等同整体荐股有效。

## 10. Implementation Plan / 执行顺序与终止

用户方向修订多轮审核/合入→同步自己的M5树与最新main，移除正前序限制并核对M1冻结reader真实兼容→源码多轮审核/最小测试、清洁提交及独立登记→原开发窗prepare→QE空闲时一次四fit/完整四臂→真实结果/源码PR/必需CI/合入/自身清理。M1正导航及缺父时钟不再阻断，不等待收益确认或天然交易日累积；旧M1研究不重跑。

工程≤4h、四fit≤30min、2线程/RSS≤2GiB/新增工件≤2GiB、≤7720候选/500000价行，不另建大数据或UI平台；长实验30min检查，短实验立即接续。截止沿48h总任务，不重计48h；源不成立或负向只结束本候选，用户停止或总预算到期才结束本轮。

## 11. Verification Plan / 三轮审核与必要测试

来源/时钟：源hash、完整40名单、重复/未来/外来证券/非法rank拒绝；calendar缺日、不足40与warmup保留原keys/UNKNOWN，不把缺名单当零次；Top40外41的删失意义与当前rank差异明确；未来排名/收益毒化不影响D字段。

学习/收益：13/16同核及4物理fit、同maturity/监督、test毒化不变、global支持无标签且不按新块过滤；JSON parity、tick/多段/成本/UNKNOWN共同mask；完整四臂及held-mark阻断、真TAKE与控制分账。共享提取只做M1精确回归，不重跑旧研究。

交付/边界：stage/registry exact hash与partial拒绝、19累计fit及各lineage隔离、QE未知不fit、X/F及SOURCE/研究/经济/activation分别报告；无DB/共享模块/服务写入。使用最小直接矩阵，广回归交必需CI，不收集旧失败额外证据。

## 12. Design Acceptance Index

| ID | 验收 |
|---|---|
| F-705 | D历史状态是实际新信息，同族选择偏差不清零 |
| F-706 | 完整40/原calendar/删失41定义，未知不删除、不补名单 |
| F-707 | 三固定D字段/时钟/共同mask，收益与未来不能改变D输出 |
| F-708 | 同13/16 GBDT四fit、同maturity/global支持及JSON纯价集 |
| F-709 | 完整四臂/干预与真TAKE/两个净增量/风险/三级证据隔离 |
| F-710 | 注册/partial/本lineage4及整批19fit、QE互斥/资源 |
| F-711 | 独立树可预备共核，M1冻结hash不变；合入/登记/fit后置，精确回归且不复跑旧候选或复制平台 |
| F-712 | 明确Advisory范围/多轮自审/合入清理与用户重启分开 |

## 13. Design Acceptance Matrix

设计已交付；独立源码树已同步最新main，保留真实新信息/同核提取，旧M1模型数学与历史结果不变，移除正前序拒绝。19直接测试及Ruff通过；以下设计与源码验收分列。正式M5登记/研究fit/四臂当前仍0，下一清洁提交后按新身份运行，不冒充收益确认。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-705 | backend/services/advisory_model_first/economic_selection_state_price_v1.py | backend/tests/advisory_model_first/test_economic_selection_state_price_v1.py | SOURCE_VERIFIED | none |
| F-706 | backend/services/advisory_model_first/economic_selection_state_price_v1.py | backend/tests/advisory_model_first/test_economic_selection_state_price_v1.py | SOURCE_VERIFIED | none |
| F-707 | backend/services/advisory_model_first/economic_selection_state_price_v1.py | backend/tests/advisory_model_first/test_economic_selection_state_price_v1.py | SOURCE_VERIFIED | none |
| F-708 | backend/services/advisory_model_first/economic_selection_state_price_v1.py; backend/services/advisory_model_first/economic_sector_price_value_v1.py | backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py; backend/tests/advisory_model_first/test_economic_selection_state_price_v1.py | SOURCE_VERIFIED | none |
| F-709 | backend/services/advisory_model_first/economic_selection_state_pipeline_v1.py | backend/tests/advisory_model_first/test_economic_selection_state_pipeline_v1.py; artifact: 原完整四臂helper复用 | SOURCE_VERIFIED_RESEARCH_PENDING | approved_by_user: 下一清洁提交后运行一次新研究，未称经济完成 |
| F-710 | backend/services/advisory_model_first/economic_selection_state_pipeline_v1.py | backend/tests/advisory_model_first/test_economic_selection_state_pipeline_v1.py | SOURCE_VERIFIED | none |
| F-711 | backend/services/advisory_model_first/economic_sector_price_value_v1.py; backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py | backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py; backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py | SOURCE_VERIFIED | none |
| F-712 | §2/3/10/11/15/16 | artifact: 19直接测试、三视角审核及精确Advisory差异 | SOURCE_VERIFIED | none |

## 14. Risks / 不可误读

排名历史信号可能仅重复已有ret/排名或预测机械持有期，无经济增量；模型能学分类/久期不代表能盈利。新增状态同源/相关，不能当多个独立发现。D历史缺名单或早期warmup降低共同可估性，不得补当前名单或删样本掩盖。跨实验监督人口不同不能判哪个信息块更赚钱。多候选同已消费窗的正结果不冒称独立确认；确认可另行设计，不作为功能研发或包使用门禁，当前不读holdout。

## 15. Rollout / Rollback / Production Gates

仅离线研究，生产bundle/family/API/UI/binding/activation=0，backend_restart_required=false；DB/DDL/DML/依赖/数据profile激活/进程控制=noop。源码错误定向修复并保留旧身份，改变模型/标签语义必须新lineage，不覆盖旧运行。

DESIGN-COMPLIANCE-001逐项：实现和研究/经济/激活分报；未知/矛盾fail closed而非silent fallback；参数/窗口/支持/成本/门事前冻结，不结果后择合同；不增加平台/自然等待/旧失败补证或跨模块越界。

## 16. 三轮设计自审

方法轮：只比较同共同监督13/16信息增量，不将持有期机械可预测性当收益；固定三字段。输入轮：完整calendarD、Top40外右删失41、缺名单保留UNKNOWN。当前执行轮：15已发生fit不清零，M5最多4令整批cap19；接受正M1终态，不再重复父资格或等待确认；更新真实源码身份再登记，不覆盖旧模型/结果，不复制模拟器或修改公共模块。三视角为本窗口自审非独立外审。

本次源码三视角复审：方法轮核对共核提取仅参数化信息块/身份，M1既有接口、训练人口、支持、gap缩放和模型SHA公式保持；时钟轮保留原calendar/完整名单/未来毒化及原UNKNOWN，不以当前包资格补证为前置；编排轮将前序正/负/执行阻断均接受并拒绝真实计数矛盾，原预算不清零。同步main的冲突分别保留新信息共核与main最新事实文档，最终差异只有登记的Advisory源码/测试及本设计，不含其它模块业务修改。
