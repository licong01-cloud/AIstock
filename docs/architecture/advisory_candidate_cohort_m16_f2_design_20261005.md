# Advisory 原候选横截面状态条件买价 M16 F2详细设计

2026-10-05；SOURCE_MERGED_RESEARCH_COMPLETE_CANDIDATE_STOPPED / EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY / ECONOMIC_NOT_CONFIRMED。研究严格串行完成，不是第二条并行训练线。

## 1. Background / Goal

实际累计51研究fit+1原索引。M14源码#5478已合入0146ff7aac，原plan46e89f53已prepare/0研究fit，冻结source树保留等待QE；M15设计#5479已合入/清理、源码#5480 HEADbcd05f179/57直接项/currentCI37260309258 SUCCESS（03:47:47UTC）后已于03:50:41UTC合入a8ae765ed，正式登记/prepare/fit0，冻结树保留。不预判二者结果、不重跑M6～M13负候选或回选控制，不为凑时长调seed/loss/阈值。

H-CANDIDATE-COHORT-PRICE-1：同一股票的D收益、同一全市场宽度或指数路径，仍可以处在表现与分化完全不同的原候选群体中。冻结原Top20同D群体的一日收益均值、横截面标准差、上涨比例，是否改善给定可见买价时的条件净进入价值及下行？三量引入其它原候选的D信息，而不是对该股已知ret_1作确定性单变量变换。不是M5历史入榜持续性、M10全市场宽度20日路径、M1行业分类，亦非QE上游alpha挖掘或Top20重新排序。预期可能只是风险/拥挤代理，必须与原12D matched及原Selection基线同场对照。

事前工程spike spec c6d4fe3871eddb7dd092d563ab1e5e73c97f56847cca2829c84da5b7804e87c7仅读原reusable prepared rows.parquet的schema/metadata：7720行具原KEY、ret_1、feature_visible_through，0实际值/label/收益数组/DB/fit。列存在不是值完整、模型有效或原生身份证明。后续prepare只这些已冻结D字段，不向数据准备窗口索要新数据。

## 2. Scope / Non-goals

独立设计树事前来自main0146ff7aac只登记本文与总蓝图，M15 source交付后已安全ff latestmain a8ae765ed；后续独立latestmain source树只登记精确12文件：

- backend/services/advisory_model_first/economic_candidate_cohort_v1.py：纯三量/plan/同核薄训练推理。
- backend/services/advisory_model_first/economic_candidate_cohort_pipeline_v1.py：原冻结D特征/0SQL、同阶段薄委托。
- backend/services/advisory_model_first/economic_moneyflow_price_pipeline_v1.py：仅显式candidate_cohort_extension及实际M15前驱，原59加4上限63，所有旧预算不改。
- backend/services/advisory_model_first/economic_sector_price_value_v1.py：仅M16块/identity/status路由，旧公式不变。
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py：仅M16/63 fit路由。
- backend/tests/advisory_model_first/test_economic_candidate_cohort_v1.py
- backend/tests/advisory_model_first/test_economic_candidate_cohort_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py
- 本文和advisory_strategy_conditioned_model_blueprint_v1_20260710.md。

不改generic输入、旧label/支持/政策/成本/daily/API/UI、QE/Selection/HMM/StrategyPackage/公共数据/行业/Execution/Paper/CI/工作流/AGENTS，不重新选股/扩池/换包或做分钟执行。无DB写/DDL/DML、profile/模型激活、补数/安装、服务或他人进程控制；临时X、正式F新hash目录、后端重启user-owned。包直接消费，无重复资格门；真实数学/未来/身份矛盾才拒绝本计算。

## 3. Architecture / Contracts / Clock

candidate_cohort_rows_v1(*, candidates, features, calendar)接受原KEY候选、原日历与原冻结D表五字段[*KEY, ret_1, feature_visible_through]，输出原KEY原顺序+三量/candidate_cohort_feature_status/candidate_cohort_feature_visible_through/candidate_cohort_unknown_fields规范JSON。

候选人口必须来自原冻结rankings的原is_candidate_decision与rank<=20投影，不访问Top21、完整市场或当前名单；按原D群体聚合，群体定义绑定原candidate_roster_sha256。先按请求的原KEY投影宽feature表，再检查实际重复/时钟；不读未来或无关证券字段判断早D。只有所有原群体成员记录可用、ret_1已知且源clock已知<=D才可计算群体量，缺一原成员/值/clock则该群体三量UNKNOWN，保留所有原股。不删除缺行后用较少N计算，不从新成员、Top40、收益标签或别日补数据。

N为该D原冻结实际候选数量，允许合法N=1（std=0）和空名单返回typed空schema；不将固定研究Top20变成其它包的20股门禁。若原列表本身是20，计算必须消费其全20原键。原D/T须原日历相邻，KEY/每D证券唯一且只有一个目标T；source<=7720、原候选<=7720、按D聚合总O(N)、one_to_one KEY回写原顺序，不爆行。相同D三个群体量在各原股行重复，但统计支持仍按原日数计，不把20股当20个独立市场状态。

clock仅D盘后，T实际open沿原研究作为可见价格观察，不读T收盘/高低或未来同伴收益。源的D计算可见声明不等于capture时间；source_evidence/native UNPROVEN保持原级别，不伪造receipt或要求父包资格重验。

## 4. Fixed information / Numerical and missing semantics

| 固定字段 | 公式/单位 | 同依赖缺失行为 |
|---|---|---|
| cohort_ret1_mean | mean(original D ret_1)，fraction | 原群体任一成员缺键/值/源clock：UNKNOWN_COHORT_SOURCE |
| cohort_ret1_std | 原完整群体population std(ddof=0)，fraction | 同上，不缩群体/不填0 |
| cohort_advance_share | mean(original D ret_1>0)，dimensionless | 同上；完整已知全不涨为0、全上涨为1 |

ret_1沿原冻结定义，是D可见历史一日收益，不是label或test盈利。NULL/pd.NA/普通NaN/quiet Decimal NaN正常UNKNOWN；bool/string/Infinity/signaling NaN/不能代表的数（包括非零Decimal转float下溢0）为typed真实错误，已知ret_1必须>-1（正价格比的数学定义），不加事后上下百分比截断。所有原群体收益已知相等时std=0，0均值/0上涨比均合法，不误作缺失；源clock正常NULL为UNKNOWN，future clock真实矛盾报错。

用scale-normalized中心二阶矩及均值计算，避免直接平方造成伪溢出；派生量最终须finite且std>=0、share在[0,1]，真实不可表示则报数值错误，不泛化clip或除0补0。只计算声明需要的五字段，feature表中的Y/label成熟度/未来收益不消费。同群体的普通缺值会导致当天价格模型UNKNOWN控制，但不会阻断整个研究或删除该日/股票；最小训练人口不足只报告本模型实际条件。

## 5. Model / Labels / Policy / Information boundary

candidate原12D+三群体量+g=16，matched原12D+g=13，共同成熟/可用监督、固定GBDT200/lr.05/depth3/min_leaf30/subsample1/seed20261004，两臂mean/path-min q10共4fit。群体量必须先在未按label/values_available/training_eligible/模型动作筛选的完整原候选上计算，监督成熟度只随后影响fit行，不能偷换同伴人口或借未来标签确定群体。不叠加M5～M15新块或事后回选控制。原train12D观测g的支持100bps桶>=30行/5D及2.5～97.5%/支持洞不改，UNKNOWN不会缩小或重建支持人口。validation只诊断，test禁止fit/校准/选点。

沿原VALUE_REVIEW_5_V1五有效复评，不叫五交易日；原Top5/Top40 review、buy.95/sell5.95bps一次、T+1/停牌/涨跌停延期、五槽现金0/不Top6补位、净期望>0/下行<=800bps、多段tick/空集/UNKNOWN/float32树parity不改。原train2024-07-04～2025-05-30/val06-03～09-30/已消费test10-09～2026-02-02/label截止2026-03-10仅本研究，非QE全局窗口；通用独立目标选择不在此默定。已有D终值/买价转换不再立项或重复乘gap。

## 6. Registry / Budget / Atomic stages

campaign advisory_candidate_cohort_v1_20261005/model M16/schema economic_candidate_cohort_v1/experiment advcandidatecohort_+plan_sha前24；原M2 budget_anchor同campaign_root，predecessor_manifest_ref role=candidate_cohort_predecessor必须实际M15 evaluated。当前实际51，不宣称59已发生；M14和M15真实各4fit/四臂后才可M16实际登记/prepare，显式candidate_cohort_extension要求limit_state_extension及原完整链，实际59加4上限63，所有旧23～59及原index不改、不重置counter。

typed plan dump-revalidate/干净producer/实施闭包/原recipe/profile先登记，preregistered→prepared→trained→evaluated原子hash链，exact prepared retry不重算/重新SQL，partial_attempt不能重fit。只原reusable_prepared_manifest_ref的D表，0新source SQL/候选重建。source工程可先经设计/审核/currentCI交付，但保留M14/M15/M16各实际对应的冻结WT，不能pull未来routing后伪装exact-retry；fit始终用对应原实施闭包。

每研究固定1800秒/2GiB RSS/2GiB artifact、4fit/0新index。fit前后fresh QE三个running全0，busy/unknown只暂停拟合约30min复查，不控制QE、不将元数据或源码任务算研究trial。NAVIGATION_ONLY/共享开发期/no sealed；负只STOP本candidate/continue_next=true，正NAV只另设计独立确认，不自动启用。

## 7. Evaluation / Evidence tiers / Economic meaning

同81D/1620原候选、100共同NAV日完整baseline/rule/matched/candidate，原shadow政策/成本/held-mark/endpoint/episode全部结算，真model TAKE与UNKNOWN baseline控制分账。干预T映射原D、81分母，群体同日共享变量不能拿stock-day数量当独立证据。

固定开发导航条件不改：两配对日增量各>=5bps、真TAKE>=30、干预各>=12D且>=15%、MDD恶化<=200bps、最差5%共同日均恶化<=20bps，bootstrap5dayblock/2000/seed20261004仅描述。失败不换阈值/窗口/seed/回选matched，记录自身结果即可，不为旧失败补证或判所有QE包无alpha。原生身份/独立OOS/经济确认/真实fill/运行态均另报，不由source/CI/表中统计PASS升级。

## 8. API / UI / Production gates

仅离线日频价格信息研究，不新增family/API/UI/scheduler/binding或资金仓位/分钟执行规则。M1六UI/BUG公共smoke独立待交付，不借新研究通过；无部署/DDL/依赖/重启，无runtime回滚。撤回仅停止新未部署研究调用，原模型/数据不动。

## 9. Verification plan / Design Acceptance Index

三量手算/同股及同市场但同伴不同的可识别新信息、0/1/flat/N1/empty；全原群体缺一键/ret/clock仍保留人口UNKNOWN、不用其它股/Top40/未来数据缩补；known坏数/future/重复/D-T错误与稳定极值几何。source五字段投影/0SQL/原KEY保序、atomic/exact/partial/QEunknown、共同成熟13/16/test毒化/支持不缩样、Dclock/oldbundle、实际M15完整stage/fourheads才能59→63/旧cap拒绝新journal。

至少三轮本窗口合同/依赖、数值/缺失、预算/业务自审与针对修订，F2七项/diff/精确两设计文件/currentHEADCI后交付设计；后续源树登记精确12文件、失败节点修复后稳定五文件最小矩阵/Ruff/两L0/原bundle只读兼容，广回归交CI。不冒称独立外审或模型已生效。

## 10. Implementation plan / Risks / Rollout / Rollback

设计多轮修订/交付→M15源码交付后latestmain独立12文件实现→source多轮审核/直接测试/currentCI→M15真实59/evaluated后新lineage登记/一次prepare→QE idle单次4fit/四臂→真实蓝图进度/必要CI/自己官方清理。最多同一价格主线串行，不因QE等待就开第二训练线或重复旧负fit。原18h10-05 18:08/48h10-06 02:36不重计。

风险为该群体状态可能只是全市场/个股momentum代理、同伴收益相关不保证前瞻价值；三量数学可定义不等于可学。先同监督matched识别本块增量，零新数据/平台/配套治理工程，不宣称新的alpha发现。若正常原群体缺值常见，保留真实UNKNOWN并如实评价控制贡献，不要求补证重跑。工程源码、真实研究、经济确认、runtime四状态分开。

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-970 | §1/3/4；economic_candidate_cohort_v1.py | test: backend/tests/advisory_model_first/test_economic_candidate_cohort_v1.py；artifact: 三量手算/不同同伴识别 | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-971 | §3/4；economic_candidate_cohort_v1.py | test: backend/tests/advisory_model_first/test_economic_candidate_cohort_v1.py；artifact: 原全群体缺失/坏数/未来/N1/empty | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-972 | §3/6；economic_candidate_cohort_pipeline_v1.py | test: backend/tests/advisory_model_first/test_economic_candidate_cohort_pipeline_v1.py；artifact: 原D源0SQL/原KEY/atomic | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-973 | §5；economic_sector_price_value_v1.py | test: backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py；artifact: 同成熟13/16/test不消费/支持 | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-974 | §6；economic_moneyflow_price_pipeline_v1.py | test: backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py；artifact: 真M15/evaluated/fourheads59→63/旧cap | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-975 | §7/8；economic_sector_price_pipeline_v1.py | test: backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py；artifact: 同四臂/TAKE分账/旧bundle | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-976 | §2/6/9/10范围/交付 | test: backend/tests/advisory_model_first/test_economic_candidate_cohort_pipeline_v1.py；artifact: scope/F2/L0/运行边界 | IMPLEMENTED_LOCAL_VERIFIED | none |

## 12. Current state

设计#5481已合入8ffce17015/自身清理；12文件source稳定53项/Ruff/F2/L0/原M1 bundle兼容，#5482 HEAD5f611fe25/currentCI37262710010 SUCCESS后合入7521de2fdd。工程交付时研究0且保留原树；实际M15 evaluated/59后，run advcandidatecohort_99155bf19bf9cf1af6b5fa7e/plan99155bf1.../implementation6e97f322...一次0SQL prepare2.25秒、7720原键全群体AVAILABLE。07:18:22UTC前、07:18:55UTC后QE三count0，一次四fit/完整四臂30.844秒，train4049/214D、validation1591仅诊断。81D/1620候选/100共同NAV日candidate/baseline/matched/rule25.5593/21.3220/15.3803/20.5747%，paired日baseline+3.4848 CI[−19.1286,24.5484]、matched+8.5645 CI[−5.5995,25.6092]bps；83真TAKE/6UNKNOWN控制、89episodes全settled，其它四项通过而net未过原5bps/区间跨零，STOP本candidate。正名义收益不冒称可复现净增量/确认/上线，不重选阈值/变体。真实累计63fit+1旧index，所有旧预算/身份/标签/支持/政策保持；各冻结闭包已消费可精确自身官方清理但正式F产物保留，M1UI/BUG公共smoke两未合入树仍保留，无sealed/独立OOS/公共或QE改动、DB写/安装/激活/服务控制。

## 13. 三轮设计审核与修订

第一轮按原D表schema/原12D与M5/M10范围核对信息可识别性：群体同伴D变化可在该股D信息和全市场宽度不变时改变三量，不把单股ret变换当新信息，也不修改Selection排序。第二轮修订数值边界为scale-normalized稳定计算、quiet NaN/正常缺clock与真实坏数分离、非零Decimal下溢拒绝；完整flat/N1的std0是已知状态，不以退化造UNKNOWN。第三轮修订原群体必须在监督成熟筛选前计算、禁止借Y或已买入集合定义同伴；实际51与未来59/63分开、M14/M15两冻结树保留、原source交付先于研究而研究前驱不省略。补丁曾因两个段落顺序逆向造成匹配失败、未写入；改为文档顺序的精确段落后成功，不改变方案或范围。

DESIGN-COMPLIANCE-001 F-970～976逐项对照原population/clock、数值、0SQL/atomic、共同成熟/支持、真实累计budget、四臂/证据层、边界交付；设计交付时七项满足，实现/模型/经济当时仍0。修订后F2七项、两文件scope/diff和当前HEAD CI通过后方交付；仅本窗口多视角自审而非独立外审。不为原失败候选收集经济补证或占据QE研究资源。

## 14. 源码三轮审核与修复

第一轮合同/时钟核对纯helper只投影原KEY五D字段、每D原群体在监督/动作筛选前定义，未来/外股毒化不消费、少一原成员不改分母；0SQL prepared只复用原D表与原名单、one_to_one保序。第二轮数值/正常缺值核对quietNaN、N1/flat的已知0/1、scale-normalized 1e308手算及非零下溢、真实坏数/未来clock typed失败；新source模板CRLF未命中时在发布前拒绝残留price字段，归一LF再生成且读回修正说明，无数据写。第三轮预算/模型核对M15完整四heads/stages才59→63、model_copy重验证、旧identity/cap公式不变、共同成熟13/16/test毒化/原支持，20定向PASS后修复fixture同日rank重复（不改业务唯一性），失败节点PASS再稳定53PASS/Ruff/F2/L0；原真实M1 bundle metadata/hash只读兼容0fit/行情或label数组/DB。

DESIGN-COMPLIANCE-001 F-970～976工程逐项与源码和直接证据相符；该源码审核阶段模型/经济PENDING；随后真实研究完成而经济仍NOT_CONFIRMED见§12。新prepare one_to_one KEY join与两旧join为三P2：原source/candidates各<=7720、固定每D一次群体聚合总O(N)、键唯一/保序/无笛卡尔，已核对复杂度；不新增缓存/平台来消警。currentHEAD必需CI全绿才按长任务已有授权源码合入；保留未完成研究的冻结source树，不对其pull未来budget/routing而伪装exact-retry。所有旧负fit/20D读回均未重跑。
