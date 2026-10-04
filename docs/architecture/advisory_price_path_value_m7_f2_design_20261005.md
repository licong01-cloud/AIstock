# Advisory 日频价格路径状态条件买入价值 M7 F2详细设计 v1

2026-10-05；EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY。目标仍是收益与风险驱动的买入价格集合，不预测开盘coverage、不研发分钟执行或上游QE alpha。

## 1. Background / 事实、唯一变量与可行性

本次立项时，同一连续R2任务已完成M2/M3/M4/M1/M5/M6，累计23 physical-fit+1 index；除M1正开发导航且区间跨零，其余精确candidate均负向终止。M6源码#5451已合入fa3947c4ede4a8a78918b031475ee48f76f81188；不能在同三资金流上换seed/loss/阈值或补证救活。M1日频/API/UI #5445仍待六UI，BUG-1726仍待公共端点smoke；两交付依赖不封锁其它Advisory设计。M7本次完成后的当前事实与累计27计数见§14。

H-DAILY-PRICE-PATH-1：相同累计收益下，平滑单向路径与反复震荡/已离开近期峰值，是否具有不同的买入后净价值与下行风险？原12D仅有ret1/5/10、ATR、当日close location等摘要；本次显式新增20个D可见复权收盘的路径形状，不换模型族/loss，也不把同基础行情来源说成新独立数据或必然alpha。新增三字段与旧returns可能冗余，需同人口matched比较。

事前工程spike SHA=dac492f1a1b205053e352b546a380e0d4bcc347719e1c49ee242504586b5e2c7，只读取冻结Parquet footer及trade_date/instrument键。原raw_daily.parquet已有380240行、raw_close_cny/close_li/adj_factor；原7720键/386D的20-session键窗7330完整、380原calendar warmup、10缺键，0.469秒；没有价格值/收益/labels/DB查询/fit/sealed。键完整不等于价格/因子有效或经济有效。

## 2. Scope / 精确文件与顺序

设计阶段仅在当时最新main fa3947c4e独立`advisory-price-path-value-m7-design-20261005`树新增本文件，已按审核/F2/当前HEAD CI合入和自己官方清理。源码随后从最新main 131e6606a独立`advisory-price-path-value-m7-20261005`树事前登记下列精确范围：

- backend/services/advisory_model_first/economic_price_path_value_v1.py（固定三字段/plan/M7模型包装）
- backend/services/advisory_model_first/economic_price_path_pipeline_v1.py（原冻结行情投影/阶段薄编排）
- backend/services/advisory_model_first/economic_moneyflow_price_pipeline_v1.py（仅预算核验增加显式M7 extension参数；旧M6默认语义及23cap不变）
- backend/services/advisory_model_first/economic_sector_price_value_v1.py（仅M7字段/身份/status路由，旧M1/M5/M6数学及SHA公式不变）
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py（仅新增M7/27累计budget路由）
- backend/tests/advisory_model_first/test_economic_price_path_value_v1.py
- backend/tests/advisory_model_first/test_economic_price_path_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py（仅显式预算extension/旧默认直接回归）
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py（仅固定M7路由/旧数学直接回归）
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py（仅27与旧15/19/23路由直接回归）
- 本设计和docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

不是另建source/label/simulator/registry平台：没有新DB source，直接复用父prepared原行情，原子stage、JSON核、train/load/evaluate_information_study_v1完整四臂与已有预算锚点。只增两个Advisory叶文件及必要显式路由；不复制旧研究产物或拟合旧模型。future consumer/UI不在本研究范围。

## 3. Non-goals / 权限与证据

不修改QE/Selection/HMM/StrategyPackage/公共数据/行业/Execution/Paper/CI/工作流/AGENTS；不提交QE、不重新候选/池、不重验包资格、不做DB读写/DDL/DML/数据或模型激活/依赖安装/用户服务或其它进程控制。临时X，独立正式工件F，用户 owns backend restart；本离线交付无需重启。现行蓝图分钟特征研究禁令保持。

仅原已消费开发窗，sealed/new holdout不读，确认/实盘效果不声称。旧M6/M5负结果不重跑或作历史固化；M7不是重新评估这些信号，改变的是显式模型信息块。新增lineage不重置多重搜索次数或成为独立OOS。

## 4. Architecture / Contracts / 来源与原名单

复用已知`campaign_sources_v2`的原消费授权、父plan/价格prepared/12Dprepared/value-label prepared/profile/包/股票池/policy hash。原raw_daily.parquet/calendar.json由父prepared manifest验证；读取本次价格/labels前先清洁源码冻结并预登记，不能只信目录/键统计或调用当前Selection重新生成名单。

只投影trade_date、instrument、raw_close_cny、adj_factor；raw_close_cny是父原价格坐标已经规范的元，明确拒绝再转换close_li或猜单位。20-session含D、沿原完整calendar，不把20条稀疏行情当20交易日。source读取按原最大D裁剪，纯计算对每个原D又按自身20-session窗口截取；不能用全批最大D或最后factor作为早D的尺度。原source完整证券集及hash不修改，投影不改变候选人口。

所有原D/T/rank/order/价格坐标和7720键保留，D必须在原calendar且T是下一交易日。对每个u≤D，C(u)=raw_close_cny(u)×adj_factor(u)，按当D已含的历史因子做可比路径；所需比率/log差对常数尺度不变，不用D以后因子归一化。历史DB捕获非vintage/native UNPROVEN继承，不冒称发布时钟恢复或消除历史修订偏差。

重复证券日、未来输入被消费、外来候选/错D/T/hash、bool/坏数/非正价格或因子等矛盾fail closed。NULL、真实缺行情、正常停牌导致缺行、早期不足20-session均逐原键UNKNOWN；不填零/前填/删证券日期，当前输入不能作为未来停止资格。flat path分母0也UNKNOWN，不能默认为0效率。暂停或缺报价只限制该价格模型块，原业务名单仍在。

## 5. Frozen information block / 只三字段

固定最近20个正式交易日s0…s19=D，C[j]>0均完整，r[j]=log(C[j]/C[j-1])，j=1…19。返回KEY、三字段、price_path_feature_status、price_path_feature_visible_through=D；状态固定AVAILABLE / UNKNOWN_20D_WARMUP / UNKNOWN_PRICE_PATH_SOURCE / UNKNOWN_FLAT_PATH。

| 字段 | 精确定义/范围 | 未知处理 |
|---|---|---|
| price_drawdown20 | C19/max(C0…C19)-1，[-1,0] | 任一必需价格/factor未知→UNKNOWN；不是盘中回撤 |
| price_path_efficiency19 | Σr[j]/Σabs(r[j])，[-1,1]，数学上等于端点log差除总路径变化 | 总绝对log变化为0→UNKNOWN，非补零；仅≤1e-12浮点边界容差，不改变有效经济数值 |
| price_up_day_share19 | count(rj>0)/19，[0,1] | 同20-session完整cohort；不把停牌缺记录当平盘 |

三字段都有限且price_path_feature_status=AVAILABLE才有共同mask；新增未知时candidate和matched共同UNKNOWN，原UNKNOWN研究控制分账且全日期保留。既有12D仍独立来源/语义，不能据label maturity/收益/T HLC决定D块可估性。未来价格/factor或收益毒化不改变早D的合法函数；input-g是合法价格坐标，不是预测的开盘或因果限价成交。

## 6. Model / 公平监督与价格支持

matched=12D+g（13维），candidate=12D+三路径字段+g（16维）；同既存GBDT 200 trees/lr.05/depth3/min_leaf30/subsample1/max_features=None/seed20261004/no early-stop，gross Y mean与日级path L q10各两头，共4物理fit、一candidate，不尝试更多窗口/feature组合/seed或loss。

共同mature train keys：12D/三新字段完整、真实label_information_end及T≤train_end、原合法gap位于无标签global支持，最低100行/20D。global支持仍仅无标签train完整12D/原观察g，100bps桶/≥30行/≥5D及2.5～97.5%/洞，不按路径AVAILABLE或收益重筛。跨M1/M5/M6监督人口不同，不借旧模型结果判信息增量。

父实例train2024-07-04～2025-05-30、validation2025-06-03～2025-09-30、已消费test2025-10-09～2026-02-02、label cutoff2026-03-10，全从原plan读、不是公共硬编码；maturity purge不变。validation只诊断，test不fit/校准/择阈值。JSON公开树/float32分裂/sklearn1.8.0/scipy1.16.3 parity，费用buy.95/sell5.95bps一次，expected net>0且下行参考≤800bps；法律tick完整多段/空集/UNKNOWN，不连支持洞。

## 7. Trial / 累计23加4与最小编排

campaign=advisory_price_path_v1_20261005 / model_id=M7；plan固定源投影/字段/20-session/算法/政策/支持/评价/资源/source与implementation SHA。budget_anchor_ref沿原M2 preregistered manifest，predecessor_manifest_ref为实际M6 evaluated manifest；同根F:/Dev/AIstock_model_artifacts/advisory_price_research_campaign_r2_20261004独立experiment_id，原registry/journal只追加。

核定原M2/3/4/M1/M5/M6实际计数4/5/2/4/4/4和一M4 index，新M7最多4，cap27fit+1index。M6预算核验器只接受显式同源/同根/固定M7 extension；旧M6无extension仍23cap且不接受外来study。M7源码先登记该小扩展再实现，原M6源收据/结果不改；不重新解析其负收益，只核定已发生身份/阶段/数量。foreign/duplicate/missing账本及partial拒绝，单测拟合与研究分账。

source/preregister/prepared/trained/evaluated原子阶段复用；prepare只读冻结raw_daily列生成块，与原value rows一对一左合并且order一致。发布后的prepared重用精确快照/hash，不新查DB补历史。source_rows≤500000，candidate≤7720，2线程、RSS/新增工件各≤2GiB、4fit≤1800秒。

fit前后只读QE experiment/custom_evo/multi-alpha三running；非idle或unknown只暂停fit，继续设计/工程，不停止QE。长实验30分钟检查。原18h/48h窗口不重计；清洁源码/四fit预算/源hash/所有窗口先登记，再读取本研究价格和旧开发labels。

## 8. Evaluation / 完整同核四臂

完整原81D/1620候选及共同100估值日，baseline Top5/±300bps规则/matched/candidate；同VALUE_REVIEW_5_V1的Top40复评、五次有效复评、五槽/现金0、A股T+1/停牌/限价延期，no Top6补位。UNKNOWN仅市场证明可执行才作原动作研究控制，分列而非模型TAKE；任一endpoint/held-mark/unsettled不证则经济BLOCKED，不删记录凑净值。

与原baseline及matched两个配对日均net增量各≥5bps，两实际进入差异各≥12D且≥15%原D，模型真TAKE≥30episode；MDD恶化≤200bps，最差5%日均恶化≤20bps。block5/reps2000/seed20261004区间只NAV；不以胜率/coverage/风险改善替代收益，不跨cohort判胜。正仅CONSIDER_CONFIRMATION_DESIGN_ONLY，负仅STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION，未证执行另BLOCKED，不调门重跑。

## 9. Implementation Plan / 验证与终止

设计三视角自审修订/F2/diff/必需CI合入→最新main独立源码树登记§2→纯路径核及薄编排/旧共核显式路由、多轮修复→失败节点优先、稳定后一次小矩阵/F2/L0清洁提交→M7先登记、一次prepare→QE idle才一次四fit/完整四臂→真实进度与源码PR/CI/合入/自己清理。工程≤4h，不为预算凑训练或历史固化；必要源缺只暂停M7。整轮只用户停止/原预算到期/预定有价值路线全部完成或真阻断且无余价值设计时结束。

### 9.1 Verification Plan / 最小直接矩阵

来源轮：父manifest/hash/原键/calendar/D/T、20-session缺日/warmup/正常停牌UNKNOWN、正价/factor/非法数/重复矛盾，复权拆分手算与未来毒化，零DB或source激活。学习轮：三个手算字段/常数尺度不变/flat UNKNOWN，13/16共同cohort及无标签global支持/成熟purge/test不消费/JSON parity/成本/支持洞；新M7路由直接验证，旧M1/M5/M6核不变，不复跑旧研究。编排轮：23+4/固定extension/partial/源快照精确retry/QE未知拒绝，完整四臂和控制/真干预/证据分层。不要复制已有同核快照和数百fixture；广回归交当前HEAD CI。

## 10. Design Acceptance Index

| ID | 验收 |
|---|---|
| F-888 | 独立路径信息假设/三冻结字段，非旧资金流调参或上游alpha |
| F-889 | 原raw_daily单位/hash/自身20-session/因子/名单PIT与正常UNKNOWN |
| F-890 | 同13/16核、共同监督/原global支持/JSON价格集合 |
| F-891 | 固定四臂与净增量/干预/风险/证据边界 |
| F-892 | 先登记与原23+新4 cap27、固定extension/partial/源快照不替换 |
| F-893 | QE互斥、X/F、资源和原任务截止不重置 |
| F-894 | 精确叶范围、多轮审核、源码/研究/经济/运行分报 |

## 11. Design Acceptance Matrix

设计#5452已合入131e6606aee54f3eadd0948a46b71ae8cfe8eefd并完成自己官方清理。源码61ed666992660c06d9818290fdabd31209deb778、30项直接测试及一次正式登记/prepare/四fit/完整四臂完成，见§14；研究结果负向结束当前candidate，经济确认/消费者接入/正式启用仍0。下表验收的是已执行的研究合同，不是收益达标或生产完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-888 | economic_price_path_value_v1.py；§14 | test_economic_price_path_value_v1.py；artifact: 已冻结假设/三字段 | SOURCE_VERIFIED | none |
| F-889 | economic_price_path_value_v1.py；economic_price_path_pipeline_v1.py | test_economic_price_path_value_v1.py；test_economic_price_path_pipeline_v1.py；artifact: prepared/preparation.json完整原7720键 | SOURCE_VERIFIED | none |
| F-890 | economic_sector_price_value_v1.py；§14 | test_economic_price_path_value_v1.py；test_economic_sector_price_value_v1.py；artifact: trained/metadata.json共同3693train/1591val | SOURCE_VERIFIED | none |
| F-891 | §8/14；evaluate_information_study_v1 | artifact: evaluated/evaluation.json原81D/1620候选/100日完整四臂，当前候选净增量条件失败 | VERIFIED_RESEARCH_COMPLETED_NEGATIVE | none |
| F-892 | economic_moneyflow_price_pipeline_v1.py；economic_price_path_pipeline_v1.py | test_economic_moneyflow_price_pipeline_v1.py；test_economic_price_path_pipeline_v1.py；artifact: 原23+新4实际总账27/1index，五阶段registry | SOURCE_VERIFIED | none |
| F-893 | §3/7/14；economic_sector_price_pipeline_v1.py | test_economic_sector_price_pipeline_v1.py；artifact: 拟合前后QE三running均0、零DB/分钟/sealed/激活 | SOURCE_VERIFIED | none |
| F-894 | §2/9/12/13/14 | artifact: 精确10源码/测试、三轮自审修复、30直接项/Ruff/L0及源/研究/经济/运行分层 | SOURCE_VERIFIED | none |

## 12. Risks / 自审与设计符合性

路径字段可能与已用动量/ATR冗余，复权因子历史修订和极低可学习性/多次开发选择偏差不会被键覆盖PASS消除。当前正常缺日UNKNOWN不变，不向数据窗口索要业务样本或给QE策略包另设资格。观察g下的历史价格价值不是最佳分钟成交或因果反事实保证。

第一轮方法/路径自审：明确这是同基础行情的新显式路径信息，不声称独立数据或自然alpha；raw_close_cny×各u自身factor，所有比率/log变化尺度不变，不能用未来因子或最终批D缩放早D；修正log索引歧义和效率数值容差，flat path保持UNKNOWN。第二轮源/监督自审：spike键窗只证明7330行可能可估，不预填最终AVAILABLE；380warmup/10缺键不删除，maturity/global支持及13/16共同mask保留，已消费窗口不冒称独立确认。第三轮编排/范围自审：实际M6预算接口尚无extension，必须在Advisory精确范围显式增加固定M7/同根同源参数，旧M6默认行为不变，不通过改旧产物清账。F2初检缺Contracts/Implementation Plan/Verification Plan标题，修正正文标题及独立验证段，不改校验器或验收语义。均为本窗口自审非独立外审。

DESIGN-COMPLIANCE逐条：设计/源码/经济/运行分开，无子集/mock-only完成宣称；未知/矛盾分层而非silent success；三个字段/政策/支持/成本/数值/预算事前冻结，不事后放宽；来源和范围真实复用、无越界平台/重启/资格/旧失败固化。

## 13. Rollout / Rollback / Production Gates

纯离线：API/UI/binding/activation/DB/DDL/依赖/服务操作全部noop、backend_restart_required=false。无新增消费者功能就不请求重启。源身份或语义变化必须新lineage，原输入/计数/模型结果不覆盖；负向只停止本candidate，不把本设计变成旧失败留档项目。

## 14. 本次实际研究与源码验收（2026-10-05）

独立run=`advpricepathvalue_cca7c7c33e508d2b429682d2`，同原campaign根；plan SHA=`cca7c7c33e508d2b429682d2396578596ae073f69201c73ec2a7da33d9ac45bc`，implementation SHA=`c8aaacc0872a964ae0d466f0347f19340fe4b9bc4b923f3d51fba372dd505985`，真实拟合源码HEAD=`61ed666992660c06d9818290fdabd31209deb778`。先预登记后一次prepare，7.391秒：原7720键全部保留，7330 AVAILABLE、380 UNKNOWN_20D_WARMUP、10 UNKNOWN_PRICE_PATH_SOURCE，原380240行情行按声明四列投影；0数据库读写，不补数据、不查询分钟线、不重建候选。来源仍RECOVERED_LIMITED/native UNPROVEN。

2026-10-04T20:50:20Z读取QE experiment/custom_evo/multi-alpha三running均0后，一次四fit、共同3693成熟train/195D、1591诊断validation；训练阶段含校验7.281秒，训练加完整四臂20.969秒。20:51:33Z三个running仍0。原23加本次4合计27 physical-fit+1历史index，单位测试拟合不计研究，原registry仅追加PREREGISTERED/PREPARED/FIT_STARTED/TRAINED/EVALUATED，无partial或重复研究。

| 指标 | 本次事实 | 结论边界 |
|---|---|---|
| 原完整四臂 | 81决策日/1620候选/100共同估值日；candidate/baseline/matched/rule net=23.8248%/21.3220%/4.5719%/20.5747% | 同VALUE_REVIEW_5_V1/成本/全人口；非指数超额或真实fill |
| 配对日均增量 | candidate减baseline +2.0581bps、描述性95%[-16.2035,22.1539]；减matched +16.9174bps、[5.8398,31.6462] | baseline项低于冻结5bps且跨0；matched正区间不构成整体达标或独立OOS |
| 支持/风险 | candidate真TAKE81、UNKNOWN控制4；matched真TAKE79/控制4；干预/TAKE/MDD/tail条件通过 | 控制不算模型TAKE，不用风险或胜率替代收益 |
| MDD/尾部 | candidate/baseline/matched MDD=-8.1173%/-10.3314%/-11.8944%；candidate/baseline最差5%日均=-230.4099/-254.3254bps | 下行改善仍不能补偿未达事前净增量 |
| 完整性/结果 | 四臂端点限制0/held-mark问题0/未退出0；STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION | 只结束当前候选；0confirmation、consumer binding、sealed、新holdout、数据/模型激活或用户服务操作 |

三轮本窗口源码自审/修复：第一轮来源/PIT核对自身20-session窗口、同session复权因子、手算/拆分/常数尺度/未来毒化、正常缺失与flat UNKNOWN；第二轮公平监督和编排核对共同13/16D、无标签global支持、成熟purge、test毒化、JSON和完整四臂复用、原M6无extension仍23cap；预算测试首次因夹具未创建研究目录失败，修正夹具，不改变产品行为。第三轮边界/可空值检查发现pd.NA允许但float转换可能失败，显式映射正常NULL为缺值并增加针对性反例；最终30直接项PASS，Ruff无问题，两官方静态入口输出X分别0finding及3finding/0blocking，F2通过。未宣称独立外审或借旧研究补证。

源码PR/当前HEAD CI/合入/自身官方清理另按实际状态记录；本次后续文档修改不冒称新HEAD重新拟合。负结果不回选matched、改变收益条件或追加旧失败验证；M1工程依赖仍单独处理，不阻断下一真正不同信息的设计可行性检查。
