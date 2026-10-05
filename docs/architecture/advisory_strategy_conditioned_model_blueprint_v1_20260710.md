# AIstock 荐股策略条件化模型体系 F2 架构蓝图 v4.33

> 初始日期：2026-07-10
> 修订日期：2026-10-05

> 现行消费原则（用户2026-10-04明确指令）：QE负责其全部实验及进入策略包组合的无未来数据泄露，Advisory直接使用策略包，不再重复设置父/processor/组合时钟、资产评分、原native收据、收益确认或MDE功效等准入/研发门禁。缺旧conf或data_split不安排补证/重训；价格模型效果诚实展示，不承诺收益。真正输入矛盾、损坏、缺推理依赖及Advisory自己的D/T未来读取仍按正常计算错误处理，正常缺失保留UNKNOWN/原候选。历史试验及旧合同原值不改判，其中曾用于晋级/准入的要求只解释当时结果，不是现行包消费或项目停止条件。实现范围与当前执行顺序见[直接消费F2](advisory_qe_package_direct_consumer_v1_f2_design_20261004.md)及§16。
> 本版方向：业务目标、六层架构及Advisory/QE所有权不变。R1/#5388与R2三路线/#5404已交付并停止各负candidate；[M1](advisory_sector_dynamic_price_value_v1_f2_design_20261004.md)一次四fit/完整四臂已完成，#5414交付。100共同估值日candidate/baseline/matched净收益29.7444%/21.3220%/23.1821%，配对增量6.9758/5.3007bps，真TAKE36、UNKNOWN控制57，两增量区间跨零，经济确认/启用0。重复QE资格与M1正后全局停止已由#5428/#5429撤销；M5已负向完成。[M6](advisory_moneyflow_price_value_m6_f2_design_20261005.md)#5450/#5451及[M7](advisory_price_path_value_m7_f2_design_20261005.md)#5452/#5453均已合入/自身清理，不重跑负结果。M7相对原基线日+2.0581bps未过事前5bps。[M8市场时序风险](advisory_market_risk_price_value_m8_f2_design_20261005.md)#5454设计已合入/清理，源码39项直接测试及一次7720键prepare/四fit/完整四臂完成；candidate 12.5727%低于baseline 21.3220%，配对日-7.7890bps且区间跨零，停止本候选，源码#5455已合入476537d77并自己官方清理。实际累计67fit+1index不清零，不把风险改善当盈利。M1日频/API/UI #5445已同步main并push d1ccaae19，新CI37240066029 SUCCESS且六UI未执行、BUG-1726仍待公共端点smoke，两工程依赖不阻断其它真正新信息设计。[M9日频量价状态](advisory_volume_context_price_value_m9_f2_design_20261005.md)#5456设计已合入/清理，43直接项、一次准备/四fit/完整四臂完成；净收益25.9681%高于基线21.3220%，但配对日+3.9776bps低于事前5且区间跨零，仅停止本候选，源码#5457已合入4f7793a0f并自身官方清理；不降低原条件或开展QE因子/分钟线。[M10宽度历史](advisory_breadth_state_price_value_m10_f2_design_20261005.md)#5458设计合入/清理，38直接项、一次7720键准备/四fit/完整四臂完成；candidate11.4050%<baseline21.3220%，配对日-8.8603bps且区间跨零，仅STOP本候选，源码#5459已合入7aed83cc6并自身官方清理。可选独立确认不作包消费或研发前置，sealed不读、旧结果不改判、不回选matched或补证救活。 已完成[通用日频输入F1](advisory_generic_price_input_v1_f1_design_20261005.md)已通过设计#5460合入及四文件叶实现：九字段不强制父score/rank/腿，不把输入可移植冒称通用权重；独立收益目标另决策，不改原退出。 26直接项/Ruff/两L0及原20候选真实D输入通过，源码#5461已合入0c33faa5c并自身清理；该输入功能未接现有family，不增加fit或升级效果。 [M11成交量加权价格分布](advisory_traded_price_distribution_m11_f2_design_20261005.md)设计#5462已合入/清理，12文件叶实现51直接项/Ruff/F2/L0通过；一次prepare/4fit/四臂完成，candidate22.5023%/baseline21.3220%、配对日+1.0512bps<原5且尾部恶化46.9917bps，仅停止本candidate，实际43+1，源码#5463已合入f6aef87db/自己清理。现有明确标签的一条新信息研究不替用户选择独立通用label；[M12历史隔夜/日内路径F2](advisory_session_path_m12_f2_design_20261005.md)设计#5464已合入5bebffe93/清理，12文件源码42直接项/Ruff/L0和M1实际bundle兼容通过，一次四fit/完整四臂已完成，candidate4.8425%<baseline21.3220%、配对日-14.8813bps区间跨零，只STOP本candidate，实际47+1/0确认，源码#5465已合入1f85b44ff/自身官方清理；M13自由流通信息研究已一次完成见§16.6.6，累计51+1，源码#5470已合入e86e2ca9c/自己清理；M14/M15/M16工程源码#5478/#5480/#5482均已合入，在各原冻结实施闭包内等待QE空闲后于2026-10-05 15:14～15:19上海时间依次完成一次prepare（M14已准备不重跑）/各四fit/完整四臂，真实累计63fit+1index。三者candidate净收益20.2050%/14.1018%/25.5593%，共同基线21.3220%；相对基线日增量−0.9490/−6.2150/+3.4848bps，均未满足原净增量条件且区间跨零，只停止各自身candidate，不调门槛/回选matched/补证或启用。M16源码#5482已合入7521de2fdd；三研究已完成后才精确官方清理自身冻结源树，清理状态另报；M1六UI/公共BUGsmoke仍独立待交付，不阻断真正新信息研究。 M17有序资金流路径已一次完成并停止本候选：候选16.2467%对基线21.3220%，paired日−4.3050bps/区间跨零，67+1；源码审核通过/待currentCI交付，详§16.6.10。
> 本轮最新事实：[H-VALUE-ANCHOR-1](advisory_economic_value_anchor_v1_f2_design_20261003.md)已完成设计、内核及一次同场景三臂研究，run=`advvalue_a4bc66a30cfa7d9d5078850c`。基线/常数锚/D模型在100共同估值日名义净收益`21.3220%/25.4597%/17.4642%`；D模型减常数/减基线日增量`-6.9022/-3.4196bps`，两个描述性区间均跨零。虽模型MDD/胜率改善且有58个实际进入差异日，仍未满足预注册收益条件，停止当前candidate，不回选常数控制、调整阈值/期限/seed或扩窗补证。独立VALUE_REVIEW_5_V1未改变生产退出，不能与旧19.17%跨场景判胜；经济确认/ENTRY_VALUE启用仍0。设计#5344及内核#5346已合入；研究源码#5347的合入状态见§16。
> 上一轮H-TIMING-1事实：计算/来源PR #5331已合入`592fd305f9147ddfc3eaf87b8bed52d905352bed`；一次配对研究已完成，run=`advtiming_cd2ddd9832129255c14f9a80`。新15字段/同核13字段/原基线在100共同估值日的成本后名义收益分别`3.0673%/1.6174%/19.1729%`；新增两量相对控制日增量`+1.0789bps`，但相对基线`-15.0261bps`，未满足预注册的两个正增量条件，停止当前candidate、不进入消费者接入或确认。研究源码PR #5336已通过CI并合入`5ec8c8e2d1deee16a5587afd604d61481162669e`；工程交付、研究结果、经济确认和生产启用分报。下列2026-10-02及较早2026-10-03接续段是历史实施检查点，其待办只以§16的最新队列为准。
> 文档类型：F2 顶层架构蓝图，`docs-fast-update`
> 当前状态：`ECONOMIC_ENTRY_NOT_CONFIRMED_DAILY_SOURCE_MERGED_RUNTIME_READONLY_VERIFIED`（2026-10-03）。独立entry、历史四阶段及每日接入经PR #5099合入，merge=`ad6d73e591a1666490cffae84f6518d8c2694efd`；用户重启及BUG-1623、BUG-1632/1636语义验证已完成。BUG-1640 / PR #5150已合入，重启验收及close-sync PR #5190完成；原29日/580样本四阶段探索回放NOT_CONFIRMED，仅NAVIGATION_ONLY。原分布v3/v4和P0/N3研究结论不改判，零新binding/数据库写入。新的主动目标是收益与风险驱动的条件式买入价格建议，开盘分布仅为辅助能力；收益型v1/v3/v4均已有真实研究但尚无经济确认模型，不能继续调coverage或风险阈值。每日消费者#5324已合入68ff7aaaa，必需CI通过，用户重启后health/identity及三个实际Program的只读状态验证通过；当前无合格ENTRY_VALUE角色，NOT_CONFIGURED且不自动捕获；十三字段新模型日常路由尚未实现，独立12D计算/只读输入已交付。设计、源码、业务交付、模型效果和正式启用分别报告；本页下方有明确日期的旧检查点是历史事实，不是自动待办。
> 当前价格/执行边界：Advisory只研发日频PIT价格分布、条件式买入价值和日频退出价值建议，不研发分钟线择时、最佳分钟买卖点、拆单或成交执行策略。收益型建议与开盘分布采用独立目标和证据，不以coverage证明盈利。未来QE、Paper或Execution可通过版本化只读合同独立消费建议，但执行研发和激活仍不属于本蓝图。
> 最新接续：2026-10-02用户批准按调整后目标开始超过10小时长任务，计划12～16小时按§16.1顺序执行。20日恢复证据均非原生、其中三日完整成员未证明；完整候选和日期不变。新的[收益型买入设计](advisory_economic_entry_value_v1_f2_design_20261002.md)先明确价格条件化净价值、风险、真实观察点、支持域和matched比较，不以重新命名旧M4完成业务目标。
> 2026-10-03当前接续：十小时任务已交付四字段F1 PR #5301、十三字段设计PR #5303，并实现六叶模块离线源码及一次真实study `advinfo_153e00c8509484155d3c8ae0`。同eligible九字段/十三字段2配置4头、1candidate，100日名义净收益9.6268%/19.0748%，baseline19.1729%；十三相对baseline日增量-0.1545bps、95%区间跨零，仍EXPLORATORY_NOT_CONFIRMED/NAVIGATION_ONLY。27笔实际模型TAKE（16正收益）与8笔UNKNOWN基线控制分开；不是独立OOS、沪深300超额或真实成交。随后消费者六项浏览器验收已由专用隔离runner通过，当前进入源码CI/PR交付；每日新family分派与原生资格另行交付，不以研究替代UI。完整实施证据见[十三字段设计§13](advisory_economic_entry_information_v4_f2_design_20261003.md)。旧v3及原生身份限制不改判，sealed/新binding/DB写入/服务控制均0。
> 2026-10-03后续事实：离线源码PR #5304已经合入`dd92ba849b59a8ee5c80fd051e598f1f83ba009d`，必需CI run37070978918全绿。[只读模型消费F1](advisory_information_v4_readonly_bundle_f1_20261003.md)经PR #5305合入`cb8415a05c25514ec0f4dd6d39c7a338aa16806c`、必需CI run37074721633全绿，主线真实权重读回成功；普通CRLF树原81D/1620行预测一致，14定向测试通过（含并发权重变更拒绝）。旧训练/续跑实施字节门禁不改，scope继续NAV/unproven/nondeploy；两项源码交付移出待办，模型读取不等于每日API/UI或经济激活。旧精确研究树和未合入消费者树被动保留，不新增旧研究续跑或清理项目。
> 2026-10-02风险接续：v2风险/每日身份设计PR #5233合入19719beb；独立标签审计及D网格内核27直接测试和L0通过，真实prepare保留7720条原候选。31条进入/退出开盘可执行性未证明，使原train/validation eligible变化22/2，因此按已审定合同停止旧return权重复用，新增研究模型0，不能声称v2模型失败或有效。后续优先显式审定同一entry-loss目标下双头eligible一致的新拟合方案，再做一次固定导航；日常API/UI尚未实现。无新alpha研究/数据库/服务操作，旧结果与原生身份限制不变。
> 2026-10-02当前实施：v2内核PR #5242已合入ea659553；[一致监督集合双头v3设计](advisory_economic_entry_aligned_cohort_v3_f2_design_20261002.md)PR #5245已合入d638dc3d，源码PR #5249已合入c4ce9566。Top5 TAKE27/SKIP368/UNKNOWN10；实际模型进入16笔（10盈利），UNKNOWN控制9笔。模型/基线名义收益9.63%/19.17%、MDD -5.26%/-10.23%，配对日增量-8.888bps、固定block95%区间[-29.3835,12.0840]。有干预但增量未通过，停止本candidate、不改800/参数/窗口、不激活。每日消费者设计PR #5250已合入；新增源码55定向测试通过，真实81日完整D网格/API读回已验证，仅受限导航。消费者源码处于本地验证阶段，浏览器/CI/正式验收和源码PR/合入未完成（§16.1）。旧v1模型TAKE0和v2复用generated0不改判。
> 当前能力基线：Top5、收益/周期、价格范围和页面/API 均有真实实现与独立验证，但尚无当前同时提供四类输出的组合bundle。`AdvisoryDailyPriceEnvelopeV1`源码、v3三头真实训练 artifact `30e8a75b...` 和 v4 validation-only 校准 artifact `508fedfe...` 均已完成；validation/test 覆盖率分别为 `0.811702/0.733125`，校准扩张量为 0。PR #4732 / merge `3be76e742...` 已交付独立自然前向收集通道；首次正式 request `advprpros_405d704a7dbe866eb0b6ae0e` 和 prediction bundle `631d5011858684403d84d1d0975ec0e0642c1d8f52ebd62048829187a1e5435c` 在 T=`2026-09-15` 开盘前完成，20/20候选可用并通过exact retry，且零目标结果访问、零binding、零数据库写入、零sealed holdout消费。T日18:00后自然settlement `advprsett_14c46af1fa2bdb92088b425c` 已发布：20/20市场及模型可用，业务coverage `0.70`、lower/upper miss `0.25/0.05`、平均/中位宽度 `288.615/249.832 bps`、平均/中位mid误差 `116.422/63.188 bps`；单日只进入`ACCUMULATING`，不能选择或激活模型。PR #4785 / merge `8021790ab...` 又交付固定rolling-20D matured CQR历史导航审计；正式 request `advpradapt_8c81ea2bd2f70d75e1683fe9` 在已消费80日回放上selected=0，故保持静态v4、零新binding和零运行时激活，P0-D exact descriptor仍只绑定meta-label shadow，M3/M4 child typed unavailable。
> 2026-09-28接入核查：上述v4与历史结果不变，权威本地price prospective目录仍仅1日20行；自然CLI未自动接入scheduler。当前P0-D分支及V1 M3依赖阻断独立entry输出。新详细设计已补齐独立角色、新窗口确认、Program级binding和每日接入方案，源码及确认均待执行。
> 当前价格历史回放：BUG-1512 已补齐与自然前向证据隔离的两阶段批量PIT回放。正式 v4 已消费test窗口 `2025-11-07..2026-03-10` 一次处理80个决策日/1600候选，耗时5.4秒，数据库历史结果1600/1600可用、零停牌、零不明缺行、零crossing；连续模型gap空间coverage为`0.733125`，最终0.01元tick/涨跌停投影后的业务价格coverage为`0.81375`，rounding rescue/harm为`129/0`，平均区间宽度`141.74 bps`、平均mid误差`47.52 bps`。receipt `advprhist_23e8ce5a...`固定为`HISTORICAL_REPLAY/NAVIGATION_ONLY`、不读sealed、不激活。固定的rolling-20D matured CQR在75个active交易日/1500行上把连续coverage从`0.731333`提高到`0.782667`，但按交易日聚类bootstrap的增益点估计仅`0.012667`、95%区间`[-0.003333,0.030000]`，下界未过0；因此lineage以selected=0终止，不调参、不绑定。18:00只约束自然前向结算，不再阻塞功能、回归或历史统计验证。
> 当前策略包边界：荐股编排和动态 binding 已支持按 Program 解析不同 StrategyPackage，但当前学习模型不是“策略包无关模型”，未来共享预测层也不豁免包/政策条件验证。Top20 候选来自目标策略包，M5/P0 重排、M3 outcome/holding 和 M4 价格区间均绑定该包的候选、父 Alpha 特征、manifest/style/runtime semantics 与 exact descriptor；当前只有目标多 Alpha 包具备模型 bundle，其他包无 bundle 时基线继续且模型 typed unavailable
> 当前源码/运行时：P0-A/P0-B/P0-C/P0-D、descriptor rotation/maturity修复、forward evaluation、历史虚拟前向、P0-E至P0-L Stage A、N0控制面、QE Alpha generator、N3融资融券、财务事件及同包评分/市场/HMM辅助准入源码均已进入`main`。Advisory指数股票池消费与正式forward的D-1 universe as-of切片、日频`DAILY_DB_ONLY`价格合同已合入并经用户重启激活；2026-09-13合入后只读smoke确认运行时源码身份为`b5e1270a...`，D=`2026-09-11`、T=`2026-09-14`逐股使用`market.kline_daily_raw.close:2026-09-11`且零TDX调用。正常停牌缺少D-1行时保留候选、使用更早最后收盘价并记录真实价格日；只有截止历史无有效价格或查询失败才typed fail closed。QE交付消费预检已由PR #4629合入，merge commit `56273a91...`；同批次基线/候选Historical Range可交互对比已由PR #4635合入，merge commit `aacf717a...`。2026-09-14用户重启后的只读验证确认FastAPI健康、运行身份为`174682552...`、Advisory scheduler为running/thread_alive，两项endpoint均已加载：交付预检对全部6个非退役旧包返回`BLOCKED`，共同原因为`PACKAGE_ASSET_INELIGIBLE:runtime_asset_admission`且股票池为`LEGACY_UNIVERSE_UNSPECIFIED`；两个既有完成批次的比较均按`SUMMARY_POLICY_HASH_MISMATCH`返回`INCOMPATIBLE`、delta为空且不声明winner/significance。随后主线前进至`d3b1eb1e...`，新增差异仅为CI、工作流和BUG元数据，不含Advisory或后端业务源码，因此不要求再次重启来重复验证本次Advisory激活。当前future exact正路径只等待QE正式交付满足运行时资产及冻结股票池身份的新包；Advisory不补跑QE实验、不放宽准入。对比功能不生成新summary或研究证据。Score/HMM v1 正式 bundle `f8da2f70...`已完成三个可执行arm且selected=0，sector两臂因当时canonical source缺失保持NOT_RUN；失败分解已排除target错接/符号反向并确认当前信息集没有可靠阈值增量。因果Admission v2.1 R1源码经PR #4395合入，跨OS clean-repo与多arm registry阻断分别由PR #4402/#4404修复；clean `main@a317f7c2...`正式bundle `7e739be5...`完成固定两臂并selected=0、outer未读、registry/route exact retry通过。N1 bundle `74827d03...`、N2-A bundle `6784df1a...`、N2-B v2 bundle `bcdcb31d...`、Entry/Exit action bundle `5c5946a7...`及Exit fixed-information learnability bundle `03d17a18...`均已完成且仅为开发窗口诊断。N3 QE上游Alpha MVE `09137f0c...`、父包增量overlay `fdca2130...`、腿间共识/分歧 `42ac23b6...`、分钟信息集 `0076a3a6...`、自动generator `9327330c...`、融资融券 `b50411d8...`及财务事件 `ad234f4c...`均已正式完成且selected=0。上游HMM的direct-v2 v3适配已由PR #4343合入，G2-A v1.2输入构建、15-fit battery、两个12-fit fresh process与39-fit执行器已由PR #4353合入；但v1.2仅完成17/39 fits并结构验收停止，尚无通过完整acceptance的canonical development OOF；tail、repository/API/UI/DDL和runtime均未完成。生产descriptor仍指向P0-D exact bundle，只激活`meta_label_take_skip_confidence` shadow role；M3/M4是已实现但当前descriptor未组合的独立历史bundle。上述研究与上游实现均不修改baseline或运行时；交付预检与Historical Range对比均不改变score、rank、模型、候选生成或任何QE公共代码
> 当前离线价格审计交付：PR #4785 / merge `8021790ab4fbbb1758d0bdc5a0904dc22686ce29` 只新增Advisory离线contracts/service/CLI/tests及F2设计，不修改router、scheduler、QE、数据库或运行时binding，因此源码合入无需后端重启来执行正式历史审计，也不声称当前后端已加载这些离线模块。
> P0-J权威结果：正式request `advselpriorresreq_3a50e2f6fd9cf43cb1f6ad3e`在首条outer path的inner block 3形成完全平坦的decreasing-isotonic prior，按预登记条件以`ADVISORY_P0J_SELECTION_PRIOR_DEGENERATE`停止；evidence-only bundle为`eb8ade9b...`，exact retry返回同identity，零trial、无winner/PBO/Stage B。该结果证明rank-to-return单调关系跨时间分区不稳定，不证明Selection全局无效
> P0-K权威结果：源码、PR/CI、合入和正式 Stage A 均已完成。request `advselgatereq_943f9e551d5fee35e57340cc`完成`168/168`，bundle为`fee9b561...`，结果`NEGATIVE_STOP_NOT_ADVANCED`且未激活。168条trial全部选择`0.4`、拒绝数均为0，策略与Selection恒等；liability日Spearman约`0.254589`，但约束选择器没有让信号进入决策。`PBO=1.0`来自六个arm的block分数完全相同和固定tie-break，不按普通过拟合解释
> P0-L权威结果：BUG-1251修复后的正式request `advp0lreq_b86425d3b5ce508904fa01b0`生成evidence-only bundle `4476afeb...`。第一条outer path的identity control精确复现P0-G但无真实干预；gain `12/8/4/1`分别产生`33/71/85/85`次实际entry变化并把OOF换手从`0.276692`降至`0.272180/0.272180/0.269173/0.269173`，均低于P0-D预算`0.299248`，但cash day从`1`增至`2`、active-slot coverage从`0.999248`降至`0.998496/0.997744`，不满足冻结完整性合同，以`ADVISORY_P0L_LOCAL_RERANK_INFEASIBLE`在`0/168`停止。结果为`NEGATIVE_STOP_INCOMPLETE_CPCV`，无winner、无可计算PBO、无Stage B、无激活；exact retry返回同一bundle identity
> 最近一次已核验的生产前向状态：2026-09-14 00:21（Asia/Shanghai）后端健康且Advisory scheduler运行正常；两个ENABLED Program最新复评日均为`2026-09-11`且状态`SUCCEEDED`，本轮目标日`2026-09-14`处于`WAITING_DATA/TARGET_OPEN_SETTLE`，reason为`DATA_UNAVAILABLE target-open market rows are unavailable`。该时点早于目标日开盘，属于尚未到达可结算数据边界，不是荐股失败、市场主动SKIP或需要修复的系统错误；后续由既有调度自然结算，不占用主动研发工时。多Alpha模型自然成熟证据仍独立积累，历史回放不得冒充该前向证据。
> 当前模型质量：M5A/M5B/M5C均不建议激活；P0-D虽在CPCV相对matched Selection Top5提升`3.6556 bps`、path win rate `64.29%`，但历史虚拟前向累计净收益、回撤和换手均劣于Selection。P0-E至P0-L没有一个满足既定Stage A晋级合同。当前有真实工程能力、局部预测信号和局部指标改善，但没有已证明可稳定替换Selection的荐股主模型
> 演进结果：P0-D历史虚拟前向证明二分类概率不能稳定表达收益幅度；P0-E outcome weighting同样负向停止。P0-F/P0-G保留收益提升但未满足换手。P0-H把相对P0-D换手压低`0.022708`并改善MDD `0.010688`，但return head日Spearman仅`0.041731`、PBO `0.90`；P0-I/P0-J证明grouped rank与Selection rank收益先验跨分区不稳定。P0-K证明liability信号虽稳定，但绝对阈值策略退化为Selection identity；P0-L进一步证明在冻结P0-G anchor、最大位移1和每日一次相邻交换的动作空间内，真实干预会降低coverage并增加cash day，冻结可行集合为空。P0-D至P0-L研究族已经事实收敛并正式冻结，不再派生P0-M或继续同数据、同候选、同特征和同模型族的局部变体
> 历史验证零工作状态：44 日 A/B/C v6 golden 与 P0-D 历史虚拟前向结论保持只读；24决策日+20日tail的权威 artifact 为 `fbf072f0d8c4a637a48aa8c2ed63c3b61c245abd08ac4e1417b2a0fcc8eb59a9`。该窗口已被 P0-D 质量判断消费，不得在后续调模后继续标为新的 OOT，也不得为补账、固化或归档重新投入主动工时
> N1权威结果：canonical PIT覆盖5067只股票/5077段；386个决策日、19300条Top50标签、382个可评价日。全市场Top5赢家的Top20/40/50平均召回仅`0.8808%/1.6062%/1.7617%`；Top20内perfect Top5成本后增量为`1314.27 bps/五槽日`，95%区间`[1136.61,1516.57]`，oracle为`HIGH/DIRECTION_GATE`。固定Ridge cross-fit增量为`98.83 bps/五槽日`，95%区间`[1.18,193.73]`，MDE `136.01 bps`，为`INCONCLUSIVE/EXPLORATORY`。综合typed结果为`INCONCLUSIVE__THEORETICAL_HIGH__LEARNABILITY_HIGH`、`direction_ready=false`；不得据此激活ranker或跳过N2
> N2-A权威结果：正式request `advalpha3req_e7295a31a4e1953e9048cec5`、bundle `6784df1a...`覆盖386日和1,710,301条共同signal outcome；父包与N1 ranking精确parity。LSTM/FUND/父包RankIC分别为`0.11677/0.05628/0.12284`，Top5 H20净超额均值为`397.89/245.73/446.52 bps`。父包相对FUND的RankIC与Top5增量区间均大于0，但相对LSTM的RankIC增量`+0.00607`区间`[-0.00223,0.01484]`、Top5增量`+48.63 bps`区间`[-103.99,186.53]`均跨0；现有Alpha主要由LSTM贡献，FUND较弱但不同源，固定IC组合尚无确认性证据优于LSTM。父包Top50召回虽为随机期望`1.75×`，绝对值仍仅`1.7617%`。结果为0-trial `ORACLE_DIAGNOSTIC/NAVIGATION_ONLY`，sealed holdout未读，不支持调权、激活或生产替换
> N2-B权威结果：v2 bundle `bcdcb31d...`在同一386日PIT/H20/cost窗口比较当前父包与两个合格独立旧包。当前父包RankIC `0.12284`、Top5净超额`446.52 bps`；`pkg_378eb9...`为`0.00107/166.32 bps`，`pkg_5a5ccb...`为`0.01124/101.88 bps`且Top5区间跨0。父包相对两包的RankIC与Top5配对增量下界均大于0；没有旧包可替换父包。当前父包Top50全市场赢家召回仍仅`1.7617%`、上界`2.5907%`，平均每日约`0.088`个全市场Top5赢家，显著低于预注册“一日至少一个赢家”的`20%`结构门槛
> N2 Entry/Exit权威结果：Entry动态Q90具备干预支持但配对lift显著为负；固定3%/5%正point均因仅3至5个干预日而欠功效，没有confirmatory-positive arm。Exit clairvoyant oracle为`386.60 bps/episode`；固定22项T-visible Ridge learnability正式结果为`-56.81 bps/五槽entry-day`，95%区间`[-200.33,52.62]`、MDE `181.29`，1928 episode/384 entry-day支持充分但`INCONCLUSIVE/NAVIGATION_ONLY`，不能把oracle上限或liability信号当成可学Exit策略
> N3上游Alpha MVE权威结果：request `advqemvereq_28ac7e998080dd2258cf4c23`、bundle `09137f0c...`在386日、1,709,387条current-parent outcome上完成`24/24/24`；elapsed `91.21s`、peak RSS `7.42GB`、temp `175.1MB`。24个单信号Top5相对父包lift全部为负且family-wise下界均不大于0，因此`selected=0`、next task=`N3_ALPHA_INFORMATION_SET_REVIEW`。同时有5个全窗口proposal和1个下行regime proposal的family-wise RankIC下界为正、父包相关均低于0.8；这不能改判本轮frontier，但说明“没有独立替换者”不等于“没有可组合弱信号”
> N3父包增量overlay权威结果：request `advn3ovlreq_152dc894211c967347155ceb`、bundle `fdca2130...`完成`24/24/24/0`；elapsed `94.25s`、peak RSS `2.14GB`、temp `202MB`。所有24项均形成真实干预，但family-wise Top5成本后lift下界全部不大于0；exact retry为registry duplicate-noop和route exact-noop。该结果关闭当前六信号小权重overlay frontier，固定next task=`N3_ALPHA_INFORMATION_SET_EXPANSION_MVE`
> N3腿间共识/分歧权威结果：request `advn3legreq_d267b3646b727505db3274c6`、bundle `42ac23b6...`在clean main `4cd08263...`完成`2/2/2/0`；382个paired-evaluable日中380日形成真实干预，support充分。expanded相对parent的RankIC delta/Top5 lift为`-0.003564/-151.22 bps`，family-wise下界为`-0.006159/-332.91 bps`；相对linear为`-0.001469/-58.02 bps`，family-wise下界为`-0.003176/-172.00 bps`。expanded与parent日均score Spearman仍为`0.98209`，说明显式腿间交互是近似重表达且经济结果退化。inspect与exact retry通过，selected=0，固定next task=`N3_MINUTE_INFORMATION_SET_MVE`
> N3分钟source-ready权威结果：target-free/PIT全扫描读取386日、1,710,301个N2-A键和12个minute字段，不读取target/label/sealed/DB/network/runtime。raw结果为1,695,153 complete、13,473 partial、1,675 whole-day missing；三个241-slot日期的13,461个partial均为每个候选恰少一个合法OHLC bar，按session分类归一化后为1,708,614 complete、12 partial、1,675 whole-day missing，any-bar availability为`99.9021%`。实现期真实Qlib烟测确认各股票缺失位置并不完全相同，故正式代码保留raw `240/241` coverage并另报`SESSION_WIDE_SINGLE_BAR_DEFICIT`，不再把它误写成严格的全市场共同`13:00`空slot；正常缺失全部保留，不删股票、不删日期、不移动bar、不填零。总耗时1,282.87秒、峰值RSS 896,438,272 bytes；corrected multi-span manifest覆盖100%。source-ready receipt为`F:/Dev/AIstock_model_artifacts/advisory_n3_minute_source_spike_v1_20260903/source_spike_receipt.json`，SHA256 `20b2f763...`
> N3分钟MVE权威结果：bundle `0076a3a6...`在386日、1,710,301行上完成`2/2/2/0`，两个trial每行恰好7 OOF。384个可评价日全部真实干预；candidate RankIC/Top5净超额为`0.09020/128.31 bps`，parent/comparator为`0.12284/443.65 bps`，delta/lift为`-0.03264/-315.32 bps`，四项family-wise门槛全部失败。elapsed `853.45s`、peak RSS `2.33GB`、temp `184.27MB`；exact retry同bundle、registry duplicate-noop、route exact-noop。该frontier已消费关闭，不证明所有分钟信息全局不可学，但证明当前八聚合+冻结Ridge不能提供增量
> N3 QE Alpha generator权威结果：最终prompt-v2 request `advqegenreq_7b0e785b3c0f05a2ef2ceb39`、generation bundle `5f6ff834...`和经济bundle `9327330c...`均从clean main完成；6次调用生成24项，23项完成经济评价，selected=0。最佳Top5 lift point仍为`-5.21 bps`，23项累计family-wise Top5 lift下界均不大于0，四段稳定性全部失败；elapsed `347.40s`、peak RSS约`15.37GB`。结果为`EXPLORATORY/NAVIGATION_ONLY`，sealed=false，未写因子库、StrategyPackage或运行时；daily/static grammar generator lineage关闭，next task固定为`N3_UPSTREAM_ALPHA_NEW_DATA_SOURCE_MVE_DESIGN`
> N3融资融券信息集权威结果：正式request `advn3margreq_0e6f63e49ae12d5b2e79f789`、source bundle `633c4058...`和经济bundle `b50411d8...`完成`3/3/0`。candidate RankIC/Top5净超额为`0.11385/252.80 bps`，父包为`0.12284/443.65 bps`，相对父包delta/lift为`-0.00899/-202.21 bps`；四个时间块没有一个同时取得正RankIC增量与正Top5 lift。323个共同可评价日低于预注册支持度要求，故结果为`EXPLORATORY_INSUFFICIENT_SUPPORT/NAVIGATION_ONLY`且selected=0；sealed=false，数据库/网络/因子库/StrategyPackage/runtime写入均为0。该结果关闭本次十二项融资融券动态+冻结Ridge的精确frontier，不外推为全部融资融券信息不可学，next task为`N3_FINANCIAL_EVENT_SOURCE_READINESS_DESIGN`
> N3财务事件source-readiness正式结果：F2 v1.2与源码经PR #4288合入merge commit `5f2fda091...`。clean main正式bundle `211b8db1...`在单一repeatable-read/read-only snapshot中选择三类raw的最早本地观察版本，保留84,272行projection，其中qualifying 44,953、neutral 39,319；120交易日Top20 disclosure/qualifying支持为`100%/87.8756%`，Top50 qualifying为`83.4974%`，378/386日具有Top50混合干预。forecast/express/fina event-type revision drift为`0/0/1.6528%`，耗时9.937716秒、采样RSS 1,089,490,944 bytes、temp 7,114,549 bytes、8次SELECT且DB write/network/Tushare均为0；inspect通过，第二次deliver为registry duplicate-noop与route exact-noop。因数据最早于2026-05本地回填，证据固定为`DATE_ONLY_BACKFILLED_NON_VINTAGE/NAVIGATION_ONLY`，只放行事件信息集MVE设计
> N3财务事件信息集正式结果：clean-main bundle `ad234f4c...`完成固定`3/3/0`；signed-content candidate 的 RankIC `0.063685`低于parent `0.122839`，Top5成本后净超额`359.41 bps`低于parent `443.65 bps`，四个时间块无一同时取得正RankIC增量与正Top5 lift。结果为non-vintage `EXPLORATORY_NOT_SELECTED/NAVIGATION_ONLY`，不进入vintage source、confirmation或activation；route按预注册结果进入同包评分/市场/HMM辅助准入实现
> N3同包评分/市场/HMM正式结果：request `advscorehmm_2a442c84ecdac872a4e56e45`、bundle `f8da2f70...`覆盖386日/1,930个Top5槽位。score-only、raw-market、market-HMM三臂分别只在3/5/9日TAKE，日均相对父基线lift为`-26.71/-30.77/-28.55 bps`且区间均为负；正式结论`AUX_EXECUTED_FRONTIER_INSUFFICIENT_SUPPORT`、selected=0。243点zero-trial失败分解中117点支持充分但方向性正增量为0，最佳仍为`-2.292 bps/day`、95%区间`[-6.234,1.294]`；exact retry的diagnostic identity为`7f565394...`。结果排除target错接和符号反向，同时发现CPCV补集train intercept/base rate主导跨日期绝对预测；v1冻结且不得作为activation calibration
> N3因果Admission v2.1 R1正式结果：request `advcausal_9ed1c38ef6d6ff7fb5d3ef33`、bundle `7e739be5...`在48日inner完成静态与20日expanding Ridge两项固定trial。静态/expanding分别形成9/11个干预日，coverage为`90.42%/87.50%`，日均绝对净lift point为`+2.408/+3.811 bps`，95% lower为`-4.090/-1.704 bps`；两者均低于12日/25%干预支持且point也未超过冻结5 bps经济门，Brier `0.2775/0.2687`均劣于base-rate `0.2495`。因此inner selected=0、outer未读取，正式分类为`CAUSAL_ADMISSION_V2_1_EXPLORATORY_INSUFFICIENT_SUPPORT/NAVIGATION_ONLY`。registry以一条聚合记录登记2个trial；exact retry同bundle、registry duplicate-noop、route exact-noop。该结果关闭当前score/raw+Ridge静态/expanding frontier，不允许降低支持度、回选arm或直接进入R2/HMM；它不证明所有因果准入全局不可学
> G2-A上游依赖进度：PR #4343于2026-09-05合入 `3b4781a2c...`，PR #4353于2026-09-06合入 `a7609c41b...`；真实r4 input为1,373日×31板块=42,563行、manifest `ad2e3b5e...`。v1.2已执行17/39 fits：15-fit battery选择10D，process 1 首个GBDT的tree 131 leaf 1覆盖18个日期，未满足旧20日期要求，以 `STRUCTURAL_ACCEPTANCE_FAILED` 停止，process 2未运行、tail未读。PR #4359提出v1.3 leaf-distribution修订但仍开放且依赖BUG-1382分支；它不是v1.2成功重试，也不是正式新OOF结果。无通过验收的完整canonical source/capability；只阻断使用该源的Advisory信息增量阶段，不阻断基础因果Admission。
> QE上游相邻进展：PR #4347已把MA-E19R3/D1B-D1D事实写入QE权威文档。MA-E19R3 LGBM 12/12完成，但2026H1 fixed/expanding/rolling CAGR为`-13.24%/-0.56%/-10.64%`，确认近期收益转化退化。rolling LSTM seed123四个vintage CAGR为`87.05%/84.21%/46.54%/36.23%`，3/4胜rolling LGBM且2026H1相对LGBM改善`46.87pp`；但2026H1 Top50仍为`-1.01%`，只有单seed且未完成LOO，因此状态仅为`CANDIDATE_LEG_PENDING_MULTI_SEED_AND_LOO`。D2真实2026H1四格显示现实sector/stock tail recall仅`0.0135`，oracle-sector/reality-stock、reality-sector/oracle-stock和双oracle分别为`0.0873/0.0482/0.1339`，说明板块与板块内右尾均有空间；oracle永久不可部署。固定50/50 rank blend已因2026H1退化拒绝，D3 Brinson因冻结benchmark板块权重缺失保持`NOT_COMPUTABLE`且不阻断演进。该线为解决上游召回/Alpha不足提供当前系统主线，但尚未形成可替换当前父包或可直接供Advisory绑定的新StrategyPackage
> QE因子分析数据权威：BUG-1381 / PR #4352已于2026-09-06合入`983739db3...`，把正式因子指标、相关性和universe mask读取切到2026-08-31 direct-v2 candidate并通过DEV/WSL验证。该修复只更新因子分析数据源，不改写MA-E19、Advisory或已消费实验结果；生产`backend-main`仍需用户重启后才生效，但该重启不阻断直接从最新源码运行的离线LSTM/G2-A/Admission研究
> QE数据集/股票池进度：PR #4357设计已合入；PR #4361于2026-09-07合入 `2f653687d`，实现活动文件profile、run-scoped direct-v2 v3 binding与 `stock_universe/single_index/index_union` 创建入口。该PR明确未激活global profile、未提交实验、未写candidate数据、未执行DDL或重启；不能把源码合入写成运行时已切换或证券缺口已补齐。既有142个历史证券/近窗9个缺口按目标实验交集核对；现有同release复验不等待全量修复，不建设数据平台。
> Advisory股票池进度：2026-09-12确认旧Program/Binding没有显式股票池合同，旧matched-canary还固定`stock_universe`，因此此前不能声明荐股已支持指数池。本次F2切片在Advisory自有边界新增与QE同形的`universe_selection={mode,pool_ids}`，支持全市场、单个P0核心指数及多个P0核心指数并集；普通复评按复评日解析，正式每日forward则保存推荐目标日D并严格使用`selection_as_of_trade_date=D-1`读取共享`market.core_index_membership_pit`与股票资格PIT交集，在Selection候选进入Advisory排名前过滤并重新编号，正式forward、结算与模型子层消费同一冻结候选投影，并保存成员revision、集合hash、PIT source/key/rule/revision、目标日D、准入截止D-1和排除计数。历史优先使用QE同源冻结canonical PIT；仅当其明确因截止日不可用时，使用Selection已验证的ready/clean实盘滚动PIT，其他成员错误不切源，双源不可用即fail closed。DEV真实读回已验证`2026-09-11`沪深300为299只、沪深300与中证500并集为795只；直接请求未覆盖的`2026-09-14`正确拒绝，但周一forward使用9月11日截止，无需未来PIT。日频数据库价格合同由后续独立小PR关闭，盘中实时荐股不在当前目标。该能力约束新增荐股准入，不把全市场包的事后过滤冒充指数全集内重新推理；严格复现QE指数实验仍需消费QE正式发布的对应指数StrategyPackage。该切片不修改StrategyPackage Alpha、不发起QE实验，也不改QE/Selection/StrategyPackage/数据集源码；源码合入、运行时重启和业务读回仍分开报告。
> 相邻Exit/执行证据：Position Timing PR #4346的L2正式bundle `eef1f771...`得到Ridge negative、GBDT/study inconclusive且`selected_model_id=null`；PR #4348/#4351的L4b-1经可达SELL人口修正后，bundle `450f8c82...`仍因自然prospective action cards不足而无selected side。这些结果只属于Position Timing自身合同，不冒充Advisory Exit可学习或分钟执行成功；当前保持规则基线和自然样本积累，不抢占上游Alpha/Admission主线
> 当前主动路线：遵循2026-10-03业务优先要求和§16末尾P1～P5：先修每日价格建议的真实正确性问题，完成通用日频消费者，再针对实质新信息/目标立项；已有未达标模型不默认进入历史确认、补证或启用。2026-09-28的独立ENTRY_PRICE路线是已发生的阶段安排，不覆盖此顺序。QE仍是历史全量Alpha复验、多seed、因子/模型组合搜索的唯一所有者；Advisory不提交Q-CANARY或重复上游训练。不等待QE新包才研发价格功能，不修改QE、Selection、StrategyPackage或数据集源码。
> 最终决策者：用户人工决定是否买入；系统不下单、不形成交易执行输入

## 0. 权威边界与本次纠偏

本文档是 AIstock 荐股模型研发的当前顶层权威。用户最新明确要求优先于历史设计、历史实现顺序和旧任务状态。

早期纠偏记录：当时开发把 Phase 1R 历史范围任务、Source Catalog、逐窗口哈希、capture/build/SEALED、CAS、lease/fencing、source revision、历史固化和完整证据链放在模型训练之前，曾延迟真实模型和用户可见荐股能力。该训练前置顺序已废止；这不是2026-09-07仍无模型实现的现状。本次进一步纠正单一Ridge表达、预测分位数/均值置信区间混淆、静态老化、HMM全线依赖及QE收益口径脱节；历史数据与失败分类不改判。

M0-M5C 已完成模型组件、固定日期推理和三轮负面质量实验。自2026-09-02起，所有工作必须先归入以下四类，只有第一类默认获得研发和算力：

1. **主动业务主线**：直接实现用户可见荐股能力并修复每日荐股阻塞。当前按§16的P1正确性/P2日频交付/P3实质新假设顺序推进，只有符合开发门槛的新候选才进入P4独立确认；不为旧未达标模型自动补跑历史确认，不等待QE新训练才开发。历史全量复验、多seed、LOO、因子/模型组合Alpha审计和新组合搜索全部由QE统一规划，Advisory不建立平行Alpha实验线；需要新包时只消费QE正式交付，不重训上游。
2. **被动业务观察**：每日自然 forward observation/outcome 按现有调度形成，不回填、不等待、不派生独立开发项目。
3. **条件性阻塞修复**：只修直接阻碍主动主线或每日荐股正确性的 BUG；H0 只有满足该条件时才执行最小范围。
4. **零工作约束与历史事实**：研究族冻结、已完成实验、已消费窗口、trial registry 身份和旧 artifact 只防止重复犯错，不构成待办；历史分析、证据固化、归档和旧任务清理分配零主动工时。

以下编号记录已完成演进事实和条件性后续，不是可并行领取的任务队列：

1. P0-A 建立每天自然向前运行的 baseline publish、challenger observation 和 episode 跟踪，不回填旧日期。
2. P0-B 同期解除单一目标常量，用 Program active binding 动态解析 exact bundle。
3. P0-C 直接读取现有 QE H5/Parquet/Qlib Bin 和目标策略包预测 PKL，构造 Top5 shadow policy episode 标签与 purged rolling/CPCV 评价。
4. P0-D 在 WSL Conda 训练真实 meta-label 模型并进入 challenger 前向发布；正式预测只读取数据库 decision-cutoff 输入。
5. P0-E 复用P0-C连续净超额收益，在每条CPCV path内用train-only幅度统计训练收益感知meta-label；模型选择只用冻结validation，已消费历史窗口只作回放诊断。
6. P0-F 复用P0-C连续净超额收益，使用train-only median/MAD和固定Huber regression直接学习policy utility；真实Stage A因换手门禁失败已负向终止。
7. P0-G只改变训练label并完成真实Stage A；相对P0-D仍因换手`+0.004096`负向停止，禁止Stage B、replay、runtime和结果后调参。
8. P0-H把收益和换手负担分头学习，在outer-train nested OOF模型输出上约束实际entry priority；outer validation只评价，不参与price、rounds或transform拟合。
9. P0-I只替换P0-H收益头为policy-aligned grouped rank head，同日预测百分位进入原output constraint；P0-H的liability、候选、policy、cost、CPCV和advancement保持不变。
10. P0-J不延续P0-I grouped-rank目标；它在每个inner-train内拟合Selection rank的非增单调收益先验，以Huber学习残差，并用同一outer-train的nested OOF解析式可靠度系数收缩残差输出，再进入原liability/output constraint。
11. P0-K停止收益排序建模，仅训练liability head；在inner OOF选择预冻结物理holding-day阈值过滤高换手ENTER候选，eligible集合内保持Selection顺序。
12. P0-L冻结P0-G收益anchor与P0-K liability head，用relative liability rank执行有界局部ENTER重排；源码、BUG-1251修复和正式Stage A均已完成，结果为`NEGATIVE_STOP_INCOMPLETE_CPCV`，不进入Stage B。
13. P0-D至P0-L作为一个共享开发数据、候选、特征和模型族的研究族正式冻结；负面结果与已消费窗口保持原结论，不派生P0-M，不以结果后阈值、gain、family、seed或新loss改判。
14. 新主线先建立最小JSONL trial registry、可由其生成的单页活动路线和父包预测延伸可行性spike，再在开发窗口执行分层clairvoyant oracle与固定cross-fitted learnability audit；oracle、探索和模型trial分类计数，sealed holdout不参与诊断。
15. N1/N2三臂审计与Entry/Exit诊断已完成；N3各固定frontier、Score/HMM v1及因果Admission v2.1 R1均已selected=0，Advisory实验frontier关闭。后续所有Alpha、seed、因子和模型组合实验由QE单线完成；Advisory只消费其通过治理的交付物，不重复提交实验。HMM只作QE交付或独立产品能力中的可选信息，不由Advisory另起训练线。
16. H0详细设计继续保留，但状态降为条件性阻塞修复；仅当实盘/历史同核错误或已测得的回放资源瓶颈直接阻断当前模型验证时，才以已冻结44日A/B/C结果为golden执行最小必要范围。它不获得默认并行资源，也不决定模型方向。
17. 上述真实功能和新模型路线均不自动解禁历史补账、历史归档、ModelOps、旧任务清理、动态资金仓位或通用数据/缓存平台；H0也不得成为恢复这些任务的入口。

以下内容不再是模型训练、模型推理、页面展示或模型启用的前置条件；H0 也不得借此恢复无界历史平台建设：

- Historical Range/Phase 1R 全链路 DML。
- 为模型训练新建 Source Catalog，或对所有来源无差别执行逐日、逐窗口全量内容哈希。H0 可以复用既有冻结 catalog，并把校验优化为批次 full seal、chunk revision token、逐日实际读取 receipt 和异常时 full rehash。
- retrospective observation/capture/label bridge。
- 新建 SEALED base snapshot、CAS publish、blob reference、invalidation 或 GC。
- 把 checkpoint、lease、fencing、recovery successor 或 exact retry 证据闭合作为首次模型训练前置；H0 仅保留长回放自身必需的逐日 checkpoint、heartbeat 和 exact resume。
- 旧 batch、旧 artifact root、orphan build 和历史 operation 的处理。
- 2/3/5 年全部窗口、全部消融、全部种子和统计检验完成后才允许首次训练。

已有 Phase 0A、Phase 1、Phase 1R、Phase 0B 源码和历史事实保持可审计，不删除、不迁移、不归档。H0只有在已证明的历史回放重复计算或业务语义错误直接阻塞当前模型验证时，才允许复用或定向重构其执行边界；单纯可提速但不阻塞主线时保持休眠，也不得恢复已废止的训练前置路径。

### 0.1 H0 授权边界

H0 的权威详细设计为
`docs/architecture/advisory_live_daily_historical_batch_shared_kernel_f2_detailed_design_20260816.md`。

该设计当前为`CONDITIONAL_DORMANT`，不是主动后续任务。只有同时满足“存在可复现的主线阻塞”“最小H0改动能够直接解除阻塞”“不延迟当前Alpha/Ranking候选实验”三项时才可进入实现；单纯追求更快历史回放、补齐历史证据或提高工程完整度均不能启动H0。

该授权只包含：

- 实盘 `LiveDailyExecutor` 保持每日单日运行；历史 `HistoricalBatchExecutor` 接受冻结日期区间并在持久 worker 中分块处理。
- 两种执行器共同调用唯一的 StrategyPackage 日信号、Selection/HMM/risk/tradability 和 Advisory list transition 语义；禁止复制第二套回测选股算法。
- 静态模型/因子工作区按内容寻址复用；日期动态数据、PIT 视图、结果和 receipt 仍逐日隔离。
- A/B 仅在 raw-affecting identity 完全相同时共享不可变 raw Alpha artifact，之后分别执行各自增强逻辑。
- source validation 从“每日两次全量扫描”改为批次 seal、chunk token、逐日读 receipt 和异常 full rehash；日期 cutoff、availability、source revision 和未来毒化测试不得削弱。

该授权不包含修改、覆盖或回写已冻结v6代码身份/结果，不包含把历史批量结果写入实盘binding，也不包含多进程并发提速承诺，
更不包含通用缓存平台、通用回测引擎或生产调度平台。已冻结v6保持原code-release和golden evidence只读。

### 0.2 从属设计处置

`docs/architecture/advisory_phase2_phase3_short_rebound_reranker_f2_design_20260802.md` 中以下内容已被本蓝图替代，不能继续作为开发或运行依据：

- 用完整 Historical Range/Phase 1R bridge 生成训练输入。
- 以新 SEALED base snapshot 作为 WSL 训练硬前置。
- Batch B 对既有 Advisory capture/build/snapshot 表执行生产 DML。
- 先完成完整 2/3/5 年矩阵、三种子、全量 bootstrap，再进行首次真实训练。
- 没有正式 capability readiness 时禁止显示明确标记的实验性真实模型结果。

该详细设计中仍可复用的内容仅限：SHORT_REBOUND 风格定义、候选特征公式、时间切分原则、WSL 环境约束、LightGBM LambdaRank 历史对照和错误可见性。旧5日标签只保留历史结果；P0-C Top5 shadow-policy episode标签继续作为冻结排名基线，新的Ranking、Entry与Exit研究分别使用§6.6至§6.10定义的角色化增量价值标签。后续代码不得继续调用其已废止的 Batch B 历史证据编排路径。

## 1. Background / 业务目标与当前差距

当前策略包具有产生有序候选的能力；历史R-PUBLISH输入阻塞按其发生日期保留，不在未重新读回时宣称当前仍阻断。已经完成的独立模型组件分别可以输出 Top5、收益/周期和价格区间；两个生产 Program 已按交易日形成真实 `PUBLISHED` baseline、target-open settlement 和 active episode，目标多Alpha Program的P0-D challenger也已通过exact descriptor进入在线shadow。但当前P0-D meta-label descriptor只提供take/skip/confidence重排，不组合M3 outcome或M4 price-range child；页面上的Selection `rule_default`价格指导也不能冒充M4模型输出。当前直接缺口是新收益型价格模型尚无经济确认和正式启用；#5324默认未配置的重启后读回已完成，不能把工程可用解释为模型有效。后续不再寻找一个同时承担候选召回、排名、买入、仓位、退出和风险的单一Selection替代模型，而以瓶颈诊断驱动的角色分离决策栈推进：

1. **前向运行能力**：对每个 ENABLED Program 按交易日持久化基线推荐、模型 challenger、outcome/价格区间和 episode 结果。
2. **上游alpha与候选召回**：StrategyPackage/QE负责把潜在赢家送入可交易候选池；赢家召回、净正机会覆盖和同policy收益共同定位瓶颈，不能只因全市场Top5赢家召回低就认定无alpha或关闭下游。
3. **角色分离能力**：Top20排名、Admission Risk、日级价格区间、日频Entry Guard、固定槽位下的现金/空槽、日频Exit和组合风险分别拥有独立决策时钟、标签、shadow、验收和回滚；Admission是组合风险/现金动作的角色化接口而非新增候选层。价格区间只发布预测事实，日频Entry/Exit只表达荐股建议，二者均不生成分钟级执行动作；目标架构仍不是多条并行工作线，正式路线最多一主线一辅线。
4. **双目标合同**：`ALPHA_RANKING`只评价固定动作空间内的成本后超额收益；`RISK_MANAGED_ADVISORY`评价Entry/Exit/现金暴露带来的绝对收益、MDD和尾部风险。两类结果不折算成一个加权总分，实验立项时冻结归属，按合同独立激活和展示。
5. **增量价值口径**：各层标签衡量相对当前冻结基线动作的增量价值并绑定policy hash；历史可确定性重放两臂时使用配对shadow-policy模拟，只有未来日志仅观察单臂时才评估OPE估计器。
6. **用户可见能力**：页面同时展示基线与实验模型的当期建议、目标合同、状态、价格/周期范围和前向表现，不把模型输出写回 Selection、Paper 或模拟盘，也不把超额收益合同描述为绝对收益承诺。日级价格区间不得显示为“最佳分钟买点/卖点”或成交保证。
7. **策略条件化边界**：StrategyPackage负责候选召回和父Alpha，Advisory负责在该候选/信息集上学习Ranking、Outcome、Entry与Exit增量价值。代码编排应尽量复用，但模型输入、artifact和激活结论必须按package/style/policy条件化；未经跨包matched与leave-one-package-out证据，不得声称一个bundle可用于任意策略包。
8. **评分与市场条件化准入**：父包原始score只保留同日横截面排序语义；Advisory在exact package/style/manifest/policy下学习跨日可比较的score percentile、成本后收益概率/区间和准入价值。市场宽度等原始市场形态是独立control，市场HMM负责“当天是否值得承担风险”，板块HMM/轮动负责“候选处于何种板块环境”；三者不是独立候选生成器，也不得混成一个不可解释总分。

长期趋势策略另行完成长期重排、生存、time-to-hit 和大行情捕获模型，不阻断短反弹主线。

### 1.1 StrategyPackage 与荐股模型独立性边界

当前系统不是“上游换任意策略包，荐股仍使用同一套已训练模型”的包无关实现。

当前数据流是：

`StrategyPackage/QE Alpha -> Selection候选及父分数/排名 -> Advisory独立模型头 -> Ranking/Outcome/价格区间shadow`。

目标数据流在不改变候选所有权的前提下增加：

`QE/Selection管理的候选源 -> 公共股票/PIT特征与收益风险预测 + 显式包adapter -> 独立Ranking/Admission/Entry/Exit动作 -> 最多Top5或NO_ELIGIBLE_RECOMMENDATION`。HMM/sector是可选输入，不是串行必经网关；多源候选是上游另立合同的条件路线，当前运行时仍为单包。

各层当前边界如下：

| 层 | 是否依赖目标策略包 | 当前事实 |
|---|---|---|
| 候选召回与原始排序 | 必然依赖 | Top20/40/50及父score由目标StrategyPackage产生；上游没有召回的股票，下游模型不能重新发现 |
| 同包评分校准与准入 | 条件化依赖 | 当前父包各腿按交易日标准化后加权，combined score只具有同日排序意义；跨日筛选必须使用绑定package/manifest/style/policy的past-only chronological收益/概率校准，禁止直接对raw score或CPCV补集绝对输出设全历史阈值 |
| 市场形态与HMM上下文 | 输入可复用、结论条件化 | 原始市场宽度是独立control；市场HMM用于日级风险/准入，板块HMM或rotation score用于候选级排名/Entry。模型输出仍绑定父包候选和policy，不能补回Top50以外股票 |
| Program/binding编排 | 部分解耦 | 动态resolver可按Program读取不同package，但要求单一原生包和exact descriptor；无bundle时仅返回typed unavailable |
| Top20重排/Top5 | 条件化依赖 | LightGBM是独立训练模型，但必需特征包括父包combined score/rank、LSTM/FUND腿分数、rank、weight及腿间分歧，并绑定package manifest/style/runtime semantics |
| 收益与持股周期 | 条件化依赖 | M3的46个独立模型头使用同一package-conditioned feature schema；不是直接复用策略包预测值，也不是跨包通用模型。当前P0-D meta-label descriptor未挂载M3 child |
| 买入价格区间 | 条件化依赖 | M4的entry executable/gap模型头使用同一特征矩阵、实时价格上下文和exact bundle，并依赖匹配的M3 outcome；当前P0-D meta-label descriptor未挂载M4 child |
| 止盈/止损/保护区间 | 模型与policy共同决定 | M3预测MFE/MAE和holding horizon，M4负责价格投影；review policy、涨跌停、tick和hard stop提供不可突破的风险边界 |
| Selection页面规则区间 | 直接继承父信号 | `selection_center/price_guidance.py`是规则型`rule_default`，以候选score/rank推导alpha budget；它不是M4学习模型，不得与模型价格区间混称 |

目标为“公共股票/PIT预测层 + 显式package/style/policy adapter + 独立动作层”：先允许模型框架和基础收益/风险预测复用，再以matched/留包实验判断参数是否共享；不强行删除策略信息，也不要求先为每包建设完整生产bundle才允许共享研究。每个正式新包仍须独立验证的exact binding或明确compatible-set证据。当前动态编排基本具备，但特征投影仍硬编码`lstm/fund`两角色；只有一个多Alpha包拥有P0-D重排shadow，而且该descriptor没有组合M3/M4 child，因此跨策略包通用性和单Program完整模型覆盖均尚未完成。

截至 2026-09-16（Advisory最新事实快照；QE研究条目保留其各自已记录的事实日期）：

| 功能 | 状态 | 完成口径 |
|---|---|---|
| SHORT_REBOUND Top20→Top5 | `PURE_RERANKER_RESEARCH_COMPLETE_NOT_ACTIVATED` | M5A 已完成 45 个 booster 和一次冻结 test；winner 平均 5 日超额收益 `0.0071894`，低于 selection rank 的 `0.0085591`，95% block-bootstrap lift 区间跨 0。纯重排保留为历史基线，不再是唯一质量主线 |
| 预期收益与持股周期 | `M5B_REAL_CALIBRATION_COMPLETE_NOT_ACTIVATED_CURRENT_P0D_CHILD_UNAVAILABLE` | 最终 request `advoutcal_ec16422ad1a97040583e5273` 生成 v2 bundle `a2dea5157f1b768dff42ea844f7dc5a2d31563652967a6535adf89b228bd5533` 并通过 exact retry；8/10 binary head 可校准、2 个五日 head 因排序反转明确保持 `UNCALIBRATED`，holding 仍独立 `UNCALIBRATED`。冻结 test 未显示足以支持激活的改善；当前P0-D meta-label shadow返回`OUTCOME_UNAVAILABLE` |
| 买入/止盈/止损区间 | `DAILY_ENVELOPE_V4_FIRST_PROSPECTIVE_SETTLED_ADAPTIVE_CQR_SELECTED_ZERO_CURRENT_P0D_CHILD_UNAVAILABLE` | 三头v3、validation-only v4、独立盘前收集与成熟评价均已实现；首个T=`2026-09-15` prediction 20/20可用，自然settlement也为20/20可用但单日业务coverage仅`0.70`，只计`ACCUMULATING`。固定rolling-20D matured CQR历史导航审计selected=0，保持静态v4且不发布binding。当前P0-D meta-label shadow仍返回`PRICE_RANGE_UNAVAILABLE`，Selection `rule_default`区间不得记为M4模型覆盖 |
| 荐股页面模型展示 | `FORWARD_API_AND_PAGE_SOURCE_COMPLETE_PARTIAL_MODEL_ROLE` | 页面/API具备Top5、五期限收益、概率、MFE/MAE、持股、价格范围和前向状态字段；当前P0-D runtime只实际提供重排/take-skip-confidence，outcome和price-range以typed unavailable显示 |
| 每日前向发布与 episode | `LAST_VERIFIED_2026_09_14_RUNTIME_HEALTHY_LATER_STATE_NOT_REASSERTED` | 2026-09-14重启后health、runtime identity及scheduler已读回；当时两Program最新复评日09-11均为`SUCCEEDED`，09-14零点为未到期的`WAITING_DATA/TARGET_OPEN_SETTLE`。本次只新增离线价格审计且未重启后端，不把旧快照冒充2026-09-16实时状态；多Alpha自然成熟证据仍与历史回放分级 |
| 多 Program 模型分发 | `DYNAMIC_BINDING_VERIFIED_ONE_P0D_PACKAGE` | active binding动态解析已完成；目标多Alpha Program绑定P0-D exact bundle `e555903e...`，单Alpha无bundle时基线继续且模型typed unavailable。P0-E至P0-L均未接入descriptor |
| 跨策略包荐股模型覆盖 | `FRAMEWORK_DYNAMIC_MODEL_PACKAGE_CONDITIONED_RERANK_ONE_PACKAGE_ONLY` | 编排可动态解析package，但模型特征和descriptor仍精确绑定目标多Alpha包；当前只有该包的P0-D重排role在线，M3/M4 child未组合，不同包也不能直接复用这些历史bundle，P1-B共享模型实验未就绪 |
| 同包评分与市场/HMM条件化 | `V1_SELECTED_ZERO_V2_1_R1_SELECTED_ZERO_FRONTIER_CLOSED` | v1结果不变；v2.1 R1正式bundle `7e739be5...`完成固定两Ridge trial。9/11个干预日低于12日/25%支持，lift point `+2.408/+3.811 bps`也低于5 bps且区间跨0，selected=0、outer未读；不得调阈值或自动进入R2 |
| `rotation_L1`离线sector输入 | `V1_2_17_OF_39_STRUCTURAL_STOP_NO_ACCEPTED_OOF` | input/executor已合入；battery选10D，process1首GBDT叶日期18<20触发结构停止；v1.3 PR #4359仍开放，不是已运行经济失败或source成功 |
| QE上游Alpha候选 | `ROLLING_LSTM_SEED123_CANDIDATE_PENDING_MULTI_SEED_AND_LOO` | PR #4347记录四vintage rolling LSTM均为正、3/4胜rolling LGBM，但2026H1 Top50仍为负且只有单seed；固定50/50 blend已拒绝。下一步为seed314/2718 matched复验、跨vintage/LOO，再决定soft top-down/右尾候选；当前父包不变 |
| QE因子分析数据源 | `SOURCE_MERGED_RUNTIME_RESTART_PENDING_NOT_EXPERIMENT_BLOCKER` | BUG-1381 / PR #4352 `983739db3...`已把指标/相关性/universe mask切换到direct-v2 candidate；DEV/WSL验证通过。生产backend尚待用户重启，但不阻断latest-source离线模型实验，也不改变已有研究结论 |
| QE全局数据集/PIT股票池 | `SOURCE_MERGED_PROFILE_NOT_ACTIVATED` | PR #4361实现创建入口与resolve-once，global profile activation/数据补齐/实验/运行时readback未执行；当前冻结release复验不受阻 |
| 相邻Exit/分钟执行研究 | `POSITION_TIMING_NO_SELECTED_MODEL_OR_SIDE` | Position Timing L2为Ridge negative、GBDT inconclusive、selected model为空；L4b-1修正后仍因prospective action cards不足无selected side。仅自然积累支持度，不计为Advisory Exit完成，也不阻塞当前主线 |
| LONG_TREND 专家 | `DEFERRED_UNTIL_PACKAGE_READY` | 对应长期趋势包形成稳定输入后训练和接入 |
| P0-D至P0-L研究族 | `FROZEN_NO_ACTIVATABLE_WINNER` | 同一P0-C开发数据/候选/feature schema/CORE家族上的九轮自适应研究已事实收敛；旧结果、合同和消费窗口不改写，不派生P0-M |
| 新模型演进路线 | `QE_SINGLE_ALPHA_EXPERIMENT_OWNER_ADVISORY_DAILY_PRICE_OWNER` | 已执行Advisory Alpha/Admission frontier均selected=0并关闭；历史全量复验、多seed、LOO、因子/模型组合Alpha审计和新组合搜索由QE统一规划。Advisory不再提交matched canary或平行Alpha实验，但继续研发自身日级价格区间模型、正式binding、API/UI和直接BUG修复 |

历史表、schema、证据链、报告、任务状态、artifact 数量和测试数量均不得计入上述功能完成度。

### 1.2 当前进度口径

为避免把“代码存在”“单点可调用”“持续运行”和“模型有效”混为同一完成状态，本蓝图固定使用以下独立维度，不再汇总成单一百分比：

本表早期自然前向、上游QE/HMM及相邻模块条目均为其注明日期的最后观察，不是2026-10-03重新运行的实时检查；Advisory本次最新收益型状态见“新路线实现状态”。不为刷新无关历史状态占用本轮研发资源。

| 维度 | 当前进度 | 准确含义 |
|---|---:|---|
| 模型组件实现 | `4/4` | Top5、收益/周期、价格范围、页面/API 均有真实模型实现 |
| 固定日期按需推理 | `TARGET_MULTI_ALPHA_VERIFIED` | 仅目标多 Alpha Program 在 `2026-07-16` 的 persisted replay 上验证；不是生产前向运行 |
| 每日前向发布 | `HISTORICAL_PUBLISHED / LAST_RUNTIME_READBACK_2026_09_14` | 最后一次正式运行态读回为2026-09-14健康、两Program最新复评09-11成功；当时未到期的`WAITING_DATA`不是故障。本次没有重启或重新声明运行态，仅确认离线价格审计不改变既有调度 |
| episode 前向跟踪 | `0 MATURE MODEL OUTCOMES` | 第一个Program最早模型成熟日为2026-09-22，第二个Program尚无模型observation；任何open-mark均不得冒充成熟future OOS |
| 多 Program 模型覆盖 | `DYNAMIC RESOLVER, 1 P0D-CONFIGURED PACKAGE` | P0-D exact descriptor已作用于目标多Alpha Program；无bundle的单Alpha typed unavailable不阻断基线 |
| 当前组合角色覆盖 | `RERANK_ONLY / OUTCOME_AND_PRICE_TYPED_UNAVAILABLE` | 目标多Alpha Program的P0-D shadow提供Top20重排；当前descriptor没有M3/M4 child，四类组件分别实现不等于当前组合已完成 |
| 跨策略包模型独立性 | `CURRENT_PACKAGE_CONDITIONED / SHARED_PREDICTOR_NOT_IMPLEMENTED` | 当前只有一包重排shadow；目标改为公共股票/PIT预测+显式包adapter，P1-B先做离线matched/留包验证，不强制等待两个生产bundle |
| 模型质量升级 | `0 ACTIVATED SELECTOR CHALLENGERS` | M5A/M5B/M5C及P0-D至P0-L均未证明可以替换Selection；P0-D只作为experimental shadow，M4继续提供价格范围而非选股alpha |
| 长期趋势模型 | `NOT_STARTED` | 长期趋势原生多 Alpha 父包尚未形成可训练输入 |
| 旧研究族状态 | `P0-D..P0-L FROZEN` | 研究事实完整但无可激活winner；不以同族新变体继续消耗相同开发证据 |
| 新路线实现状态 | `VALUE_ANCHOR_CANDIDATE_STOPPED_NO_QUALIFIED_ROLE` | BUG-1640/PR #5150及close-sync #5190已合入并经用户重启验证；29日探索完成、仍非原生。收益型v1/v3/v4、H-TIMING-1及H-VALUE-ANCHOR-1均无经济确认；两个新假设各已完成一次研究并停止其精确candidate。每日消费者#5324已合入并通过重启后的默认未配置验证；无合格ENTRY_VALUE角色、无新binding，不等待或补旧身份来挽救模型。 |
| Admission v2上游依赖 | `BASELINE_STAGE_DECOUPLED / OPTIONAL_SECTOR_SOURCE_NOT_READY` | G2-A v1.2 17/39结构停止，没有完整accepted OOF；仅R2 sector阶段受阻，R0/R1使用真实基础source不需等待 |
| 系统级上游Alpha | `QE_ROLLING_LSTM_CANDIDATE_ONLY` | rolling LSTM seed123完成四vintage并显示相对rolling LGBM改善，但仍待两seed、LOO和2026H1 Top50负收益解释；尚无新StrategyPackage或Advisory binding |
| QE因子分析运行态 | `DIRECT_V2_SOURCE_MERGED_BACKEND_RESTART_PENDING` | PR #4352源码和DEV/WSL验证已完成；生产API/后台任务是否加载新源须用户重启后另行readback。该状态不计为模型效果，也不阻断离线主线 |
| QE股票池管理能力 | `QE_PUBLIC_CONTRACT_AVAILABLE_EXPERIMENTS_OWNED_BY_QE` | QE公开合同支持`stock_universe/single_index/index_union`；活动profile、具体实验和运行态由QE窗口单独负责，Advisory不得修改或代为提交 |
| Advisory股票池管理能力 | `SOURCE_MERGED_RUNTIME_VERIFIED_DAILY_DB_ONLY` | Program binding显式保存QE同形`universe_selection`；单指数/指数并集使用共享PIT核心指数成分权威过滤Selection候选并保存receipt，全市场保持兼容且不增加成员查询。历史优先冻结canonical PIT；正式forward以D-1截止解析准入并分别保存D/D-1，双源不可用fail closed。API/UI、每日复评、历史回放和正式forward使用同一合同；无需DDL。2026-09-13用户重启后的只读smoke已核验运行时身份、D-1股票池及日频DB-only价格，详见页首；不等于新股票池的ENTRY_PRICE模型范围已确认 |
| 相邻Exit证据 | `POSITION_TIMING_INCONCLUSIVE_OR_UNDERPOWERED` | L2无selected model，L4b-1无selected side；保持自然证据积累，不进入Advisory主线完成度 |

PR #3346 已于 2026-08-12 合入 `main`，merge commit 为 `034ccd36dd94441ec8c0fe0f94010d6874b8b799`；P0-D PR #3368 已于 2026-08-13 合入 `458199cd902323e006ac23d3767c908637068fa8`，后续通过descriptor rotation作为experimental shadow接入。P0-L源码PR #3959、BUG-1251修复PR #3967和close-sync PR #3969均已合入；P0-E至P0-L均未激活。最近相关主线为：PR #4343 `3b4781a2c...`合入G2-A direct-v2 v3 input适配，PR #4347 `d71479b31...`记录QE rolling LSTM/sector诊断，PR #4352 `983739db3...`修复QE因子分析direct-v2数据权威但生产backend待用户重启，PR #4353 `a7609c41b...`合入G2-A development执行器，PR #4357 `f05ee69d2...`合入设计，PR #4361 `2f653687d`合入QE活动数据集/PIT股票池源码但未激活profile；G2-A v1.2已17/39结构停止，PR #4359仍开放。因果Admission v2.1 R1源码PR #4395、跨OS仓库身份BUG-1395/PR #4402及registry聚合BUG-1396/PR #4404已合入；正式研究从`main@a317f7c2...`完成且selected=0。M4 v1 artifact保持原身份，但当前Program descriptor已旋转为不含M3/M4 child的P0-D meta-label。源码合入、正式实验、descriptor接入、运行时加载、模型角色覆盖和自然OOS成熟继续分别报告。

### 1.3 真实训练与实验数据总表

下表只记录真实冻结输入、真实模型或真实校准运行；mock、fixture、历史证据平台和测试数量不计入模型实验：

| 阶段 | 冻结输入与产物 | 样本/模型 | 资源 | 冻结 test 或真实运行结果 | 当前结论 |
|---|---|---|---|---|---|
| H-VALUE-ANCHOR-1 D-only价值锚 | plan `advvalue_a4bc66a30cfa7d9d5078850c`；源码HEAD `1c8545ab870f5e12b9e128e09b1b13a4bdf0ae2f`、研究PR #5347；结果 `F:/Dev/AIstock_model_artifacts/advisory_value_anchor_v1_20261003/advvalue_a4bc66a30cfa7d9d5078850c/evaluated/evaluation.json` | 386D/7720原候选，7699新场景标签可用/21 UNKNOWN；12D未知10保留；新共同train4250/validation1622，label-end purge296；1配置2头、1candidate；81D/1620 test候选、100共同估值日 | 既存输入预登记/新标签准备15.163秒，零DB重查；两头fit及源核验0.880秒，fit＋评价12.169秒；拟合前后7项QE状态均0；无DB写/sealed/服务控制 | baseline/constant/model名义收益`21.3220%/25.4597%/17.4642%`，MDD`-10.3314%/-10.7283%/-8.0746%`；model减constant日增量`-6.9022bps`、95%`[-24.5301,9.9157]`；减baseline`-3.4196bps`、95%`[-18.9390,12.2020]`；模型81真实TAKE＋3 UNKNOWN控制episode，常数89＋3；相对常数实际进入不同58日；三臂端点/持有mark问题0 | `STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION / EXPLORATORY_SCREEN / NAVIGATION_ONLY`；同独立VALUE_REVIEW_5_V1复评场景，非生产政策改变/沪深300超额/独立OOS/实盘成交。停止精确candidate，不事后转选常数控制，零confirmation/binding/启用 |
| H-TIMING-1 同核买入价值探索 | plan `advtiming_cd2ddd9832129255c14f9a80`；recipe input `advtiminginput_9a929b2d2dafd1c4cf18f42f`；源码HEAD `001910b7ef7a64afb1deacc12233389b97abe4f5`、PR #5336已合入`5ec8c8e2d1deee16a5587afd604d61481162669e`；结果 `F:/Dev/AIstock_model_artifacts/advisory_entry_timing_v1_20261003/advtiming_cd2ddd9832129255c14f9a80/evaluated/evaluation.json` | 386D/7720原候选；7710个14D完整值、10 UNKNOWN保留；新共同train4153/validation1556；2配置4头、1candidate；81D/1620 test候选、100共同估值日 | 只读输入607.703秒/486 SELECT；一次4头fit6.719秒；预登记/fit/评价28.781秒；拟合前后QE活动状态均0；无DB写/sealed/服务操作 | baseline/core13/timing15名义收益`19.1729%/1.6174%/3.0673%`；MDD`-10.2278%/-9.5256%/-7.3655%`；15减13日增量`+1.0789bps`、描述性95%`[-10.5944,13.2180]`；15减baseline`-15.0261bps`、95%`[-32.6430,-0.3110]`；candidate19真实TAKE＋7 UNKNOWN控制episode，控制37＋3，相对控制进入不同30日 | `STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION / EXPLORATORY_SCREEN / NAVIGATION_ONLY`；当前来源NON_VINTAGE、原生成员限制不变；不是沪深300超额、独立OOS或真实成交。停止该candidate，零confirmation/binding/启用，不关闭价格方向 |
| M0 可训练矩阵 | request `advmreq_ac5959aa8dc14a25e3b8c139` | 406 decision dates；8120 个 Top20 候选；6960 行冻结 features | 文件只读构建 | 训练/验证/test 的父输入身份已冻结，基础行情截止 `2026-06-30`，候选共同范围 `2024-07-04..2026-03-10` | 可训练输入已完成，不再扩建历史数据平台 |
| M1 首个 reranker | bundle `9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629` | train 3818 行/191 日；validation 1139 行/57 日；test 1599 行/80 日；Top5 400 行 | 128.921 秒；RSS 2,262,388,736 bytes | model Top5 5日平均超额 `-0.0002833`、命中率 `0.5025`、NDCG@5 `0.26570`；selection rank 为 `0.0085591`，HMM 为 `0.0040167`，随机为 `0.0055652` | 真实模型已接入 shadow，但原始质量明显低于基线 |
| M3 outcome/holding | bundle `17ce7ceb429829f15b68b196ad76ffee08d45f93b0a72d0f2fb92e72515adba0` | 46 个 LightGBM heads；test 1600 行/80 日；1/3/5/10/20 日 horizon | 108.128 秒；RSS 655,581,184 bytes | 五期限预测零 NaN；5日正超额 head AUC `0.53469`、Brier `0.25186`；holding accuracy `0.36261`、bucket-day MAE `10.1374`、range coverage `0.75031` | 功能和运行时已贯通；原始概率与周期分布保持实验/未校准语义 |
| M4 entry/risk ranges | request `advprreq_2d826a7b2704137bf3a60d9d`；bundle `1a939f05a3410ce56d66f68245a77e9454be8bf38afe57d57330341c41c742c3` | 4 个 heads；test 1600 行/80 日；1599 个 executable gap；8120 行中仅 4 个 binary 负例 | 13.85 秒；RSS 491,802,624 bytes | q10-q90 coverage `0.727955`，零 quantile crossing；历史固定日期真实多 Alpha readback 20/20 返回买入、止盈、止损和移动保护范围 | M4 v1曾完成exact point inference验证；当前生产descriptor已旋转到不含M3/M4 child的P0-D meta-label，故当前price-range typed unavailable，binary仍保持`UNCALIBRATED` |
| Daily price envelope v3 | request `advprreq_e788810c50b59802ec2344c3`；bundle `30e8a75b4b321a0be31ea6b8530c5bbc0afd2f81d28c2977e74e57226ddf1081` | 3 个 quantile heads；406 决策日/8120 候选；test 1600 行/80 日 | 12.564 秒；RSS 479,895,552 bytes | q10-q90 coverage `0.733125`，mean/median width `0.0123743/0.0116404`，零 crossing | 真实 v3 artifact 完整；已退役 binary head；未校准且未激活 |
| M5A Top5 tournament | train request `advm5train_a64594d6f22f618a4afef84a`；test request `advm5test_818fe5a6c8ee323d2fbf25d4`；bundle `1757b24b854f8b5bfee8874bd442491091ea979c86522fbeef15a02930f8ecb` | 45 trials、37 candidates；winner 为 5-seed `EXPANDING_ALL__LAMBDARANK_NDCG5__MW_0.75`；test 400 个 Top5/80 日 | tournament 18.925 秒、RSS 409,169,920 bytes；test 2.131 秒、RSS 342,462,464 bytes | winner 5日平均超额 `0.0071894`、命中率 `0.5425`、NDCG@5 `0.34150`；selection rank 为 `0.0085591`、`0.5375`、`0.32399`；均值 lift `-0.0013696`，95% block-bootstrap `[-0.0093061, 0.0053392]` | 相比 M1/HMM/随机有改善，但未证明超过现行 selection rank，不激活 |
| M5B outcome calibration | request `advoutcal_ec16422ad1a97040583e5273`；bundle `a2dea5157f1b768dff42ea844f7dc5a2d31563652967a6535adf89b228bd5533` | validation 940 feature-covered/1000 labels；test 1600/1600；8/10 binary heads calibrated，2 个 h5 heads 因 order reversal 保持 raw | 11.659 秒；RSS 399,200,256 bytes | 8 个 calibrated binary head 的 test Brier/logloss/ECE 均未优于 raw；收益区间名义 coverage 平均绝对偏差 `0.00984 -> 0.03129`；path upper `0.01432 -> 0.01394` | artifact 完整、exact retry 一致，但总体质量不支持激活；M3 v1 binding 不变 |
| M5C entry-gap calibration | request `advprcal_7cb766fe38898e12a008a328`；bundle `5197ceac96c76881a506555652acc006987442024cb2d86955e7370b27968ead` | validation 940 feature-covered/1000 eligible；test 1599/1599；central-80 CQR | 2.679 秒；RSS 395,853,824 bytes | validation coverage `0.810638` 导致 `delta=0`；test raw/calibrated coverage 均 `0.727955`，mean width 均 `0.0122280` | 全局常数校准无法修正 validation→test 漂移，`activation_recommended=false`；源码已合入但不激活 |
| Daily price envelope v4 calibration | request `advprcal_9a82e951973c7e58a2bc738f`；bundle `508fedfeb48a168792d6650b06cb197556b72f54c48d97a7fa9f6b2075de437f` | validation 940/47 日；test 1600/80 日；central-80 validation-only | 2.415 秒；RSS 388,501,504 bytes | validation/test raw=calibrated coverage `0.811702/0.733125`，零 crossing，校准扩张量 0 | `FRESH_CONFIRMATION_REQUIRED`；`activation_recommended=false`；零 binding/runtime |
| 首个自然价格settlement | request `advprpros_405d704a7dbe866eb0b6ae0e`；settlement `advprsett_14c46af1fa2bdb92088b425c` | T=`2026-09-15`；20/20市场可用、20/20模型可用；kline/suspend刷新审计均可用 | settlement与exact retry完成 | 业务coverage `0.70`；lower/upper miss `0.25/0.05`；mean/median width `288.615/249.832 bps`；mean/median mid error `116.422/63.188 bps` | 单日`ACCUMULATING`；零binding/DB写入/sealed消费；不得外推稳定性 |
| rolling-20D matured CQR导航审计 | request `advpradapt_8c81ea2bd2f70d75e1683fe9`；result `490fdb606a0d95c3853b7858e6ac76fdfae27aff09d1045c9867f2e2a62ec433` | 已消费80日/1600行；active 75日/1500行；static与唯一adaptive arm | chronological evaluation `5.611`秒；total-before-publish `5.766`秒；RSS `162,861,056` bytes | active model coverage `0.731333 -> 0.782667`，business coverage `0.810000 -> 0.847333`；model/business width ratio `1.149768/1.130019`；cluster bootstrap point/95% `0.012667/[-0.003333,0.030000]`；tick rescue/harm `56/0` | bootstrap lower-bound gate失败，selected=0；`NAVIGATION_ONLY`、exact retry no-op、lineage终止、不激活 |
| 生产前向基线 | 两个 ENABLED Program；真实baseline publish、target-open settlement和episode持续运行 | 截至2026-09-02 14:21第一个Program有6条P0-D observation，第二个Program尚无模型observation | scheduler running/thread_alive、run_count=28且last_error为空 | 第一个Program最早模型成熟日为2026-09-22；当前无成熟自然model outcome | 每日发布、target-open和episode闭环已验证；继续自然积累，open mark不冒充成熟胜率 |
| P0-C policy dataset | bundle `81e2c9bac5ce1f8e2fdc5a6174bc948dfbe984cf5028726c89ea72eb59fc69bd` | 386 candidate days；7,720 candidates；7,716 matured labels；28/28 READY CPCV paths | 28.9 秒；RSS 1.72GB | take 4,199 / skip 3,517；holding median 6 days；Selection rank buckets 均约 54% take rate | policy-aligned 标签和评价输入已完成；PR #3367 已合入 `49973d6e` |
| P0-D meta-label | final-source request v2 `advmetareq_0451bd4cb1f8cc7add8b9956`；bundle `e555903ec928fd39ea09180133401a6490a4e6d5440e3ef63642909e1329e03a` | 2 families × 3 seeds × 28 paths = 168；winner `FAMILY_CORE_HMM/20260817` | 332.182 秒；RSS 2.91GB；exact retry 3.529 秒；与旧 bundle 12/12 功能 identity hashes 一致 | winner `19.4357 bps` vs Selection `15.7801 bps`，lift `+3.6556 bps`，path win rate `64.29%`，PBO `0.40`，AUC `0.5142` | PR #3368 已合入 `458199cd`；exact descriptor已接入目标多Alpha Program，保持`EXPERIMENTAL_SHADOW/UNCALIBRATED`，不替换baseline |
| P0-D 历史虚拟前向 | artifact `fbf072f0d8c4a637a48aa8c2ed63c3b61c245abd08ac4e1417b2a0fcc8eb59a9`；父run `ahrr_e46883bcdf217a14d8e7a0abf01aeb18` | 44日context；24个成熟decision；20日tail；24/24 resolved | 冷日评分约15–20秒；恢复后完整复跑约18.5秒；最终24日 artifact exact retry 同hash | P0-D hit rate `36.67%` vs Selection `26.92%`，但累计净收益 `-19.45%` vs `-16.90%`，最大回撤 `-22.54%` vs `-16.90%`，平均换手 `40.00%` vs `34.67%` | 功能验证通过但模型质量不支持激活；窗口已消费，后续调模重跑降级为 `HISTORICAL_REPLAY` |
| P0-E outcome-weighted meta-label | final-source request `advmetareq_4d2393bcb776cf7d6a3aace2`；bundle `cb9e61e9c54d89263f76f2f2bcefb515070c96908aa2bca790c064fd339fb270` | 2 families × 3 seeds × 28 paths = 168；winner `FAMILY_CORE_HMM/20260817` | 346.325秒；RSS 2.74GB；exact retry 3.3秒 | `18.9626 bps` vs P0-D `19.4357 bps`，lift `-0.4731 bps`，path win `35.71%`，PBO `0.8143` | `research_improvement=false`；负面实验，不激活、不追加调参；rebase前后功能hash一致 |
| P0-E HISTORICAL_REPLAY | artifact `6bba37f8804af38f4357c3939a380cca3be2bc915a62149108518b6d4948dba4` | 同24决策日+20日tail；24/24 resolved；证据已消费 | 首日约31秒、后续约15–20秒；exact retry同hash | hit `23.08%`；累计 `-13.39%` vs Selection `-16.90%` / P0-D `-19.45%`；回撤 `-14.92%`；换手 `29.71%` | 幅度结构在该窗口改善但未被CPCV确认，只作诊断，不晋级 |
| P0-F continuous policy utility | request `advutilityreq_9e8036ee4a8ea785b6ba8742`；bundle `ff336ead...` | 3 arms × 2 families × 3 seeds × 28 paths；advancement arm为Huber utility | PBO `0.30`；完整168个candidate trial-path | 相对P0-D收益`+2.612087 bps`、path win`53.57%`、MDD`+0.001286`，相对Selection`+5.578137 bps`；换手`+0.007419` | 仅换手门槛失败，`NEGATIVE_STOP_NOT_ADVANCED`，不激活 |
| P0-G turnover-constrained utility | request `advturnutilityreq_369c2d23fb1d30e1ce801506`；bundle `433ff217...` | 2 families × 3 seeds × 28 paths = 168 | winner `20.937724 bps`；PBO `0.40` | 相对P0-D收益`+2.191621 bps`、path win`53.57%`、MDD`+0.002908`，相对Selection`+5.157670 bps`；换手仍`+0.004096` | 仅换手门槛失败，`NEGATIVE_STOP_NOT_ADVANCED`，不激活 |
| P0-H dual-head output constraint | request `advdualheadreq_027b41bd7b996fd25eab7b54`；bundle `82afdb81...` | 2 families × 3 seeds × 28 paths = 168 | winner `18.419750 bps`；PBO `0.90` | 相对P0-D收益`-0.326353 bps`、path win`46.43%`；MDD改善`0.010688`、换手改善`-0.022708`；return head日Spearman仅`0.041731` | 收益和path-win失败，`NEGATIVE_STOP_NOT_ADVANCED`，不激活 |
| P0-I grouped-rank return head | request `advgroupedrankreq_314c84d158a42be1b87e71d8`；evidence-only bundle `2378358c...` | 前10条path完成60个trial-path；第11条path首个family/seed停止 | 810.577秒；RSS 2.96GB；PBO不可计算 | 冻结8档price均不能满足exact P0-D OOF换手预算；已完成trial的return日Spearman均值`-0.002229`、NDCG@5 `0.385322`，liability Spearman `0.236089` | `NEGATIVE_STOP_INCOMPLETE_CPCV`，无winner/Stage B/激活 |
| P0-J Selection-prior residual | request `advselpriorresreq_3a50e2f6fd9cf43cb1f6ad3e`；evidence-only bundle `eb8ade9b...` | 第一条path的inner block 3停止；0 trial | 213.867秒；RSS 2.89GB；PBO不可计算 | decreasing-isotonic Selection rank prior完全退化为`-1.5357 bps`平线，reason `ADVISORY_P0J_SELECTION_PRIOR_DEGENERATE` | `NEGATIVE_STOP_INCOMPLETE_CPCV`；证明rank-to-return单调关系跨分区不稳定，不证明Selection全局无效 |
| P0-K Selection-preserving liability gate | request `advselgatereq_943f9e551d5fee35e57340cc`；bundle `fee9b561...` | 2 families × 3 seeds × 28 paths = 168 | 1173.362秒；RSS 2.93GB；PBO `1.0` | liability日Spearman`0.254589`；168个trial均选阈值`0.4`且零拒绝，完全等同Selection；相对P0-D收益`-2.966049 bps`、path win`32.14%`、MDD`-0.004162`、换手`-0.068009` | `NEGATIVE_STOP_NOT_ADVANCED`；PBO来自相同arm和固定tie-break，不按普通过拟合解释 |
| P0-L P0-G-anchored liability local reranker | request `advp0lreq_b86425d3b5ce508904fa01b0`；evidence-only bundle `4476afeb...` | BUG-1251修复后在第一条path、首个family/seed选择阶段停止；0/168 | 320.195秒；RSS 2.86GB；PBO不可计算 | identity control无干预；gain `12/8/4/1`产生`33/71/85/85`次entry变化并降低换手，但cash day由1增至2、coverage下降，全部非零档不满足完整性 | `ADVISORY_P0L_LOCAL_RERANK_INFEASIBLE`；`NEGATIVE_STOP_INCOMPLETE_CPCV`，无winner/Stage B/激活 |
| N1 Tier-1 oracle + fixed learnability | request `advn1req_d8116a79d72d9c1813485053`；bundle `74827d037128b9a4716afdf3a17221fda8c18b0af885be33c9f6978699283348` | canonical PIT 5067只/5077段；386 decision days；19300条Top50；7720条Top20 OOF；28 READY paths，每行7个OOF；382日可评价 | 397.922秒；peak RSS 2,584,461,312 bytes；exact delivery retry为duplicate no-op | Top20/40/50赢家召回`0.8808%/1.6062%/1.7617%`；perfect Top5 lift `1314.27 bps`，95%区间`[1136.61,1516.57]`；固定Ridge lift `98.83 bps`，95%区间`[1.18,193.73]`，MDE `136.01 bps` | oracle=`HIGH/CONTROL_READY/DIRECTION_GATE`；learnability=`INCONCLUSIVE/EXPLORATORY/NAVIGATION_ONLY`；综合`INCONCLUSIVE__THEORETICAL_HIGH__LEARNABILITY_HIGH`、`direction_ready=false`；不部署、不激活，route进入N2 |
| N2-A StrategyPackage三臂Alpha审计 | request `advalpha3req_e7295a31a4e1953e9048cec5`；bundle `6784df1abe1dcbb802220d03db70674638eda18b5001c2e022fc099c6bb3e9cd` | 386日；共同signal outcome 1,710,301行；LSTM/FUND/父包各19,300条Top50；父包与N1 ranking按keys和`1e-12`分数精确一致 | 70.861秒；peak RSS 2,588,618,752 bytes；inspect=`VALID`；exact retry=`EXISTING_BUNDLE`且registry duplicate no-op | LSTM/FUND/父包RankIC=`0.11677/0.05628/0.12284`；Top5 H20净超额=`397.89/245.73/446.52 bps`；父包-LSTM RankIC增量`+0.00607`、95%区间`[-0.00223,0.01484]`，Top5增量`+48.63 bps`、区间`[-103.99,186.53]`；父包Top50召回`1.7617%`、随机lift `1.75×` | `EXPLORATORY/ORACLE_DIAGNOSTIC/NAVIGATION_ONLY`、0 trial；Alpha主要来自LSTM，FUND较弱但不同源，固定组合未确认优于LSTM；不调权、不激活，route保持N2 |
| N3 margin information set | request `advn3margreq_0e6f63e49ae12d5b2e79f789`；source `633c4058...`；bundle `b50411d8...` | 386日、1,710,301个父包键；3个固定Ridge trial；323个共同可评价Top5日 | 74.567秒；peak RSS 2,742,165,504 bytes；temp 199,395,885 bytes；DB/network均0 | candidate/parent RankIC=`0.11385/0.12284`；Top5净超额=`252.80/443.65 bps`；candidate-parent delta/lift=`-0.00899/-202.21 bps`；四段joint-positive=`0/4` | `EXPLORATORY_INSUFFICIENT_SUPPORT/NAVIGATION_ONLY`、selected=0；精确frontier关闭，route进入财务事件source-readiness，不读sealed、不写factor/package/runtime |
| N3 financial event information set | source bundle `211b8db1...`；经济bundle `ad234f4c...` | 84,272行最早本地projection；固定3 trial；386日；non-vintage/date-only | source readiness 9.94秒；8次SELECT；DB write/network/Tushare均0 | candidate/parent RankIC=`0.063685/0.122839`；Top5净超额=`359.41/443.65 bps`；四段joint-positive=`0/4` | `EXPLORATORY_NOT_SELECTED/NAVIGATION_ONLY`、selected=0；事件frontier关闭，不进入vintage、confirmation或activation |
| N3 package-score/market-HMM Admission v1 | request `advscorehmm_2a442c84ecdac872a4e56e45`；bundle `f8da2f70...`；diagnostic `7f565394...` | 386日、1,930个父Top5槽位；3个可执行arm，2个sector arm因source缺失NOT_RUN；243个固定边际诊断点 | 正式运行与exact retry完成；sealed/DB write/runtime均0 | score/raw-market/market-HMM仅3/5/9个TAKE日，lift=`-26.71/-30.77/-28.55 bps/day`；117个support充分诊断点中0个方向性正增量，最佳`-2.292 bps/day`、95% `[-6.234,1.294]` | `AUX_EXECUTED_FRONTIER_INSUFFICIENT_SUPPORT`、selected=0；v1冻结，不能放宽阈值或充当跨日绝对校准 |
| N3 causal Admission v2.1 R1 | request `advcausal_9ed1c38ef6d6ff7fb5d3ef33`；bundle `7e739be5...`；`main@a317f7c2...` | 48日inner、240个父Top5槽位；静态/20日expanding Ridge共2 trial；outer按合同未读取 | PR #4395/#4402/#4404合入；正式首跑registry append 1；exact retry duplicate-noop/route exact-noop；sealed/DB/network/runtime均0 | 静态/expanding干预日`9/11`、coverage`90.42%/87.50%`；lift point`+2.408/+3.811 bps`、95% lower`-4.090/-1.704 bps`；Brier`0.2775/0.2687`劣于base-rate`0.2495` | `EXPLORATORY_INSUFFICIENT_SUPPORT/NAVIGATION_ONLY`、selected=0；当前score/raw+Ridge静态/更新frontier关闭，不调支持门、不进outer/R2/activation |
| HMM G2-A `rotation_L1` development source/executor | PR #4343/#4353；input manifest `ad2e3b5e...`；v1.3 proposal PR #4359 OPEN | 1,373日、31板块、42,563行；v1.2实际17/39 fits | 15-fit battery已选10D；process1 market+首GBDT后停止，process2未启动；未核验完整资源收据 | tree131 leaf1覆盖18日<旧20日要求，`STRUCTURAL_ACCEPTANCE_FAILED`；无完整accepted OOF，tail未读 | `PARTIAL_EXECUTION_STRUCTURAL_STOP_NO_ACCEPTED_OOF`；不是经济负结果，仅sector依赖受阻 |
| QE MA-E19/D2/D3上游诊断与rolling LSTM候选 | release `qe_hmm_full_v2_20260831`；MA-E19R3、D1B/D1C/D1D、D2；PR #4347 | LGBM 12/12；LSTM seed123 4个vintage；真实2026H1 D2 panel 295,661行/115日；同一CE3/h20/Top50/n_drop1/分钟Tail-TWAP合同 | 真实训练/预测/分钟组合评价已在QE线完成；oracle不可部署；不是Advisory trial | LGBM 2026H1三臂均负；rolling LSTM CAGR=`87.05%/84.21%/46.54%/36.23%`、3/4胜rolling LGBM，但2026H1 Top50=`-1.01%`；D2 reality tail recall `0.0135`、双oracle `0.1339`；固定blend拒绝；D3 Brinson缺冻结权重为NOT_COMPUTABLE | `CANDIDATE_LEG_PENDING_MULTI_SEED_AND_LOO`；当前父包、StrategyPackage、Advisory descriptor均不变；D2只导航soft top-down/右尾方向 |
| Position Timing L2/L4b-1相邻证据 | L2 bundle `eef1f771...`；L4b-1 corrected bundle `450f8c82...` | L2冻结Ridge+GBDT；L4b-1只接受真实可达SELL/AT_OPEN action card | PR #4346/#4348/#4351均合入；无DB/runtime/订单变更 | L2 Ridge negative、GBDT/study inconclusive、selected model为空；L4b-1为`INSUFFICIENT_PROSPECTIVE_ACTION_CARDS`且无selected side | 不计作Advisory Exit或分钟执行成功；保持规则路径与自然支持度积累，不抢占当前主线 |
| H0 v6 golden | report `docs/analysis/advisory_historical_fullstack_comparison_result_20260817.md`；artifact `F:/Dev/AIstock_model_artifacts/advisory_fullstack_comparison_configfix_20260817/comparison_result_v6.json` | 44个decision dates，`2026-05-15..2026-07-16`；A/B/C三臂；C修复后独立重跑44/44 | result hash `500d96e0...`；contract hash `652eef96...`；artifact file SHA256 `9c59219d...` | 市场全窗沪深300`-4.40%`；在28个matched交易日上，HMM/risk B5相对A5的3/5/10日平均收益改善`2.43%/3.17%/3.48%`且配对95%区间不跨0，Episode胜率`35.42%` vs `27.45%`；M5A C5相对A5各期限收益改善区间均跨0，Episode胜率`18.37%` | HMM/risk存在该窗口局部相对效果但绝对收益仍负；M5A无稳健增量。PR #3558已合入`1d1fc932`，结果只作H0不可变行为oracle |

### 1.4 方向一致性复核

2026-10-03复核：工程链路已具备，经济有效性仍未确认，二者分别计进度。

- 历史M/P/N研究与原始指标保留在§1.3/§9；因果Admission v2.1 R1实际已完成且selected=0，不再写成只有设计或等待训练。
- 收益型v1/v3/v4、H-TIMING-1、H-VALUE-ANCHOR-1均没有经济确认。最新价值锚相对基线/常数增量均负；更高胜率和较小回撤不改变其事前判定。
- #5344/#5346/#5347/#5348已合入；#5348 merge为`00df1475bcf0c623eaf56a4f4a77ed0bab0c9585`，本轮之前的四个任务树已官方清理。这些是已完成交付，不是下一轮待办。
- M5C原文件训练合同不变；后续已批准的收益型研究也消费有界只读历史DB投影，按§4.1记录NON_VINTAGE限制，不能把该来源冒充历史原生捕获。
- 最新D-only价值锚没有学到条件于T价格的收益分布；v4早已使用实际gap。因此加入gap、换分布头、换loss或把市场/HMM改名都不自动构成新信息。
- §6.3.3已完成严格D可知类别与价格交互的一次验证，H-CONTEXT-VALUE-1因信息增量及真实模型TAKE支持未通过而停止，不把matched改善当行业alpha。G0核对活动profile `20260928-v15-unified-moneyflow1`；不删14条池外候选，不以3977条可选分类UNKNOWN阻断基础业务，不扩支持或强补知晓时钟挽救本候选。用户另授权§16.6连续不同算法R2；真正新增经济信息（如板块强弱/相对偏离）仍先查信息重叠/公开时钟和映射再另立lineage。行业行情未验证不阻断其它预注册模型，HMM仍为可选后续对照。
- QE继续独占上游alpha研究；Advisory最多一条价格研究主线及必要正确性辅线。旧上游/相邻模块快照不作为当前运行状态或本轮任务。
- 复用当前离线内核，先小规模验证可学习性和动作增量，再做完整组合比较；仅幸存候选进入独立确认与生产适配。没有值得继续的新假设时停止，不转向平台建设或为旧失败补证。

## 2. Scope / 当前实施范围

本蓝图当前只覆盖直接产生真实模型和荐股能力的工作：

- 读取已有 QE H5/Parquet/Qlib Bin 基础数据和已有模型预测 PKL，并核对可用于训练的字段。
- 从现有文件构造候选级训练矩阵、标签和时间切分。
- WSL Conda 中复现既有LightGBM排序、分类、分位数和生存模型；新候选模型只在oracle+learnability分流后按预注册角色、信息集和lineage训练，不预先固定为P0同族或单一模型家族。
- 对开发窗口产生cross-fitted诊断；主线、特征、阈值、policy和目标合同冻结后，才在独立sealed holdout执行一次方向确认。
- 在正式预测时从数据库读取决策截止前可见的日频行情、行业、资金、HMM、停牌、ST和可交易性输入；不把盘中实时行情纳入当前Advisory合同。
- 使用同一特征定义完成训练文件与数据库预测输入的 schema parity。
- 每个 Advisory Program 独立运行；一个 Program 绑定一个单 Alpha 包或一个原生多 Alpha 父包。
- 模型编排按Program/package动态解析，但当前模型artifact按exact package/style/policy条件化；候选、父分数和父rank不得被省略后伪装成“包无关”推理。
- 不同策略包使用同一框架时，必须拥有独立验证的exact bundle，或通过P1-B matched/leave-one-package-out共享实验；不得把目标多Alpha包的M5/M3/M4直接投射到无bundle包。
- ENABLED Program 按交易日自动执行基线 review，持久化 `PUBLISHED` list version 和 episode；模型存在时同时持久化 challenger observation，模型不存在时基线照常发布并返回 typed unavailable。
- P0-D历史meta-label继续以冻结review policy下的episode净收益输出`take/skip/confidence`和Top5研究shortlist；新研究按角色输出Ranking、Admission、日频Entry或日频Exit建议及相对冻结基线动作的增量价值，均不形成分钟动作、自动下单或动态资金仓位。
- train/validation 内使用 purged rolling/CPCV 或样本规模允许的等价时序重采样，报告 trial 选择偏差；已经读取的冻结 80 日 test 不再用于方向、参数或阈值选择。
- 在荐股页面展示 Top5、收益范围、持股周期和价格区间。
- Advisory价格能力止于版本化日级价格区间：只使用决策截止前可见的日频PIT输入，发布次交易日开盘/进场、止盈、保护和止损参考区间及其不确定性、可用性和模型身份；不产生分钟时间点、订单数量、拆单计划或成交指令。
- 为未来外部消费保留只读`AdvisoryDailyPriceEnvelopeV1`边界；QE、Paper和Execution是否消费及如何转为分钟动作由其各自设计、训练、回测和激活，Advisory不修改这些模块，也不为其复制执行算法。
- 模型不可用、字段缺失和版本不兼容时错误可见，但不得阻断现有规则荐股基线。
- 若H0因可复现主线阻塞被条件性启动，历史验证才使用批量执行器连续处理冻结日期区间，复用静态工作区、区间读取和 raw Alpha artifact；每个交易日仍保持独立 decision cutoff、业务语义 hash、结果、receipt 和 checkpoint。
- 条件性H0中的实盘单日执行器与历史批量执行器必须共享同一逐日业务内核；允许运行信封不同，不允许候选、排序、增强、名单生命周期或 outcome 口径分叉。
- P0-D至P0-L研究族冻结为历史事实；最小JSONL trial registry和由其生成的单页路线只索引实验身份、分类、消费窗口、decision use和既有receipt，不复制证据或建设治理平台。
- 父包预测延伸先执行可行性spike，区分“冻结模型可直接推理新窗口”“只有历史预测且运行资产不足”“必须重训并形成新StrategyPackage identity”三种状态。
- 综合oracle按Tier 1至Tier 3逐层放行；每层同时区分clairvoyant/action上限与固定cross-fitted learnability结果，全部只使用开发窗口，不读取sealed holdout。
- Entry Guard与Exit-label组成一个边界明确的辅助工作包，但分别使用独立目标合同、decision clock、policy hash和增量价值标签；同一时刻只允许其中一个进入candidate训练/confirmation。条件化上游alpha仅准备无研究证据的输入/预算。动态资金权重不在当前范围，第一版只允许固定等权槽位中的`SKIP`和现金/空槽结果。
- 同包评分校准只消费exact StrategyPackage的父score/rank、当日score分布和冻结outcome；原始combined score只用于同日横截面排序，跨日期准入只能使用evaluation前成熟标签拟合的past-only chronological成本后绝对收益、概率和区间。v1 CPCV输出只保留历史研究用途。
- 市场形态、市场HMM和板块HMM/rotation作为三类可分离上下文进入一个后续辅助F2设计；原始市场宽度必须是HMM增量对照，HMM不可直接乘父score、不可单独形成默认硬否决，也不可把Top50外股票带入Advisory候选池。

## 3. Non-goals / 明确禁止

以下任务持续禁止进入当前主线。每日前向发布、模型 challenger observation和episode跟踪属于既有业务运行；边界明确的H0只有在直接解除主线阻塞时才不属于禁止范围，不能据此获得默认开发优先级：

- 与当前已授权回放无关的历史数据证据链建设、历史补账、历史归档和旧任务修复。
- 新建通用 Source Catalog、全历史 lineage、source revision union 或历史 correction E2E；H0 仅复用当前冻结 catalog，并定向减少重复全量扫描。
- 新建通用 observation/capture/label/snapshot 数据平台。
- 为训练重新执行多年 Historical Range/Phase 1R 业务任务；H0 只服务独立历史验证，不向模型训练回灌业务结果。
- 为训练向 Advisory 历史业务表写入候选、列表、Outcome、Summary 或 bridge DML。
- 处理旧 PARTIAL/RUNNING batch、旧 root、orphan artifact 或遗留状态。
- 通用自动重训平台、通用 ModelOps、自动模型激活、canary 发布平台、通用漂移治理、通用缓存平台和灾备。仅允许当前 P0 所需的 Advisory 收盘后执行器、基线/challenger 前向对照，以及 H0 所需的任务内内容寻址工作区和 PIT 区间读取。
- 把六层决策栈拆成六个并行项目，或同时运行多于一条模型主线和一条独立辅助线。
- 在oracle、learnability、特征筛选或探索阶段读取新的sealed holdout；holdout只允许主线冻结后执行一次方向确认，随后立即降级为已消费历史证据。
- 动态资金权重、组合仓位优化、自动下单或交易执行输入；Entry Guard产生`SKIP`和固定槽位空缺不等于资金仓位模型。
- Advisory内的分钟线择时、分钟特征研发、最佳分钟买卖点预测、成交概率模型、订单拆分、成交计划、滑点优化以及QE/Paper执行适配。已经完成的N3分钟信息集MVE只保留为历史研究事实，不构成后续任务或运行能力。
- 为trial registry、活动路线、oracle或holdout另建UI、审批、角色、数据库平台、证据仓库或通用研究调度系统。
- 除本次用户明确批准、仍处于设计态的`ADMISSION_RISK`外，未经用户确认新增角色、审批、授权流、人工放行、策略包二次准入或运行时package preflight。
- 改动 Selection、Paper、模拟盘、QMT 或策略包既有业务逻辑。
- 把当前package-conditioned exact bundle宣传为任意策略包可复用，或为了制造表面“包无关”而删除父Alpha、候选rank、style/policy等必要条件信息。
- 对按日标准化的父包raw/combined score设置跨日期固定绝对阈值，或把未校准score描述为收益概率、预期收益和统一包间尺度。
- 把市场形态、市场HMM、板块HMM和父包score无消融地相乘/加总，或以`BEAR`、`fading`等单状态直接形成默认`SKIP_ALL`硬门禁；硬规则只能作为透明control arm。
- 在`rotation_L1`尚无canonical causal OOF/prediction bundle或未取得对应Advisory能力时，把研究页面结果、旧HMM系数、smoothed/Viterbi全序列状态或latest snapshot静默用于正式荐股。
- 由板块HMM从Top20/40/50以外补入股票；该动作改变候选召回所有权，必须另行进入QE/StrategyPackage上游Alpha合同。
- 使用 QE 回测组合净值、Paper/模拟盘结果作为训练输入。由 QE 文件行情按冻结 Advisory review policy 确定性模拟的 episode 结果是正式监督标签，不是回测结果跨模块耦合。
- 用 mock、随机输出、规则排序或静态 JSON 冒充真实模型预测。

不禁止读取 QE 实验中已有的 H5/Parquet/Qlib Bin 基础数据，以及 Prediction Store 中的 `pred.pkl`、各腿 seed 预测、`combined_prediction.pkl`、权重和必要模型产物。历史M/P文件模型继续按原合同消费基础行情文件；后续已批准的日频价格研究允许§4.1的有界只读DB投影。预测、模型参数和其它非基础行情产物允许使用 PKL。所有读取必须只读，不修改 QE 实验资产，也不把 QE 组合回测净值、交易或持仓结果当作模型监督信号。

## 4. 数据权威与防泄漏边界

### 4.1 模型训练数据

模型训练默认且优先使用已有 QE H5/Parquet/Qlib Bin 基础数据和已有 QE 预测 PKL，不从生产数据库重新构建多年历史数据。

日频价格研究已批准的例外：优先复用原候选/窗口的既存只读投影；确缺本次新增日频信息时，可在Advisory现有只读source中按固定日期、股票及列预算消费数据库现有记录，不补数据、不重建候选、不写库。正式拟合只消费该次固定输入，原历史模型文件合同不追溯改变。可见性、行业成员生效日、版本与标签时钟必须证明；当前历史回填值只能按实际NON_VINTAGE资格用于探索，无法证明业务所需PIT时停止该假设。不得仅为研究把raw状态包装成原生receipt。

允许的训练输入包括：

- 已有 QE H5/Parquet/Qlib Bin 中的日线、复权、成交量、资金、估值、行业、指数、停牌、涨跌停和因子特征。
- 仅使用上述基础数据按本蓝图标签公式派生的收益、MFE/MAE、周期和价格标签；只有 QE `label.pkl` 与目标标签定义逐字段一致时才可直接复用，否则只作为对照，不得偷换标签语义。
- Prediction Store 中目标父包精确 roster 引用的各腿 `pred.pkl`、模型元数据、历史 seed ensemble、`combined_prediction.pkl` 和逐日 weight。允许直接读取 PKL，不要求为形式统一复制成 Parquet。
- 当前父包正式 runtime 每腿只运行代表 seed，并使用 `frozen_backtest_terminal_weights`。首模历史候选必须按两个代表 seed、当前 zscore、terminal weights、raw Top25 和 Program target_count=20 确定性重建；完整 38-seed ensemble、逐日 weight 和 `combined_prediction.pkl` 只作不进入首模特征的显式分布诊断。
- 不得把不同父包、不同演进实验、不同 roster 的“最新腿”临时拼成训练输入，也不得把代表模型结果复制为多 seed 后生成伪离散度。
- Qlib 分钟 Bin不进入后续Advisory价格区间训练或正式推理。既有N3分钟信息集MVE及其数据覆盖收据只保留为已消费历史证据；任何新的分钟择时或执行模型由QE/Execution另立合同，不属于本蓝图。

训练过程只需记录直接保证模型可加载和特征一致性的最小信息：

```text
training_run_id
experiment_id and hypothesis_family_id
study_type and objective_contract
input_file_paths
input_schema_version
feature_names and dtypes
label_definition_version
train/validation/test date ranges
consumed_windows and decision_use
model_family and parameters
model_file_path and sha256
code_commit
WSL environment identity
```

禁止为训练输入额外生成逐日 source receipt、逐分区 revision chain、capture membership、SEALED manifest、CAS publication 或历史 correction 记录。

#### 4.1.1 2026-08-07 已验证输入清单

| 输入 | 已验证范围/内容 | 当前用途 |
|---|---|---|
| WSL `/home/lc999/data/qlib_bin` | 日线 `2018-08-01..2026-06-30`；包含 OHLCV、复权、`limit_up/down`、涨跌停价和 `prev_close` | 首模基础特征、标签、HMM重训和日线价格范围 |
| WSL H5/Parquet candidate | `/home/lc999/data/factor_data_versions/qlib_st_pit_active_h5_daily_candidate_20180801_20260630_moneyflow_v2`；日线、基本面、资金、行业、筹码和静态因子 | 首模候选级特征；训练不在 Windows 读取 |
| WSL `/home/lc999/data/qlib_minute_bin` | `2024-01-02 09:30..2026-06-30 15:00`，约33GB，含分钟OHLCV与涨跌停字段 | 仅为已完成N3分钟信息集MVE的历史输入；不进入后续Advisory价格区间研发或运行时 |
| Prediction Store | 当前目标父包精确 roster 为 LSTM 33 seed + FUNDGROWTH 5 seed；两个 runtime 代表 seed 与完整 38 个 `pred.pkl` 均存在 | 代表 seed 用于 runtime-equivalent 候选；完整 ensemble 仅作诊断 |
| combine workspace | 406 日 `combined_prediction.pkl`、逐日权重和组合因子文件存在 | 已回测 walk-forward 组合参考，不作为当前 runtime 候选权威 |
| suspend sidecar | `suspend_d_daily_candidate_20180801_20260630/suspend_d.parquet` | 历史停牌状态 |
| 沪深300 | 日线 Bin 内 `000300.SH` 可读 | benchmark和HMM超额收益 |

当前基础数据能够启动真实训练。目标多 Alpha 各腿共同预测范围实际形成 406 个 decision dates：`2024-07-04..2026-03-10`，因此依赖各腿预测的第二阶段模型样本不得超出该共同范围。基础行情和 H5/Parquet 截止 `2026-06-30`，用于完成 `2026-03-10` 候选的未来标签，不代表候选或 HMM 连续状态可以延伸到 `2026-06-30`。任何需要未来收益的训练头都必须按各自 horizon 剔除或右删失尾部未成熟样本；不得把无未来结果的候选当作完整标签。

当前活跃日线 Bin 未包含中证500、中证1000、创业板指或科创50。它们不是首个 Top20→Top5、收益、周期、日线价格范围或现有 HMM 架构的阻断输入；只有后续模型合同明确使用且实验证明有必要时才补充，不预建通用指数库。

M0 已实现 `OFFLINE_RUNTIME_EQUIVALENT_SELECTION_EFFECTIVE_TOP20_V2`：代表 seed + current zscore + terminal weights 先生成 raw Top25，再取 Program target_count 前20。它同时绑定 `decision_as_of_trade_date` 和下一交易日 `target_trade_date`；正式特征只能读取前者 cutoff。真实文件得到 406 日、8120 个候选且每日深度固定 20；combined/ensemble 只作为诊断，不进入 runtime-equivalent 候选语义。M1 bundle、M3 outcome bundle 和 M4A price-range bundle 均已生成，后续阶段继续精确绑定这些身份。

#### 4.1.2 研究登记、开发窗口与sealed holdout

研究登记使用一个只追加JSONL权威索引；Parquet或单页路线只能由该索引派生。每条记录至少包含：

```text
experiment_id
study_type = ORACLE_DIAGNOSTIC | LEARNABILITY_AUDIT | EXPLORATORY_SCREEN | CANDIDATE_MODEL | CONFIRMATION | ACTIVATION
hypothesis_family_id and parent_lineage
unique_variable
objective_contract = ALPHA_RANKING | RISK_MANAGED_ADVISORY
dataset/schema/policy identities
generated/evaluated/selected trial counts
consumed_windows
result_class
decision_use = NAVIGATION_ONLY | DIRECTION_GATE | ACTIVATION_EVIDENCE
evidence_refs
```

- registry只索引既有request、bundle、report和receipt，不复制完整结果，不替代QE warehouse、Prediction Store或模型artifact。
- oracle诊断、learnability audit和候选模型trial分类计数；DSR/PBO只消费与其统计定义匹配的模型/策略trial，不把一个oracle指标机械计为独立模型trial。
- 探索性结果可以调整下一步研究优先级，但不能单独关闭整个方向、声称稳定效果或作为激活证据；机器校验必须拒绝`NAVIGATION_ONLY`被引用为`ACTIVATION_EVIDENCE`。
- 开发窗口允许反复用于预注册oracle、learnability和探索，但每次使用都登记为已消费；新的sealed holdout使用不同dataset/window identity，并拒绝所有oracle、learnability、特征筛选和调参请求。
- sealed holdout只在主线candidate、模型、特征、阈值、policy和目标合同全部冻结后执行一次方向确认；读回后立即登记为已消费，失败不得返回同一frontier重选候选。
- holdout隔离以明确日期分区、dataset identity、命令访问拒绝和消费receipt实现；不为此建设新数据平台。

父包预测延伸spike必须在承诺新holdout前给出三态结果：

1. 冻结父模型和完整runtime assets可以对`2026-03-10`之后的PIT特征直接推理；新prediction artifact使用新日期/输入identity，但父模型identity不变。
2. 只有历史`pred.pkl`可用，冻结运行资产或因子输入不足，不能生成新窗口；报告精确缺口，不以最后一日预测外推。
3. 必须重训父模型；这形成新模型和新StrategyPackage lineage，不能冒充旧父包自然延伸，训练/确认时间边界需重新冻结。

N0已登记开发窗口`2024-07-04..2026-03-10`及已消费回放`2026-05-15..2026-07-16`；sealed元数据登记为`2026-08-31..2026-11-30 / SEALED_UNCONSUMED`。本次只读`research_window_contract.json`元数据，不读取sealed收益。新candidate冻结时间晚于窗口起点时，必须核实父包训练/研究消费重叠并界定真正未消费的确认人口；不得事后挪窗、删登记或以数日样本冒充完整确认。父包spike的`FROZEN_MODEL_CAN_INFER`只证明可产预测，不证明延伸窗口是独立OOS。

### 4.2 正式预测数据

只有正式执行 Advisory 模型预测时才读取数据库中的实际当前/实时数据：

- 当前策略包生成的候选、Alpha rank/score 和原生多 Alpha component evidence。
- 数据库中截至决策截止时刻已落库的日频行情、复权、资金、估值、行业和交易状态；当前范围不消费目标日实时分钟行情。
- 本轮新训练模型产生的当前 HMM预测，以及当前risk policy、ST、停牌、涨跌停和股票池输入。
- 当前 Program、binding 和模型配置。

正式预测不得读取 QE H5/Parquet/Qlib Bin 作为当前行情替代，也不得用训练文件中最后一日数据冒充实时数据。历史 PKL只用于模型训练和离线评价；正式预测的 Alpha/多 Alpha分数必须来自该 Program 当次实际候选和父包组件输出。Advisory 的 `target_trade_date` 与 `decision_as_of_trade_date` 必须分别保存，任何正式数据库特征的 business date 都不得晚于 decision cutoff。

### 4.3 训练与预测 schema parity

训练和预测必须调用同一个特征公式注册表，并分别通过：

```text
QEFileFeatureSource -> SharedAdvisoryFeatureBuilder
DatabaseRealtimeFeatureSource -> SharedAdvisoryFeatureBuilder
```

必须核对特征名、dtype、缺失值语义、单位、复权口径和顺序。schema 不兼容时该模型预测明确失败，现有规则荐股继续运行；禁止静默丢列、补零、换特征或返回基线排名冒充模型结果。

### 4.4 最小防泄漏规则

防止未来数据泄漏只保留直接影响模型正确性的规则：

- 时间切分按交易日执行，训练、验证和测试不得随机混合日期。
- 特征时间必须不晚于预测决策时点。
- 标签只能使用决策时点之后的收益或路径。
- 相邻标签窗口按需要设置 purge/embargo。
- scaler、缺失值统计、行业编码和任何拟合转换只能在训练区间拟合。
- 训练期模型选择不得读取最终测试集结果。

这些规则由训练代码和定向测试验证，不建设独立证据平台。

### 4.5 HMM重新训练、外部预测与因果边界

HMM是市场/行业状态先验、候选上下文和风险对照，不是Top5模型的主要监督目标。允许两种且仅两种研究输入：在每个train fold内从当前文件数据重新拟合并对validation逐日forward-filter；或消费另一个已冻结研究合同产生的canonical causal OOF/prediction bundle。禁止扫描或拼接任意旧/latest HMM模型、状态、系数和结果作为新模型输入；历史产物只作明确标记的对照。

- 保留现有市场/行业级两状态Gaussian HMM、因果forward-filter和状态解释架构；训练窗口结束后的每个posterior只从过去posterior与当时可见观测递推，禁止full-sequence smoothed posterior、事后Viterbi state或未来命名状态进入特征。
- 使用当前QE H5/Parquet/Qlib Bin中的指数/行业收益、相对沪深300收益、行业成交量、市场宽度和真实`$limit_up`比例；原始市场宽度单独保留，不能因进入HMM observation而失去无HMM control。
- 每个预注册静态或更新版本只在对应fit截止前拟合HMM参数和observation transform，版本内固定参数逐日forward-filter；状态按train-only经济含义确定性规范，不得用`date.today()`或全窗口收益隐式决定状态名称。
- 板块上下文必须使用decision date当时有效的PIT行业映射；停牌、映射缺失、posterior unavailable和rotation unavailable保留候选并输出typed reason，不删股票、不填造neutral state。
- 禁止用涨幅大于9.8%的近似值替代Bin中已存在的真实涨停标记，以免ST、创业板和科创板语义错误。
- 当前数据库版`SectorHMMTrainer`算法可复用，但训练数据适配必须改为文件读取；正式预测加载与holdout相同的参数和文件截止posterior，并仅追加数据库decision-cutoff后续观测执行因果预测。若重拟合HMM，必须同步重建依赖它的特征和模型，不能单独替换。
- 新`rotation_L1`只有通过自身合同的canonical causal OOF/model/input/mapping/availability校验后，才能进入未来新的Advisory信息增量lineage。v1.2已17/39结构停止，PR #4359仍开放；不存在完整accepted OOF。该依赖此前未阻断且现在也不改写已完成的v2.1 R1基础因果Admission。development OOF可在不读tail、不等待HMM API/UI/DDL的前提下进行target-free preflight；不可部署研究信号不等于capability，运行时仍需来源自身与Advisory各自确认。
- 若父包score、Selection或基线已经消费HMM系数，输入身份必须显式标记并提供pre-HMM control或factorial ablation；禁止把同一HMM暴露重复计入父score与Advisory上下文后误报增量。
- HMM重训或预测失败必须显式记录，不得静默复用旧HMM系数，也不得阻断不依赖HMM的现有规则荐股。

## 5. Architecture / 唯一目标架构

### 5.1 训练链路

```text
existing QE H5/Parquet/Qlib Bin base data
  + exact parent-roster prediction PKL
  -> read-only schema and coverage inspection
  -> QEFileFeatureSource
  -> SharedAdvisoryFeatureBuilder
  -> candidate groups + frozen review-policy episode labels
  -> development-window candidate/rank oracle + fixed cross-fitted learnability audit
  -> bounded Entry/Exit action-space diagnostics when Tier 1 identity is complete
  -> optional package-score calibration and market/HMM auxiliary ablation under a separate lineage
  -> combined typed route selects exactly one main research line
  -> task-appropriate validation: ranking may use purged CPCV; absolute Admission uses past-only chronological fits
  -> independent confirmation window + natural forward stream, never read by development diagnostics
  -> WSL Conda real model training for the selected role
  -> validation predictions, intervention support and family-level trial-selection-bias diagnostics
  -> one frontier candidate selected from inner development OOF/validation only; outer confirmation is not selection data
  -> one-time sealed-holdout direction confirmation when eligible
  -> model file + minimal load manifest
  -> historical research report + append-only registry row; no reuse of consumed windows for selection
```

该链路不创建 Historical Range batch，不写生产数据库，不进入 Phase 1R bridge，也不依赖 Source Catalog 或 SEALED snapshot。

### 5.2 正式荐股预测链路

```text
existing admitted StrategyPackage
  -> current single-Alpha or native multi-Alpha candidate Top20
  -> persisted baseline Advisory review and bounded Top20 list
  -> active binding resolves exact package/style/model bundle
  -> exact-package score calibration when independently confirmed
  -> raw market-shape control + causal market/sector HMM context when independently available
  -> DatabaseRealtimeFeatureSource
  -> SharedAdvisoryFeatureBuilder
  -> loaded role-specific WSL-trained bundle when available
  -> optional Top20 rank + Admission Risk + daily Entry/Exit advice, each on its own clock
  -> at most Top5 research shortlist or typed NO_ELIGIBLE_RECOMMENDATION
  -> fixed-slot cash/empty-slot outcome; no dynamic capital weights or silent backfill
  -> return/holding/daily price-range models and risk context when available
  -> persisted daily challenger observation + forward outcome/episode maturation
  -> Advisory API
  -> Advisory page
```

模型服务位于 Advisory 消费层，不反写 Selection、StrategyPackage、Paper 或模拟盘。多个 Program 独立执行；运行时不得继续依赖单一 `PROGRAM_ID/PACKAGE_ID/MANIFEST_SHA256` 常量，而应由 Program active binding 精确解析 bundle。没有 bundle 的 Program 仍发布原始基线并显式返回模型不可用。bundle 必须匹配 package/manifest/style/schema，参数不得因显示风格相同而自动跨包共享，候选、排名、列表、observation 和 episode 不能跨 Program 混合。可先按P1-B设计离线共享实验；只有matched证据证明兼容后，才部署显式compatible-set共享binding。

价格区间发布到Advisory API/页面即为本蓝图链路终点。任何下游分钟回测、模拟成交或实盘执行只能读取已经冻结、带身份的日级价格区间，不能由Advisory主动调用、编排或写入其运行状态。

### 5.3 角色分离决策栈与双目标合同

目标架构包含六层，但工作计划最多保持一条主实验线和一条不依赖主线结果的辅助线：

| 层 | 责任 | 决策时钟 | 首要验收 |
|---|---|---|---|
| 上游alpha/候选召回 | 把未来潜在赢家送入Top20/40/50 | T日收盘及以前 | 可交易赢家召回、成本后候选流质量 |
| Top20排名 | 在冻结候选池内提高优先级 | T日收盘后 | Top5/Top10增量净超额、真实干预支持度 |
| 日频Entry Guard | T+1开盘或指定日频观察点决定是否仍值得买；不是逐分钟执行器 | T日冻结阈值；T+1只读该观察点已可见的权威open/current | 相对无保护基线的增量价值、追高损失、现金暴露；不输出分钟计划 |
| 固定槽位现金 | 对`SKIP/WAITING`保留空槽而不强制补位 | 与Entry Guard一致 | 空槽/补位matched frontier；不输出资金权重 |
| Exit | 每个持有决策时点判断继续持有或退出 | 持仓后每日as-of | 退出相对继续基线policy的剩余净价值、MDD和尾部损失 |
| 组合风险 | 约束regime、beta、集中度和尾部风险 | 与对应Advisory动作同clock | 风险改善及机会损失；当前仅研究overlay，不形成资金仓位 |

同包评分校准和市场/HMM条件化不是第七、第八条并行模型线。评分校准为Ranking、Admission和Entry提供package-bound可比尺度，但跨日absolute尺度必须来自past-only chronological训练；原始市场形态、市场HMM和板块rotation是决策上下文。市场HMM回答“当天是否值得承担风险”，板块rotation回答“候选处于何种板块环境”，StrategyPackage父Alpha仍回答“候选池内哪只股票优先”。任何一层均不能单独补回上游未召回股票。

双目标合同在实验创建时冻结，结果后不得改判：

| objective_contract | 评价口径 | 合法动作 | 激活/展示 |
|---|---|---|---|
| `ALPHA_RANKING` | 相对Selection与benchmark的成本后超额收益 | 冻结候选、槽位和Entry/Exit基线下的排名变化 | 只声明排名alpha，不承诺绝对收益 |
| `RISK_MANAGED_ADVISORY` | 绝对收益、MDD、尾部风险及相对原Advisory policy增量 | Entry Guard、Exit和固定槽位现金/空槽 | 与alpha合同独立激活；动态资金权重仍未授权 |

同一模型在两个合同下的状态分别记录；页面/API必须显示`objective_contract`、基线policy和证据等级。禁止选择结果更好看的合同事后归类，禁止把两个合同压成一个加权总分。

未来`RISK_MANAGED_ADVISORY`新研究还须在立项时固定primary endpoint：本次后续计划默认检验成本后收益增量，风险作预定义约束和独立报告。若另立“允许收益代价换风险下降”的假设，须在结果前明确最大可接受收益损失、最小风险改善、置信口径与简单风险对照，不把任意低收益/低回撤称为通过，也不复活旧失败候选。可比暴露分析按§6.3.3实现，不授权动态资金权重。

#### 5.3.1 日级价格区间发布与外部消费边界

既有合同为`AdvisoryDailyPriceEnvelopeV1`（legacy-v1完整价格信封）。显式`entry-v2`附加独立买入区间响应已随PR #5099合入并经用户重启加载，保持V1默认行为及原artifact不变；ENTRY_PRICE角色尚未绑定。其可用性独立于M3/Ranking，训练来源身份和实际父Alpha特征依赖仍保留。旧P0-D直接短路价格输出、V1要求全部辅助区间成功的问题，由[独立角色设计](advisory_entry_price_independent_role_f2_design_20260928.md)的新通道解决，旧通道语义不变。每个价格预测仍至少绑定以下适用字段；V2未启用辅助角色时按角色返回unavailable，不补规则值：

```text
symbol
decision_as_of_trade_date
target_trade_date
price_basis = UNADJUSTED_CNY_DECISION_CLOSE
decision_reference_price
entry_price_range = {condition, low, mid, high}
calibrated_entry_price_range and entry_gap_calibration_state
take_profit_price_range
protective_price_range
stop_loss_price_range and hard_risk_boundary
calibration_state and coverage_evidence
package_id / manifest_sha256 / program_id / binding_version_id
model_bundle_id / model_manifest_sha256 / review_policy_sha256
status / reason_code / availability
```

既有legacy-v1/entry-v2合同语义仍为“决策截止时可见信息下的次交易日价格分布和风险参考范围”，不是报价、成交保证或订单指令。2026-10-02起主动业务主线按§6.3.2另立ENTRY_VALUE经济建议合同：不改旧schema、不把旧q90当最高值得买价，不把开盘分布确认当收益确认。以下字段和行为继续禁止进入Advisory价格合同：

- 最佳买入分钟、最佳卖出分钟、未来全日最低/最高点；
- `BUY_NOW/WAIT/SELL_NOW`逐分钟动作、订单数量、参与率、拆单权重、限价单计划或fill结果；
- 任何尚未发生的目标日分钟bar、VWAP、最低/最高价或完整日路径；
- QE执行配置、Paper持仓/成交状态、Execution plan或实盘broker对象。

外部模块若消费该合同，必须自行冻结消费版本、验证PIT时钟和经济增量，并保持Advisory只读无回写。外部执行失败不得改写已发布价格区间；Advisory模型不可用则返回typed unavailable，不以规则区间填充模型字段。

### 5.4 基线连续性

- 模型通道失败时保留当前规则荐股结果。
- 页面必须明确区分 `rule_default`、`experimental_model` 和后续 `validated_model`。
- 禁止把规则结果填入 model 字段。
- 禁止因模型缺失阻断单 Alpha或原生多 Alpha现有荐股。

### 5.5 条件性实盘单日与历史批量双执行形态

本节只定义H0被主线阻塞条件触发后的备用执行合同，不构成当前任务。触发后，H0将“业务语义”和“执行拓扑”分离：

```text
LiveDailyExecutor ───────────────┐
                                ├─> authoritative day business composition
HistoricalBatchExecutor ────────┘     -> StrategyPackage day signal
                                      -> HMM/risk/tradability
                                      -> Selection projection
                                      -> AdvisoryListTransitionEngine
                                      -> day semantic result + receipt
```

- 实盘每天只传入一个交易日和当前正式 Program/binding，保持现有 after-close、target-open 和发布语义。
- 历史执行器传入冻结 date plan、source catalog 和 research identity，以默认5日 chunk 在同一 worker 内顺序处理；chunk 可调，但不改变日期计划。
- 共享业务层只接受日级 `AsOfDataView`，无权访问批量源的未裁剪数据。历史批量读取可以减少数据库往返，但每个视图必须同时执行 `business_date <= decision_date`、`available_at <= decision_timestamp` 和冻结 revision 约束。
- 静态模型、因子代码和配置在 package/manifest/model/factor/runtime identity 不变时整批复用；日期动态数据、HMM coefficient、风险事实、候选、名单状态和 receipt 不跨日复用。
- 批量模式不意味着把多个交易日合并为一个决策。每个交易日仍产生独立业务语义 hash、typed failure、artifact 和 checkpoint。
- 整个 artifact 的 batch/worker/timing 字段可以不同；逐日候选顺序、分数、stage trace、source refs、HMM/risk结果和名单动作必须与相同输入的单日执行语义一致。

详细组件、缓存键、失败恢复和等价性合同见 H0 详细设计。任何实现若需要另写 Selection、HMM、risk 或 list transition 算法，视为设计违例。

### 5.6 角色独立binding与组合合同

目标运行时必须把`RANKING`、`OUTCOME_HOLDING`、`ADMISSION_RISK`、`ENTRY_PRICE`和`EXIT_RISK`建模为同一Program下相互独立的角色槽位，而不是用一个互斥descriptor代表“全部荐股模型”。每个槽位分别记录package/style/feature schema/policy/model identity、状态和证据等级：

- 一个新Ranking descriptor不得静默移除已经验证兼容的Outcome或Price角色；明确不兼容时，对受影响角色返回typed unavailable并保留原因，不能用Selection规则区间填充model字段。
- 角色组合只允许发生在相同Program、package manifest、style、decision clock和兼容feature/policy identity下；跨包、跨style或跨policy拼接必须fail closed。
- 每个角色独立激活、回滚和展示。页面必须区分“组件曾经实现”“当前角色已绑定”“当前推理可用”“业务效果已确认”四种状态。
- 当前生产P0-D仍是meta-label互斥descriptor，M3/M4 child typed unavailable；ENTRY_PRICE独立推理和Program级binding源码/历史时钟回归已实现，尚未部署。发布active角色仍需对应确认通过。只实现一个价格角色，不预建通用role-stack平台。
- `ADMISSION_RISK`只输出逐候选`TAKE/SKIP/UNAVAILABLE`和日级`TAKE_SOME/SKIP_ALL/UNAVAILABLE`，保留最多五个固定等权槽位；它不输出资金权重、不自动补位，也不改变`RANKING`合同下的Alpha结论。

### 5.7 公共预测层、策略适配与独立动作层

六层业务职责保持不变；实现目标从“每包一套全功能主模型”调整为三类清晰接口，不强制一个多任务大网络：

1. **候选来源**：当前为单包Top20；未来可由QE/Selection在独立合同下提供多源候选或PIT universe的廉价初筛。Advisory不偷偷扩池，不混合不同包的原始score。
2. **预测层**：公共stock/date/as-of特征产生收益均值、概率、分位数、波动/路径风险；包adapter显式提供score/rank、风格、候选来源和政策条件。先以独立训练的小模型完成预测对象分离，再比较是否共享representation；当前`shared_feature_builder.py`仍要求lstm/fund两腿，通用adapter尚未实现。
3. **荐股动作层**：Ranking优化同池优先级；Admission判断净价值与风险；日频Entry用开盘或明确日级观察点的实际可见价重估是否进入；日频Exit比较下一可交易日卖出与继续policy的剩余价值。各层使用自己的时钟和policy hash，不以一个总分代理全部任务，也不产生分钟执行计划。

公共预测不等于包无关效果。纯价格/收益 estimand 在明确适用人口中可跨包研究；policy value标签随policy变化必须重建。相同stock/date跨包重复记录按簇分组、禁止跨fold泄漏或重复计有效样本；按包分别报告负迁移、matched benchmark、leave-one-package-out与模型来源。新包未验证仍typed unavailable。

### 5.8 候选扩展、层级建模与运行韧性

- **多源召回的最小实验**：在相同PIT、成本、候选预算和policy下比较单包、多源去重、简单全市场初筛；记录provenance与重合，先看净正机会覆盖/分桶和实际Top5组合增量。由QE/Selection拥有候选生成，进入Program前另立精确合同；本版不改变单包运行时或允许静默补位。
- **市场—板块—个股分解**：将公共市场风险、板块相对强弱、个股残差作为可归因特征/预测对象；原始上下文为control，HMM/soft router为可选增量。分量联合分布和相关性需校验，不能把独立分位数相加成总收益区间，也不能重复计算父包已有市场/HMM暴露。
- **有版本的可用性**：复用现有FeatureBuilder和来源适配；schema记录字段available_at、版本和正常缺失类型。仅可使用预先训练并验证的no-HMM/缺源变体；禁止故障时删列、全填零、借旧模型冒充成功。
- **业务连续性边界**：独立模型失败不应阻断可计算的规则基线；但父包本身无法推理时，基线也不能伪造。系统输入故障、正常停牌、模型不确定弃权和合法全SKIP必须分报。
- **实施节奏**：先修实际发布故障与一个预测/动作切片，再按实验瓶颈落地adapter或候选扩展。现有批量/单日共享能力可复用，H0依然休眠；不为上述接口建设新平台。

## 6. 模型功能

### 6.1 已冻结的SHORT_REBOUND selection/meta-label研究族

现行Selection继续产生生产基线候选和Top20顺序。P0-D至P0-L曾在固定P0-C数据、候选、feature schema v2和CORE/CORE_HMM家族内依次研究坏候选过滤、连续收益、换手约束、liability和局部重排；下列合同保留为历史事实和运行兼容边界，不再作为下一模型主线：

- group：同一 Program、package、`decision_as_of_trade_date/target_trade_date` 的 runtime-equivalent 候选 Top20。
- 输入：selection score/rank、父包腿分数与腿间分歧、HMM regime、行业状态、流动性、拥挤度、波动、可交易性和决策时可见的价格/资金特征；未来 episode path、MFE/MAE 和退出原因只能构造标签或评价，不能进入当时特征。
- 冻结 policy：生产 baseline 继续使用当前 Top20 Program policy。Top5 challenger 另建显式 `model_shadow_review_policy`，只把 `target_count/rank_enter_threshold` 固定为5，保留当前 `rank_exit_threshold=40`、确认天数、止盈止损、移动保护、20日 time stop、每日替换预算、entry/exit price basis 和成本口径。两者分别持有 hash；shadow policy 只用于研究标签和虚拟 episode，不修改生产 Top20 policy。任一政策变化产生新标签/model identity，不能静默复用旧模型。
- 候选级标签：对 target day 的每个候选独立建立反事实 entry，在既有预测文件中重建后续每日至少 `rank_exit_threshold=40` 的排名并继续追踪已持有 symbol，按同一 review policy 模拟 `rank_exit`、stop loss、trailing take profit 和 time stop，得到 realized episode net excess return、是否优于 skip/cash 以及 confidence target。只有 Top20 而没有后续 Top40/held-symbol rank 时不得伪造 rank-exit 标签。
- 输出：每个候选的 `take_probability`、`skip_probability`、`advisory_model_confidence`、Top5 challenger 和可解释 reason；系统不自动下单，也不生成资金仓位。
- 对照：selection 原始前5、现有 M5A reranker、HMM前5、meta-label Top5、随机5和候选20等权。

现有 M1/M5A 5日 LambdaRank 保留为已完成历史基线，不重写其结果。多期限复合 relevance 只可作为独立 matched 对照，不能用已消费的冻结 test 决定权重。

### 6.1.1 评价、研究族与选择偏差

- 当前 406 个 decision dates 和单一 80 日 test 无法证明约 `0.001` 级 lift；该 test 已被读取，后续只保留历史报告。
- 新模型的参数、阈值和候选 family 仅在 train/validation 的 purged rolling/CPCV 路径选择；purge 至少覆盖 review policy 的最长标签窗口，当前默认最长 20 个交易日。
- CPCV/PBO 或适用等价诊断用于报告 45 trials 及后续 family 的选择偏差，不是人工审批门禁，也不制造新的独立 OOS。样本或策略路径不足时显式 `NOT_COMPUTABLE`。
- P0-D至P0-L共享开发窗口且后续假设受前轮结果影响，全部归入同一`hypothesis_family_id`；单轮CPCV/PBO不得冒充跨轮独立OOS，未来DSR/PBO必须同时报告原始trial数、有效trial口径和相关性假设。
- 真正未来 OOS 以每日 challenger observation 及其成熟 episode 为主；不能因前向天数暂少就回头调已消费 test。
- 候选级 meta-label 指标只衡量 take/skip；最终 Top5 challenger 必须另行按冻结的 shadow portfolio policy 逐日组合重放，包含 target count、replacement budget、持仓继承和现金状态，不能用独立候选收益冒充组合 episode 收益。

### 6.1.2 P0-F 连续 policy utility 排序（历史冻结合同）

P0-D/P0-E证明继续调整binary take/skip loss不能稳定修复收益幅度。P0-F当时只改变一个实验变量：以P0-C `MATURED net_excess_return_bps`为连续目标，直接预测候选在冻结shadow policy下的净超额收益，并按预测utility重排exact Selection Top20。

- objective固定为LightGBM Huber regression，`alpha=0.90`；family仍只有CORE/CORE_HMM，seed和28-path CPCV roster不变。
- 每条path仅用train rows拟合median/MAD可逆仿射变换；不clipping、不winsorize，不使用validation或历史回放拟合transform。
- 每日排序键固定为预测policy净超额收益降序、Selection rank升序、instrument升序；entry priority由utility决定，selection exit和全部policy transition保持不变。
- winner仍由shared shadow portfolio的validation `mean_daily_net_excess_return_bps`选择，不由MAE、Spearman、PBO或历史回放选择。
- 冻结晋级合同要求同时超过exact P0-D和Selection，28-path对P0-D胜率大于50%，且配对平均回撤、换手不恶化。真实Stage A仅换手失败，已按合同完整负向终止，没有增加family、target transform、rank guard或blend。
- 因冻结CPCV合同未通过，新显式model role、runtime、descriptor和Stage B均未实现；禁止把regression score映射后伪装成take probability。
- 当前24决策日窗口已消费；P0-F未获准进入`HISTORICAL_REPLAY`，自然future OOS继续只积累既有P0-D challenger证据。

权威详细设计：`docs/architecture/advisory_p0f_policy_utility_ranker_f2_design_20260824.md`。

### 6.2 预期收益与持股周期

该组件已经使用同一历史候选矩阵训练并完成固定日期推理，作为各决策角色的辅助预测，不再被描述为当前第二主线：

- 多期限净收益分位数。
- 正收益概率。
- 信号存活概率和建议持股周期范围。
- MFE/MAE 与回撤风险。

第一版优先使用 LightGBM quantile/classification 和离散生存模型。它可以与 reranker 共用特征文件，但输出头和评价指标独立。

### 6.3 买入、止盈和止损区间

日线级价格分布参考范围已经实现并完成历史固定日期功能验证，但没有完成收益型买入建议。2026-10-02起后续优先实现§6.3.2的价格条件化进入价值，再完善独立binding、API和页面展示；开盘分布校准不再是买入建议研发的前置任务。不在Advisory研发分钟路径或执行模型：

- 第一版使用日线 Bin 的 OHLC、复权、波动、跳空、真实涨跌停价及收益/MFE/MAE模型，生成明确标记的日线级买入、止盈、保护和止损参考区间。
- 后续Advisory价格模型不读取分钟Bin，不训练盘中买入时点、成交概率、事件先后或动态路径模型；既有分钟MVE保持历史已消费状态。
- 正式区间预测只读取决策截止前数据库日线、昨收、涨跌停、复权/PIT公司行动和已批准的日频特征，不读取目标日未来行情或实时分钟行情。

输出必须是范围，不是保证价格；硬止损和行业黑名单不能被模型覆盖。

M4 executable 二分类头在 8120 行中只有 4 个负例，当前定义下不可学习。该头保持 `UNCALIBRATED` 并停止重复校准；下一轮只能先重审标签语义，或直接退役该二分类输出，连续 entry-gap quantile 和风险边界不受影响。

#### 6.3.2 收益与风险驱动的条件式买入区间（当前主线）

用户2026-10-02明确：开盘价不在建议范围是合法不买情形，荐股重点不是覆盖开盘价，而是给出有净收益价值、风险可接受的买入/卖出条件。现有§6.7的正净动作价值目标保留并提升为主动任务，按[经济进入价值F2设计](advisory_economic_entry_value_v1_f2_design_20261002.md)落实。

- OPEN_DISTRIBUTION、ENTRY_VALUE和EXIT_VALUE相互独立。原M4/ENTRY_PRICE仍用原coverage/宽度合同，新角色不沿用其模型、标签或确认状态，不修改旧实验结果。
- D截止冻结候选、日频PIT特征、价格假设网格、policy及成本；输出“若T开盘满足这些价格与有效条件”的建议。T开盘只选择事前冻结条件，不倒填到D特征，不读取当日未来路径。
- 真正的买入标签是“按实际允许价格进入并继续冻结exit policy”相对“相同固定槽位留空”的净价值及下行风险。价格条件需建模，不能保持卖价预测不变、仅降低分母制造抄底利润；旧q10/q50/q90不直接成为安全/最佳/最高买价。
- 高开或低开超出建议条件可SKIP，但偏离本身不证明股票有问题或无收益。公司行动/参考价处理正确，缺信息或支持域外为typed unavailable，不能把未知伪装成正常拒买。
- 价格集合允许多区间或空集，保留全部候选、固定Top5空槽且不自动补位。覆盖率不是输出股票比例或收益胜率；不能用全不买冒充效果。
- 首版只验证T真实开盘这一日频观察点，不用日低/高/区间触及假设成交。收益和风险按同policy/cost模拟；评价净收益增量、回撤/尾损、避免亏损、错失机会、真实干预支持度和现金暴露。未通过matched组合parity不能把episode收益当组合收益。
- 买入先行；Exit另学下一合法可交易点退出相对继续持有的剩余净价值，不将MFE/MAE或盈利百分比当最佳退出。上述均为建议，不生成分钟策略、订单或资金仓位，不修改QE及其他模块。

新角色的支持域、风险预算、模型/阈值、窗口和trial在收益选择前预登记，past-only chronological训练并按label end purge；旧窗口只作历史导航。v1的TAKE=0、v2旧权重复用generated=0是历史事实；随后v3/v4/H-TIMING-1/H-VALUE-ANCHOR-1均已真实训练并停止当前candidate。通用经济消费者/API/UI已交付且#5324默认未配置状态完成重启验证；新recipe生产适配仅随值得继续的模型实现，经济确认/正式启用仍为0。开盘分布未确认不阻塞该业务研发，也不豁免原分布发布合同。

#### 6.3.3 市场/板块条件化价格价值：候选及验证分流

本节是对既有条件价格与动作价值方向的细化，不是已验证生产模型或新的并行项目。候选假设为：D可见、此前未进入当前价格模型的市场/板块上下文，可以在真实价格观察的合法支持内识别不值得执行的买入，并在完整组合中产生成本后增量。严格类别块H-CONTEXT-VALUE-1已完成§16.5 G0～G3并停止当前候选；新信息、可见时钟或来源不成立时暂停对应信息模型，不阻断无此依赖的其它预注册路线。§16.6已获用户单独授权检验不同模型结构，但不将换模型族/共同支持设计称为新增信息，也不授权旧候选调参/回选。

1. **新信息必须具体可证伪。** 优先核对历史PIT板块归属、板块相对强弱/宽度、个股相对板块偏离及有经济解释的流动性压力，详细设计从中事前选一个有界信息块，不把它们自动全加入或事后择优。比较当前12D、v4 gap及已关闭N3/Admission信息，注明哪些只是重表达。HMM只有相同raw-context控制和合格因果输出时才另立比较，不成为本轮来源前置。
2. **保持两个信息时钟。** D日数据库记录生成冻结的条件价格函数；T只在既定日频观察点用当时实际价格评价，不把T市场/板块涨幅、盘中数据或夜间尚未发生事件写入D输入。D模型无法识别尚未观测的隔夜消息，价格支持内也不承诺“低开安全”；若某假设必须读取T上下文，则另立时钟设计，不静默扩张本轮。条件收益不是任意买价干预，日线触价不是成交证明。
3. **估计对象对齐实际决策。** 在冻结exit policy/cost及有支持的实际价格条件下预测净收益及尾损；盈利概率、盈利幅度、损失尾部、估计不确定性分别表达。可接受价格集合由事前规定的价值/风险条件导出，可为空集或多段，不强制输出股票。分布模型不能仅靠更换loss被当作新增信息，独立分位数不可相加冒充联合风险。
4. **候选先简单且强正则。** 分层收缩或少量交互的广义加性模型只是优先评估选项，并非已选生产架构；G1须冻结一个可在现有环境实现的模型/参数和训练目标。一个无新增块的matched模型、一个含新增块的candidate，使用相同模型族/监督/切分/支持与参数，分别登记全部fit；不同时比较多家族、seed、窗口和阈值。改动作/标签时两臂同时使用同一新语义，不把其改善归功于信息。
5. **动作价值与组合价值分开。** 有效标签比较同一决策前状态下买入和留空，使用同一现有模拟器、后续policy规则、共同终端及成本；两臂持仓可分化，不能强行复用买入臂退出轨迹。历史动作后才可见的持仓状态不进入D模型，只有两臂均可识别时才生成增量标签。单笔避免亏损/错失盈利用于解释，不直接相加成组合收益；最终以完整五槽回放裁决，无第6名补位、无权重优化。
6. **排除单纯少买的效果。** 原Top5、无新增信息matched模型、新candidate及一个事前冻结的简单风险控制构成最小比较。简单控制只按past-only信息保留或空出固定槽位，支持多少由训练段定义；禁止按test模型的每日交易数/未来波动反向调控制。实际持仓占比、现金、波动、MDD和尾损同时报告；若暴露仍不可比，标记不能完成归因。事后缩放净值最多为描述性诊断，不是可执行臂、候选选择或激活证据。
7. **少样本与研究选择。** 股票行数不当作独立样本数；purge按实际label end，推断考虑日期/持有期重叠和同股簇。小规模来源/单元烟测不算经济结果；筛选须使用预先定义的连续开发切分且保留完整候选，不挑日期/股票。支持阈值、经济最小效应和研究条件在读取评价结果前固定，探索可导航、不能升级独立确认。已有受限历史身份不升级。

H-CONTEXT-VALUE-1设计由#5383合入，五源码叶/14定向测试、857/304共同监督和四真实fit及原Top5/matched/candidate/固定规则完整回放均已完成。candidate仅4笔真实模型TAKE、89笔UNKNOWN研究控制；减matched日净增量-0.8533bps，停止当前candidate而非全局行业方向。当前源码交付不包含生产family/API接入，不回选matched或调整支持继续跑。Advisory使用统一只读准备消费者绑定profile/release/池与原冻结身份，行业增强就绪独立于core输入身份；原未知分支、RECOVERED_LIMITED及当前池外键保留，未伪造D知晓或原生捕获。未来新信息仍按本节事前合同单线推进，不以复杂平台、新family或100%行业覆盖为前置，不将行业指数成分身份强加为普通分类合同。

### 6.3.1 因果更新与区间校准

M5B/M5C的静态校准负结果保持不变。下一步允许先用历史已成熟标签做past-only rolling/expanding比较，不等待自然20日后才能开发；生产校准只能消费该Program/model/head自然已成熟residual。静态control与一个更新协议先比较，更新训练/transform/HMM均按当时可见时钟重建身份，不默认三个月周期或自动重训平台。

预测分位数、期望收益估计误差和confidence不得混称。Residual q20不是条件期望的LCB；CQR/Adaptive Conformal需分别说明可交换性、边际/时序覆盖及refit限制，不保证股票条件覆盖。未实现有效校准时保持UNCALIBRATED，不用规则价格冒充模型。完整周期搜索和自动生产更新留到有增量证据后。

### 6.4 训练资源边界

- 所有训练只在WSL Conda环境运行，Windows只负责触发、读取结果和正式在线推理。
- 后续单进程默认预算低于8GB，超出须在该实验request中显式解释和设定上限，不自动新增硬件或平台；基础数据按日期、股票和列投影分批读取，不同时全量加载多个H5，后续Advisory价格区间任务不加载33GB分钟Bin。
- 允许把本次训练所需切片写成临时Parquet并结合内存缓存；禁止为此建设SQLite历史证据库、通用缓存平台或长期数据固化链。
- 首次经济反馈目标在小时级形成，不是收益保证或欠功效淘汰门。超过目标时先定位I/O、特征构建或训练瓶颈并做批处理优化，不得转向额外基础设施研发。

历史generator实测peak约15.37GB，不满足8GB默认目标，保留实测异常，不把它记为资源通过或默认为后续许可；无需为旧资源记录重跑实验。

### 6.5 LONG_TREND

长期趋势策略包形成稳定候选文件后，复用同一文件训练和实时数据库预测架构，独立训练20至180日重排、有序收益、生存、time-to-hit和趋势捕获模型。LONG_TREND与SHORT_REBOUND不得共用同一个标签头。

### 6.6 分层oracle与learnability双诊断

oracle只用于定位瓶颈，不进入模型或运行时。所有层使用同一PIT股票池、上市年限、ST、停牌、涨跌停、成本、benchmark、winner label、执行价和冻结review policy，并登记开发窗口消费。

| Tier | 最小内容 | 放行语义 |
|---|---|---|
| Tier 1 | 全可交易股票赢家在Top20/40/50的召回；Top20内perfect Top5排序；rank分桶和成本后上限 | 天级诊断；先回答候选召回与排名动作空间 |
| Tier 2 | perfect Entry Guard、perfect Exit及其相对冻结基线policy的增量价值 | Tier 1身份与候选/排名诊断闭合后运行，回答风险管理动作是否存在独立空间 |
| Tier 3 | 固定等权槽位下允许空槽、保留现金和向后补位的frontier | 不包含动态资金权重；只有Tier 2需要现金动作时运行 |

每个Tier同时报告两种证据：

1. `CLAIRVOYANT_ACTION_CEILING`：使用未来结果测量候选池和动作空间的理论上限，永远标记不可部署。
2. `FIXED_CROSSFITTED_LEARNABILITY`：历史N1/N2固定Ridge结果不变；后续审计先注册一个线性基线，只有明确非线性假设时再加一个受限树模型对照，冻结特征、超参、时钟与判定线，全部计入trial。单族失败只约束该表达，不等于全局不可学；不做模型/loss网格。

四象限处置固定为：

- 理论低、可学习低：该层降级，检查上游召回、股票池或policy动作空间。
- 理论高、可学习低：结论仅为“当前信息集+冻结简单模型不可学”；先检查标签/时钟/信息表达；优先扩有效信息，但已扩信息仍只用Ridge时允许另立有界非线性对照，禁止无假设轮换loss/模型。
- 理论高、可学习高：允许创建一项确认性candidate，不能直接激活。
- 理论低、可学习异常高：先排查泄漏、标签、基线和评价错误，不据此晋级。

N1/N2的全市场Top5赢家召回1.7617%及旧20%门槛保留为历史结构诊断，不再是未来方向的单项生死线：抓住全市场极少数极端赢家不是盈利必要条件。父包RankIC 0.12284、H20 Top5净超额446.52bps提示存在局部信号，但这是重叠H20标签均值，不是可实现组合年化。未来同时看可执行净正机会覆盖、rank分桶、同policy组合收益、理论增量、可学增量与跨regime稳定性；不重跑N1/N2补账。

判定线使用成本、容量折损、最小经济收益和block/cluster置信下界共同定义，不预设全局固定bps。oracle与learnability不得读取sealed holdout。

### 6.7 日频Entry Guard独立能力

M4预测次日开盘gap分布和价格区间，但尚未回答“实际开盘价格到达后是否仍值得买”。本节只保留日频/单观察点Advisory建议的历史研究合同，不形成逐分钟策略。Entry Guard冻结Selection顺序和现有Exit规则，只在T+1开盘或一个明确观察点改变entry advice：

```text
T close: reference_price + predicted_gap_q10/q50/q90 + max_acceptable_gap + max_buy_price
T+1 open or explicit point-in-time snapshot: ACCEPT | REDUCE | SKIP | WAITING
```

- 至少比较无保护、固定3%、固定5%和冻结动态阈值；固定阈值是透明基线，不是所有策略/regime的默认生产规则。
- `REDUCE`仅是面向用户的谨慎建议状态，本阶段不输出数量、权重或资金分配；任何数值仓位语义需另行扩权。
- 首版`SKIP`不自动用第6名补位，固定槽位空缺和现金是合法研究结果；另行比较保留现金与向后补位，不形成动态仓位。
- 标签是“按实际可执行价进入”相对“冻结基线动作/跳过”的增量净价值，绑定entry policy hash、成本、价格基础和可交易性。
- 主要评价执行净收益、相对无保护基线收益、MDD、尾部损失、追高区间alpha、错过alpha、fillable rate、现金暴露、换手和真实干预支持度；胜率只作辅助。

买入价格区间最终应表示“在可成交且受风险约束的价格集合内，预期净动作价值仍为正”，不能直接将gap q90当最高值得买价。只在已验证价格/特征支持范围内推断；T+1实际价格到达后如调用日频Entry建议，不能将未来open灌入T收盘特征。MFE/MAE区间是描述性路径预测，不自动构成最佳止盈/止损动作，更不构成最佳分钟买卖点或执行计划。本蓝图不继续研发分钟Entry Guard。

### 6.8 Exit-label oracle与独立Exit能力

P0-H/P0-K的liability Spearman约0.25只证明当前policy下的持有/换手负担可预测，不证明最佳退出时机可预测。Exit首步是标签与action oracle，不直接训练正式模型：

- 在每个持有决策时点构造“下一可交易时点退出”相对“继续执行冻结baseline exit policy”的剩余净价值；同一shadow-policy模拟器生成两臂并绑定policy hash。
- 处理T+1、停牌、跌停、成本、删失、rank-drop、stop loss、trailing take profit和time stop；未来路径只作标签和评价。
- oracle通过后首版保持日频，候选输出`HOLD/REDUCE/EXIT_NEXT_OPEN/WAITING`、预测剩余收益、下行风险、time-to-hit、保护价和reason；`REDUCE`不携带数值仓位或自动交易语义。
- 主要评价利润回吐、避免亏损、过早退出机会损失、whipsaw、持有期、MDD、尾部损失、动作次数和跨regime覆盖。

### 6.9 上游alpha分流与角色化信号组合

若N1证明Top40/50赢家召回或成本后候选流不足，且N3综合分流确认上游为当前主瓶颈，主线转入QE/StrategyPackage上游alpha MVE，而不是继续修改Advisory下游模型。准备件可以并行完成数据身份、表达式grammar/operator白名单、生成预算、资源和registry schema；候选生成、IC/收益评价和结果驱动提示词调整必须等待分流。生成后的正结果还必须检查与既有因子/已知市场效应的重合、时间衰减和经济归因，避免把重复暴露或非线性复刻误报为新alpha。

截至2026-09-06，上游QE不再只有Advisory内的24项daily/static generator负结果：独立MA-E19 D1B/D1C已在相同2026-08-31 release、CE3/h20、Top50/n_drop1和分钟Tail-TWAP口径下完成rolling LSTM seed123四vintage，形成`CANDIDATE_LEG_PENDING_MULTI_SEED_AND_LOO`。该候选在四窗保持正CAGR且3/4胜rolling LGBM，但只有单seed、2025H1 MDD较大且2026H1 Top50仍为负；固定50/50 blend也已拒绝。D2四格把上游机会进一步拆为sector选择与sector内右尾，两边oracle均显著高于现实但永久不可部署；D3完整Brinson因冻结benchmark板块权重缺失保持NOT_COMPUTABLE。自2026-09-12起，后续seed、LOO、历史全量复验、组合Alpha和新候选搜索全部由QE统一规划；Advisory不提前运行同policy canary，只在QE交付正式StrategyPackage/预测后执行消费侧业务验证。任何失败不得退回测试期权重搜索、固定blend或扩大Advisory旧generator frontier。

PR #4361已实现QE活动数据集/PIT股票池入口。QE负责活动profile、目标实验交集和新任务身份；Advisory不重做入口或处理QE数据缺口。QE Top50分钟TWAP的正CAGR不能直接迁移到Advisory合同；只有通过QE治理并交付的新StrategyPackage/预测，才进入同PIT股票池、Top5 review/exit与可执行成本的Advisory消费验证。

P0系列输出不因存在局部信号而自动成为ensemble成员。进入组合前必须按角色分类并证明：独立时段残差相关较低、逐种子与种子平均相关结构稳定、LOO有边际增量、成本后仍存在、跨regime稳定，并附特征/SHAP归因及与已知效应的重叠检查。return/rank可以进入alpha组合；liability进入risk/日频Exit；M4 gap归属于日级价格角色，并可作为日频Entry Guard的输入；HMM进入regime/risk。禁止把不同决策时钟的输出任意加成一个总分，也不得把M4 gap转换为分钟择时动作。

### 6.10 Frontier、确认与证据使用

未来实验使用`frontier -> candidate -> confirmation -> activation`四层合同：

1. frontier仅在inner-train按预注册网格报告收益、换手、现金、coverage和MDD的完整Pareto集合；PIT、泄漏、身份或可交易语义错误仍立即失败。
2. candidate只能依据inner-train和预定义经济效用选择一次；所有探索arm计入对应研究族trial数。
3. confirmation只评价冻结candidate。经济失败后整个frontier和窗口已消费，不得返回重选；只有代码/数据身份错误且未利用经济结果重新选点时，才允许同candidate exact retry。
4. activation独立使用对应objective contract、安全边界和prospective evidence；研究frontier不得自动放宽既有生产合同。

确认性实验预注册最低干预次数、干预交易日比例和regime分布，阈值由MDE及block/cluster有效样本量推导，不使用任意固定数字。探索性结果可导航，不可关闭方向、声称稳定效果或支持激活。

### 6.11 同包评分校准与市场/HMM条件化准入

两类辅助方案继续保留：同一策略包score转换到跨日收益尺度，以及原始市场形态/HMM/sector的信息增量。v1三臂selected=0及失败分解不改判；v2.1不重用旧absolute CPCV输出、不回选243点。后继详细设计为[因果Admission v2 F2](advisory_causal_admission_v2_f2_detailed_design_20260906.md) v1.4；正式R1静态/20日expanding两arm均未通过支持度、5 bps经济门与校准要求，`selected=0`、outer未读，当前score/raw + Ridge frontier关闭。

#### 6.11.1 同包评分、预测和动作

父包按日标准化的raw/combined score只有同日排序语义；模型使用rank/percentile/dispersion、包adapter和当时可见市场输入，输出对应冻结policy的预期净收益、正收益概率、预测收益分位数及有效的估计不确定性。1/5/10/20日均报告，只有预注册primary horizon决定动作，其余不事后择优。

当前实现把inner residual q20加到Ridge预测后命名为`expected_net_return_lcb80_bps`，它不是期望收益置信下界；inner/final refit和非平稳数据也不能保证条件80%覆盖。v2.1必须分开mean、predictive downside和estimation uncertainty。收益已扣成本时不重复扣；正期望不要求胜率必达50%或q20为正。新动作由预注册净效用与风险预算决定，不以TAKE数量调阈值，也不宣称修正统计定义已创造Alpha。

#### 6.11.2 因果更新与可选上下文

每个arm/date只有一个chronological prediction，所有scaler/model/calibrator/HMM仅用fit时点之前可见输入及已成熟标签。R1固定比较静态Ridge和20交易日expanding Ridge；受限树模型因没有独立非线性假设而不注册、不运行。不采用多窗口×多模型×阈值网格，也不等待自然240日验证才开发。

HMM/rotation是可选上下文，不是必经网关。raw-market必须保留为control，市场HMM不得直接乘父score或默认BEAR全拒绝；sector按PIT mapping连接并核对父暴露重复。R1 score/raw因果模型可独立运行，R2某个sector/HMM arm仅在其canonical source通过后运行；缺源只停止依赖它的阶段，不能运行时自动删列降级。

#### 6.11.3 阶段、动作和证据

此处R0～R3是该lineage的历史设计阶段：R0参数化与R1正式两臂实验现已完成，R1 selected=0且frontier关闭，不再作为“下一项”执行；R2/R3也不自动启动。旧v1.0“全部v2等待sector、三个controls永不可选、q20>0且prob>=0.5”的未执行规格被已执行revision替代，原累计试验数和负结果保留。当前任务仅以§16的业务路线为准。

本辅助模型只作用父Top5固定槽，允许0～5，无重排/第6名补位/资金权重；其他Ranking role扩候选另属合同。五槽合法SKIP才是`NO_ELIGIBLE_RECOMMENDATION`，source/inference unavailable必须显示故障/覆盖原因。研究报告完整收益、换手、cash、coverage、MDD frontier、干预支持和MDE；不以任一指标恶化就抹去测量结果。欠功效可导航，不关全局方向、不激活；任何阶段均不读sealed holdout。

### 6.12 技术路线选择、文献依据与适用限制

以下是条件性技术选项，不是额外并行项目。先完成§16的业务阻塞/小实验，再按瓶颈只选一条；文献正结果不等于本系统A股成本后收益。

| 路线 | 当前取舍 | 启动条件与最小对照 |
|---|---|---|
| 预测/动作分离 + 因果更新 | 当前优先 | 先用简单模型、已成熟历史和独立policy验证；不先建设自动更新平台 |
| 公共预测 + 包adapter | 次级业务切片 | 两个以上可评估候选流即可离线做公共/条件化/独立baseline和留包比较，不需先建两个生产bundle |
| 多源候选/全市场廉价初筛 | 上游条件路线 | 当前召回/净正机会确为瓶颈时，由QE/Selection按同预算、去重、同policy比较 |
| 市场—板块—个股条件信息 | G0/#5372、G1/#5383及源码#5388已合入/清理；H-CONTEXT-VALUE-1四头/四臂负向停止，行业行情未实现 | §16.5原7720键/386D及PARTIAL保留；candidate减matched-0.8533bps、真实TAKE4不足，不回选matched或拓支持补证。§16.6不同模型R2为新授权；动态板块M1仍须独立来源/设计 |
| 生存/竞争风险价格路径 | Entry/Exit条件路线 | 先离散hazard/time-to-hit透明模型；处理T+1、停牌、涨跌停、删失和日线无法识别的触价先后，再考虑复杂生存网络 |
| LLM/RL因子生成 | 仍归QE上游 | 仅新信息/grammar/经济假设的有界MVE；已关闭daily/static generator不扩预算重选 |
| 时序/图/金融基础模型 | 观察项 | 新时序/关系信息先证明价值；冻结pretraining数据截止和金融matched baseline，不能用规模/SOTA替代证据 |
| 离线RL/策略学习 | 暂缓 | 先有可信反事实模拟或真实动作覆盖；排查未观测动作支持与OPE假设，不虚构counterfactual、不实盘探索 |

依据：[CQR](https://arxiv.org/abs/1905.03222)研究预测区间而非均值置信区间；[Adaptive Conformal](https://arxiv.org/abs/2106.00170)讨论分布变化下覆盖，不能自动移植为逐股票条件保证。[DoubleAdapt](https://arxiv.org/abs/2306.09862)提供金融增量更新参考，本系统先测简单rolling/expanding，不直接引入元学习。[TRA](https://arxiv.org/abs/2106.12950)与[MASTER](https://arxiv.org/abs/2312.15235)支持条件专家、市场引导时序/横截面关系作为候选路线，不能推导AIstock已存在增量。

[RD-Agent(Q)](https://arxiv.org/abs/2505.15155)作为因子/模型联合研究的参考；[IQL](https://arxiv.org/abs/2110.06169)提示离线动作支持与分布外估值风险。上述方法只服务经诊断的瓶颈。多任务共享可能负迁移、复杂模型可能重复已知市场暴露，必须用消融、种子稳定性和成本后配对增量裁决。

[Policy Learning with Observational Data](https://arxiv.org/abs/1702.02896)要求明确识别条件，不能据此虚构任意价格的成交；[Smart Predict, then Optimize](https://arxiv.org/abs/1710.08005)提供将决策代价纳入学习的依据，不代表换loss能产生alpha；[Deflated Sharpe Ratio](https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf)说明多轮选择偏差，JSONL登记不能使反复消费窗口恢复独立。这三项只约束方法，不作为本候选有效性的证据。

## 7. 荐股产品行为

### 7.1 多策略包

- 一个 Advisory Program 绑定一个已准入单 Alpha 包或一个已准入原生多 Alpha 父包。
- 同时支持多个 Program 独立运行。
- 不恢复页面内手工多策略包融合。
- 策略包进入系统时已经完成准入；Advisory 不做二次资产、因子、模型或可执行性验证。
- Program active binding 是模型 bundle 解析的唯一运行身份；单 Alpha 和原生多 Alpha 使用相同解析流程，一个 Program 缺模型不得影响其它 Program。
- 当前仅目标多 Alpha Program 有模型 bundle；“支持多个 Program 基线荐股”和“多个 Program 都有模型覆盖”必须分开报告。

### 7.2 实验模型展示

真实模型完成后可以立即在研究页面显示，状态固定为：

```text
EXPERIMENTAL_SHADOW
training_source = QE_FILE
calibration_state = UNCALIBRATED or PARTIAL
objective_contract = ALPHA_RANKING or RISK_MANAGED_ADVISORY
evidence_level = HISTORICAL_REPLAY or SEALED_HOLDOUT or PROSPECTIVE_OOS
```

实验状态不得描述为已校准概率或确定性收益，但不能因为尚未完成正式OOS、全量统计检验或ModelOps而隐藏真实模型排名。

评分/HMM辅助输出还必须分开显示父包raw score、同日percentile、calibrated expected return/probability、市场形态、市场HMM、板块HMM及各自availability/reason。未完成校准时不得显示“收益概率”；HMM研究状态不得显示为生产风控结论。

### 7.3 Top5与每日列表

- Top5 shortlist是模型研究输出，不自动把现有 `target_count=20` 缩成5。
- 模型启用前不覆盖 `selection_effective_rank`。
- 每日活跃列表必须保持有界，显式记录 `ENTER/HOLD/EXIT/WATCH`；不得将每日候选简单并集。
- Entry Guard的`SKIP/WAITING`可以在固定等权槽位中形成空槽和现金，但不得输出动态资金权重；“保留现金”和“向后补位”作为独立matched arm报告。
- `ADMISSION_RISK`可以让零至五只候选通过；零只时发布`NO_ELIGIBLE_RECOMMENDATION`及可解释reason，不把正常空推荐当作失败，也不复用旧日Top5或向后静默补位。
- 是否把正式 active target 从20迁移到5，由用户在看到真实影子模型结果后单独确认。

### 7.4 每日前向发布与 episode

- `daily_after_close` 必须对应真实执行器，而不是仅保存配置。交易日 D 收盘且 Selection 输入就绪后，执行器为每个 ENABLED Program 生成 `decision_as_of_trade_date=D`、`target_trade_date=next_trading_day(D)` 的 baseline/challenger 推荐。不得读取 target 日行情或假设 target 日已经成交。
- `PUBLISHED` recommendation 与 episode entry 分阶段：D 收盘先发布目标日建议；到 target 日获得权威 `next_open_executable` 后，才按 Program 的 entry price basis 建立 episode。target 日价格缺失时保持 `WAITING_DATA`，不得回退 signal close 或伪造进入价格。
- 基线 list 始终来自该 Program 的 StrategyPackage/Selection 结果。模型存在时追加独立 challenger observation，不改变 `selection_effective_rank`、target count 或正式 episode 的当前基线语义。
- challenger 至少保存 Program/binding/package/model identity、decision/target 双日期、候选、take/skip/confidence、Top5、outcome/价格范围和 typed status。不得把按需 GET 响应冒充已持久化前向事实。
- 新角色challenger还必须保存`objective_contract`、role、baseline policy hash、decision clock、action、增量价值语义和evidence level；一个合同下验证不得改变另一个合同状态。
- 评分/HMM challenger还必须保存score calibration identity、raw-market feature identity、market/sector HMM model与as-of identity、PIT行业映射、HMM是否已在父score中消费、逐候选admission结果和日级`TAKE_SOME/SKIP_ALL`状态。
- baseline episode 仅由正式 Program review policy 推进；模型 challenger 的虚拟 episode 必须使用独立身份和冻结的 Top5 `model_shadow_review_policy` 模拟，不能混入基线排行榜或 Paper/模拟盘持仓。
- 运行失败必须记录具体 Program、日期、阶段和 reason；一个 Program 失败不阻断其它 Program，不能静默跳过并继续显示旧日期为最新结果。
- 本功能无资金、无下单、无 QMT 输入，是荐股研究的前向质量评价，不属于实盘交易执行。

## 8. Contracts / 最小实现合同

当前只允许实现直接支撑真实模型的合同：

| 合同 | 必需内容 |
|---|---|
| `QETrainingDatasetDescriptorV1` | H5/Parquet/Qlib Bin基础数据路径、Prediction Store PKL引用、目标父包roster、schema、日期、features、labels和split |
| `AdvisoryFeatureSchemaV1` | 训练/预测共享的特征名、dtype、单位和缺失值语义 |
| `AdvisoryTrainingRequestV1` | model family、参数、seed、WSL环境、输入和输出路径 |
| `AdvisoryModelBundleV1` | 模型文件、feature schema、label version、训练区间、指标和SHA256 |
| `AdvisoryPredictionV1` | program/package、decision/target双日期、候选、model score/rank、Top5、状态和reason |
| `AdvisoryOutcomePredictionV1` | 收益分位数、正收益概率、周期、MFE/MAE和状态 |
| `AdvisoryPriceRangePredictionV1` | entry/take-profit/protection/stop范围、单位、置信状态和reason |
| `AdvisoryForwardObservationV1` | Program/binding/package/model、decision/target双日期、baseline/challenger、Top5、预测、状态和成熟截止日 |
| `AdvisoryPolicyEpisodeLabelV1` | baseline或Top5 shadow policy/hash、entry/exit basis、成本、退出原因、持有期、净收益/超额收益、label maturity和censoring |
| `AdvisoryModelBindingResolutionV1` | active Program binding 到 exact package/style/schema/model bundle 的确定性解析；无 bundle 为 typed unavailable |
| `AdvisoryDaySemanticResultV1` | 与执行拓扑无关的逐日候选、stage trace、名单动作、source refs 和业务语义 hash；实盘单日与历史批量共同生成 |
| `AdvisoryPITAsOfViewV1` | 只暴露一个 decision date 可见的数据视图，绑定 cutoff、availability policy、source catalog/revision 和视图 hash |
| `AdvisoryHistoricalBatchExecutionV1` | 冻结 date plan、chunk policy、workspace identity、逐日 checkpoint 和 typed failure；不含新的选股算法 |
| `AdvisorySemanticParityReceiptV1` | 单日与批量在相同业务输入下的逐字段/逐 hash 等价结果，排除 batch/worker/timing 等运行信封 |
| `AdvisoryResearchTrialRegistryV1` | append-only JSONL研究索引；study type、family/lineage、唯一变量、目标合同、trial计数、消费窗口、decision use和证据引用 |
| `AdvisoryObjectiveContractV1` | `ALPHA_RANKING`或`RISK_MANAGED_ADVISORY`、冻结基线、评价指标、合法动作、展示和独立激活状态 |
| `AdvisoryOracleMiniContractV1` | 开发窗口、PIT股票池、winner/成本/容量/benchmark/执行价/review policy、Tier和不可部署标记；拒绝sealed holdout |
| `AdvisoryLearnabilityAuditV1` | 冻结简单模型、特征、超参、cross-fitting、判定线和四象限结果；失败不外推为全局不可学 |
| `AdvisoryIncrementalValueLabelV1` | role、两臂action、同一shadow-policy模拟器、policy hash、成本、价格基础、成熟/删失和增量价值 |
| `AdvisorySealedHoldoutContractV1` | 独立dataset/window identity、允许一次confirmation、访问拒绝、消费receipt和禁止回选 |
| `AdvisoryEntryGuardDecisionV1` | T日冻结阈值/价格，T+1权威open/current下的ACCEPT/REDUCE/SKIP/WAITING、固定槽位现金和reason |
| `AdvisoryExitDecisionV1` | 持有as-of时点的HOLD/REDUCE/EXIT_NEXT_OPEN/WAITING、剩余价值/风险、baseline policy和reason |
| `AdvisoryPackageScoreCalibrationV1` | exact package/manifest/style/policy、raw score语义、同日percentile、train-only transform、1/5/10/20日绝对/超额收益与概率/区间、primary horizon及calibration状态 |
| `AdvisoryMarketShapeContextV1` | decision cutoff可见的市场宽度、涨跌停比例、benchmark收益/回撤、横截面波动/离散度、source identity、availability和reason；作为无HMM control |
| `AdvisoryHMMContextV1` | causal market posterior/state/duration、PIT sector mapping与sector posterior/rotation score、model/input/as-of identity、父score重复消费标记、availability和reason |
| `AdvisoryAdmissionDecisionV1` | 逐候选TAKE/SKIP/UNAVAILABLE、日级TAKE_SOME/SKIP_ALL/UNAVAILABLE、阈值/不确定性、最多五个固定槽位、空槽与NO_ELIGIBLE_RECOMMENDATION reason；无资金权重 |

若现有 Program/list/episode/API 能承载字段，优先复用；challenger 与基线身份无法无歧义共存时才允许增加最小 Advisory 专用存储。若H0被直接阻塞条件触发，必须优先复用现有 Historical Range batch/run/day/artifact 合同，性能与校验 telemetry 先写任务 artifact。禁止为未来通用性预建模型注册中心、训练调度表、血缘仓库、审批表、第二套历史状态机或新 artifact 平台。

## 9. Implementation Plan / 历史进展与当前唯一后续顺序

### M0：QE文件可训练性核对

历史优先级：`COMPLETED_BASELINE`。

状态：`COMPLETED`。基础数据、Prediction Store、涨跌停、分钟边界、冻结 request 合同和 runtime-equivalent candidate coverage 已核实并实现；正式 request 为 `advmreq_ac5959aa8dc14a25e3b8c139`，真实文件训练覆盖 406 日、8120 个 Top20 候选。

- 绑定当前目标父包精确 roster、两个代表 seed/model SHA、zscore、terminal weights、raw Top25、Program target_count 和 runtime semantics hash。
- 完整 38 seed、逐日权重和 `combined_prediction.pkl` 只生成显式对照诊断，不进入在线 feature，也不替代 current runtime candidate。
- 冻结 decision/target 双日期和 `OFFLINE_RUNTIME_EQUIVALENT_SELECTION_EFFECTIVE_TOP20_V2`，与正式预测入口保持一致。
- 绑定当前 QE H5/Parquet/Qlib Bin 基础数据，不复制或转换合法预测PKL。
- 只读核对日期范围、候选/特征/标签、缺失率和时间切分可行性。
- 选择覆盖最完整的一个真实训练范围。
- 冻结5日标签、最多5日延迟退出和10交易日 purge，显式排除各 horizon 尾部未成熟或跨 split 边界样本。
- 若首模合同必需字段缺失，继续只读搜索已有QE H5/Parquet/Qlib Bin或Prediction Store；仍不存在时报告精确阻断并停止M1，禁止静默简化特征或转向历史数据库证据工程。

完成判定：产生一个可由WSL读取的真实训练请求，不要求新数据库DML或新历史snapshot。

### M1：首个真实Top20→Top5模型

历史优先级：`COMPLETED_BASELINE`。

状态：`COMPLETED_EXPERIMENTAL_SHADOW`。WSL 真实训练生成 bundle `9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629`，80 日 test 均有 Top5，峰值 RSS 约 2.11GB，总耗时约 129 秒。真实 test 平均超额收益低于原始排名与随机对照，因此 M1 完成时未激活；M2 随后为验证完整真实链路发布了该 exact shadow binding。运行时可用不改变其负面质量结论，也不得被隐藏或描述为优化成功。

- 实现QE文件reader、共享FeatureBuilder和WSL launcher/trainer。
- 使用当前文件数据从头训练HMM，固定状态映射并生成可从文件 cutoff 连续追加数据库观测的 posterior；旧HMM结果只进入对照报告。
- 在WSL训练真实LightGBM LambdaRank。
- 在时间留出集生成逐日Top5和基线对照。
- 保存可加载模型文件和最小manifest。

完成判定：真实reranker与本轮重新拟合的HMM均生成可加载模型、连续续推状态和非空留出预测；旧HMM产物未作为输入；Top5留出预测非空且无未来数据泄漏。shadow 使用与 holdout 相同的 HMM/reranker 参数，不单独 refit HMM。HMM失败不影响现有规则荐股，但不能把HMM增强对照标记为完成。

### M2：固定日期按需影子推理与页面

历史优先级：`COMPLETED_POINT_INFERENCE`。

状态：`COMPLETED_POINT_INFERENCE_NOT_FORWARD_RUNTIME`。PR #3225 已合入；重启后运行时 commit 为 `41504a205b9372a4e709587dc2310fd8143c6c6d`。目标多 Alpha Program 在 `decision=2026-07-15 / target=2026-07-16` 返回 20 个真实候选、5 个模型 shortlist、exact bundle 和 11 个显式 HMM unavailable；单 Alpha Program 返回 `ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE`。该 GET 依赖既有 REPLAY list/review，只证明单点推理；它没有生成 `PUBLISHED` list、调度执行记录或 episode。

- 实现数据库 decision-cutoff 特征source；目标交易日数据不得进入特征。
- 对当前单 Alpha或原生多 Alpha候选执行模型推理。
- API返回真实model score/rank/Top5。
- 页面展示`EXPERIMENTAL_SHADOW`结果和基线对照。

完成判定：从 persisted replay 候选、Advisory decision/target context 和数据库 decision-cutoff 行情到页面 readback 贯通；不修改 Selection、Paper 或模拟盘。该判定仅关闭 M2 单点推理，不关闭每日前向发布。

### M3：预期收益与持股周期

历史优先级：`COMPLETED_POINT_INFERENCE`。

状态：`COMPLETED_POINT_INFERENCE_NOT_FORWARD_RUNTIME`。详细设计为 `docs/architecture/advisory_model_first_m3_outcome_holding_period_f2_design_20260809.md`；M3A bundle `17ce7ceb429829f15b68b196ad76ffee08d45f93b0a72d0f2fb92e72515adba0` 含 46 个真实 LightGBM 模型、1600 行/80 日零 NaN test 预测，训练 108 秒、峰值 RSS 655,581,184 bytes。M3B 随 PR #3234 合入，merge commit 为 `84362027da8f6e87ec5b627a5b7df15b88c5763b`；outcome binding 已存在。运行时 commit `0ab6dec36c6bc05f7d9655de63b07bbd5353dfd2` 对固定日期的 20 个 persisted 候选完成推理，耗时 33.236 秒，五个 horizon 均非空；尚无每日 forward observation 或成熟 outcome。

- 在现有QE文件上训练收益分位数、正收益概率和周期模型。
- 接入同一Advisory预测和页面。

### M4：买入、止盈和止损范围

历史优先级：`COMPLETED_POINT_INFERENCE`。

状态：`M4_DAILY_ENVELOPE_V3_V4_COMPLETE_FIRST_PROSPECTIVE_SETTLED_ADAPTIVE_CQR_SELECTED_ZERO_NOT_ACTIVATED`。历史 M4A v1 结果与 binding/readback 事实保持不变；其二分类 head 因 8120 行仅 4 个权威负例已从新业务合同退役。v3 request `advprreq_e788810c50b59802ec2344c3` 生成三分位数 bundle `30e8a75b...`；v4 request `advprcal_9a82e951973c7e58a2bc738f` 生成 validation-only bundle `508fedfe...`，validation/test coverage为`0.811702/0.733125`。首个T=`2026-09-15`自然settlement已完成但仅一日；固定rolling-20D matured CQR历史导航审计又因cluster-bootstrap下界未过0而selected=0。故保持静态v4，不发布新binding、不触发后端重启或运行时readback；当前P0-D descriptor仍没有M3/M4 child，price-range保持typed unavailable。

- 先使用日线Bin完成真实日线级价格范围模型。
- 当前及后续Advisory范围只保留日级价格区间；分钟路径、分钟择时和执行模型归属QE/Execution/Paper另立任务，本蓝图不读取分钟Bin、不实现适配器。
- 接入价格转换和硬风险边界。

### M5：模型质量迭代

历史优先级：`COMPLETED_NEGATIVE_RESEARCH`。

状态：`M5A_M5B_M5C_REAL_RESEARCH_COMPLETE_ZERO_ACTIVATION_RECOMMENDATIONS`。M5A、M5B、M5C 的真实 bundle 和负面质量结果见 §1.3；PR #3346 已合入 `034ccd36dd94441ec8c0fe0f94010d6874b8b799`，但三个研究 bundle 均不激活。冻结 80 日 test 已消费，不得围绕这些结果继续调参。

- M5A 首先改善 Top20→Top5：当前 M1 test `mean_excess_return_5=-0.0002833`，明显低于 `selection_rank_top5=0.0085591`，不得把“模型已运行”误写为“模型排序有效”。
- M5A 在现有 406 日/8120 候选、103 特征和冻结 test 上做有限窗口、种子、模型配置及 selection-prior 混合比较；所有选择只使用 train/validation，test 只做一次最终报告。
- M5A 真实冻结 test 的 winner 平均 5 日超额收益为 `0.0071894`，原始 selection rank 为 `0.0085591`，lift 为 `-0.0013696`，95% moving-block bootstrap 区间为 `[-0.0093061, 0.0053392]`。结果说明 M5A 修复了 M1 明显为负的问题，但没有证明优于原始 selection，当前不替换已激活 binding，也不得围绕 test 继续调参。
- M5B 再处理 M3 概率校准与 quantile coverage；M5C 处理 M4 entry-gap quantile coverage。M4 executable 负例仅 4 条，禁止伪造二分类校准。
- 3/5 年实验只有在同一目标父包的合法既有预测确实存在时才执行，不能用其它实验或新模型回填；不得为 M5 新建历史证据、缓存或 ModelOps 平台。

### P0-A：每日基线发布与前向 observation

优先级：`COMPLETED_FORWARD_RUNTIME`。

状态：`SOURCE_MERGED_RUNTIME_PUBLISH_AND_SETTLEMENT_VERIFIED_FORWARD_ACCUMULATING`。

运行时证据：两个ENABLED Program均有decision `2026-08-13/14/17` 的PUBLISHED run；target `2026-08-14/17` 已SETTLED并各形成20个active episode。target `2026-08-18` 在开盘前保持WAITING_DATA，继续按同一时钟自然结算。

任务列表：

1. 设计并实现 Advisory 专用收盘后执行器，读取交易日和 ENABLED Program，调用既有正式 review 服务；不启动通用 scheduler 平台。
2. 每个 Program/target trade date 幂等产生一个 `PUBLISHED` baseline recommendation；D收盘发布与target日开盘后的 episode transition 使用同一 date context，但分阶段执行，复用现有 bounded `ENTER/HOLD/EXIT/WATCH` 语义。
3. 对有模型的 Program 在同次运行中生成并持久化 challenger observation；没有模型时写入 typed unavailable，不阻断 baseline publish。
4. 保存 decision/target、Program/binding/package/model、baseline/challenger Top5、outcome、价格范围和 maturity date；禁止只保存 GET 临时响应。
5. 页面与 API 分别展示 baseline 最新发布、challenger 最新发布、成熟 forward metrics 和明确失败状态。
6. 完成两个现有 ENABLED Program 的单日真实发布 readback；后续交易日由执行器自然积累，不回填 2026-07-17 以来历史缺口。

完成判定：两个 Program 均存在同一 target 日的真实 `PUBLISHED` recommendation；目标多 Alpha Program 存在同日模型 observation；单 Alpha Program 在无 bundle 时 baseline 成功且模型状态明确不可用。target 日开盘数据到达后，至少一个 episode 被创建，或所有未进入均有可解释的 `WAITING_DATA/NOT_ENTERED`；D收盘发布不得因尚无target open而伪造episode。服务重启和首次生产调度仍由用户分别确认。

### P0-B：动态 Program/package bundle 分发

优先级：`COMPLETED_DYNAMIC_BINDING`。

状态：`SOURCE_MERGED_DYNAMIC_BINDING_RUNTIME_FORWARD_VERIFIED`。

任务列表：

1. 用 Program active binding 替代 `target_binding.py` 中单一 Program/package/manifest 的运行时常量判断。
2. 通过 exact package/manifest/style/schema/model identity 查找 bundle，不扫描 latest、不跨 Program 共享状态。
3. 单 Alpha、原生多 Alpha 使用相同解析合同；不存在兼容 bundle 时返回 typed unavailable。
4. 保持已合入目标多 Alpha bundle 的字节级加载语义和现有 point readback。
5. 不为测试单 Alpha 包伪造模型；该包只有完成独立真实训练后才增加 bundle。

完成判定：目标多 Alpha point/forward 推理不回归，单 Alpha baseline 不受阻，新增真实 bundle 后无需修改源码常量即可被精确解析。

### P0-C：review-policy episode 标签与稳健评价

优先级：`COMPLETED_POLICY_DATASET`。

状态：`SOURCE_MERGED_REAL_FILE_DATASET_VERIFIED`。

任务列表：

1. 从现有 QE 文件价格和目标父包既有预测 PKL 确定性重放冻结的 Top5 `model_shadow_review_policy`；每个后续交易日至少重建 Top40，并对已持有但跌出 Top40 的 symbol 保留可判定的缺席 rank 语义，生成 entry、rank exit、stop loss、trailing take profit、time stop、成本和 benchmark 对齐后的 episode label。生产 Top20 policy 只作独立 baseline 对照，不被修改。
2. 明确 censoring、label maturity、停牌/涨跌停和缺价语义；不得读取 Paper、模拟盘或生产历史 episode 作为训练输入。
3. 生成 purged rolling/CPCV train-validation paths，purge/embargo 覆盖最长20日政策窗口。
4. 保存全部候选 family/trial 的 validation 路径结果；计算 PBO/等价选择偏差，不能计算时写 `NOT_COMPUTABLE`。
5. 候选级 take/skip 标签与 Top5 shadow portfolio 评价分开：后者逐日执行 target count、daily replacement budget、持仓继承和现金状态。
6. 已消费的80日 test只作为历史对照，不参与阈值、特征、窗口或模型选择。

完成判定：相同 request 确定性得到相同 episode label；边界日无未来泄漏；一个固定 policy 的 selection baseline 与 M5A 历史模型可在同一政策收益口径比较。

### P0-D：SHORT_REBOUND meta-label 模型

优先级：`COMPLETED_EXPERIMENTAL_SHADOW_NOT_ACTIVATED`。

状态：`DESCRIPTOR_ACTIVE_FORWARD_EVALUATION_MERGED_HISTORICAL_FORWARD_VALIDATED_MODEL_NOT_ACTIVATED`。

PR #3368 已合入 `458199cd902323e006ac23d3767c908637068fa8`；后续安全 descriptor rotation、exact runtime contract 与 maturity 修复也已合入并由用户重启。P0-D exact bundle 已作为 `EXPERIMENTAL_MODEL/UNCALIBRATED` challenger 接入，baseline和正式Program policy仍未被替换。

任务列表：

1. 训练真实 LightGBM take/skip/confidence 模型，消费 selection、策略腿分歧、HMM regime、行业、流动性、拥挤和可交易性特征。
2. 严格使用 P0-C 的 policy episode 标签与 train-validation paths；不使用未来 MFE/MAE/path 作为特征。
3. 同时报 selection Top5、M5A reranker、meta-label Top5、HMM、随机和 Top20 等权的政策净收益、hit rate、drawdown、turnover 和 coverage。
4. 固定模型后进入 P0-A challenger 前向发布；前向样本不足时只标记 `EVIDENCE_IMMATURE`，不回看旧 test 调参。
5. 仅当 validation 多路径与后续前向证据支持时，才提出新的 bundle 激活建议；系统不自动激活。
6. observation 到期后按同一 Top5 shadow policy、Selection Top40 exit context、冻结成本和数据库权威行情形成不可变 outcome/episode label与模型指标；固定持有期Top5收益、baseline Program Episode和open mark均不得替代。
7. 历史虚拟前向使用与正式推理相同的 exact bundle/scorer、同一 shadow policy 和数据库 PIT 行情，以批量虚拟时钟快速验证；输出与自然 future OOS 物理隔离。已用于模型判断的窗口必须标记已消费，禁止经调模后继续宣称新的历史 OOT。

完成判定：真实 WSL bundle、可重复 validation、PBO/选择偏差说明、每日 challenger observation、历史虚拟前向和 typed runtime 状态齐全；效果不佳也如实完成实验，不回到平台工程。当前 P0-D 已完成能力验证，但模型质量结论为不激活。

### P0-F：SHORT_REBOUND 连续 policy utility ranker

优先级：`HISTORICAL_COMPLETED_VALID_NEGATIVE`。

状态：`STAGE_A_NEGATIVE_STOP_NOT_ADVANCED`。

权威详细设计：
`docs/architecture/advisory_p0f_policy_utility_ranker_f2_design_20260824.md`。

Stage A源码已由PR #3758合入；停牌语义BUG-1180/1181已修复、完成重启验证并关闭。真实v2训练bundle为
`ff336eadb131cb6a3d431a846de4e9949ad984da1dcc4d9c231aa313886ebc10`：相对exact P0-D平均收益提高`2.612087 bps`、path win rate `53.57%`、相对Selection提高`5.578137 bps`、配对平均回撤改善`0.001286`，但配对平均换手增加`0.007419`。因此六项advancement只有换手失败，正式结论为`NEGATIVE_STOP_NOT_ADVANCED`；Stage B、runtime、descriptor和历史回放均禁止继续。

任务列表：

1. 复用exact P0-C dataset、feature schema和28 READY CPCV paths；不修改candidate、label、policy或成本。
2. 在每条path内以train-only median/MAD标准化连续`net_excess_return_bps`，训练固定Huber CORE/CORE_HMM × 3 seeds。
3. 预测值逆变换为`predicted_policy_net_excess_return_bps`，以确定性排序生成Selection Top20 entry priority；退出继续使用Selection Top40 rank。
4. 使用shared shadow portfolio评价并与exact P0-D/P0-E、Selection、HMM、random和Candidate20比较；candidate MAE/Spearman只作诊断。
5. 预注册advancement：对P0-D平均收益lift>0、path win>50%、对Selection lift>0，且配对平均回撤和换手不恶化。
6. advancement失败即发布负面离线结果并停止，不开发runtime/replay、不追加调参；这是一项完整实验终止，不是partial。
7. advancement通过才实现`policy_utility_ranker_with_meta_label_confidence`；entry由utility决定，概率继续来自exact P0-D binary model。
8. 历史回放只能标为`HISTORICAL_REPLAY`且不改变winner；自然future OOS继续按交易日形成并由用户决定是否激活。

完成判定：Stage A真实WSL 168 trial-path、不可变bundle、exact retry、PBO/paired comparison/advancement receipt齐全。若advancement通过，Stage B还需显式role、旧role兼容、isolated descriptor和历史回放；无论结果如何均不自动激活。

### P0-G：SHORT_REBOUND turnover-constrained policy utility

优先级：`HISTORICAL_COMPLETED_VALID_NEGATIVE`。

状态：`STAGE_A_NEGATIVE_STOP_NOT_ADVANCED`。

权威详细设计：
`docs/architecture/advisory_p0g_turnover_constrained_utility_f2_design_20260825.md`。

任务列表：

1. 复用exact P0-C dataset、P0-F feature schema v2、28 READY CPCV paths和shared shadow policy；不修改candidate、exit、policy或成本。
2. 用7716行成熟candidate episode的`holding_trading_days`构造与组合每日换手同单位的`2/(target_count*holding_days)`换手负担；3行涨停未入场和1行右删失不填默认label，其所在日期不参与constraint calibration；该未来字段只作label，禁止进入feature。
3. 每条outer path只在train blocks计算utility/liability MAD、固定shadow-price候选和exact P0-D train turnover budget，选择第一个满足预算的最小影子价格；validation不参与。
4. 用`net_excess_return_bps - shadow_price * turnover_liability`训练固定Huber CORE/CORE_HMM×3 seeds×28 paths，entry按预测adjusted utility排序，Selection Top40 exit保持不变。
5. 使用shared policy评价原始组合净收益、回撤和换手；train constraint、candidate loss、PBO、winner和advancement分别报告。
6. 复用P0-F六项advancement并要求每path constraint可行；任一失败完整负向停止，不追加price roster、family、seed、rank guard或blend。
7. 只有Stage A通过后才允许另行批准Stage B；P0-D概率、P0-F和P0-G bps score不得混淆。

完成判定：已完成真实WSL 168/168 trial-path、不可变bundle `433ff217...`、exact retry、资源/PBO/constraint/paired/advancement receipts；唯一失败门槛为相对P0-D换手`+0.004096`，已负向停止且未激活。

### P0-H：SHORT_REBOUND dual-head output-constrained utility

优先级：`HISTORICAL_COMPLETED_VALID_NEGATIVE`。

状态：`STAGE_A_NEGATIVE_STOP_NOT_ADVANCED`。

权威详细设计：
`docs/architecture/advisory_p0h_dual_head_output_constraint_f2_design_20260825.md`。

任务列表：

1. 复用exact P0-C、feature schema v2、28 outer CPCV paths和shared policy；不修改candidate、exit、policy或cost。
2. 分别训练return bps与`2/(5*holding_days)` liability fraction/day两个Huber head，future holding只作label，预测按冻结policy物理边界clip。
3. 每条outer path在保留train blocks内执行nested inner OOF、二次purge/embargo和block-reset shared evaluation；outer validation不参与拟合。
4. exact P0-D预算也使用同一inner OOF dates和固定winner model spec重建；每个family/seed/path从固定8个multiplier选择第一个满足预算的最小price。
5. 两头rounds取inner best-iteration中位数，在full outer train refit后只评价一次outer validation；固定CORE/CORE_HMM×3 seeds×28 paths。
6. 沿用P0-F/P0-G六项advancement；P0-F/P0-G和head diagnostics/PBO只作诊断，失败禁止Stage B和结果后调参。
7. Stage A只生成immutable dual-head bundle和完整receipts，零DDL/DML、零runtime/descriptor/activation。

完成判定：已完成完整nested OOF双头Stage A、真实WSL 168/168 trial-path、不可变bundle `82afdb81...`、exact retry和资源/constraint/paired/advancement receipts；收益与path-win门槛失败，已负向停止且未激活。

### P0-I：SHORT_REBOUND grouped-rank return head with output constraint

优先级：`HISTORICAL_COMPLETED_INCOMPLETE_NEGATIVE`。

状态：`STAGE_A_NEGATIVE_STOP_INCOMPLETE_CPCV`。

权威详细设计：
`docs/architecture/advisory_p0i_grouped_rank_return_head_f2_design_20260826.md`。

任务列表：

1. 精确复用P0-H的P0-C数据、feature schema v2、28条outer CPCV path、shared policy、cost、liability label/Huber head和exact P0-D OOF换手预算。
2. 唯一新变量是return head：在每个decision date的成熟policy episode候选中，将`net_excess_return_bps`确定性映射为0..4 ordinal relevance，训练固定`rank_xendcg`。
3. score date不读取label；将同日20只候选的raw model score转为确定性`[0,1]`百分位，再与liability预测组合并在train-only OOF选择最小可行price。
4. outer validation只作shared policy评价；不参与relevance规则、类别词表、rounds、price、score transform、family、seed或winner拟合。
5. 固定CORE/CORE_HMM×P0-H相同3 seeds×28 paths=168；不增加LambdaRank对照、blend、label-gain搜索、price roster或历史回放选择。
6. 沿用P0-H相对exact P0-D的六项advancement；P0-H paired、rank diagnostics和PBO只作诊断。
7. Stage A仅生成immutable grouped-rank dual-head bundle和完整receipts，零DDL/DML、零runtime/descriptor/activation；完整负向结果也作为有效实验合入。

完成判定：真实WSL在完成前10条path的60个trial-path后，第11条path的CORE/20260813在冻结8档price上均不能满足exact P0-D OOF换手预算，按预登记规则立即`NEGATIVE_STOP_INCOMPLETE_CPCV`。不可变evidence-only bundle为`2378358...`，exact retry复用同一identity；无winner/model/PBO/advancement，不得扩展price roster或继续Stage B。已完成的60项return daily Spearman均值为`-0.002229`、NDCG@5为`0.385322`，说明该grouped-rank收益头未形成稳定收益排序信号；liability Spearman `0.236089`继续证明原liability机制有效。

### P0-J：SHORT_REBOUND Selection-prior residual return with OOF reliability shrinkage

优先级：`HISTORICAL_COMPLETED_INCOMPLETE_NEGATIVE`。

状态：`STAGE_A_NEGATIVE_STOP_INCOMPLETE_CPCV`（source PR #3811；fix PR #3821；bundle `eb8ade9b...`）。

权威详细设计：
`docs/architecture/advisory_p0j_selection_prior_residual_return_f2_design_20260826.md`。

任务列表：

1. 精确复用P0-C数据、feature schema v2、28条outer CPCV path、shared policy/cost、P0-H liability head和exact P0-D OOF换手预算；P0-I只作为失败诊断，不作为模型或门槛reference。
2. 在每个inner-train成熟样本内，按`selection_effective_rank=1..20`计算收益中位数，并以样本数加权的非增isotonic回归形成train-only Selection收益先验曲线；score date只按rank查曲线，不读label。
3. 以`net_excess_return_bps - selection_prior_bps(rank)`为唯一return label，训练与P0-H同参数的Huber CORE/CORE_HMM残差头。
4. 聚合六fold成熟inner OOF残差预测后，使用预冻结零截距解析系数`alpha=clip(sum(pred*actual_residual)/sum(pred^2),0,1)`；零方差或非正可靠度显式得到`alpha=0`及typed receipt，不搜索weight、不读取outer validation。
5. 形成`anchored_return_bps=selection_prior_bps + alpha*predicted_residual_bps`，再与P0-H liability预测及固定8档price进入相同output constraint；Selection Top40 exit和停牌/涨停处理不变。
6. 固定CORE/CORE_HMM×原3 seeds×28 paths=168；沿用实际代码权威`build_policy_utility_advancement_receipt`的六项门槛，P0-H/P0-I和prior/residual diagnostics只作诊断。
7. Stage A只生成immutable request/bundle和完整receipts，零API/UI/DDL/DML/runtime/descriptor/activation；负向结果禁止同结果后调参。

完成判定：正式Stage A在首条outer path的inner block 3发现Selection rank中位数高度非单调，decreasing-isotonic prior完全平坦为`-1.5357 bps`，按预登记规则以`ADVISORY_P0J_SELECTION_PRIOR_DEGENERATE`立即`NEGATIVE_STOP_INCOMPLETE_CPCV`。evidence-only bundle `eb8ade9b...`和exact retry identity均已验证；零trial、无winner/PBO/Stage B，禁止扩展prior、alpha、family、seed或继续调参。

### P0-K：SHORT_REBOUND Selection-preserving liability gate

优先级：`COMPLETED_VALID_NEGATIVE`。

状态：`STAGE_A_NEGATIVE_STOP_NOT_ADVANCED_NOT_ACTIVATED`。

权威详细设计：
`docs/architecture/advisory_p0k_selection_preserving_liability_gate_f2_design_20260828.md`。

任务列表：

1. 精确复用P0-C数据、feature schema v2、28条outer CPCV path、shared policy/cost、P0-H liability label/Huber head和exact P0-D OOF换手预算。
2. 不训练或组合任何return、rank、prior、residual或confidence head；唯一模型输出为`turnover_liability_fraction_per_day`。
3. 冻结1/2/3/5/10/20日物理holding roster，对应最大liability `0.4/0.2/0.13333333333333333/0.08/0.04/0.02`；每个family/seed/path只在自己的inner OOF选择满足完整性和exact P0-D预算的最宽松阈值，并冻结P0-H相同的385/386约束日期identity。
4. gate只过滤新的ENTER候选；held candidate继续由Top40/review policy退出。eligible候选内严格按Selection rank和instrument tie-break排序，不允许模型细粒度重排。
5. 每日过滤后必须保留至少5个candidate，active-slot coverage和cash-day不得劣于matched Selection；禁止少荐股或空仓冒充低换手。
6. 固定CORE/CORE_HMM×3 seeds×28 paths=168；outer validation不参与threshold、rounds或roster拟合，168项完整后才按每个family/seed的28-path平均主指标选择winner，并沿用实际代码权威六项advancement。
7. Stage A只生成immutable request/bundle和完整receipts，零API/UI/DDL/DML/runtime/descriptor/activation；负向结果禁止同结果后调参。

完成判定：正式 request `advselgatereq_943f9e551d5fee35e57340cc`在合入源码上完成真实WSL `168/168`和exact retry，生成bundle `fee9b561...`。winner为CORE/20260813，liability日Spearman `0.254589`，但全部168条trial均在最宽`0.4`阈值停止、拒绝数为0、主指标与Selection逐位相同。相对P0-D收益`-2.966049 bps`、path win`32.14%`、MDD差`-0.004162`，仅换手改善`-0.068009`；结果为`NEGATIVE_STOP_NOT_ADVANCED`。`PBO=1.0`由六个arm策略分数完全相同导致，不作为普通PBO过拟合结论。P0-K不进入Stage B/runtime/replay，不扩展绝对阈值。

### P0-L：SHORT_REBOUND P0-G-anchored liability local reranker

优先级：`COMPLETED_VALID_NEGATIVE_RESEARCH_FAMILY_FROZEN`。

状态：`STAGE_A_NEGATIVE_STOP_INCOMPLETE_CPCV_NOT_ACTIVATED`。

权威详细设计：
`docs/architecture/advisory_p0l_p0g_anchored_liability_local_reranker_f2_design_20260829.md`。

任务列表：

1. 精确复用P0-C数据、feature schema v2、28条outer CPCV path、shared policy/cost、exact P0-D、P0-G `433ff217...`和P0-H/P0-K liability证据。
2. 固定P0-G winner `FAMILY_TURNOVER_CONSTRAINED_CORE/20260817`为唯一收益anchor；不改变P0-G family、seed、objective、rounds或shadow-price selector合同，inner/path-local price仅在各自train边界机械重算，不训练新return head。
3. 训练P0-K同合同的liability CORE/CORE_HMM×3 seeds，并在同一nested OOF中生成anchor rank和liability rank。
4. 冻结relative liability gain roster `(12,8,4,1)`；只考察anchor `(1,2)..(5,6)`，每个日期最多执行一组稳定相邻swap，每个候选相对anchor最多移动一位，只改变ENTER priority。
5. identity control必须精确复现P0-G但不得成为candidate winner；每条path只在自身outer-train OOF选择最保守、至少产生一次真实ENTER差异且满足exact P0-D换手预算的干预档。
6. winner必须形成真实priority/entry干预；Top20、active-slot和cash不得劣化。少于两个唯一block-score vector时PBO显式标为不可解释且不输出伪数值，但PBO保持diagnostic-only，不私增第七项advancement。
7. 固定2 family×3 seed×28 path=168，outer validation只评价；沿用相对P0-D/Selection收益、path win、MDD、换手和完整性的六项advancement，P0-G只作paired诊断。
8. Stage A仅生成immutable request/bundle和receipts；零API/UI/DDL/DML/runtime/descriptor/activation，只有`ADVANCED_TO_STAGE_B`才另行设计后续。

完成判定：P0-L设计PR #3951、源码PR #3959、BUG-1251 anchor日期职责修复PR #3967和close-sync PR #3969均已合入。BUG修复后的正式request `advp0lreq_b86425d3b5ce508904fa01b0`生成evidence-only bundle `4476afeb...`。identity control精确复现P0-G但无真实干预；gain `12/8/4/1`分别产生`33/71/85/85`次实际entry变化，且换手均低于P0-D OOF预算，但四个非零档均使cash day从`1`增至`2`并降低active-slot coverage，按冻结合同均非完整候选。Stage A在第一条outer path、首个family/seed的train-only selector以`ADVISORY_P0L_LOCAL_RERANK_INFEASIBLE`停止，结果为`NEGATIVE_STOP_INCOMPLETE_CPCV`、`0/168`、无winner、无可计算PBO、无Stage B、无runtime/descriptor/activation；exact retry复用同一bundle identity。P0-L不再是待实现任务，P0-D至P0-L研究族按本蓝图冻结。

### N0：最小研究控制面与父包预测延伸spike

优先级：`COMPLETED_CONTROL_PREREQUISITE`。

状态：`FORMAL_COMPLETE`。

1. 建立只追加JSONL trial registry和由其生成的单页活动路线；首批回填仅登记P0-D至P0-L、已消费80日test、24决策日历史回放和44日H0窗口的既有身份与结论，不重算结果、不补建证据平台。
2. 对目标父包执行只读/离线prediction extension spike，输出`FROZEN_MODEL_CAN_INFER`、`HISTORICAL_PREDICTION_ONLY`或`RETRAIN_NEW_LINEAGE_REQUIRED`三态及资源估计。
3. 冻结开发窗口与未来sealed holdout身份；holdout访问拒绝在任何oracle或learnability执行前生效。
4. N0不生成新因子、候选模型、IC或收益证据，不改变生产descriptor、Selection或运行时。

当前事实：独立F2详细设计为`docs/architecture/advisory_n0_research_control_f2_detailed_design_20260830.md`。正式root为`F:/Dev/AIstock_model_artifacts/advisory_n0_research_control_20260830/`，completion semantic SHA256为`9b460c294472daddd02375f0e68752dd741ab43617b9d0af1b6213c0b3ee9a9`，父包状态为`FROZEN_MODEL_CAN_INFER`。window contract固定P0-C开发窗口与唯一`SEALED_UNCONSUMED` holdout，N1结束后registry为16条、SHA256 `8d7ae5cd...`，派生route SHA256 `1b8164ce...`并指向N2。旧candidate root只保留为历史候选，不再作为当前状态依据。

完成判定：已满足。registry可机器区分study/objective/decision use并生成无互斥状态的路线页；父包spike给出可复核三态；sealed holdout对开发命令不可见。N0不再是后续任务。

### N1：Tier 1候选/排名oracle与固定learnability audit

优先级：`COMPLETED_DIAGNOSTIC`。

状态：`FORMAL_COMPLETE_DIRECTION_INCONCLUSIVE_SOURCE_MERGED`。

1. 仅在开发窗口计算全可交易赢家的Top20/40/50召回、Top20 perfect Top5、rank分桶、成本与容量折损后的clairvoyant上限。
2. 使用预注册的单一简单模型族、超参、特征和cross-fitting生成learnability结果；不得搜索模型族、loss、阈值或特征组合。
3. 同时报告真实干预支持度、block/cluster置信区间和四象限分流；判定线由MDE、成本、容量和最小经济收益推导。
4. oracle与learnability均登记为已消费开发诊断，不得进入sealed holdout、模型特征或运行时。

当前事实：F2详细设计为`docs/architecture/advisory_n1_tier1_oracle_learnability_f2_detailed_design_20260831.md`。正式request `advn1req_d8116a79d72d9c1813485053`绑定compute commit `f6f2edb93b8df8078795332c75db3505b9e9c5bb`，不可变bundle为`74827d03...`。PIT、成本、容量、H20时钟、全市场winner、Top50和N1标签区间CPCV均已闭合；oracle与learnability分别登记，sealed holdout未读，runtime/DDL/restart均为noop。交付恢复commit `c64f052b...`只处理Windows/WSL路径别名和route恢复，不改变bundle或结果。

结果：全市场赢家Top20/40/50召回极低，明确暴露上游候选召回瓶颈；Top20内clairvoyant空间很高。固定Ridge虽有`98.83 bps`正point lift，但95%下界`1.18 bps`未越过5 bps经济门槛且MDE高于point，不能证明当前信息集可学习。typed结果保留point象限但为`INCONCLUSIVE`，不得把理论上限当可部署模型，也不得把探索性正point当方向确认。

完成判定：已满足；N1科学诊断、bundle、registry和route完整，源码PR #4014已合入merge commit `cfc490a17...`。源码合入不改变N1结果；N2也已完成，N1不再是下一步，不重复N1或派生新的同信息集Ridge/loss变体。

### N2：Entry Guard、Exit-label与QE上游准备

优先级：`COMPLETED_AUXILIARY_DIAGNOSTICS`。

状态：`N2_COMPLETE_ALL_REQUIRED_DIAGNOSTICS_DELIVERED_NO_ACTIVATION`。

- N2-A 的权威详细设计为`docs/architecture/advisory_strategy_package_three_arm_alpha_audit_f2_detailed_design_20260831.md`。它固定比较`LSTM_ONLY`、`FUNDGROWTH_ONLY`和`IC_WEIGHTED_PARENT`，复用N1 development window、canonical PIT、H20 outcome、成本和benchmark，在共同预测交集上报告full-universe IC/RankIC、Top20/40/50赢家召回、Top5、oracle、腿间相关和组合配对边际增量。
- N2-A是零模型trial的`ORACLE_DIAGNOSTIC/NAVIGATION_ONLY`。Top25与Top50引用同一prediction identity，只能作为候选深度/政策参数，不能冒充两个Alpha；季度只作预冻结sensitivity，不允许结果后选择窗口、权重或arm。
- N2-A不替代Entry Guard或Exit-label诊断，也不提前生成QE新Alpha。它先回答现有父包的Alpha由哪条腿贡献、组合是否优于最佳单腿，再把结果作为N3上游StrategyPackage/QE分流的输入之一。
- N2-A正式request `advalpha3req_e7295a31a4e1953e9048cec5`绑定clean compute commit `30a8a74e...`，bundle `6784df1a...`已通过manifest inspect和exact retry；registry总数由16增至17，route hash刷新为`db910830...`且next task保持`N2_ENTRY_EXIT_QE_PREPARATION`。正式运行trial=0、sealed holdout未读、runtime/DDL/DML均为noop。
- N2-A源码PR #4048已合入merge commit `55d376b3...`，任务worktree、本地分支和远端分支均已精确清理；源码交付不改变formal bundle identity、registry结果或生产运行时。
- 正式结果显示：LSTM/FUND/父包RankIC为`0.11677/0.05628/0.12284`，Top5 H20净超额为`397.89/245.73/446.52 bps`；父包相对FUND的RankIC与Top5增量95%区间均大于0，但相对LSTM的两项区间均跨0。LSTM与FUND score相关仅约`0.18`，父包与LSTM约`0.90`；因此FUND包含不同信息但较弱，固定IC组合尚未证明稳定优于LSTM。父包Top50召回`1.7617%`虽为随机期望`1.75×`，绝对候选召回仍低，继续支持扩大候选源而非在同一信息集更换loss。
- 2026-08-31只读清单原有19个registry包、7个未退役包；当前Top25/Top50父包共享prediction SHA256 `e0c571f...`，仍只算一个组合信号。除当前两腿外，当时识别出的三条独立旧单Alpha均无现成Prediction Store历史预测，但单日frozen-runtime可产性曾逐一通过。该可产性只说明artifact可执行，不证明PIT安全、共同窗口Alpha或当前生命周期合格。
- N2-B v1冻结request `advpkgareq_ef3d0abb...`原计划比较父包与`pkg_378eb9...`、`pkg_5a5ccb...`、`pkg_b668f8...`。运行约6小时后，第三包使用的`neg_vol_adjusted_momentum`被独立追加不变性检查证实存在PIT因果违规，进程退出且未发布bundle、未追加registry、未形成任何可用于导航或激活的研究证据。该失败只消费工程运行，不得解释为第三包收益差或据此选择剩余包。
- BUG-1302已在DEV完成因子隔离、策略包退役和重启后验证：`pkg_b668f8...`当前为`RETIRED`，旧因子禁用且不可rehab；PIT正确替代`neg_vol_adjusted_momentum_pit_v2`为独立新因子identity，正式评级D、保持禁用、当前没有StrategyPackage引用。退役包不得继续进入v2，也不得把替代因子静默注入旧包重写其身份。
- N2-B v2权威详细设计为`docs/architecture/advisory_independent_strategy_package_alpha_audit_f2_detailed_design_20260831.md` v1.6。新request/schema/experiment identity固定绑定已验证BUG-1302 exclusion receipt、被排除包id/status/manifest和违规因子名；仅比较`CURRENT_IC_PARENT`、`pkg_378eb9...`与`pkg_5a5ccb...`三臂。两个存续包共享57因子closure，执行386个primary group run、3个causality diagnostic和1个file-backed parity，共390个执行单元；每个冻结模型只加载一次。所有三组pairwise比较复用相同PIT/H20/cost审计，仍为0-trial `ORACLE_DIAGNOSTIC/NAVIGATION_ONLY`。
- v2不是对v1 request的续跑或修改：必须从合入后的新源码冻结新request和独立artifact root；`resource_max_wall_seconds=null`，墙钟只记录、不自动终止，RSS/temp、COW输入隔离、decision-date投影、逐值/dtype/index/hash parity、精确计算计数和sealed holdout拒绝继续fail closed。`pkg_378eb9...`与`pkg_5a5ccb...`的原生label、窗口、执行和成本不一致，原生Sharpe/RankIC只作inventory，正式结论只能来自共同窗口输出。
- N2-B v2正式bundle `bcdcb31de4dc1409f74fd5f4ef760e6bd8f6da8230aac4dbc1eadef8b2d50518`已完成。当前父包RankIC `0.12284`、Top5 H20净超额`446.52 bps`，显著优于`pkg_378eb9...`的`0.00107/166.32 bps`和`pkg_5a5ccb...`的`0.01124/101.88 bps`；两个旧包均不构成替换候选。Top50全市场赢家召回仍为`1.7617%`、95%上界`2.5907%`，候选源瓶颈未解除。

- Entry/Exit action bundle `5c5946a7...`已完成：动态Q90 Entry arm具备支持但lift显著为负；固定3%/5% arm的正point均欠干预支持，没有confirmatory-positive Entry策略。Exit clairvoyant上限为`386.60 bps/episode`，只证明动作空间存在，不证明可学。
- Exit fixed-information learnability bundle `03d17a18...`以固定22项T-visible特征、Ridge和cross-fit完成；point `-56.81 bps/五槽entry-day`、95%区间`[-200.33,52.62]`、MDE `181.29`，support充分但结论`INCONCLUSIVE/NAVIGATION_ONLY`。Entry与Exit因此均不抢占N3主线。
- QE上游无证据preparation `advqeprep_7d28b455e667312d00cf54f9`曾冻结数据、表达式空间、24 proposal预算、资源与registry lineage；它本身保持0 trial。其后继固定24-proposal、overlay、腿间、分钟和generator实验现均已完成且selected=0；该 preparation 只保留为历史lineage输入，不再构成待运行任务，也不得扩预算、改方向或写因子库。
- 同一时间最多一条主线和一个独立辅助工作包；N2不得演变为三个并行模型项目。

完成判定：已满足。Entry和Exit分别绑定决策时钟、objective contract、policy hash和增量价值标签；所有结果为独立shadow，未形成动态仓位、下单或Selection写入。N2只负责诊断与N3路由，不产生可激活winner。

### N3：四象限分流后的唯一模型主线

优先级：`ADVISORY_INTERNAL_FRONTIERS_COMPLETE_SYSTEM_UPSTREAM_QE_MAIN_ACTIVE`。

状态：`ADVISORY_GENERATOR_MARGIN_EVENT_AND_SCORE_HMM_V1_SELECTED_ZERO__QE_ROLLING_LSTM_CANDIDATE_PENDING_MULTI_SEED_AND_LOO`。

- Top40/50赢家召回或候选流不足：进入QE/StrategyPackage上游alpha MVE。
- 召回充足且Top20理论、learnability均高：进入包含新信息的Top20 ranker；禁止继续P0同信息集loss/model轮换。
- 排名空间低而日频Entry/Exit空间高：只推进对应的日频建议主线，不扩展为分钟执行研究。
- 全部空间低：审查PIT股票池、候选生成和review policy动作空间，不以复杂模型掩盖低上限。

每条主线建立新hypothesis lineage和frontier合同，只能从inner-train选择一次candidate；confirmation失败后不得回选。

历史分流事实：N2-B全市场Top5赢家召回上界2.59%低于当时20%结构门，N1 direction_ready=false，Entry无确认正arm，Exit固定信息集未确认可学；N3各精确frontier及因果Admission v2.1 R1均selected=0。当前解释修正：这些事实不证明策略包严重无alpha或所有下游不可学；H20标签与实际组合口径、模型表达、校准定义和模型老化需要分开验证。新路线按§16由QE统一执行历史复验、多seed、LOO、因子/模型组合及新候选搜索，Advisory只消费交付物；不能单凭召回门关闭下游，也不能在已关闭的score/raw+Ridge frontier继续换loss。

N3首批是固定6族×4 proposal、总计24次的轻量探索屏：声明式AST只消费Qlib daily与冻结`static_factors.parquet`的T-visible字段，目标使用N2-B `CURRENT_IC_PARENT`共同窗口H20成本后outcome；所有same-date rank/zscore只在当日canonical PIT成员内计算，窗口并集中的非成员必须mask。每个proposal报告RankIC、Top5净超额、相对父包Top5 lift、coverage、churn、父包相关、20日block 95%区间、24-trial family-wise区间和DSR；只有全部预注册门槛满足时一次选择0或1个candidate。结果始终为`EXPLORATORY_SCREEN/NAVIGATION_ONLY`，不写因子库、不生成StrategyPackage、不激活运行时。

正式结果：request `advqemvereq_28ac7e998080dd2258cf4c23`绑定`main@dc3ace36...`、N1/N2 receipts、119,953,459-byte outcome hash和2,182,434,551-byte static hash；bundle `09137f0c...`完成24个proposal，registry由21增至22。所有proposal的family-wise Top5 lift下界均不大于0，故本轮frontier按合同一次选择0个candidate；exact retry复用相同bundle、registry duplicate-noop、route exact-noop。该结论关闭的是“本批单信号独立替换父包”假设，不关闭弱信号组合或新信息集方向。

父包overlay正式结果：request `advn3ovlreq_152dc894211c967347155ceb`从clean `main@7bbd56c6...`运行，bundle `fdca2130...`在386日完成`24/24/24/0`。五个候选干预386日/7季度，regime候选干预158日/6季度，故失败不是零干预。24项family-wise Top5 lift下界全部不大于0；13项同时未通过RankIC delta下界。`CROWDING_DISPERSION_01`虽把RankIC point提高约`0.00233..0.00747`，却使Top5 lift为`-68.43..-24.27 bps`并增加churn；`REGIME_CONDITIONED_02`的最佳Top5 point约`+7.79 bps`，但family-wise下界约`-118.94 bps`。同窗弱信号overlay因此关闭，不再调权、反向或挑窗口。

历史信息集可行性审计：N2-A三腿panel在1,710,301行、386日上具有100%共同score覆盖；LSTM/FUND日内rank相关约`0.186`，腿间rank dispersion中位数`0.133`、P90 `0.327`。N2-A parent与N2-B current parent在全量键上score/outcome精确parity。N2-B独立包三臂完整交集仅422,997行/24.3%，分钟数据工程成本更高，因此当时的唯一后继lineage先消费完整N2-A腿间分歧；该lineage及其分钟后继现均已完成且关闭。

该阶段MVE权威设计与源码为`advisory_n3_leg_disagreement_information_set_mve_f2_detailed_design_20260902.md`及`leg_disagreement_*`：固定`LEG_LINEAR_COMPARATOR_V1`与`LEG_DISAGREEMENT_EXPANDED_V1`两个Ridge trial，expanded只增加signed/absolute gap、consensus min/product及parent×agreement五项显式交互；复用N1 28 READY CPCV并要求全部1,710,301个source row各有7个OOF prediction，只有1,705,332个finite-evaluable row进入训练。normal missing不填零、不在排序前删股票；若Top5含unknown/nonfinite标签，该模型当日Top5指标typed unavailable且不以部分持仓均值替代。candidate必须同时显著优于current parent与linear comparator的RankIC和Top5成本后lift；同窗结果仍只可导航。其冻结route为selected=0转`N3_MINUTE_INFORMATION_SET_MVE`、selected=1只进入独立confirmation设计。

腿间正式结果：源码PR #4198、request `advn3legreq_d267...`及bundle `42ac23b6...`均已完成。support为382个paired日/380个干预日，四项family-wise lower全部不通过，selected=0且exact retry稳定。本lineage已消费并关闭，不得回选；当时按冻结route转入的分钟信息集也已完成且selected=0。

分钟source-ready结果：活跃snapshot `qlib_minute_authoritative_full_candidate_20240102_20260630`覆盖N2-A全部386日/1,710,301键。raw扫描为1,695,153 complete、13,473 partial和1,675 whole-day missing；其中13,461个partial落在`2025-11-27/2025-12-08/2025-12-12`三个241-slot session，且每个候选都恰有240个合法OHLC bar。source spike按bar count归一化后为1,708,614 complete、12 partial和1,675 whole-day missing。实现期真实WSL/Qlib烟测确认缺失位置不是严格同一slot：`2025-12-08`有4,474只缺`13:00`、另13只缺`11:30`且`13:00`有值。因此实现保留raw coverage，并另报session-normalized分类；不移动bar、不删除候选/日期、不填零。扫描耗时21.38分钟、峰值RSS约896MB，证明按日八列流式聚合可直接实现，无需缓存或分钟特征平台。

分钟MVE权威设计与结果为`advisory_n3_minute_information_set_mve_f2_detailed_design_20260903.md` v1.2：只读取T日`open/high/low/close/volume/amount/limit_up/limit_down`，冻结八项聚合、parent-only Ridge comparator与parent+minute Ridge candidate，并复用N1 28 READY path/7 OOF。源码PR #4210已合入；正式request `advn3minreq_333ff0cf8e102efef88961c3`、bundle `0076a3a6...`完成`2/2/2/0`。candidate相对两个baseline的RankIC delta均为`-0.032643`，Top5 lift均为`-315.32 bps`；相对parent的family-wise lower为`-0.072894/-480.67 bps`，相对comparator为`-0.077498/-472.57 bps`。384个可评价日全部干预且score Spearman仅`0.78918`，说明失败不是恒等输出，而是新分钟表达导致经济退化。本frontier关闭，selected=0固定转`N3_QE_ALPHA_GENERATOR_MVE_DESIGN`；结果仍package-conditioned、navigation-only、不可激活。

QE Alpha generator MVE权威设计与结果为`advisory_n3_qe_alpha_generator_mve_f2_detailed_design_20260903.md` v1.7及bundle `9327330c...`。它以READ ONLY目录快照、target-free声明式AST生成和固定10% parent overlay完成正式运行：24项生成、23项评价、0项入选，旧bundle内累计trial count为71；所有Top5累计family-wise lower均不大于0，且四段稳定性全部失败。该结果关闭daily/static grammar generator，不允许扩大prompt、模型、字段、调用或表达式预算。后继margin设计另行纠正旧累计数未计腿间/分钟可选候选的问题，不改写该历史bundle。

融资融券单源设计为`advisory_n3_margin_information_set_mve_f2_detailed_design_20260904.md` v1.7。源码、薄CLI和三组direct tests已合入main；正式request `advn3margreq_0e6f63e49ae12d5b2e79f789`生成source bundle `633c4058...`与经济bundle `b50411d8...`，完成3个固定trial且selected=0。candidate RankIC为`0.113847`，低于parent `0.122839`；candidate Top5成本后净超额为`252.80 bps`，低于parent `443.65 bps`，相对parent lift `-202.21 bps`。323个共同可评价日低于预注册最低支持，四段joint-positive为0，结果保持`EXPLORATORY_INSUFFICIENT_SUPPORT/NAVIGATION_ONLY`。该结论只关闭十二项融资融券动态+冻结Ridge的精确frontier；正式route为`N3_FINANCIAL_EVENT_SOURCE_READINESS_DESIGN`。该margin receipt自身不授权事件抓取或回填；本轮用户按蓝图授权的后继仅执行已有主库数据的只读source-readiness，Tushare/网络、DDL/DML和训练仍为noop。

财务事件source-readiness权威详细设计为`advisory_n3_financial_event_source_readiness_f2_detailed_design_20260904.md` v1.2。源码、薄CLI、direct tests和exact CI mapping已通过PR #4288合入merge commit `5f2fda091...`；clean main正式只读bundle `211b8db1...`得到`SOURCE_READY_NAVIGATION_ONLY_NON_VINTAGE`。projection保留84,272个最早本地版本及neutral disclosure，全部date-only行只能从公告日后首个交易日生效；source revision只作整源诊断，不按后来修订删行。该ready随后只放行一次固定事件MVE，不独立支持经济结论、confirmation或activation。

财务事件信息集MVE权威详细设计为`advisory_n3_financial_event_information_set_mve_f2_detailed_design_20260905.md` v1.0。正式 bundle `ad234f4c...`已完成固定`3/3/0`：signed-content candidate RankIC/Top5为`0.063685/359.41 bps`，parent为`0.122839/443.65 bps`，四段joint-positive为0。结果保持`DATE_ONLY_BACKFILLED_NON_VINTAGE/EXPLORATORY_NOT_SELECTED/NAVIGATION_ONLY`，关闭本次事件方向/窗口/特征/Ridge frontier并按预注册route转同包评分/市场/HMM辅助准入；不进入vintage source、confirmation或activation。

### N3-AUX：因果Admission与可选市场/HMM信息

优先级：`R1_FORMAL_COMPLETE_AUXILIARY_FRONTIER_CLOSED`。

状态：`V1_SELECTED_ZERO_FROZEN__V2_1_R1_FORMAL_SELECTED_ZERO_NO_R2`。

1. v1正式request `advscorehmm_2a442c84ecdac872a4e56e45`、bundle `f8da2f70...`三臂已执行、sector两臂NOT_RUN，3/5/9日TAKE且lift全负。243点诊断117点支持充分但无可靠正增量，历史结果不改判。
2. 实现期源合同修复仍有效：从N1 Prediction Store重建405日/20,250行Top50 context；386候选日parity后形成7,720行同源policy labels。宽PIT以创建N1的primary canonical source只读投影，DEV两个symbol成员结束日漂移被拒绝；正式MVE本身DB访问0。不为新设计重复固化该数据。
3. v1 complement intercept/base rate不能生成跨日生产absolute threshold；residual q20亦不等于mean LCB。v2.1详细设计v1.4替代未运行的v1.0固定静态/sector-only门禁，保留新revision、累计trial和窗口消费。
4. R0在因果Admission F2中冻结blocks 0～1初始训练、block 2的48日inner一次选点、blocks 3～7的240日outer一次readout；primary target为`POLICY_EPISODE_NET_RETURN_BPS_MAX20_V1`，比较静态Ridge与20交易日expanding Ridge共2个trial，动作效用为预测净收益减5 bps，MDE为`44.2485 bps`。R1源码经PR #4395合入；BUG-1395/PR #4402修复WSL对Windows-clean主仓库的换行误报，BUG-1396/PR #4404把两臂frontier按registry合同聚合为单记录。
5. clean `main@a317f7c2...`正式request `advcausal_9ed1c38ef6d6ff7fb5d3ef33`与bundle `7e739be5...`完成2/2/0。静态/expanding分别仅9/11个干预日、lift point `+2.408/+3.811 bps`且95% lower跨0，Brier均劣于base-rate；无inner candidate，outer未读。exact retry验证同bundle、registry duplicate-noop、route exact-noop。
6. `INSUFFICIENT_SUPPORT_REVIEW`结论：两臂虽接近支持门但同时未过5 bps经济门与校准readout，不能把失败归结为单纯“过度保守”，也不得降低支持门、回选arm或用HMM扩列挽救同frontier。G2-A仍无完整accepted OOF；R2不自动启动。Advisory Alpha/Admission实验线关闭，后续Alpha模型由QE统一规划；Advisory只保留日级价格区间模型和正式交付物消费侧验证。

### N4：信号组合、重训窗口与prospective activation

优先级：`AFTER_ONE_ROLE_HAS_CONFIRMED_INCREMENTAL_VALUE`。

状态：`WAITING_CONFIRMED_SIGNAL`。

只有一个角色形成独立确认增量后，才执行逐种子/种子平均稳定性、残差正交、LOO、成本后组合和模块factorial ablation。同包评分、原始市场形态、市场HMM和板块HMM先在N3-AUX完成单项/增量归因，N4只组合已确认角色。基础静态/单一因果更新已在N3-AUX前置；此时才比较更完整的expanding/rolling/regime-weighted周期，不预设三个月、不建设自动重训平台。历史回放、一次sealed holdout和自然prospective OOS分级报告，激活按objective contract独立决定。

### H0：条件性实盘单日与历史批量同核执行

优先级：`CONDITIONAL_BLOCKER_ONLY_NO_DEFAULT_ALLOCATION`。

状态：`DESIGN_READY_CONDITIONAL_DORMANT_NOT_ACTIVE`。

H0不是当前主动任务，也不与N3并行占用开发、审核或算力。只有实盘/历史业务语义不一致，或已测得的回放耗时、I/O、内存问题直接阻断当前候选验证时，才按最小可解除阻塞范围启动；没有直接阻塞证据时，下列任务列表仅是备用设计，不执行。

详细设计：
`docs/architecture/advisory_live_daily_historical_batch_shared_kernel_f2_detailed_design_20260816.md`。

任务列表：

1. 以已完成44日A/B/C回放的冻结输入、逐日业务产物和可用性能收据作为golden baseline；不得修改、覆盖或回写其code-release和旧run。
2. 将实盘单日和历史批量执行器收敛到唯一的日信号、Selection/HMM/risk/tradability 与 list transition 业务组合，禁止复制回测专用选股算法。
3. 历史执行采用持久 worker 和默认5日 chunk；静态工作区按内容 identity 复用，日期动态目录、PIT 视图、结果和 checkpoint 逐日隔离。
4. 批量读取必须通过 `AdvisoryPITAsOfViewV1` 向日内核授权；增加未来行/未来修订毒化测试，证明较早日期语义 hash 不变。
5. 当 A/B 的 raw-affecting identity 完全相同时只计算一次 raw Alpha，并发布不可变共享 artifact；B 的 HMM/risk/tradability 从 raw 后分叉，任何 raw identity 差异都拒绝共享。
6. source validation 改为批次 full seal、chunk 前后 revision token、逐日实际读取 receipt；无可靠 token 或发现漂移时执行 full rehash，禁止仅以日期 cutoff 取代 revision/PIT 校验。
7. 在单 worker、零额外并发条件下先完成语义等价和资源基准；只有内存、I/O 和失败注入验收后才评估并发，不能以并发掩盖重复工作。

条件性启动后的完成判定：代表日与完整 golden 窗口的逐日业务语义全部等价；未来毒化、缺 revision、chunk 中断、exact resume、缓存污染和 A/B raw 共享反例测试通过；性能收据分阶段报告 workspace、source validation、raw inference、overlay、publish 的耗时/I/O/RSS。性能目标是验证指标而非业务成功门禁，未达到目标时如实保留结果，不改变语义换取速度。未启动时状态保持`CONDITIONAL_DORMANT`，不得形成pending完成度或阻塞N3。

### P1-A：outcome/price因果校准

优先级：`HISTORICAL_NAVIGATION_COMPLETE_SELECTED_ZERO_NATURAL_MATURITY_FOR_FUTURE_CONFIRMATION`。

历史原始M3/M4、M5B/M5C负结果保留。PR #4785 / merge `8021790ab...` 已实现固定static v4对唯一rolling-20D matured CQR的past-only比较；正式已消费窗口审计虽然把active模型coverage从`0.731333`改善到`0.782667`，但75个交易日聚类bootstrap下界为`-0.003333`，未过预注册稳定性门，selected=0。该lineage在此终止，不回选窗口、不换阈值、不将点估计冒充确认。生产仍只允许消费真实自然成熟residual；当前继续静态v4与自然前向累积，未来只有独立confirmation或新的信息集假设才能另立lineage。M4 binary标签不可学的问题不以继续校准解决。

### P1-B：公共预测与策略条件化共享实验

优先级：`AFTER_BASE_METHOD_READY_AND_TWO_EVALUABLE_STREAMS`。

状态：`TARGET_DESIGN_NOT_IMPLEMENTED`。

1. 按§5.7将股票/PIT基本特征与package-specific score/roster/policy adapter分离，不删除父Alpha或伪装包无关。仅改一个可验证接口切片。
2. 在两个以上具有合法PIT来源和标签的候选流做公共模型、公共+条件化、独立包baseline的matched比较；无需先交付两个完整生产bundle。相同stock/date按簇防泄漏和重复样本。
3. leave-one-package-out、逐包收益/校准、来源选择偏差、负迁移、成本及动作policy一致性共同评价；独立基线可先离线训练，不能用共享模型自己的输出作baseline。
4. 共享收益/风险预测不等于共享policy value。显式compatible set和角色合同确认后才绑定；无验证包仍typed unavailable。复杂多任务网络仅在简单共享有证据后考虑。

### P2：长期趋势专家

优先级：`P2_WHEN_PACKAGE_READY`。

- 使用长期趋势包对应的现有 QE 文件训练独立模型，并复用 P0-A/P0-B 的前向发布和动态 bundle 分发。
- 标签必须按长期趋势自己的 review policy、生存/time-to-hit 和20至180日目标构造，不复制短反弹5日或20日标签。

### 用户未来单独确认后才可恢复的可选任务

以下任务不属于当前路线；P0-A 的前向运行只保存今后自然产生的业务事实，条件性H0即使被直接阻塞触发也只优化已授权回放执行，不解禁任何历史补账：

- 历史证据、历史数据固化和归档。
- Phase 1R完整历史链E2E。
- 新建通用 Source Catalog、SEALED、CAS、invalidation和GC平台；H0 仅复用现有 catalog/CAS 并定向优化重复校验和 raw artifact 共享。
- 自动训练平台、ModelOps、通用漂移治理、灾备和治理后台；不禁止任务内预注册的历史因果更新比较。
- 旧batch/root/orphan清理或修复。

## 10. Design Acceptance Index

| ID | 验收要求 |
|---|---|
| F-101 | 基础历史数据只读取现有QE H5/Parquet/Qlib Bin；预测/模型产物允许PKL；训练不读取生产数据库历史数据 |
| F-102 | 真实训练仅在WSL Conda执行，Windows不训练 |
| F-103 | 首模是实际LightGBM模型，不是mock、规则或随机结果 |
| F-104 | M1/M3/M4历史切分按既有合同保留；P0-C新policy标签按最长20日及实际退出窗口重算purge/embargo，不照搬旧5日/25日边界 |
| F-105 | 训练与正式预测共享同一逐列FeatureBuilder/schema、公式、单位、missing和decision cutoff |
| F-106 | 正式预测区分decision/target双日期，只读取数据库decision cutoff行情和实际Program输入 |
| F-107 | 真实模型输出Top5并与原始/HMM/随机/等权基线比较 |
| F-108 | 页面可展示明确标记的`EXPERIMENTAL_SHADOW`真实结果 |
| F-109 | 模型失败不阻断现有荐股，不用基线冒充模型；bundle只按exact shadow binding加载，不扫描latest |
| F-110 | 多个Program的基线荐股独立运行；当前模型覆盖仅限目标多Alpha Program，单Alpha typed unavailable不得冒充模型支持完成 |
| F-111 | 收益、周期和价格范围均来自真实模型；规则只作为单独标记的对照或硬风险边界，不能替代模型结果或伪造概率 |
| F-112 | Selection、Paper、模拟盘、QE资产和策略包业务逻辑零写入 |
| F-113 | 不新增未经用户确认的角色、审批、package二次准入或门禁；本次明确批准的`ADMISSION_RISK`仍须按F-202/F-204从设计、验证到激活分阶段交付 |
| F-114 | 不处理旧历史任务、旧root、归档和非阻碍性平台工作 |
| F-115 | 进度分开报告组件实现、固定日期推理、每日发布、episode、Program模型覆盖和质量净增量；不再汇总为易误读的总体百分比 |
| F-116 | 原生多Alpha首模按当前代表seed、zscore和terminal weights重建runtime-equivalent候选；完整seed/逐日权重/combined只作诊断，不跨实验拼腿或制造伪seed特征 |
| F-117 | 历史涨跌停直接读取日线/分钟Bin中的状态、价格和昨收，不重复建设`stk_limit`训练文件 |
| F-118 | HMM按当前文件数据从头拟合、确定性规范状态并保存可续推posterior；shadow不单独refit HMM，旧模型/状态/系数只作对照 |
| F-119 | 分钟Bin不进入后续Advisory模型训练或正式推理；既有N3分钟MVE只保留历史证据，分钟择时/执行另属QE/Execution/Paper |
| F-120 | 首模只要求沪深300；其它宽基指数在模型合同明确需要前不补充、不阻断 |
| F-121 | WSL训练默认8GB任务预算、小时级首次反馈目标；超额上限需在实验内显式登记，历史超额如实保留；不新建缓存或证据平台 |
| F-122 | M5A 所有窗口、种子、模型配置和融合权重只由 train/validation 选择；冻结 80 日 test 仅在 winner 固定后评价一次 |
| F-123 | M5A 必须同时报告模型、selection rank、HMM、随机和 Top20 等权基线；质量差如实保留，不用 hidden fallback 或 test 调参 |
| F-124 | selection-prior 混合是显式模型合同并保存权重，不把规则基线冒充模型；多个 Program 仍按 exact package/style binding 隔离 |
| F-125 | M3 概率校准只使用 validation 拟合并独立报告 discrimination 与 calibration；M4 二分类负例不足时保持 `UNCALIBRATED` |
| F-126 | M3/M4 quantile 校准不得读取 test 标签或实时未来行情；模型发布、binding、重启和 deployed readback继续分开报告 |
| F-127 | ENABLED Program 的 `daily_after_close` 对应真实 Advisory 执行器，按交易日幂等产生 `PUBLISHED` baseline list，不回填旧缺口 |
| F-128 | baseline、model challenger 和 replay 身份分离；按需 GET、源码合入、bundle激活、每日发布和前向成熟分别报告 |
| F-129 | challenger observation 持久化 Program/binding/package/model、decision/target、Top5、outcome/价格区间、状态和成熟截止日 |
| F-130 | Program active binding 动态解析 exact bundle；移除单一 Program/package 运行常量限制，无 bundle 时基线继续且模型 typed unavailable |
| F-131 | 历史P0-D SHORT_REBOUND meta-label只学习take/skip/confidence，不自动下单或生成资金仓位，也不冒充候选召回或未来角色模型 |
| F-132 | meta-label 使用独立冻结的Top5 shadow policy模拟episode净收益，绑定policy hash、价格基础、成本、退出原因、成熟与删失语义；生产Top20 policy不变 |
| F-133 | 模型选择只使用 purged rolling/CPCV train-validation；冻结80日test不再选择；PBO/等价诊断不可计算时显式记录而不形成审批门禁 |
| F-134 | 真正未来OOS来自每日 challenger observation 和成熟 episode；前向样本少标记证据未成熟，不回看冻结test调参 |
| F-135 | 历史校准研究使用当时已成熟residual，生产仅用自然成熟residual；区分预测分位数/估计误差，M4近单类binary先重审标签 |
| F-136 | 至少两个合法可评估候选流可做离线独立/公共/条件化matched与留包研究，不以两个生产bundle为前置；正式共享需逐包验证 |
| F-137 | 每日前向研究无资金、无下单、无QMT/Paper/模拟盘写入；QE包直接用，不新增审批、资格角色、策略包二次准入或历史证据平台；价格family配置与效果分报 |
| F-138 | rank-exit标签必须从既有预测文件重建后续至少Top40并处理held-symbol缺席；只有Top20时不得伪造完整review-policy标签 |
| F-139 | 候选级take/skip标签与Top5 shadow portfolio评价分离；后者真实执行target count、replacement budget、持仓继承和现金状态 |
| F-140 | D收盘发布绑定decision=D/target=下一交易日且不读取target行情；episode只在target日权威next-open到达后进入，缺价保持WAITING_DATA且不回退signal close |
| F-141 | 实盘单日与历史批量仅执行拓扑不同，共享唯一逐日业务语义；禁止第二套回测选股、HMM、risk或名单算法 |
| F-142 | 历史批量日内核只能读取绑定decision cutoff、availability和source revision的PIT AsOfDataView；未来数据毒化不得改变较早日期结果 |
| F-143 | 静态工作区按package/manifest/model/factor/runtime内容identity复用，日期动态数据和结果逐日隔离；缓存错误不得静默重建为成功 |
| F-144 | A/B只在raw-affecting identity完全相同时共享不可变raw Alpha artifact；HMM/risk/tradability在raw之后分叉，identity差异必须拒绝共享 |
| F-145 | source validation使用批次full seal、chunk revision token、逐日读取receipt和异常full rehash；不得用单纯日期过滤替代revision校验 |
| F-146 | 每个历史交易日独立artifact、typed failure和checkpoint；raw可预计算，顺序相关的list/episode transition在首个未完成日停止并exact resume |
| F-147 | 当前v6冻结为golden baseline；代表日和完整窗口以业务语义hash验证batch与single-day等价，运行信封字段单独比较 |
| F-148 | 首版使用单持久worker和默认5日chunk，先验证内存/I/O/恢复再评估并发；并发不是业务完成或性能验收的替代 |
| F-149 | H0不修改实盘binding、Program发布语义、策略包状态、Selection/Paper/QMT，也不修改、覆盖或回写已冻结v6的code-release与结果 |
| F-150 | 性能receipt分解workspace/source/raw/overlay/publish耗时、I/O、RSS和cache hit；目标未达不得删减PIT、typed failure或业务逻辑 |
| F-151 | P0-D descriptor 接入不得覆盖现有 M1 文件；已存在 binding 只允许 expected-current hash CAS 原子切换，切换前保存不可变快照并支持同契约精确回滚 |
| F-152 | P0-F唯一实验变量为连续policy utility entry priority；P0-C label/feature、Selection candidate/exit、shadow policy和成本不变 |
| F-153 | P0-F每path仅用train median/MAD执行可逆连续label transform，不clipping；非有限或zero-scale fail closed |
| F-154 | P0-F固定Huber CORE/CORE_HMM×3 seeds×28 paths，winner由shared-policy收益选择，不做结果后family/transform/rank guard搜索 |
| F-155 | P0-F advancement必须同时超过P0-D和Selection、path win>50%，且配对平均回撤和换手不恶化；失败完整终止Stage B |
| F-156 | P0-F candidate regression诊断、PBO、shared-policy winner和advancement分别报告，不互相冒充 |
| F-157 | P0-F conditional runtime使用显式role；utility只决定entry rank，take/skip/confidence继续来自exact P0-D binary model |
| F-158 | P0-F历史回放只在advancement通过后运行并固定为HISTORICAL_REPLAY；自然future OOS不回填且独立确认 |
| F-159 | P0-F Stage A/B均无DDL/DML和默认生产激活；descriptor、restart、activation和cleanup保持独立用户授权 |
| F-160 | P0-F真实Stage A换手门禁失败后完整终止；P0-G不得修改P0-F参数、rank guard、blend或运行时来规避负向结论 |
| F-161 | P0-G换手负担与shared evaluator同单位，holding/exit/future return只作label/evaluation；非成熟行不填默认值且只允许同日期集合的P0-G/P0-D约束校准 |
| F-162 | P0-G影子价格候选固定，只在每条outer CPCV path的train blocks选择最小可行值；validation和历史回放不得参与 |
| F-163 | P0-G不连续train block之间强制空仓重置，purge/embargo和label span不得跨validation继承portfolio state |
| F-164 | P0-G固定Huber CORE/CORE_HMM×3 seeds×28 paths，entry只按adjusted utility变化，Selection Top40 exit、policy和成本保持不变 |
| F-165 | P0-G沿用收益、path win、Selection、回撤、换手和完整性六项advancement；失败停止Stage B且不自动激活 |
| F-166 | P0-G Stage A生成不可变request/bundle、constraint/PBO/paired/advancement/resource/exact-retry receipt，零DDL/DML和零运行时写入 |
| F-167 | P0-H以nested OOF双头output constraint分离return/liability并复用exact P0-D预算；真实负向结果证明约束有效但return泛化不足，未激活 |
| F-168 | P0-I只以grouped-rank替换return head；冻结price不可行时按预登记停止，不扩展roster、不补跑、不生成winner或runtime bundle |
| F-169 | P0-J以inner-train Selection单调收益先验加OOF解析收缩残差替换from-scratch return；liability、业务逻辑、exact P0-D预算和六项门槛不变 |
| F-170 | P0-K删除return head，只用train-only OOF liability gate过滤高换手ENTER候选；eligible集合保持Selection顺序，完整性、业务逻辑、exact P0-D预算和六项门槛不变 |
| F-171 | P0-L固定P0-G收益anchor，只用relative liability rank执行最大位移1/每日一次的相邻ENTER重排；identity control不得成为winner，机制必须产生真实干预且保持shared policy与六项门槛 |
| F-172 | P0-D至P0-L属于同一共享开发研究族并永久冻结；不派生P0-M、不回选旧frontier、不结果后扩展阈值/family/seed/loss |
| F-173 | 六层决策栈是目标架构而非六条工作线；正式研究最多一条主线和一条结果独立的辅助线 |
| F-174 | 每项新研究预注册`ALPHA_RANKING`或`RISK_MANAGED_ADVISORY`，标签、指标、激活状态和页面展示按合同隔离，不折算为一个加权总分 |
| F-175 | 最小append-only JSONL registry区分study type、family/lineage、唯一变量、objective、trial计数、消费窗口、decision use和证据引用；单页路线由其派生，不建设治理平台 |
| F-176 | 父包预测延伸spike输出冻结模型可推理、仅历史预测或需重训新lineage三态；禁止外推最后一日预测或把重训冒充旧模型延伸 |
| F-177 | 每层同时报告clairvoyant/action ceiling与固定cross-fitted learnability；理论上限不可部署，learnability失败只约束当前信息集和冻结简单模型 |
| F-178 | oracle mini-contract冻结PIT股票池、上市年限、ST/停牌/涨跌停、winner、成本、容量、benchmark、执行价和policy；oracle/learnability不得读取sealed holdout |
| F-179 | sealed holdout只在主线完全冻结后执行一次confirmation并立即登记消费；失败不得返回同一frontier重选，exact retry仅限未利用经济结果的代码/数据身份修复 |
| F-180 | frontier只在inner-train报告完整收益/换手/现金/coverage/MDD Pareto集合并选择一次candidate；activation合同独立且不得结果后放宽 |
| F-181 | 探索性结果可用于研究导航，但不能单独关闭方向、声称稳定效果或支持激活；`decision_use`错误引用必须机器拒绝 |
| F-182 | 确认性实验预注册最低干预次数、干预交易日比例和regime覆盖；阈值由MDE与block/cluster有效样本量推导，恒等策略或稀疏高方差不冒充有效 |
| F-183 | Ranking、Entry、Exit和Risk标签均表达相对冻结基线动作的增量价值并绑定policy hash；历史两臂可重放时使用同一shadow simulator，不强制引入OPE平台 |
| F-184 | 日频Entry Guard只消费T日冻结信息和T+1指定日级观察点当时可见的权威open/current；`SKIP`可形成固定槽位现金/空槽，但不生成逐分钟动作或动态资金权重 |
| F-185 | Exit先验证“现在退出相对继续baseline policy”的label/oracle；liability或holding相关性不得冒充Exit可预测性 |
| F-186 | QE alpha MVE准备件可与N0/N1并行；N3正式分流前禁止生成/评价候选、读取IC/收益或按结果调整提示词与搜索预算 |
| F-187 | 组合只接纳角色一致且具备独立时段残差、逐种子/种子平均稳定性、LOO边际、成本后、跨regime和经济归因/已知效应重叠检查的信号；不同决策时钟不得任意加总 |
| F-188 | 历史批量回放、一次sealed holdout和自然prospective OOS分级使用，任何一类不得冒充另一类；只有对应objective contract的前向证据可支持激活 |
| F-189 | 动态资金仓位、自动下单和交易执行输入不在当前授权；固定等权槽位的`SKIP/WAITING`不构成仓位模型 |
| F-190 | 重训周期通过expanding、不同rolling window和regime-weighted对照选择；不预设三个月、不建设自动重训平台 |
| F-191 | Top5、M3和M4均为独立训练但package-conditioned的bundle；模型输入绑定目标包候选、父Alpha特征、manifest/style/policy/runtime semantics。当前P0-D descriptor只提供重排role，M3/M4 child typed unavailable；无bundle包也必须typed unavailable，不得声称包无关或完整组合已上线 |
| F-192 | 公共股票/PIT预测加显式package adapter；共享参数必须通过独立离线baseline、compatible-set matched与留包检验，禁止负迁移或重复样本伪证据 |
| F-193 | 后续工作分为主动业务主线、被动自然观察、条件性阻塞修复和零工作历史约束；只有主动主线默认获得研发/算力，H0、冻结、历史分析、固化、归档和旧清理不得进入默认队列 |
| F-194 | Ranking、Outcome/Holding、Entry Price和Exit/Risk使用独立role binding；新增或旋转一个角色不得静默覆盖其它角色，组合必须满足exact Program/package/style/clock/schema/policy兼容并按角色typed unavailable/激活/回滚 |
| F-195 | 父overlay正式selected=0后只启动固定N2-A三腿共识/分歧learnability MVE；两个Ridge trial、28-path/7-OOF、四项family-wise增量门槛和selected=0转分钟/selected=1转confirmation路线必须预注册且同窗只可导航 |
| F-196 | 腿间正式selected=0后先完成target-free分钟source-ready，再只实现固定parent-only comparator与固定parent+minute candidate；T日时钟、八字段/八聚合、normal missing、28-path/7-OOF、双baseline family-wise门槛和selected=0转generator/selected=1转confirmation必须预注册 |
| F-197 | 分钟正式selected=0后只实现固定QE Alpha generator MVE：目录只读准备、LLM无target声明式AST生成和DB/network-off经济评价三阶段隔离；固定生成/经济trial预算、原创性/已知效应/防衰减、10%父包rank overlay、累计多重检验与0/1一次性route |
| F-198 | generator正式selected=0后只选择融资融券新信息源：冻结candidate H5 source projection、T-1时钟、十二项动态、parent/membership controls、三个Ridge trial、28-path/7-OOF、累计多重检验和0/1 route；不删unsupported股票、不读sealed、不写生产 |
| F-199 | 父包raw/combined score只保留同日横截面排序语义；跨日准入必须使用exact package/manifest/style/policy绑定的train-only transform与past-only chronological绝对收益/概率校准，禁止raw score固定阈值和CPCV补集绝对水平 |
| F-200 | 原始市场形态、市场HMM、板块HMM/rotation和两类HMM组合使用显式固定arm消融；原始市场形态是无HMM control，HMM必须证明增量且不得直接乘父score或单状态默认硬否决 |
| F-201 | HMM只消费causal forward-filter或canonical causal OOF/prediction bundle，使用PIT行业映射并记录父score重复消费；smoothed/Viterbi未来状态、latest snapshot、neutral静默填充和缺失删股均禁止 |
| F-202 | `ADMISSION_RISK`按`RISK_MANAGED_ADVISORY`输出逐候选TAKE/SKIP和日级TAKE_SOME/SKIP_ALL；零只通过是`NO_ELIGIBLE_RECOMMENDATION`正常状态，最多五个固定槽位，不补位、不形成资金权重 |
| F-203 | 冻结Program动作候选深度内的HMM条件化重排使用独立`ALPHA_RANKING`experiment；当前Top20与研究Top50不得混成一个动作集，同包评分准入与HMM风险使用独立目标合同、trial和激活状态，不得结果后改合同或压成一个总分 |
| F-204 | N3-AUX详细设计只能在margin不可变结果后启动，代码/实验必须等待F2冻结；独立确认通过后才进入N4组合，sector runtime还必须等待`rotation_L1`自身Advisory capability/forward证据 |
| F-205 | margin selected=0后财务事件source readiness只读三类raw最早本地版本并保留neutral disclosure；date-only只从公告后首个交易日生效，revision/support只作target-free诊断，non-vintage ready只放行后续MVE设计 |
| F-230 | 财务事件MVE只消费immutable non-vintage projection，固定parent/disclosure/signed三trial、事件时钟、28-path/7-OOF、双control增量门槛、一次0/1 route；正结果也只放行vintage source决策，不得直接确认或激活 |
| F-231 | v1 负结果、诊断身份与 CPCV 绝对校准限制不改判；v1.0 未运行规格被新 revision 替代 |
| F-232 | 每个 arm/日期 past-only 单预测，静态与一个因果更新对照；全部 transform 和标签按 fit 时钟成熟 |
| F-233 | 分阶段 source gate；R1 不依赖 sector，R2 必须 canonical causal OOF，缺源零 trial |
| F-234 | 有界线性/更新/可选非线性与信息增量比较，全部模型候选计累计 trial；不做笛卡尔网格 |
| F-235 | 期望、概率、收益分位数与估计区间分离；动作效用独立；§5.7 参数化后才可开跑 |
| F-236 | 父 Top5 固定槽位 0～5、不重排不补位、无资金权重；故障不等于主动 SKIP |
| F-237 | same-policy 配对、完整 frontier、MDE/干预/经济证据分报，不要求全部维度同时改善 |
| F-238 | 新 revision、研究族累计、一次选点、修复/重试身份及 objective/decision-use 一致 |
| F-239 | PIT、maturity、normal missing、holdout 隔离与三级证据边界 |
| F-240 | 最小实现，无 HMM 产品重复建设，无 DB/API/UI/runtime/DDL/重启 |
| F-241 | 公共stock/PIT预测、显式包adapter与独立动作层分离；不冒充跨包通用已实现 |
| F-242 | 候选扩展归QE/Selection，同预算同policy、多源去重/provenance；无静默扩池补位 |
| F-243 | 市场—板块—个股分层与可选HMM可归因；禁止重复暴露和独立分位数相加 |
| F-244 | 全市场Top5召回20%只保留历史诊断；新路由综合净正机会、可学性与同policy收益 |
| F-245 | QE正CAGR不能外推Advisory；QE完成多seed/组合审计并交付正式包后，Advisory才做同Top5/review/cost消费验证 |
| F-246 | 正常缺失、系统缺源、模型无效与主动SKIP分离；发布阻塞先修，不用scheduler心跳冒充健康 |
| F-247 | 价格区间受净动作价值、可成交与风险约束；MFE/MAE只形成日级描述性区间，不冒充最佳分钟买卖点或执行动作 |
| F-248 | 架构替代路线只条件启动；无平台化、无旧结果改判、sealed消费与候选身份分离 |
| F-249 | QE是历史全量复验、多seed、因子/模型组合Alpha审计和新组合搜索的唯一实验所有者；Advisory不提交重复Alpha训练或回测，但拥有日级价格区间模型研发及正式交付物消费验证 |
| F-250 | Advisory Program/Binding股票池合同与QE公开语义同形：`stock_universe`、`single_index`、`index_union`及P0核心指数pool ID；绑定后运行时不得漂移 |
| F-251 | 指数股票池普通复评按复评日、正式forward按D-1决策截止读取PIT成分与股票资格PIT交集，在Selection候选之后、Advisory排名之前过滤并重排；冻结canonical优先，仅其截止不可用时使用Selection ready/clean实盘滚动PIT并固化来源身份；空结果正常返回且不得静默扩池补位 |
| F-252 | 单日复评、历史回放与正式forward共用同一股票池准入核，结算及模型子层消费发布时冻结的候选投影；分别保存target trade date与universe as-of trade date、selection、membership revision、symbol hash和排除计数；全市场旧Program保持透传兼容 |
| F-253 | 本切片只修改Advisory源码、API/UI、测试和对应文档；不修改QE、Selection、StrategyPackage、数据集或其它模块，不执行实验、DDL和进程控制 |
| F-254 | 正式日频forward在`selection_as_of_trade_date=D < target_trade_date=T`时强制`DAILY_DB_ONLY`；Selection逐股只读不晚于D的最后数据库close、零实时行情调用，正常停牌保留并记录真实价格日，无历史价格或查询失败typed fail closed，普通`AUTO`调用保持兼容 |
| F-255 | Advisory在Program创建或Binding切换前以只读预检核对StrategyPackage/asset、冻结股票池证据、目标universe和descriptor阶段；exact、后置过滤、旧包未声明、identity mismatch、合同不完整与baseline-only必须分别报告，后置过滤不得冒充QE指数池训练/推理；预检不运行QE、不写包/Program/数据库 |
| F-256 | legacy M4价格分布能力保持`AdvisoryDailyPriceEnvelopeV1`及显式entry-v2：日级PIT分布、校准/可用性和身份，不输出分钟时间点、订单、拆单、fill或执行状态；§6.3.2新经济建议角色另立设计/证据，不改该旧合同 |
| F-257 | QE、Paper、Execution未来只能以只读、版本化方式选择性消费日级价格区间；消费、分钟训练/回测、适配、执行和激活不属于Advisory蓝图，不得由Advisory修改或编排 |
| F-258 | 后续M4价格区间训练和正式推理不读取分钟Bin或目标日未来行情；当前P0-D未绑定M4的typed unavailable事实不因新增合同而改写 |
| F-259 | 自然前向价格预测必须在T日开盘前，以显式冻结M1/M3/v4、Program/list/Selection、D/T、package/policy/schema身份发布不可变request/prediction/receipt；不得回填、读取T日结果、消费sealed holdout或写数据库/binding，成熟评价另立confirmation合同 |
| F-260 | 自然前向价格结果只在T日18:00及kline/suspend双refresh audit完成后结算；停牌保留、未知缺行fail closed，逐日settlement不可变；累计至少20日/300个有效候选并按目标日cluster bootstrap，支持不足只ACCUMULATING且本切片不建议激活 |
| F-261 | 下一价格候选必须证明新增信息/可识别动作区别于旧研究；raw市场/板块先核PIT，gap/分布头/换loss不能单独构成新假设，HMM非前置 |
| F-262 | D冻结条件函数与T实际价格观察分离，条件收益不冒充价格干预或成交；未知隔夜事件/支持外不承诺盈利 |
| F-263 | 同模型/监督/参数/人口的新增信息对照，买入/留空共用起始状态及后续policy，单笔解释不能替代组合收益 |
| F-264 | 事前简单固定槽位风险对照及暴露报告；不按test调整控制，不以事后缩放、低回撤或换合同升级旧失败 |
| F-265 | 来源预检→有界详细设计→最小研究→组合评价→有条件确认/生产适配；经济失败后停止，完整新family工程不前置 |
| F-266 | §16.5 R1预算/四fit及停止事实保留；下一轮按§16.6/R2独立设计明确预算/状态，候选停止不等于任务停止；只读来源、Advisory范围、X临时/F持久、QE训练互斥及用户重启边界保持 |

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-101 | `backend/services/advisory_model_first/qe_file_source.py` | `backend/tests/advisory_model_first/test_qe_file_source.py`; M1/M3/M4 frozen artifacts | implemented_verified | none |
| F-102 | Windows launcher + `scripts/wsl/advisory_*_train.py` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_model_first/bundles/9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629/training_log.json`; M3/M4同类receipt | implemented_verified | none |
| F-103 | `reranker_training.py`, `outcome_training.py`, `price_range_training.py` | `backend/tests/advisory_model_first/test_price_range_training.py`; `backend/tests/advisory_model_first/test_outcome_training.py`; artifact: M1 `model.txt` | implemented_verified | none |
| F-104 | historical `time_split.py`/`outcome_split.py`; `policy_cpcv.py` P0-C policy split | `backend/tests/advisory_model_first/test_outcome_split.py`; `backend/tests/advisory_model_first/test_policy_cpcv.py`; artifact: M1/M3/M4 `split.json` and P0-C 28 READY paths | IMPLEMENTED_HISTORICAL_AND_POLICY_SPLIT_VERIFIED | none |
| F-105 | `shared_feature_builder.py`, `feature_schema_v1.py` | `backend/tests/advisory_model_first/test_shared_feature_builder.py`; `backend/tests/advisory_model_first/test_realtime_feature_source.py` | implemented_verified | none |
| F-106 | `realtime_feature_source.py` | `backend/tests/advisory_model_first/test_realtime_feature_source.py`; artifact: deployed model-shadow readback for decision `2026-07-15`, target `2026-07-16` | implemented_verified | none |
| F-107 | `reranker_training.py` baseline comparison | artifact: `F:/Dev/AIstock_model_artifacts/advisory_model_first/bundles/9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629/baseline_comparison.json`; `backend/tests/advisory_model_first/test_candidate_and_contracts.py` | implemented_verified | none |
| F-108 | `backend/routers/advisory.py`; `frontend/src/app/paper-v2/advisory/page.tsx` | `backend/tests/advisory_model_first/test_model_shadow_api.py`; `frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts`; runtime route HTTP 200 | implemented_verified | none |
| F-109 | `model_inference.py`, exact binding loaders | `backend/tests/advisory_model_first/test_model_inference.py`; `backend/tests/advisory_model_first/test_model_shadow_api.py`; artifact: typed single Alpha unavailable readback | implemented_verified | none |
| F-110 | Program-level baseline composition + `model_binding_resolution.py` dynamic exact-bundle resolver | `backend/tests/advisory_model_first/test_model_inference.py`; `backend/tests/advisory_model_first/test_dynamic_model_binding.py`; runtime: target multi-Alpha exact P0-D + single-Alpha typed unavailable | IMPLEMENTED_DYNAMIC_MODEL_COVERAGE_VERIFIED | none |
| F-111 | M3 46 heads; M4 4 heads | `backend/tests/advisory_model_first/test_outcome_training.py`; `backend/tests/advisory_model_first/test_price_range_training.py`; M3/M4 `metrics.json` artifacts | implemented_verified | none |
| F-112 | Advisory-only model-first modules | `backend/tests/advisory_model_first/test_outcome_boundaries.py`; `backend/tests/advisory_model_first/test_price_range_boundaries.py` | implemented_verified | none |
| F-113 | blueprint §§3,14 and model-first error contracts | artifact: `docs/architecture/advisory_model_first_m4_price_ranges_f2_design_20260810.md`; F2 validator receipts | implemented_verified | none |
| F-114 | model-first import and changed-file boundaries | `backend/tests/advisory_model_first/test_outcome_boundaries.py`; `backend/tests/advisory_model_first/test_price_range_boundaries.py` | implemented_verified | none |
| F-115 | blueprint分层进度 | artifact: `docs/architecture/hmm_evolution_phase2_rotation_l1_g2a_detailed_design_20260903.md`; 2026-09-07 forward/status、forward-runs、forward-model-metrics；G2-A v1.2 17/39结构停止；PR #4361合入且profile未激活；历史M0～N3 identities | DESIGN_VERIFIED_CURRENT_TRUTH_20260907 | none |
| F-116 | `candidate_group.py`, `prediction_source.py`, `qe_file_source.py` | `backend/tests/advisory_model_first/test_candidate_and_contracts.py`; artifact: M1 406-date/8120-row request | implemented_verified | none |
| F-117 | Qlib daily/limit fields and price-range labels | `backend/tests/advisory_model_first/test_labels.py`; `backend/tests/advisory_model_first/test_price_range_labels.py` | implemented_verified | none |
| F-118 | `fresh_hmm.py` | `backend/tests/advisory_model_first/test_fresh_hmm.py`; artifact: M1 `fresh_hmm_models.json` | implemented_verified | none |
| F-119 | M1/M3/M4 daily input contracts | `backend/tests/advisory_model_first/test_qe_file_source.py`; `backend/tests/advisory_model_first/test_outcome_boundaries.py`; `backend/tests/advisory_model_first/test_price_range_boundaries.py` | implemented_verified | none |
| F-120 | QE `000300.SH` daily Bin | `backend/tests/advisory_model_first/test_qe_file_source.py`; artifact: M1 `training_request.json` benchmark identity | implemented_verified | none |
| F-121 | §6.4 WSL预算与资源报告 | artifact: M1/M3/M4 training_log.json；generator bundle 9327330c... peak 15.37GB | HISTORICAL_CORE_VERIFIED_GENERATOR_BUDGET_EXCEEDED | approved_by_user: 8GB不是全部历史实验均已满足的事实；后续单独资源合同 |
| F-122 | M5A train/test dual request and winner policy | `backend/tests/advisory_model_first/test_quality_contracts.py`; artifact: `quality_runs/advm5train_a64594d6f22f618a4afef84a/winner_receipt.json`; `quality_evaluations/advm5test_818fe5a6c8ee323d2fbf25d4/test_once_receipt.json` | implemented_real_trained_verified | none |
| F-123 | M5A shared baseline evaluator；冻结 test 结果低于 selection rank 的事实见 §9 | `backend/tests/advisory_model_first/test_quality_pipeline.py`; artifact: `quality_evaluations/advm5test_818fe5a6c8ee323d2fbf25d4/test_report.json` | implemented_real_evaluated_verified | none |
| F-124 | M5A ensemble and selection-prior policy；现行 M1 binding 保留的运行状态见 §9 | `backend/tests/advisory_model_first/test_quality_scoring.py`; `backend/tests/advisory_model_first/test_quality_bundle.py`; artifact: bundle `1757b24b854cf8b5bfee8874bd442491091ea979c86522fbeef15a02930f8ecb` | implemented_bundle_verified | none |
| F-125 | M5B validation-only probability calibration；8 个正斜率 head 发布 calibrated，2 个排序反转 head 明确 uncalibrated；逐 head solver/版本/迭代/收敛证据 fail-closed | `backend/services/advisory_model_first/outcome_calibration.py`; artifact: `outcome_calibration_runs/advoutcal_ec16422ad1a97040583e5273/outcome_calibration_receipt.json`; `backend/tests/advisory_model_first/test_outcome_calibration.py` | real_calibration_verified | none |
| F-126 | M5B outcome quantile calibration和 M5C entry-gap coverage calibration均只使用 validation 拟合；M5C test 零缺失、`delta=0` 且质量未改善 | `outcome_calibration.py`; `price_range_calibration.py`; artifact: M5C bundle `5197ceac96c76881a506555652acc006987442024cb2d86955e7370b27968ead`; `test_price_range_calibration.py` | m5b_m5c_real_calibration_negative_quality_verified_not_activated | none |
| F-127 | P0-A Advisory after-close runner + existing review service | `backend/tests/advisory_model_first/test_forward_publication.py`; `backend/tests/advisory_model_first/test_forward_scheduler.py`; artifact: two persisted 2026-08-14 forward runs | implemented_runtime_publication_verified | none |
| F-128 | baseline/challenger/replay identity contract | `backend/tests/advisory_model_first/test_forward_publication.py`; `backend/tests/advisory_model_first/test_forward_api.py`; artifact: multi Alpha EXPERIMENTAL_SHADOW and single Alpha typed UNAVAILABLE readback | implemented_runtime_identity_verified | none |
| F-129 | `AdvisoryForwardObservationV1` | `backend/tests/advisory_model_first/test_forward_postgres.py`; `backend/tests/advisory_model_first/test_forward_api.py`; artifact: persisted forward/model observation readback | implemented_runtime_observation_verified | none |
| F-130 | `AdvisoryModelBindingResolutionV1` | `backend/tests/advisory_model_first/test_dynamic_model_binding.py`; artifact: exact multi Alpha descriptor and package-without-bundle typed unavailable | implemented_dynamic_binding_verified | none |
| F-131 | policy-aligned meta-label take/skip/confidence | `backend/tests/advisory_model_first/test_meta_label_training.py`; `backend/tests/advisory_model_first/test_meta_label_bundle.py`; artifact: final-source bundle `e555903e...` | implemented_real_wsl_verified_experimental_not_activated | none |
| F-132 | `AdvisoryPolicyEpisodeLabelV1` + existing review transition semantics | `backend/tests/advisory_model_first/test_policy_episode_labels.py`; `backend/tests/advisory_model_first/test_policy_dataset_bundle.py`; artifact: P0-C bundle `81e2c9ba...` | implemented_real_file_dataset_verified | none |
| F-133 | purged rolling/CPCV + PBO/equivalent report | `backend/tests/advisory_model_first/test_policy_cpcv.py`; `backend/tests/advisory_model_first/test_policy_pbo.py`; artifact: P0-C 28 paths and P0-D 168 trial-path rows/70 PBO partitions | implemented_real_cpcv_pbo_verified | none |
| F-134 | daily forward observations and matured policy episodes | `backend/tests/advisory_model_first/test_forward_publication.py`; `backend/tests/advisory_model_first/test_forward_postgres.py`; runtime: eachProgram 3 PUBLISHED runs, 2 SETTLED target dates and 20 active episodes as of 2026-08-18 | APPROVED_BY_USER_FORWARD_RUNNING_EVIDENCE_IMMATURE | approved_by_user: wait for natural model episode/outcome maturity; current OPEN_MARK_TO_MARKET metrics are not mature OOS and are not backfilled |
| F-135 | §6.3.1、P1-A；`advisory_adaptive_price_calibration_v1_f2_design_20260916.md`；`adaptive_price_calibration.py`及contracts/CLI | `backend/tests/advisory_model_first/test_adaptive_price_calibration.py`、contracts/CLI测试；PR #4785 / merge `8021790ab...`；artifact `adaptive_price_calibration_runs/advpradapt_8c81ea2bd2f70d75e1683fe9` | IMPLEMENTED_HISTORICAL_NAVIGATION_SELECTED_ZERO | approved_by_user: 历史研究不等自然成熟；生产仍需真实mature residual；同lineage不调参 |
| F-136 | compatible-set pooled/multi-task experiment | `backend/tests/advisory_model_first/test_strategy_conditioned_pooling.py` (target path) | APPROVED_BY_USER_P1B_DIRECTION_READY_WAITING_COMPATIBLE_PACKAGE_DATA | none |
| F-137 | Advisory-only forward boundaries | `backend/tests/advisory_model_first/test_forward_boundaries.py`; `backend/tests/advisory_model_first/test_meta_label_boundaries.py`; artifact: Advisory forward boundary readback | implemented_runtime_boundary_verified | none |
| F-138 | P0-C Top40/held-symbol rank reconstruction | `backend/tests/advisory_model_first/test_policy_rank_source.py`; artifact: P0-C `candidate_rankings.parquet` | implemented_real_file_reconstruction_verified | none |
| F-139 | candidate meta-label evaluator + shadow portfolio policy simulator | `backend/tests/advisory_model_first/test_policy_episode_labels.py`; `backend/tests/advisory_model_first/test_shadow_portfolio_policy.py`; artifact: P0-D matched baseline report | implemented_real_policy_simulation_verified_not_activated | none |
| F-140 | after-close publication and target-open episode clock | `backend/tests/advisory_model_first/test_forward_date_clock.py`; `backend/tests/advisory_model_first/test_forward_recovery.py`; runtime: target 2026-08-14/17 SETTLED, target 2026-08-18 pre-open WAITING_DATA | APPROVED_BY_USER_RUNTIME_TARGET_OPEN_SETTLEMENT_VERIFIED | approved_by_user: no target-open fallback; each future target remains WAITING_DATA until authoritative open arrives |
| F-141 | H0 shared day business composition + two executors | `backend/tests/advisory_execution/test_single_batch_semantic_parity.py` (target path) | APPROVED_CONDITIONAL_DORMANT_DESIGN_READY | approved_by_user: only activate for a reproducible direct mainline blocker |
| F-142 | `AdvisoryPITAsOfViewV1` + historical batch source | `backend/tests/advisory_execution/test_pit_asof_view.py` (target path) | APPROVED_CONDITIONAL_DORMANT_DESIGN_READY | approved_by_user: only activate for a reproducible direct mainline blocker |
| F-143 | content-addressed runtime workspace session | `backend/tests/strategy_package/test_runtime_workspace_session.py` (target path) | APPROVED_CONDITIONAL_DORMANT_DESIGN_READY | approved_by_user: only activate for a reproducible direct mainline blocker |
| F-144 | immutable raw Alpha day artifact reuse | `backend/tests/advisory_historical_range/test_raw_alpha_reuse.py` (target path) | APPROVED_CONDITIONAL_DORMANT_DESIGN_READY | approved_by_user: only activate for a reproducible direct mainline blocker |
| F-145 | batch/chunk/day source validation policy | `backend/tests/advisory_historical_range/test_batch_source_validation.py` (target path) | APPROVED_CONDITIONAL_DORMANT_DESIGN_READY | approved_by_user: only activate for a reproducible direct mainline blocker |
| F-146 | day checkpoint + ordered list transition recovery | `backend/tests/advisory_historical_range/test_batch_recovery.py` (target path) | APPROVED_CONDITIONAL_DORMANT_DESIGN_READY | approved_by_user: only activate for a reproducible direct mainline blocker |
| F-147 | semantic hash oracle + current v6 golden receipt | `backend/tests/advisory_execution/test_single_batch_semantic_parity.py` (target path); report `docs/analysis/advisory_historical_fullstack_comparison_result_20260817.md`; artifact `comparison_result_v6.json`, result hash `500d96e0...`, contract `652eef96...`, 44 dates | GOLDEN_FROZEN_CONDITIONAL_DORMANT_ORACLE_READY | approved_by_user: batch parity implementation is not active without a direct blocker |
| F-148 | single-worker chunk policy + resource receipt | `backend/tests/advisory_historical_range/test_batch_resource_policy.py` (target path) | APPROVED_CONDITIONAL_DORMANT_DESIGN_READY | approved_by_user: only activate for a reproducible direct mainline blocker |
| F-149 | execution boundary and no-live-activation assertions | `backend/tests/advisory_execution/test_execution_boundaries.py` (target path) | APPROVED_CONDITIONAL_DORMANT_DESIGN_READY | approved_by_user: only activate for a reproducible direct mainline blocker |
| F-150 | stage telemetry and performance report | `backend/tests/advisory_historical_range/test_batch_telemetry.py` (target path) | APPROVED_CONDITIONAL_DORMANT_DESIGN_READY | approved_by_user: only activate for a reproducible direct mainline blocker |
| F-151 | `model_binding_resolution.py`; descriptor operator CLI | `backend/tests/advisory_model_first/test_dynamic_model_binding.py`; P0-D runtime F2 design；runtime descriptor `f98f2ded... -> e555903e...` | SOURCE_MERGED_RUNTIME_VERIFIED | none |
| F-152 | P0-F design §§2-5；`policy_utility_pipeline.py` | `backend/tests/advisory_model_first/test_policy_utility_pipeline.py`; artifact `ff336ead...` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-153 | `policy_utility_training.py` | `backend/tests/advisory_model_first/test_policy_utility_training.py`; artifact `ff336ead...` train-only transform receipt | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-154 | P0-F request/contracts/training/pipeline | `backend/tests/advisory_model_first/test_policy_utility_contracts.py`; `backend/tests/advisory_model_first/test_policy_utility_pipeline.py`; 168 trial-path receipt | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-155 | P0-F advancement receipt | `backend/tests/advisory_model_first/test_policy_utility_pipeline.py`; artifact `ff336ead...` six-metric advancement result | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-156 | P0-F candidate/portfolio metrics and PBO | `backend/tests/advisory_model_first/test_policy_utility_pipeline.py`; `backend/tests/advisory_model_first/test_policy_pbo.py`; artifact `ff336ead...` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-157 | conditional resolver/inference/bundle role | P0-F design §6; artifact: `F:/Dev/AIstock_model_artifacts/advisory_p0f_suspension_v2_20260824/policy_utility_bundles/ff336eadb131cb6a3d431a846de4e9949ad984da1dcc4d9c231aa313886ebc10/advancement_receipt.json` | NOT_IMPLEMENTED_BY_FROZEN_NEGATIVE_STOP | approved_by_user: Stage A failure prohibited Stage B runtime role, descriptor and activation implementation |
| F-158 | conditional historical replay union | `backend/tests/advisory_model_first/test_historical_forward_replay.py`; P0-F negative advancement receipt | NOT_RUN_BY_FROZEN_NEGATIVE_STOP | approved_by_user: historical replay was conditional on advancement and therefore was correctly not executed |
| F-159 | P0-F Stage A boundary assertions | `backend/tests/advisory_model_first/test_meta_label_boundaries.py`; `backend/tests/advisory_model_first/test_policy_utility_bundle.py`; P0-F negative receipt | STAGE_A_BOUNDARY_VERIFIED_STAGE_B_PROHIBITED | none |
| F-160 | P0-F negative receipt；P0-G contracts | `backend/tests/advisory_model_first/test_policy_utility_pipeline.py`; `backend/tests/advisory_model_first/test_turnover_constrained_utility_contracts.py` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-161 | P0-G liability label/training | `backend/tests/advisory_model_first/test_turnover_constrained_utility_training.py` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-162 | P0-G train-only price selection | `backend/tests/advisory_model_first/test_turnover_constrained_utility_pipeline.py` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-163 | P0-G split/block-reset pipeline | `backend/tests/advisory_model_first/test_turnover_constrained_utility_pipeline.py` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-164 | P0-G contracts/training/pipeline | `backend/tests/advisory_model_first/test_turnover_constrained_utility_contracts.py`; `backend/tests/advisory_model_first/test_turnover_constrained_utility_training.py`; `backend/tests/advisory_model_first/test_turnover_constrained_utility_pipeline.py` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-165 | P0-G advancement/stage guard | `backend/tests/advisory_model_first/test_turnover_constrained_utility_pipeline.py` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-166 | P0-G immutable bundle/exact retry/boundary | `backend/tests/advisory_model_first/test_turnover_constrained_utility_bundle.py`; `backend/tests/advisory_model_first/test_turnover_constrained_utility_pipeline.py` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-167 | P0-H dual-head contracts/training/pipeline/bundle | `backend/tests/advisory_model_first/test_dual_head_output_constraint_contracts.py`; `backend/tests/advisory_model_first/test_dual_head_output_constraint_training.py`; `backend/tests/advisory_model_first/test_dual_head_output_constraint_pipeline.py`; `backend/tests/advisory_model_first/test_dual_head_output_constraint_bundle.py` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-168 | P0-I grouped-rank contracts/training/pipeline/bundle | `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_contracts.py`; `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_training.py`; `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_pipeline.py`; `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_bundle.py` | STAGE_A_INCOMPLETE_STOP_VERIFIED | none |
| F-169 | P0-J F2 detailed design and target implementation | `backend/tests/advisory_model_first/test_selection_prior_residual_contracts.py`; `backend/tests/advisory_model_first/test_selection_prior_residual_training.py`; `backend/tests/advisory_model_first/test_selection_prior_residual_pipeline.py`; `backend/tests/advisory_model_first/test_selection_prior_residual_bundle.py`; artifact `eb8ade9b...` | STAGE_A_INCOMPLETE_STOP_VERIFIED | none |
| F-170 | P0-K contracts/training/pipeline/bundle/request+WSL CLI；F2 detailed design | `backend/tests/advisory_model_first/test_selection_liability_gate_contracts.py`; `backend/tests/advisory_model_first/test_selection_liability_gate_training.py`; `backend/tests/advisory_model_first/test_selection_liability_gate_pipeline.py`; `backend/tests/advisory_model_first/test_selection_liability_gate_bundle.py`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_p0k_selection_liability_gate_20260829/selection_liability_gate_bundles/fee9b561a287229d6890d478408cacfdee6ba351cf1152c81369a86cc276bbbc/advancement_receipt.json` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-171 | P0-L F2 design；contracts/training/pipeline/bundle/request+WSL CLI；BUG-1251 date-role fix；formal Stage A | `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_contracts.py`; `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_training.py`; `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_pipeline.py`; `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_bundle.py`; artifact `4476afeb...`记录非零local-rerank降低换手但违反冻结coverage/cash完整性、无winner/PBO/Stage B；PR #3959/#3967/#3969 | STAGE_A_INCOMPLETE_STOP_VERIFIED_NOT_ACTIVATED | none |
| F-172 | §0、§6.1、§9 P0研究族冻结 | `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_bundle.py`; `backend/tests/advisory_model_first/test_research_control_cli.py`; formal route SHA256 `1b8164ce...` | FORMAL_ROUTE_VERIFIED_FAMILY_FROZEN | none |
| F-173 | §5.3决策栈；§9 N2/N3单主线规则 | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n0_research_control_20260830/current_route.md`; `F:/Dev/AIstock_model_artifacts/advisory_n0_research_control_20260830/current_auxiliary_route.md`; `docs/analysis/sector_rotation_factors_develop_spec_20260710.md`; `docs/architecture/advisory_causal_admission_v2_f2_detailed_design_20260906.md` | ONE_SYSTEM_UPSTREAM_MAIN_ONE_ADVISORY_AUX_DOCUMENTED | approved_by_user: QE candidate未形成StrategyPackage；activation remains future scope |
| F-174 | `AdvisoryObjectiveContractV1`；§5.3、§7.2 | target: `backend/tests/advisory_model_first/test_objective_contracts.py`; UI/API objective-label test | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: contract implementation and contract-specific activation/display remain future work |
| F-175 | `AdvisoryResearchTrialRegistryV1`；§4.1.2、§9 N0 | `backend/tests/advisory_model_first/test_research_trial_registry.py`; `backend/tests/advisory_model_first/test_research_control_cli.py`; formal registry SHA256 `8d7ae5cd...` | IMPLEMENTED_FORMAL_VERIFIED | none |
| F-176 | §4.1.2 parent prediction extension；§9 N0 | `backend/tests/advisory_model_first/test_parent_prediction_extension.py`; formal N0 completion `9b460c29...` | IMPLEMENTED_FORMAL_VERIFIED | none |
| F-177 | `AdvisoryLearnabilityAuditV1`；§6.6 | `backend/tests/advisory_model_first/test_oracle_learnability_audit.py`; N1 receipt `1e47d33d...` | IMPLEMENTED_FORMAL_VERIFIED_EXPLORATORY_INCONCLUSIVE | none |
| F-178 | `AdvisoryOracleMiniContractV1`；§6.6 | `backend/tests/advisory_model_first/test_oracle_mini_contract.py`; `backend/tests/advisory_model_first/test_tier1_oracle_pipeline.py`; N1 receipt `28d4330d...` | IMPLEMENTED_FORMAL_CONTROL_READY | none |
| F-179 | `AdvisoryResearchWindowContractV1`；§4.1.2、§6.10 | `backend/tests/advisory_model_first/test_research_window_guard.py`; formal N0 window；N1 manifest `sealed_holdout_accessed=false` | IMPLEMENTED_FORMAL_VERIFIED_SEALED_UNCONSUMED | none |
| F-180 | §6.10 frontier/candidate/confirmation/activation | `backend/tests/advisory_model_first/test_qe_alpha_mve_pipeline.py`; `backend/tests/advisory_model_first/test_qe_alpha_mve_delivery.py` | IMPLEMENTED_FOR_N3_ONE_SELECTION_LOCAL_VERIFIED | approved_by_user: confirmation/activation remain future scope |
| F-181 | registry `decision_use`；§4.1.2、§6.10 | `backend/tests/advisory_model_first/test_research_control_contracts.py`; `backend/tests/advisory_model_first/test_research_trial_registry.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-182 | `AdvisoryActionInterventionSupportV1`；§6.7、§6.8、§6.10 | `backend/tests/advisory_model_first/test_entry_exit_formal_pipeline.py`; `backend/tests/advisory_model_first/test_exit_learnability_pipeline.py` | IMPLEMENTED_FORMAL_N2_SUPPORT_VERIFIED | none |
| F-183 | `AdvisoryIncrementalValueLabelV1`；`incremental_value_labels.py`；§6.7/§6.8 | `backend/tests/advisory_model_first/test_incremental_value_labels.py`; `backend/tests/advisory_model_first/test_entry_exit_formal_delivery.py` | IMPLEMENTED_FORMAL_N2_LABEL_VERIFIED | none |
| F-184 | `AdvisoryEntryGuardDecisionV1`；`entry_guard_decision.py`；§6.7 | `backend/tests/advisory_model_first/test_entry_guard_decision.py`; `backend/tests/advisory_model_first/test_entry_exit_formal_delivery.py` | FORMAL_ENTRY_COMPLETE_NO_CONFIRMATORY_POSITIVE_ARM | none |
| F-185 | `AdvisoryExitDecisionV1`；`exit_label_oracle.py`；fixed learnability；§6.8 | `backend/tests/advisory_model_first/test_exit_label_oracle.py`; `backend/tests/advisory_model_first/test_exit_learnability_delivery.py` | FORMAL_EXIT_ORACLE_HIGH_FIXED_INFORMATION_INCONCLUSIVE_VERIFIED | approved_by_user: no Exit candidate activated |
| F-186 | §6.9、§9 QE preparation与N3 upstream MVE | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_qe_alpha_generator_formal_v5_20260904/qe_alpha_generator_mve_bundles/9327330c11082d656463a85007f03744c47ad52224c764e006235025b5c8fc64/receipt.json`; `docs/analysis/sector_rotation_factors_develop_spec_20260710.md`; `docs/analysis/ma_e19_p0_triad_and_alpha_execution_plan_20260824.md`; `tests/aistock_validation/bugs/20260906_BUG-1381-2026-06-30-direct-v2.json`; `docs/architecture/qe_active_dataset_universe_management_f2_design_20260906.md` | ADVISORY_GENERATOR_SELECTED_ZERO_QE_ROLLING_LSTM_CANDIDATE_PENDING_MULTI_SEED_LOO | approved_by_user: current parent/package/runtime unchanged; factor analytics runtime readback and universe profile activation remain separate |
| F-187 | §6.9 signal admission/combination | `backend/tests/test_multi_alpha_orthogonality.py`; `backend/tests/test_multi_alpha_combine_backtest.py`; target role/seed stability and attribution-overlap tests | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: no P0 signal is admitted automatically; role/seed/economic-attribution evidence waits for N4 |
| F-188 | §4.1.2、§6.10 evidence levels；N2 builder historical-only gate | `backend/tests/advisory_model_first/test_evidence_level_boundaries.py`; `backend/tests/advisory_model_first/test_incremental_value_labels.py`; first natural settlement `advprsett_14c46af1fa2bdb92088b425c` | IMPLEMENTED_CONTRACT_PROSPECTIVE_ACCUMULATING_SEALED_RECEIPT_ABSENT | approved_by_user: sealed holdout不存在且不得回填；单日prospective只积累，只有对应前向证据达到支持门才可支持激活 |
| F-189 | §2、§3、§5.3 no-position boundary；Entry/Exit strict output contracts | `backend/tests/advisory_model_first/test_entry_guard_decision.py`; `backend/tests/advisory_model_first/test_exit_label_oracle.py` | IMPLEMENTED_LOCAL_VERIFIED_NO_POSITION_OUTPUT | approved_by_user: fixed-slot cash only；dynamic position scope仍未授权 |
| F-190 | §6.3.1、P1-A固定基础更新；N4仅在新独立增量证据后考虑完整周期比较 | `backend/tests/advisory_model_first/test_adaptive_price_calibration.py`；artifact `adaptive_price_calibration_runs/advpradapt_8c81ea2bd2f70d75e1683fe9` | BASE_STATIC_VS_ONE_ROLLING_PROTOCOL_COMPLETE_SELECTED_ZERO | approved_by_user: 不预设三个月、不建平台；已消费窗口不扩展为窗口搜索 |
| F-191 | `model_binding_resolution.py`; `model_inference.py`; `feature_schema_v1.py`; `outcome_inference.py`; `price_range_inference.py` | `backend/tests/advisory_model_first/test_dynamic_model_binding.py`; `backend/tests/advisory_model_first/test_model_inference.py`; 2026-08-31 live readback P0-D outcome/price typed unavailable | CURRENT_PACKAGE_CONDITIONED_PARTIAL_ROLE_BOUNDARY_VERIFIED | approved_by_user: current model coverage is one package rerank-only; no cross-package or full-composition claim |
| F-192 | §§5.7、9 P1-B；adapter/shared predictor | target: `backend/tests/advisory_model_first/test_strategy_conditioned_pooling.py` | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: 两个合法可评估流及独立离线baseline即可研究，生产绑定另验 |
| F-193 | §0、§9 H0、§16 active/passive/conditional/zero-work classification | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n0_research_control_20260830/current_route.md`; `F:/Dev/AIstock_model_artifacts/advisory_n0_research_control_20260830/current_auxiliary_route.md`; `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` | DOCUMENTED_HISTORICAL_ROUTE_AND_CURRENT_PLANNING_ROUTE_SEPARATED | approved_by_user: H0 remains dormant unless a reproducible direct blocker exists |
| F-194 | §5.6 role-specific binding stack；current `model_binding_resolution.py`/`model_inference.py` boundary | target: `backend/tests/advisory_model_first/test_entry_price_role_binding.py`; `backend/tests/advisory_model_first/test_model_inference.py` current rerank-only/typed-unavailable behavior | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: ENTRY_PRICE source and historical regression may precede confirmation; active publication requires price-distribution confirmation; no generic registry platform |
| F-195 | §9 N3腿间共识/分歧信息集MVE；F2详细设计 | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_leg_disagreement_formal_v1_20260902/leg_disagreement_bundles/42ac23b6d7cd756a035e0f8325a0f7561c9bc7a207fbaa43fed8fc158348bc81/learnability_receipt.json`；`backend/tests/advisory_model_first/test_leg_disagreement_contracts.py`；`test_leg_disagreement_pipeline.py`；`test_leg_disagreement_delivery.py` | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | approved_by_user: result is navigation-only; no runtime/factor/package write |
| F-196 | §9 N3分钟信息集MVE；`advisory_n3_minute_information_set_mve_f2_detailed_design_20260903.md` v1.2；`minute_information_set_contracts.py`；`minute_information_set_pipeline.py`；`advisory_minute_information_set_mve_run.py` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_minute_information_set_formal_v1_20260903/minute_information_set_bundles/0076a3a6c1e0fa40f6a29a73ab35c4015ae27431fb68989ce13fbb79e56a89f9/learnability_receipt.json`；99 targeted tests；Advisory suite `774 passed/16 skipped`；exact retry | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | approved_by_user: navigation-only frontier已消费关闭；no runtime/factor/package/position write |
| F-197 | §9；`advisory_n3_qe_alpha_generator_mve_f2_detailed_design_20260903.md` v1.6；`qe_alpha_generator_contracts.py`；`qe_alpha_generator_pipeline.py`；`advisory_qe_alpha_generator_mve_run.py` | `backend/tests/advisory_model_first/test_qe_alpha_generator_contracts.py`; `test_qe_alpha_generator_pipeline.py`; `test_qe_alpha_generator_delivery.py`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_qe_alpha_generator_formal_v5_20260904/qe_alpha_generator_mve_bundles/9327330c11082d656463a85007f03744c47ad52224c764e006235025b5c8fc64/receipt.json` | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | none |
| F-198 | §9；`advisory_n3_margin_information_set_mve_f2_detailed_design_20260904.md` v1.7；`margin_information_set_contracts.py`/`margin_information_set_pipeline.py`/thin CLI | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_margin_information_set_formal_v1_20260904/margin_information_set_bundles/b50411d8d68838a3162d5d4e5070259af9a0ba02a515b556c8340ad968537ae4/learnability_receipt.json`; `backend/tests/advisory_model_first/test_margin_information_set_delivery.py` | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO_INSUFFICIENT_SUPPORT | approved_by_user: navigation-only；no runtime/factor/package/position write |
| F-199 | §1.1、§6.11.1；F2 v1.2；`build_package_score_features`；v2 chronological contract | `backend/tests/advisory_model_first/test_package_score_calibration.py`；artifact: formal bundle `f8da2f70...`；v2 target tests | V1_FORMAL_COMPLETE_V2_DESIGN_READY | approved_by_user: raw threshold and CPCV-complement absolute calibration prohibited |
| F-200 | §6.11.2～§6.11.3；F2 v1.2 fixed arm/raw control/HMM | `backend/tests/advisory_model_first/test_score_hmm_admission_pipeline.py`；`backend/tests/advisory_model_first/test_score_hmm_context.py`；artifact: formal bundle `f8da2f70...` | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | approved_by_user: v1 frontier frozen; future sector uses v2 lineage |
| F-201 | §4.5；F2 v1.2 causal/PIT/double-count/missing contracts；G2-A source/executor | `backend/tests/advisory_model_first/test_score_hmm_context.py`；`backend/tests/hmm_risk/test_rotation_l1_input_bundle.py`；`backend/tests/hmm_risk/test_rotation_l1_gbdt.py`；PR #4343/#4353 | MARKET_SOURCE_READY_G2A_INPUT_AND_EXECUTOR_MERGED_FORMAL_OOF_UNAVAILABLE | approved_by_user: formal sector OOF不存在时不得伪造或运行R2 sector arm；R1不依赖sector |
| F-202 | §5.6、§6.11.3、§7.3；`AdvisoryAdmissionDecisionV1`/pipeline | `backend/tests/advisory_model_first/test_admission_decision.py`覆盖zero-to-five、NO_ELIGIBLE_RECOMMENDATION、label-independent action、no-backfill/no-position；formal v1 decisions | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | approved_by_user: dynamic position remains out of scope |
| F-203 | §5.3、§6.11；F2 v1.2 objective-specific experiment/route | `backend/tests/advisory_model_first/test_score_hmm_objective_isolation.py`覆盖trial隔离、nested predecessor与一次选点；formal v1 selected=0 | IMPLEMENTED_FORMAL_VERIFIED_FRONTIER_FROZEN | approved_by_user: no combined total score or cross-contract activation |
| F-204 | §9 N3-AUX/N4；`advisory_score_hmm_conditioned_admission_f2_detailed_design_20260904.md` v1.2；score/HMM contracts/pipeline/CLI | F2 validate；六组direct tests；artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_score_hmm_admission_20260905/score_hmm_admission_bundles/f8da2f70eb51b151b303ee5d19f12d9a651ba4b386291626f3dd33689a78f471/frontier_receipt.json` | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | approved_by_user: v1 frozen; no runtime activation/restart/DDL |
| F-205 | §9 N3财务事件source readiness；`advisory_n3_financial_event_source_readiness_f2_detailed_design_20260904.md` v1.2；`financial_event_source_readiness.py`/thin CLI | `backend/tests/advisory_model_first/test_financial_event_source_readiness.py`; `backend/tests/advisory_model_first/test_financial_event_source_delivery.py`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_financial_event_source_readiness_formal_v1_20260905/financial_event_source_bundles/211b8db192c83b79f7731649e84a2f929c1d56579e337c438d84e90aa3fb7ead/source_readiness_receipt.json` | FORMAL_SOURCE_READY_DELIVERED | approved_by_user: no model/runtime/DDL/DML；next task only MVE design |
| F-230 | §9 N3 event MVE；`advisory_n3_financial_event_information_set_mve_f2_detailed_design_20260905.md` v1.0；`financial_event_information_set_contracts.py`/pipeline/thin CLI | `backend/tests/advisory_model_first/test_financial_event_information_set_contracts.py`; `test_financial_event_information_set_pipeline.py`; `test_financial_event_information_set_delivery.py`; artifact: formal bundle `ad234f4c...` | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | approved_by_user: no runtime/restart/DDL/DML/network/Tushare |
| F-231 | v2 F2 §§1～2；历史 v1 bundle | artifact: v1 f8da2f70... frontier；§1.2 exact hashes；blueprint §1.3 | DESIGN_VERIFIED | none |
| F-232 | v2 F2 §§5.1～5.2、5.7.1；causal Admission contracts/pipeline | `backend/tests/advisory_model_first/test_causal_admission_v2_pipeline.py`；artifact: bundle `7e739be5.../fit_receipts.json` | EXPERIMENT_VERIFIED | none |
| F-233 | v2 F2 §§2.2、§5.3 | PR #4343/#4353；G2-A v1.2 17/39 structural stop；target: `backend/tests/advisory_model_first/test_causal_admission_v2_contracts.py` stage-source tests | DESIGN_READY | approved_by_user: sector OOF 尚未通过验收，R1 不以此为前置 |
| F-234 | v2 F2 §§5.4、5.7.2 | `backend/tests/advisory_model_first/test_causal_admission_v2_pipeline.py`；artifact: bundle `7e739be5.../frontier_receipt.json` | EXPERIMENT_VERIFIED | approved_by_user: 正式2/2/0；当前frontier不新增trial或回选 |
| F-235 | v2 F2 §§5.5～5.7 | artifact: bundle `7e739be5.../frontier_summary.json`，含lift、Brier、coverage、support与Holm | EXPERIMENT_VERIFIED | approved_by_user: 两arm未过支持与5 bps经济门；outer未读 |
| F-236 | v2 F2 §§5.6 | `python -m pytest backend/tests/advisory_model_first/test_causal_admission_v2_pipeline.py -q` | SOURCE_VERIFIED | none |
| F-237 | v2 F2 §§5.7.3～5.7.5、6 | artifact: bundle `7e739be5.../frontier_summary.json`；MDE `44.2485434246 bps`与9/11干预日分报 | EXPERIMENT_VERIFIED | approved_by_user: 结果只作导航，不冒充确认性证据 |
| F-238 | v2 F2 §§5.1、6.3、7 | `backend/tests/advisory_model_first/test_causal_admission_v2_delivery.py`；artifact: bundle `7e739be5.../registry_records.json`；registry append1、exact retry noop1 | EXPERIMENT_VERIFIED | approved_by_user: 一条聚合记录表示两trial；无部分写入 |
| F-239 | v2 F2 §§5.2～5.3、6.3、9 | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_causal_admission_v21_exact_retry_20260907/request.json`；`sealed_holdout_accessed=false`；outer未读 | EXPERIMENT_VERIFIED | none |
| F-240 | v2 F2 §§3、7～12 | PR #4395/#4402/#4404；`python -m nox -s advisory_modeling_backend`为938 passed、16 skipped；F2 validator 10/10 | EXPERIMENT_VERIFIED | approved_by_user: backend/DB/API/UI/runtime/DDL/restart均未变更 |
| F-241 | §§5.7、9 P1-B | target: shared-feature adapter / duplicate-stock-date / leave-one-package-out tests (`backend/tests/advisory_model_first/test_strategy_conditioned_pooling.py`, target) | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: 本次整合架构方向；代码、参数化和经济验证按§16后续交付 |
| F-242 | §5.8、§16；`advisory_qe_matched_canary_f2_detailed_design_20260909.md`；历史实现只保留兼容读取，Advisory 当前不提交 QE 实验 | `backend/tests/advisory_model_first/test_qe_advisory_matched_canary.py` | IMPLEMENTED_VERIFIED | none |
| F-243 | §§5.8、6.11～6.12 | target: raw-context control / duplicate exposure / joint distribution tests (`backend/tests/advisory_model_first/test_causal_admission_v2_pipeline.py`, target) | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: 本次整合架构方向；代码、参数化和经济验证按§16后续交付 |
| F-244 | §§6.6、9 N3、16 | artifact: N1/N2 original receipts；target: multi-metric route contract tests | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: 本次整合架构方向；代码、参数化和经济验证按§16后续交付 |
| F-245 | §6.9、§16；QE交付物到Advisory消费验证；QE Top50 与 Advisory Top5 口径隔离，Advisory 不补跑 seed | `backend/tests/watchlist/test_advisory_delivery_preflight.py`；`backend/tests/advisory_historical_range/test_comparison.py` | IMPLEMENTED_VERIFIED | none |
| F-246 | §§1.2、5.8、16 R-PUBLISH | evidence: forward/status and two forward-runs 2026-09-07；target: missing/source/publication tests (`backend/tests/advisory_model_first/test_forward_publication.py`, target) | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: 本次整合架构方向；代码、参数化和经济验证按§16后续交付 |
| F-247 | §§6.3、6.7～6.8、6.12 | target: price support / executable-clock / T+1 / censor / touch-order tests (`backend/tests/advisory_model_first/test_entry_guard_decision.py`, target) | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: 本次整合架构方向；代码、参数化和经济验证按§16后续交付 |
| F-248 | §§6.12、16；v2 F2 §6.3 | target: route-budget / evidence-level / sealed-overlap / revision tests (`backend/tests/advisory_model_first/test_research_window_guard.py`, target) | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: 本次整合架构方向；代码、参数化和经济验证按§16后续交付 |
| F-249 | 页首当前主动路线、§16；`advisory_core_index_universe_selection_f2_detailed_design_20260912.md` §2；本切片零 QE 任务提交、训练和回测 | `backend/tests/watchlist/test_advisory_delivery_preflight.py`；`backend/tests/advisory_historical_range/test_comparison.py` | IMPLEMENTED_VERIFIED | none |
| F-250 | `backend/services/advisory_universe.py`、`advisory_program.py`、`backend/routers/advisory.py`、`frontend/src/lib/api/advisory.ts`；P0 pool 目录复用共享权威 | `backend/tests/watchlist/test_advisory_universe_selection.py`；`backend/tests/watchlist/test_advisory_api.py` | IMPLEMENTED_VERIFIED | none |
| F-251 | `CoreIndexAdvisoryUniverseResolver`、`AdvisoryLiveRollingPitRepository`、`AdvisoryProgramService._universe_as_of_trade_date`、`_filter_candidates_for_universe`；Selection 生成候选，Advisory 只约束最终可推荐集合 | `backend/tests/watchlist/test_advisory_universe_selection.py`；2026-09-11 DEV真实resolver读回 | IMPLEMENTED_VERIFIED | none |
| F-252 | `run_review`、`run_replay`、`advisory_forward/service.py`、`advisory_model_first/model_inference.py`、binding version guard；旧 binding 缺字段时规范化为 `stock_universe` | `backend/tests/watchlist/test_advisory_universe_selection.py`；`backend/tests/advisory_model_first/test_forward_boundaries.py` | IMPLEMENTED_VERIFIED | none |
| F-253 | 本切片 git changed-file 边界；F2详细设计§§3、8～10；合入、运行时激活继续分开报告 | `backend/tests/advisory_historical_range/test_comparison_api.py`；`frontend/tests/paper-v2/paper-v2-advisory-historical-range.spec.ts` | IMPLEMENTED_VERIFIED | none |
| F-254 | `advisory_program.py::_with_advisory_date_context`；`selection_center/result_enrichment.py`；日频DB-only F1详细设计；仅 Advisory 正式 D/T context 启用 | `backend/tests/selection_center/test_price_guidance.py`；`backend/tests/advisory_model_first/test_forward_date_clock.py` | IMPLEMENTED_VERIFIED | none |
| F-255 | `advisory_qe_delivery_consume_preflight_f2_detailed_design_20260913.md`；`backend/services/advisory_delivery_preflight.py`、Advisory API/UI；PR #4629 / merge `56273a91` | `backend/tests/watchlist/test_advisory_delivery_preflight.py`；`backend/tests/watchlist/test_advisory_delivery_preflight_api.py`；`frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts` | IMPLEMENTED_VERIFIED | none |
| F-256 | §5.3.1、§6.3、§9 M4；`advisory_daily_price_envelope_v1_f2_design_20260914.md` v1.2；daily envelope与price range源码 | `backend/tests/advisory_model_first/test_daily_price_envelope_contract.py`；正式v3 `30e8a75b...`与v4 `508fedfe...` | IMPLEMENTED_ARTIFACT_VERIFIED | approved_by_user: Advisory只做日级价格区间，不研发分钟执行 |
| F-257 | §3、§5.2、§5.3.1；外部模块只读消费边界 | `backend/tests/advisory_model_first/test_daily_price_envelope_boundaries.py`验证Advisory不import/write/编排QE、Paper或Execution | IMPLEMENTED_VERIFIED | approved_by_user: 外部消费另立任务，不由Advisory实现 |
| F-258 | §4.1、§6.3～6.3.1、§12.1～12.3 | `backend/tests/advisory_model_first/test_daily_price_envelope_pit.py`覆盖分钟字段拒绝、目标日未来行情毒化和current P0-D typed unavailable回归 | IMPLEMENTED_VERIFIED | approved_by_user: 历史N3分钟MVE只保留事实，不重开lineage |
| F-259 | `advisory_price_prospective_prediction_v1_f2_design_20260915.md` v1.3；`prospective_price_contracts.py`/prediction/CLI及显式冻结bundle loader；PR #4732 / merge `3be76e742...` | `backend/tests/advisory_model_first/test_price_range_prospective.py`；prediction receipt `price_range_prospective_predictions/advprpros_405d704a7dbe866eb0b6ae0e/receipt.json`；20/20且exact retry一致；settlement `advprsett_14c46af1fa2bdb92088b425c` | IMPLEMENTED_FIRST_NATURAL_SETTLEMENT_ACCUMULATING | approved_by_user: 不回填、零binding/DB/holdout；单日不得支持激活 |
| F-260 | `advisory_price_prospective_evaluation_v1_f2_design_20260915.md` v1.3；evaluation contracts/source/confirmation/CLI | `backend/tests/advisory_model_first/test_price_range_prospective_evaluation.py`；settlement `advprsett_14c46af1fa2bdb92088b425c` 20/20可用，business coverage `0.70`，exact retry一致 | IMPLEMENTED_FIRST_SETTLEMENT_VERIFIED_ACCUMULATING | approved_by_user: 零QE/DB写入/binding/restart；20日/300行前只积累 |
| F-261 | §1.4、§6.3.3、§16.5 G0～G3 | artifact: §16.5.1统一只读prepare、§16.5.2单一类别块真实研究 | IMPLEMENTED_NAVIGATION_NEGATIVE | approved_by_user: 类别candidate停止；行业行情特征和生产模型未完成 |
| F-262 | §6.3.2～6.3.3；context training/inference/evaluation | `backend/tests/advisory_model_first/test_economic_context_value_evaluation_v1.py`；§16.5.2真实stage链 | SOURCE_VERIFIED_SHADOW_ONLY | approved_by_user: 条件查询和完整开发回放非任意限价成交证明，原生资格/确认未完成 |
| F-263 | §6.3.3、§12.5、§16.5 G1～G3；context training/evaluation | `backend/tests/advisory_model_first/test_economic_context_value_training_v1.py`；§16.5.2共同监督及matched增量 | IMPLEMENTED_NAVIGATION_NEGATIVE | approved_by_user: 新信息净增量未过，不换winner或启动确认 |
| F-264 | §5.3、§6.3.3、§12.5；context contracts/evaluation | `backend/tests/advisory_model_first/test_economic_context_value_evaluation_v1.py`；§16.5.2冻结数值/四臂归因 | SOURCE_VERIFIED_NAVIGATION_STOPPED | approved_by_user: 风险改善不替代净增量/真实TAKE门，旧结果不改判 |
| F-265 | §6.3.3、§16.1、§16.5 | artifact: 本文先研究后生产接入顺序 | DESIGN_VERIFIED | approved_by_user: 经济有效性及激活未完成，不建设新family生产接入 |
| F-266 | §4.1、§16.4～16.5；context pipeline | `backend/tests/advisory_model_first/test_economic_context_value_pipeline_v1.py`；§16.5.2四fit记账/QE公开空闲核对 | SOURCE_VERIFIED_OFFLINE_SCOPE | approved_by_user: fit=4，服务/数据库/QE/其它模块修改0；48h不凑工时或扩搜索 |

## 12. Verification Plan

### 12.1 训练验证

- QE H5/Parquet/Qlib Bin基础数据和目标父包Prediction Store PKL真实可读。
- feature/label schema和日期范围读回一致。
- 精确roster中的每条腿和seed均能解析，腿间对齐不跨Program或父包。
- 日线Bin的`limit_up/down`、`up/down_limit_price`和`prev_close`可读。
- HMM由本轮文件数据重新拟合，历史HMM产物未进入训练输入。
- 既有WSL/Conda/LightGBM训练身份和日志可见；新主线若采用其它预注册模型族，同样保存可复现环境、参数、资源和真实训练日志。
- policy episode label 与实际 review transition 对固定样本逐事件一致，policy hash、价格基础、成本和退出原因完整。
- purged rolling/CPCV 的 train/validation 标签窗口无交叉；已消费80日test不进入任何选择输入。
- 模型文件可重新加载并对相同输入产生确定性预测。
- 历史P0-D meta-label的take/skip/confidence非空；新主线按role验证Ranking、日级Entry或日级Exit输出，逐日group、Program/package、objective contract和policy边界正确。M4价格区间单独验证`AdvisoryDailyPriceEnvelopeV1`且拒绝分钟字段。
- 全部模型trial/family的validation结果可读，PBO/DSR或适用等价诊断为数值或明确`NOT_COMPUTABLE`；oracle与learnability按study type独立登记，不机械混入模型trial计数。
- 峰值内存、各阶段耗时和临时文件规模可见；后续Advisory价格区间训练不读取分钟Bin。

### 12.2 推理验证

- 数据库decision cutoff日频输入可以生成与训练相同schema；M4正式推理不读取实时分钟行情。
- 两个 ENABLED Program 各自产生同日 baseline `PUBLISHED` list；一个失败不阻断另一个。
- D收盘发布的decision/target交易日正确，响应和持久化均不含target日行情；target open到达前不创建带伪价格的episode。
- 目标多 Alpha 解析 exact bundle 并持久化 challenger；单 Alpha 无 bundle 时 baseline 成功、模型 typed unavailable。
- 当前P0-D descriptor必须读回`meta_label_take_skip_confidence`、20个候选和Top5 shortlist，同时M3 outcome/M4 price-range保持各自typed unavailable；Selection `rule_default`价格指导不得计入模型角色覆盖。
- 新package不得复用目标多Alpha descriptor；package/manifest/style/component-role任一不匹配必须fail closed，未来package adapter/shared bundle需单独测试matched与leave-one-package-out边界。
- 未来role-stack测试证明旋转Ranking不会改变兼容Outcome/Entry/Exit绑定；任一角色identity不兼容时只关闭该角色并返回typed reason，不能删除其它角色或用规则结果冒充。
- 定时运行、手动同日重试和进程重启后的同日重试保持幂等，不产生重复 list/observation/episode transition。
- 模型不可用、字段缺失和版本冲突均有typed reason和有效后台日志。
- 不写Selection、Paper、模拟盘、QMT或QE实验文件。
- 价格响应不包含最佳分钟、逐分钟动作、订单/拆单、fill、Paper状态或Execution plan；外部消费失败不能回写或改判已发布区间。
- forward observation 到期后按同一 policy 形成 outcome/episode label；未成熟项保持 censored/pending，不补零。

### 12.3 页面验证

- 页面展示真实API结果，不使用fixture或静态mock。
- 明确区分规则、实验模型和后续已验证模型。
- Top5、收益/周期、价格区间按已实现能力逐项出现，不等待全部模型完成。
- 价格区间标注日级目标日期、价格基准、校准/可用性和模型身份，不使用“最佳买点/卖点”“保证成交”等执行性文案。
- baseline 与 challenger、REPLAY 与 PUBLISHED、latest success 与 latest failure 分层展示，零 episode 不显示伪造胜率。
- 桌面和移动viewport无重叠、无静默网络错误。

### 12.4 H0 单日/批量等价与性能验证

- 使用已冻结v6的package/manifest/code-release/source catalog/date plan、逐日raw/candidate/list/outcome语义和可用阶段耗时，作为不可变golden evidence。
- 单交易日分别经现有 single-day 路径与新 batch size=1 路径执行；候选、分数、rank、stage trace、source refs、名单动作和业务语义 hash 必须一致。
- 代表日覆盖5月/6月/7月、ST、停牌、行业映射缺失、HMM排除、无候选和模型特征缺失；再对完整44日窗口比较逐日语义。
- 在 D+1 及以后数据中注入价格、行业、ST、HMM或修订变化，D 日结果必须不变；绕过 `AsOfDataView` 的直接 batch frame 访问测试必须失败。
- 对 workspace cache key、source catalog、runtime semantics 或 raw-affecting config 任一字段做单变量变更，错误复用必须被拒绝。
- 在 chunk 内第3日注入可重试和不可重试失败，验证前两日已提交、顺序 list transition 不跨越失败日、修复后从同一 identity exact resume。
- A/B raw identity相同时只产生一个 raw artifact；改变任一 raw-affecting字段后产生不同identity且不得共享。overlay 输出保持两臂独立。
- 记录单日、batch size=1、batch size=5 的 workspace/source/raw/overlay/publish耗时、实际读写字节、峰值RSS、GPU利用和cache hit；不以增加并发作为首轮优化。

### 12.5 Oracle、learnability与新主线验证

- 下一价格研究先核新增信息、D/T可见性与既存信息差异；未知来源不得以当前行业成员/HMM状态回填历史。纯来源检查不读新holdout收益。
- matched模型与candidate的split/eligible/成本/标签/参数相同，只改变预登记信息块；研究输入不得因盈利与否筛选。动作状态和后续policy相同不等于强行复用两臂交易轨迹。
- 简单风险控制在评价前冻结，只用当时信息形成固定槽位动作；同时报告暴露差异及归因限制。收益主终点、风险容忍、干预支持和选择次数事前固定，不把研究风险阈值当用户资金预算。
- 小样本预测诊断通过不直接交付生产family；完整组合仍需检查收益幅度、错失机会、尾损和UNKNOWN贡献，失败停止精确candidate，不扩窗/补证抢救。
- registry相同`experiment_id`的经济字段不可覆盖；study type、objective contract、decision use和消费窗口缺失时拒绝登记。
- 单页路线只能从registry生成；手工修改路线不得改变研究状态。
- 父包预测spike对真实目标identity输出三态之一，并证明冻结模型、输入和新prediction artifact边界；需要重训时必须产生新lineage。
- Tier 1赢家召回使用decision-date PIT可交易股票池，上市年限、ST、停牌、涨跌停、成本、benchmark和winner标签逐字段固定。
- clairvoyant结果带`FUTURE_INFORMATION_CEILING/NOT_DEPLOYABLE`；任何将其写入模型特征、bundle或runtime的路径测试失败。
- 历史固定Ridge结果不变；新learnability最多预注册线性与一个有明确假设的受限非线性对照，所有变体计trial。单一族失败不证明全局不可学；不得迭代loss/网格回选。
- oracle和learnability命令对sealed holdout日期/identity fail closed；confirmation读取后写消费receipt，第二次选择或同frontier回选失败。
- frontier保留完整Pareto点；candidate选择只读取inner-train。经济失败后只允许原candidate的非经济exact retry，不允许换点。
- Entry Guard的T日决定在T+1未来收盘/high/low毒化下不变；`SKIP`形成固定槽位现金但无资金权重或第6名静默补位。
- Exit标签两臂使用同一policy simulator和policy hash；停牌、跌停、WAITING和删失均有typed结果，liability指标不作为Exit PASS条件。
- intervention-support receipt包含动作数、覆盖交易日、block/cluster有效样本和regime分布；恒等策略或不足功效只标记探索/证据不足。
- 组合验证分别报告逐种子、种子平均、独立时段残差、LOO、成本和regime；不同role/clock无法通过同一分数组合接口。
- 历史回放、sealed holdout和prospective OOS使用不同evidence level；`NAVIGATION_ONLY`与错误objective contract不得进入activation引用。
- score calibration测试证明同日rank保持、跨日raw score仿射变化不会绕过校准identity，所有scaler/calibrator/threshold只在train fold拟合；future-label与future-market poison不改变更早OOF输出。
- 固定arm消融必须分别产生score-only、raw-market、market-HMM、sector-HMM和combined结果；market-HMM增量只相对相同raw-market control判断，任一缺臂不得把组合效果归因给HMM。
- HMM测试拒绝smoothed/Viterbi全序列状态、未来posterior、非PIT行业映射、latest snapshot扫描和父score重复暴露；正常停牌/映射/HMM缺失保留候选并输出typed unavailable。
- Admission测试覆盖0至5只通过、`NO_ELIGIBLE_RECOMMENDATION`、不向后补位、无动态资金权重、package/manifest/style/policy错配fail closed及ALPHA/RISK合同隔离。
- v1 sector两臂NOT_RUN是历史事实；v2.1 R1不依赖sector，R2 source缺失时零trial停止。测试区分input/executor、17/39结构停止、完整accepted OOF与runtime capability；G2-A结构失败不等于Advisory经济失败。

### 12.6 DESIGN-COMPLIANCE-001

每次合入前逐项证明：

1. 没有用简化、mock、规则或静态结果冒充真实模型。
2. 没有静默错误或无日志fallback。
3. 没有改变Selection、Paper、模拟盘、策略包或荐股基线语义。
4. 没有新增未经用户确认的门禁、审批、角色或无界历史工程；trial registry、路线、oracle和holdout保持最小任务内控制，H0保持条件性休眠且不占用默认主线资源。

v3.46历史审核记录（2026-09-06；事实快照已被本版更新，不代表当前状态）：

1. **事实轮**：逐项回读PR #4343/#4347/#4352/#4353/#4357及PR #4346/#4348/#4351，核对正式main/aux route、G2-A真实input manifest、QE MA-E19/D2/D3数值、BUG-1381 source/runtime边界、QE股票池design-only边界和Position Timing结果；把input、executor、formal OOF、tail、product/runtime分层，未把代码或设计合入写成实验完成。
2. **一致性轮**：统一页首状态、进度表、实验总表、N3/N3-AUX、rollout、验收矩阵和§16优先级；删除“#4343尚未合入”“最新main无执行器”等过期语义，并区分不可变历史route与当前规划route。
3. **方向轮**：确认工作分配为一条QE上游Alpha主线和一条Advisory因果Admission辅助线；Position Timing与自然future OOS只被动积累，H0及历史固化/归档继续零主动工时。
4. **合规轮**：`python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md --tier F2`通过`116/116`且`warnings=0`；`git diff --check`通过。四项DESIGN-COMPLIANCE-001均满足：无简化交付、无静默成功、无业务语义漂移、无新增生产审批或门禁。

v3.47审核范围（2026-09-07）：

- 事实轮：核对当前发布输入阻塞、G2-A部分fit结构停止、PR #4361源码/未激活边界；历史M0～N3数值保留。
- 方法轮：纠正分位数/均值LCB、因果更新、单族结论、召回生死线、frontier与业务口径；新revision不改旧结果。
- 一致性轮：页首、架构、N3/N4/P1、§16和F-231～F-248同步；从属v2 F2 v1.1不再保留旧sector全阻塞或q20准入规格。
- 历史保全复核：与未合入v3.46对照，§1.3的24条M/P/N实验行逐行一致，全部原64位身份仍保留；G2-A相邻状态单独更新为17/39结构停止。F2两文分别124/124与10/10，warnings=0（最终提交前再次运行）。
- 交付边界：本次只更新两份文档；R0参数化、源码、经济实验和生产操作均未完成。最终机器验证与CI以本PR收据为准，不用文档validator冒充业务效果。

v3.48 R0 参数化审核范围（2026-09-07）：

- 事实轮：直接回读 v1 immutable request/bundle、N1 八组日历和 TAKE-all `policy_daily.parquet`；固定48日inner、240日outer、两Ridge trial及 `44.2485434246 bps` 预运行MDE，不从待评价结果取数。
- 方法轮：取消 residual q20 与第二个 probability 硬阈值；以 `expected_net_return_bps-5 bps` 定义动作效用，概率只作校准readout，并冻结10%相对MDD/CVaR风险容忍、干预支持和Holm两臂推断。
- 一致性轮（当时状态）：详细设计页首、阶段表、§5.2/5.4/5.7、验收矩阵与本蓝图N3-AUX、rollout、production gate当时同步为`R0_PARAMETERIZED_R1_NOT_IMPLEMENTED`；该里程碑随后已由正式R1结果替代，历史v1 selected=0与窗口消费始终未改写。
- 交付边界（当时状态）：v3.48的R0只关闭实现前规格缺口；其后R1源码与正式实验已完成，但runtime/DDL/restart仍未发生。

v3.88三轮自审及修订（2026-10-03；同一窗口不同视角，不冒称独立外审）：

1. 事实与一致性：依据#5348合入读回及本版既有实验记录，纠正Admission v2仍设计态、经济消费者未实现和旧R-PUBLISH仍为当前阻塞的陈旧表述；§1.3完整结果与§9历史实施段逐字保持一致，不重跑旧实验。
2. 方法与业务：明确新增信息而非重命名模型；补足D/T与未知隔夜信息边界、两臂后续轨迹不能共用、简单风险控制不能用test反向配比，以及新风险合同不得改判旧失败。有限样本只支持探索，成本后主目标保留。
3. 范围与交付：本次仅一份蓝图，G0～G4仍PLANNED；新增F-261～F-266仅设计/计划验收，未来源码/经济/原生资格保持未完成。首次结构检查发现新增矩阵行被空行分隔、未显式标注本轮仅文档范围，已修订；F2最终142/142、warnings=0，diff检查通过。代码、数据库、实验、QE模块和用户服务均未操作。

DESIGN-COMPLIANCE-001四项复核：只报告完整蓝图修订，不冒称新模型完成；UNKNOWN及来源不可识别明确；保留原基线/旧结果/模块边界；新增内容为用户已接受的研究对照及计划，不增加审批平台或历史证据工程。CI和合入状态以本次PR为准。

v3.89 G0事实更新三轮自审（同一窗口不同视角，不冒称独立外审）：

1. 事实轮：分开旧full候选和活动profile实际绑定的full-v3；最终只以profile pins读回与原7720键的严格resolver计数报告现状，未读目标/收益。同期main同步到其他窗口已合入的源码不计作本任务修改。
2. 方法轮：纠正“日期覆盖等于历史知晓”和“所有股票必须是发布行业指数成分”的潜在误读；不因14未覆盖或未达100%强加停止门槛，也不擅自断言14条缺失的业务成因。D上下文尚未证明与旧负实验失败分开，部分分类可用与整个方向不可学分开。
3. 边界与一致性轮：页首、§1.4、§6.3.3、阶段表、F-261～F-266及§16队列同步为G0检查完成/G1尚未放行；历史§1.3/§9保持不变。仅本蓝图的事实更新可交付，未交付模型详细设计、源码、训练、确认或运行激活；不改变已批准研究门禁。

## 13. Rollout / Rollback

### 13.1 Rollout

以下1～19保留历史交付顺序，不是当前待办；20～24为更新后的后续状态，执行优先级以§16为准：

1. `COMPLETED`：P0-A/P0-B 详细设计、源码、真实发布和动态 bundle 解析已完成。
2. `COMPLETED`：用户重启后的两个 ENABLED Program 真实发布、target-open settlement和episode readback已完成；后续由日调度自然积累。
3. `COMPLETED`：P0-C file-based policy episode标签与purged rolling/CPCV评价已合入。
4. `COMPLETED_DESCRIPTOR_ACTIVE`：P0-D meta-label bundle已真实训练并作为独立experimental challenger接入；不自动替换baseline。
5. `IMPLEMENTED_NATURAL_OOS_IMMATURE_RUNTIME_VERIFIED`：成熟结算/指标链已有源码和运行验证；2026-09-14重启后两Program最新复评09-11均为`SUCCEEDED`，当日零点target-open数据尚未到期。自然成熟继续由既有调度积累；不能把未到期`WAITING_DATA`、历史回放或历史发布冒充成熟future OOS。
6. `COMPLETED_GOLDEN_FROZEN`：v6 A/B/C、outcome、统计和报告已完成；44日逐日结果是H0不可变oracle，不回写或重算。
7. `CONDITIONAL_DORMANT_H0`：H0不进入默认任务队列。只有已复现的实盘/历史语义错误或资源瓶颈直接阻断当前模型验证时，才在独立revision实施最小修复；没有直接阻塞证据时保持休眠，不运行完整44日或性能工程。
8. `COMPLETED_VALID_NEGATIVE`：P0-K源码、正式168/168和exact retry已完成；liability预测存在但绝对阈值策略恒等，禁止Stage B和结果后阈值扩展。
9. `COMPLETED_VALID_NEGATIVE`：P0-L设计、源码、BUG-1251修复、正式Stage A和exact retry均已完成；冻结非零局部重排档全部因coverage/cash完整性不可行而在`0/168`停止，禁止StageB和结果后扩展gain/位移/每日swap。
10. `FAMILY_FROZEN`：P0-D至P0-L没有可激活winner，正式冻结为一个研究族；不创建P0-M或继续同数据、同候选、同特征和同模型族的局部派生。
11. `COMPLETED_N0`：正式registry/路线、父包prediction spike、window contract和sealed holdout访问边界已闭合；不产生模型收益证据。
12. `COMPLETED_N1_INCONCLUSIVE`：开发窗口Tier 1 oracle与固定cross-fitted learnability已完成；oracle为高上限，learnability欠功效且综合`direction_ready=false`，不激活、不提前选主线。
13. `COMPLETED_N2_DIAGNOSTICS`：N2-A、N2-B v2、Entry/Exit action oracle和Exit fixed-information learnability均已完成；没有旧包、Entry或Exit候选可抢占主线，所有结果保持navigation-only且sealed=false。
14. `COMPLETED_N3_FIRST_ALPHA_MVE_SELECTED_ZERO`：Top50赢家召回上界远低于20%结构门槛，固定24-proposal上游Alpha探索已从clean merge SHA完成，selected=0；不回选首批frontier。
15. `COMPLETED_N3_PARENT_OVERLAY_SELECTED_ZERO`：固定6信号×4小权重overlay正式完成`24/24/24/0`；干预支持充分但所有Top5 family-wise lift下界不大于0，exact retry稳定，关闭同窗overlay权重路线。
16. `COMPLETED_N3_LEG_DISAGREEMENT_SELECTED_ZERO`：固定两trial腿间learnability MVE已完成，support充分但四项增量门槛失败；lineage关闭并转分钟信息集，不回选三腿feature/alpha/fold。
17. `COMPLETED_N3_MARGIN_SELECTED_ZERO`：融资融券源码与正式MVE已完成，bundle `b50411d8...`为`EXPLORATORY_INSUFFICIENT_SUPPORT`且selected=0；不回选margin arm，主线按receipt转财务事件source-readiness。
18. `COMPLETED_N3_EVENT_SELECTED_ZERO`：财务事件bundle `ad234f4c...`已完成`3/3/0`且关闭，不进入vintage source、confirmation或activation。
19. `COMPLETED_SCORE_HMM_V1_SELECTED_ZERO_FROZEN`：同包评分/市场/HMM v1正式bundle `f8da2f70...`已完成三个可执行arm；selected=0。243点失败分解没有可靠阈值增量并识别CPCV补集绝对校准限制，旧frontier不得放宽、反向或回选。
20. `G2A_SOURCE_MERGED_V1_2_PARTIAL_STRUCTURAL_STOP`：input/executor已合入；v1.2实际17/39 fits，battery选10D后process1结构验收停止；完整accepted OOF未形成，v1.3 PR #4359仍开放。tail和产品/runtime未实施。
21. `ACTIVE_QE_UPSTREAM_ALPHA_CANDIDATE_VALIDATION`：rolling LSTM seed123四vintage仅候选；追加seed、LOO、历史全量复验和组合Alpha审计由QE统一完成，Advisory不并行运行同类实验；不从QE CAGR直接推断荐股有效，正式包交付后再做消费验证。
22. `COMPLETED_AUX_V2_1_R1_SELECTED_ZERO_FRONTIER_CLOSED`：R0冻结的48日inner、静态/20日expanding Ridge两trial已完成正式R1；两arm点估计分别为`+2.4079645/+3.8107802 bps`，但支持度、5 bps经济门与校准均失败，`selected=0`且240日outer未读。当前score/raw + Ridge frontier关闭；不自动启动R2/HMM、不回选v1或放宽门槛，只有新信息或上游候选形成新假设后才重开辅助线。
23. `COMPLETED_PRICE_ADAPTIVE_CQR_SELECTED_ZERO`：固定static v4与rolling-20D matured CQR已在已消费回放完成正式导航审计；点估计改善但交易日cluster-bootstrap下界未过0，selected=0，lineage关闭且不绑定。
24. `PASSIVE_POSITION_TIMING_EVIDENCE_MATURATION`：L2无selected model、L4b-1无selected side；规则能力不受阻断，仅等待自然action cards/结果增加，不创建新的主动调参或补样本项目。
25. `CONDITIONAL_AFTER_CONFIRMATION`：有增量后才做组合、role binding和更完整重训周期；基础因果更新与跨包离线研究按§16前置，不再等待自然标签或两个生产bundle才开发。动态资金仓位仍未授权。

源码合入、WSL训练、模型文件生成、后端重启、模型加载和页面可见是独立状态，不得合并声明完成。

### 13.2 Rollback

- 关闭目标 Program 的 challenger 配置后保留现有 `selection_effective_rank`、baseline publish 和 `rule_default`。
- 模型加载或推理失败时只关闭模型通道，不停止现有荐股。
- 执行器失败不删除已发布事实；修复后按同一 Program/date 幂等重试，不回填未授权历史日期。
- H0 回滚只切回既有逐日 historical executor；已完成的 batch day artifact 和 checkpoint 保持审计可读，不删除、不覆盖，也不影响 LiveDailyExecutor。
- 不修改或回滚Selection、Paper、模拟盘、StrategyPackage或QE资产。
- 不删除训练文件、模型文件或已产生的预测；只停止继续使用有问题的模型版本。
- P0-A 会产生正常 Advisory 业务写入；这不是一次性修复 DML。若详细设计证明需要最小 DDL，则迁移与回滚另行设计并按 DEV-first 执行。

## 14. Production Gates / 正确性检查与生产影响（无新增业务门禁）

本蓝图不新增业务审批、人工确认门禁或运行门禁。以下仅是普通输入校验和错误可见性要求，不形成独立状态机、审批步骤或运行阻断层：

- 训练文件真实存在且schema可读。
- WSL训练环境真实可用。
- feature schema与模型兼容。
- 正式预测所需数据库字段可用。
- 模型文件可加载且预测结果合法。

输入正确时这些检查必须自动通过；失败必须输出具体reason和日志，不能要求人工审批放行。

默认生产影响：

```text
production_ddl_gate = noop
production_dml_gate = noop for training; daily Advisory publication is normal runtime business write
production_backend_dependency_gate = noop unless WSL dependency is proven missing
production_frontend_dependency_gate = noop unless UI implementation introduces a real dependency
runtime_activation_and_backend_restart = separate user-confirmed actions
historical_batch_activation = separate user-confirmed action after source merge; never activate against or overwrite the frozen v6 run identity
score_hmm_admission_runtime_activation = noop until independent confirmation and prospective evidence; any later activation remains a separate user-confirmed action
g2a_formal_development_experiment = offline file-only; no backend restart or DDL; v1.2 17/39 structural stop; no experiment or process control by this blueprint refresh
causal_admission_v2_experiment = V2_1 R1 formal complete selected zero; outer not read; current frontier closed; R2 and runtime activation are noop
qe_factor_analytics_direct_v2 = source merged in PR #4352; backend-main restart/readback pending user and does not block offline experiments
qe_active_dataset_universe = source merged in PR #4361; profile activation / candidate repair / experiments not performed by that PR or this docs task
```

训练过程不启动、停止或重启用户服务。模型源代码合入、WSL训练完成、模型文件生成、后端加载、页面可见和模型启用必须分别报告，不能合并成一个完成状态。

## 15. 风险与直接处置

| 风险 | 直接处置 |
|---|---|
| QE文件不含目标候选或合同必需特征 | 搜索其它已有QE H5/Parquet/Qlib Bin或Prediction Store PKL；仍缺失则报告精确阻断，禁止静默删列或启动历史证据工程 |
| QE文件标签口径不兼容 | 原文件模型仅用文件内价格派生标签，或明确阻断；后续价格研究的有界只读DB投影按§4.1另记来源，不回填旧模型输入或原生身份 |
| 多Alpha预测被跨实验拼接 | 只接受目标父包精确roster、seed和权重；不使用“最新腿”替换 |
| 旧HMM结果污染新模型 | 当前文件数据重新拟合；旧模型、状态和系数仅进入对照报告 |
| 分钟执行研发重新侵入Advisory主线 | 后续M4只读日频PIT输入并发布`AdvisoryDailyPriceEnvelopeV1`；分钟Bin、择时、执行模型和适配器归外部模块，本蓝图不排期 |
| 为首模补建宽基指数库 | 只使用现有沪深300；额外指数不作为前置条件 |
| H5固定格式或大Parquet造成内存超限 | 候选/日期/列投影、分批读取和临时Parquet；默认8GB预算，额外上限需实验内明确，不建设新缓存平台 |
| 训练/预测特征不一致 | 共享FeatureBuilder和schema parity测试，预测失败显式可见 |
| 首模或候选效果不佳 | 停止精确candidate；只有可证伪新信息/动作假设才另立研究，不扩窗调参挽救旧模型，不回到基础设施扩建 |
| 正式预测缺日频PIT字段 | 只补Advisory价格合同实际缺失的日频数据库查询或适配，不扩建通用数据平台、不引入实时分钟依赖 |
| 模型输出被理解为确定结论 | 页面标记实验状态、区间和不确定性 |
| 日级区间被下游误当成交指令 | 合同不提供分钟时间点、订单量、拆单或fill；外部模块必须另立消费/执行合同并独立验证，不能回写Advisory预测 |
| 模型失败影响基线 | 模型通道隔离，规则荐股继续运行 |
| ENABLED但调度未执行 | 页面/API分开显示配置状态和最近成功/失败日期；P0-A以真实PUBLISHED/readback关闭缺口 |
| challenger污染baseline | 分开身份和存储，禁止改写selection rank、baseline list或正式episode |
| 5日标签与20日运营错配 | P0-C按冻结review policy生成episode标签；旧5日reranker只作历史对照 |
| CPCV被误当新OOS | 仅用于开发路径和选择偏差；合法未消费历史holdout可作独立历史确认，自然前向仍单独报告，不要求等待实盘日更才能验证 |
| adaptive calibration过早 | 没有成熟residual时保持uncalibrated，不用规则或旧test补造 |
| 跨包共享造成负迁移 | 离线独立baseline、公共/条件化matched与留包比较；不先要求两个生产bundle，不默认共享权重，stock/date簇防泄漏 |
| 把独立模型头误称为策略包无关 | 同时展示package id、descriptor和父Alpha条件；无exact bundle返回typed unavailable，跨包复用必须经过P1-B matched/LOO无负迁移验证 |
| 互斥descriptor让一个新角色覆盖其它模型能力 | 按§5.6使用独立role slots和兼容性hash；角色旋转、回滚和typed unavailable逐槽位处理，未确认角色不提前建设通用平台 |
| H0再次扩张为历史闭合平台 | H0默认休眠；只接受能够解除已复现主线阻塞且受 F-141 至 F-150 约束的最小变更，历史补账、归档、通用缓存/调度/ModelOps仍停止 |
| 批量读取把未来行暴露给业务内核 | 日内核只接受 `AdvisoryPITAsOfViewV1`，禁止传入原始batch frame；未来毒化与访问边界测试必须通过 |
| 静态工作区复用造成跨日污染 | cache key绑定完整内容identity，工作区只读；日期数据和输出位于独立sandbox，identity冲突或写入静态区立即失败 |
| A/B raw共享混淆唯一变量 | raw key显式包含所有raw-affecting字段；overlay配置不进入raw key但在分叉后独立hash，反例测试验证拒绝错误共享 |
| 减少双重全量校验削弱数据漂移检测 | 保留批次full seal、chunk前后token和逐日read receipt；无token或token变化时强制full rehash |
| chunk失败跨越名单顺序依赖 | raw预计算与有状态list transition分层；后者在首个未完成日停止，只从最后成功checkpoint exact resume |
| 追求并发重现资源争用 | 首版单worker、默认5日chunk；先消除重复I/O和工作区重建，性能/内存收据通过后才评估并发 |
| P0-K绝对阈值形成恒等策略 | 已由P0-L改用relative liability rank和真实priority/entry干预验证；不再扩展P0-K绝对阈值 |
| P0-L非零局部干预降低coverage并增加cash day | 按冻结合同以`ADVISORY_P0L_LOCAL_RERANK_INFEASIBLE`终止；不在结果后扩大gain、位移、每日swap或回退identity冒充成功，P0研究族整体冻结 |
| hindsight oracle被误当可学习模型 | 每个Tier同时报告不可部署clairvoyant ceiling与固定cross-fitted learnability；理论高/可学习低时先查信息/时钟/表达，有界非线性对照可另立hypothesis，不继续loss网格 |
| oracle/learnability污染sealed holdout | 开发命令按dataset/window identity拒绝holdout；主线冻结后只允许一次confirmation并立即写消费receipt |
| trial registry和路线膨胀为治理平台 | 只追加JSONL索引既有artifact，路线由其生成；禁止UI、审批、数据库平台、证据复制和通用调度 |
| 双目标合同结果后改判 | experiment创建时冻结objective contract，按合同保存标签、指标、激活和页面状态；禁止加权总分和事后换合同 |
| learnability audit演变为新模型搜索 | 历史单族结果不变；新审计冻结一个线性与至多一个受限非线性对照，时钟/信息/动作唯一变量，计累计trial；不做loss/窗口笛卡尔网格 |
| 完美上限高但干预证据稀疏 | 确认性实验预注册MDE推导的动作数、日期比例和regime覆盖，使用block/cluster推断；不足只作探索 |
| liability被误当Exit alpha | Exit先构造退出相对继续policy的直接增量标签和oracle；liability仅作候选risk特征 |
| 多弱信号伪独立 | N2-A先在同一PIT/window/outcome上检查两腿score/result相关和组合配对边际；后续组合再检查独立时段残差、逐种子/种子平均相关、LOO、成本和regime，不同role/clock不得任意加总 |
| 相同预测或不同取样区间被当作不同Alpha | prediction identity相同的Top25/Top50只算一个信号；横向主结论固定共同窗口与共同预测交集，各包原生Sharpe只作inventory，季度只作描述性sensitivity |
| 父包预测延伸改变模型身份 | spike区分冻结模型推理、历史预测不足和重训新lineage；禁止用新模型补出的预测冒充旧包自然OOS |
| 分钟停牌/临停/源缺口被误当坏样本 | N2-A键集合不变；真实market-wide empty OHLC slot、session-wide single-bar deficit、股票级partial和whole-day missing分别typed；raw与归一化coverage同时报告，经济特征不填零、不移动bar，train-fold median且不删除股票或日期 |
| 分钟聚合被误称跨包通用模型 | 聚合公式可复用，但本轮request、parent score、候选、policy和证据仍绑定当前包；跨包共享继续要求独立bundle与leave-one-package-out |
| 空槽现金被扩展为未授权仓位 | 当前只允许固定等权槽位`SKIP/WAITING`；动态资金权重、组合仓位和交易输入需用户另行扩权 |
| 父包raw score被误当跨日期绝对尺度 | 明确其按日标准化语义；只允许package-bound train-only/cross-fitted校准输出进入准入阈值，raw score阈值测试必须失败 |
| 市场宽度效果被误报为HMM Alpha | 固定raw-market control，并只在相同市场输入上计算HMM增量；缺少control时不得形成HMM结论 |
| HMM状态变成一票否决或不可解释总分 | HMM是条件特征；硬BEAR/fading规则仅作control，模型分别输出Alpha与Risk合同结果，不直接乘父score |
| HMM被父包和Advisory重复消费 | identity记录pre/post-HMM状态并要求pre-HMM control或factorial ablation；无法隔离时结果typed不可归因 |
| G2-A源码或部分fit被误写成sector模型完成 | input/executor、v1.2 17/39结构停止、完整accepted OOF和capability分报；仅sector阶段需canonical source，不阻断R1 |
| sector研究结果提前接入正式荐股 | 只消费canonical causal OOF/prediction bundle作离线研究；development OOF可作不可部署learnability输入，但`rotation_L1`自身forward能力与本辅助模型confirmation均通过后才讨论runtime binding |
| QE单seed LSTM候选被直接替换父包 | QE先完成多seed、LOO和组合Alpha审计并交付正式包；QE Top50分钟收益不能直接外推Top5/review，Advisory不并行补跑实验 |
| QE股票池管理被前置成平台工程 | PR #4361源码已合入但profile未激活；当前冻结release复验不等待全量补缺；只处理目标实验交集，不建配置库/daemon |
| Position Timing样本不足触发合成卡或无界调参 | 只接受自然可达action cards和冻结policy；不足时保持typed结果与规则基线，不合成方向、不从Advisory旧episode偷换人口 |
| 无推荐被误作故障或被静默补位 | 发布`NO_ELIGIBLE_RECOMMENDATION`与阈值/reason，保留0至5固定槽位，不复用旧结果、不补第6名、不生成资金权重 |

## 16. 当前下一步

本页是当前执行路线；§1.3和§9保存完整演进结果，正式registry/route保留历史身份，不为补账或归档新增工时。业务目标始终为可验证的成本后超额收益与风险管理收益，分别按双合同评价。QE统一执行上游Alpha的历史复验、多seed、因子/模型组合和新候选搜索；Advisory不建立第二条Alpha实验线，但负责自身日级价格区间模型、正式binding、API/UI和消费侧业务验证。Advisory不研发分钟择时或执行策略。

### 16.1 主动业务任务（收益型ENTRY_VALUE优先；OPEN_DISTRIBUTION为辅助）

2026-10-02用户要求按收益/风险驱动的买入卖出价格目标开始超过10小时长任务，本节替代旧EP2→EP3开盘分布确认优先排期；不运行Advisory第二条上游Alpha实验线。PR #5099、BUG-1640 / PR #5150和重启验收已完成，29日功能探索已完成且NOT_CONFIRMED；当前主线不是继续把coverage调到75%，而是新的价格条件化进入价值。旧分布角色仍未绑定，其发布合同不变。

以下E0～E6为已发生的实施里程碑，不是需重新执行的顺序；当前唯一主动队列见本节末尾P1～P5。

| 里程碑 / 编号 | 任务、依赖与精确交付 | 当前状态及完成条件 |
|---|---|---|
| E0 / 目标及设计，2h | 统一本蓝图与[收益型进入价值F2设计](advisory_economic_entry_value_v1_f2_design_20261002.md)，明确三语义、两时钟和旧结果 | DESIGN_MERGED；PR #5215 / a0c7e5ab；两轮审核及F2结构校验通过，设计通过不等于业务有效 |
| E1 / 合同及标签，2～3h | 真实开盘观察、冻结exit policy/cost、成熟/正常缺失保留、价格坐标parity | REAL_INPUT_PREPARED；原7,720候选全部保留、7,716成熟标签，旧缺特征1,160条UNKNOWN；15,432端点坐标全匹配；正常S/R歧义不删候选、不阻断全批，只读且不重选 |
| E2 / 价值模型及支持域，3～4h | 一个预登记模型，价格条件化均值/下行风险、past-only训练与label-end purge，多区间/空集 | ONE_REAL_MODEL_TRAINED；study adveconomic_4962fd2ec68948033628954a，train3,139/validation1,509/purged349，6个价格支持桶；原子bundle/registry已闭环；真实拟合不等于经济有效 |
| E3 / 历史导航，2～3h | 已消费开发窗口，固定槽位matched回放，净增量/尾损/错失机会/干预支持度 | ACTUAL_OPEN_NAVIGATION_DONE_NOT_CONFIRMED；81 test决策日/405 Top5，TAKE0/SKIP397/UNKNOWN8；模型臂进入7笔均为UNKNOWN研究控制；完整D冻结网格发布尚未验证，不激活 |
| E4 / 审核与角色交付，2h+ | 多轮代码审核修复、风险口径审查、独立API/UI及binding设计/可交付切片；保留旧M4 | 风险v2内核已合入，eligible变化22/2导致旧return复用停止、v2 generated0保持。v3设计PR #5245已合入，一致集合双头和11项最小测试/唯一真实导航已完成；模型16笔实际TAKE但日增量-8.888bps、区间跨零，当前candidate停止。完整D网格/API/UI独立合同与源码继续，经济确认/绑定未完成，重启由用户执行 |
| E5 / Exit后续设计，1～2h | 下一合法退出vs继续policy的剩余净价值，复用既有Advisory label/oracle | DESIGN_UPDATED；经济设计§16已核对已有oracle/learnability合同，沉没入场成本不二次扣除；尚无新Exit模型或最佳分钟卖点 |
| E6 / 新增D量价信息，研究已完成未确认 | 固定四字段与十三scope，同集合九字段控制/十三candidate，不改旧模型/label/policy；NAV结果不自动启用 | #5301/#5303/#5304/#5305均已合入，离线实现/真实训练/只读模型消费完成。来源保留386D/7,720候选，fit共同train/validation3,117/1,507。一次两配置四头study：十三/九/基线100日名义净收益19.0748%/9.6268%/19.1729%；十三减九日+8.7336bps CI[-8.6091,25.5629]，减基线-0.1545bps CI跨零，EXPLORATORY_NOT_CONFIRMED。不扩大窗口或追加证据挽救当前candidate；通用每日消费、前八字段train/daily一致性按P1/P2完成，不升级旧模型native或收益资格 |
| 条件性 / 旧EP确认与绑定 | 旧[独立价格角色](advisory_entry_price_independent_role_f2_design_20260928.md)、[分布确认](advisory_entry_price_confirmation_f2_design_20260928.md)、[每日交付](advisory_entry_price_delivery_f2_design_20260928.md)的源码与兼容保留 | PR #5099及#5150已合入/重启验收；ENTRY_PRICE仍NOT_CONFIGURED，仅原分布确认通过才可同scope绑定，不阻塞E0～E3 |
| 后续 / QE-DELIVERY-CONSUME | 只读消费QE已进入策略包的组合；QE统一Alpha、seed和因子研究 | v4.02按用户指令取消Advisory二次runtime_asset_admission；legacy声明如实展示而非拒绝，不补证明或改QE代码 |
| 后续 / PRODUCT-VALIDATION | 新合格包同policy/code身份的Ranking/Admission与业务历史对比 | 已有Historical Range对比、指数股票池、DAILY_DB_ONLY源码/运行时验收保留，不重复建设 |

当前v3/v4、80日历史回放、rolling-20D导航和原29日探索均已完成，rolling selected=0；不得回选、改旧模型或窗口结果。原29日属于非原生历史探索而非正式分布确认。新的收益型建议采用新目标/标签/模型身份，不把旧价格实验升级为收益证据；旧分布正式确认输入仍缺合格连续/vintage身份，但不阻塞新的开发与功能验证。

2026-10-02 BUG-1640完成状态（PR #5150已合入、#5190 close-sync完成）：显式批准原29日legacy探索，原计划/证据/handoff SHA、日期/候选/包/policy/D-1/DSE逐项绑定；9日既存原生名单证据、20日恢复非原生证据，17日成员内容匹配，08-14/17/18完整原始成员仍未证明。20日恢复原生receipt=0，不能宣称原生COMPLETE或三日全市场复现。用户解除prepare-only后请求`advepc_4178fffbd9c93e0bd84216a5`完成29日/580样本预测→结算→评价：连续coverage=0.5637931034、tick=0.5827586207、宽度/control=2.0664613119、IS差=-0.0214053808，CI95=[-0.0245338326,-0.0175375111]；模型/市场580/580可用、未知/停牌/crossing均0。预测526.4秒、结算6.59秒、评价1.24秒，exact retry评价文件hash一致。结论NOT_CONFIRMED/NAVIGATION_ONLY，零数据库写入/绑定/收益确认；不降低旧门槛。未来Selection输入原子留档已按旧任务登记范围交付，本新任务不修改Selection。源码、模型效果、运行时和历史证据等级分别报告。

既有Advisory门禁1,029 passed/6 skipped，覆盖同核投影、PIT/缺失保留、四阶段恢复、完整scope读回、角色CAS、盘前时钟及预算公平性。BUG-1632修复消费者平铺分页、BUG-1636修复prepare误加载推理器，均已完成用户重启验证。此前资源检查被已完成但canonical_status缺失的旧实验阻断，另有三项历史paused记录；这些记录不能作为纯历史推理的实际占用证据。2026-09-30用户授权纯历史回放与QE并行，BUG-1640仅在Advisory历史确认路径替换该独占检查：每阶段/日期块检查本机可用内存和输出磁盘，512 MiB内存、128 MiB磁盘为工作缓冲下限，CPU忙不单独拒绝；独立进程LightGBM及BLAS/OpenMP最多2线程，数据库只读且单条SQL超时30秒。该许可不涵盖QE训练、调度、每日自然资源合同或其他模块修改。prepare仍只核元数据；真实predict必须在显式现存AIstock环境校验LightGBM与threadpoolctl后才能消费窗口。不自动安装依赖。尚无新增自然或正式OOS样本。

坐标差异已由provider-compatible v2关闭：理论除权参考价先按0.01元tick作ROUND_HALF_UP，目标factor以供应商四位精度投影；只使用D可见公告、D raw close和D factor。原标签SHA=c4fc72b94e9e112bcc05405e8c7f6ec28bc2890b16c4ff978e3b7dfe0ee2b148，旧v1模型及artifact不变。旧分布正式确认的输入限制保留，但不安排恢复或补证，也不阻塞新的日频价值业务。股票池缺收据的旧记录保留明确限制，不依据当前binding回填历史身份；Advisory自行验证消费侧PIT/完整名单/特征/时钟，发现基础数据确切错误才把数据集、股票、日期和字段交给数据窗口。Selection/QE产物或公共接口的源码问题交所属窗口；Advisory不重建候选、不补写数据库，数据窗口不承担荐股业务验证。

2026-09-30元数据读回：匹配包的ENABLED Top20 Program有31个PUBLISHED目标日（2026-08-14～09-30），另有1个REPLAY。仅11日带原生universe receipt（09-15～09-30），低于最低20日支持要求；日期存在缺口，不能将31日拼接成连续合格窗口。未读取目标效果，也不声明全部成熟、PIT或未消费。已准备的v4 test回放advprhist_5135d6b0f6a53c706f0fc561（D=2025-11-07～2026-03-10）仅NAVIGATION_ONLY，资源未放行前不执行。新推理输入材料如无法证明PIT/lineage则仅探索性，不可绑定。

消费者接续事实：v3源码PR #5249已合入`c4ce9566b51d1ff24ad564ef11d27e2344acd4fc`；[完整每日D价格条件消费者与独立API/UI设计](advisory_economic_entry_daily_consumer_v1_f2_design_20261002.md)已接受，本地共核网格/只读消费者/API/UI和81日功能读回已有实现，六项浏览器已实际通过，源码待必需CI及PR合入、运行时待用户重启。2026-10-03本地追加两轮修复Program坏工件隔离、空名单原生身份/盘前时钟及全局预算边界，相关62项测试通过；随后同步主线计算/输入和业务优先路线，不重复旧研究或81日回放。专用run `advisory-ui-7bbb741e-20261003`绑定HEAD7bbb741e，6 PASS/0失败/0跳过/0重试；原始结果及spec hash已核对，后续仅文档变更，backend/frontend与收据HEAD等价，不伪称新HEAD重新运行过UI。后续按P1/P2处理通用功能，不为v3/v4追加经济确认或绑定；无合格模型时正式路径明确不可用。E5退出设计暂不启动并行训练。旧OPEN_DISTRIBUTION的coverage确认不能充当新ENTRY_VALUE收益验收或研发门禁；不重建候选、不等待实盘更新替代必要功能验证、不重复QE实验。

本轮真实导航：预登记同一研究配置，零模型source失败尝试保留；最终一个模型。共同组合100日口径下，基线/±300bps规则/模型臂收益约+19.17%/+19.17%/+5.97%、MDD约-10.23%/-10.23%/-2.22%，模型减基线平均日收益-12.47 bps。这不是指数超额或真实成交；模型臂7笔均来自UNKNOWN研究基线控制，真实模型TAKE=0，不能把+5.97%解释为模型盈利。397个支持条件中197个预测均值正、风险q90≤800条件0；48个基线进入信号SKIP涉及29个原盈利/19个亏损episode，只作描述，不相加为组合净收益。首要风险是entry stop800与全episode peak-to-trough q90≤800语义不等价；先审查标签/预算关系，不换模型族或反调风险阈值。所有结论HISTORICAL_REPLAY/NAVIGATION_ONLY/RECOVERED_LIMITED，未恢复原生receipt、未消费sealed、未绑定。详见经济F2设计§15.2。

源码交付PR #5224已完成多轮审核修复、最新main同步、57定向测试和必需CI并合入`dba2028faae9f659eaf85bd2b3f834e7c332d40a`；无需为离线切片重启。[风险对齐与每日预测身份F2设计](advisory_economic_entry_risk_alignment_v2_f2_design_20261002.md)经PR #5233合入，内核PR #5242已合入；study `adventryloss_daadbb8de554e555061df5d8`保留7720候选，31条端点执行未证明，原train/validation eligible变化22/2，按合同BLOCKED_ELIGIBILITY_DRIFT，generated/evaluated=0。后续v3设计PR #5245合入后，独立study `advaligned_00089672bee4c91dd2261cf1`在同一新risk目标及共同eligible固定拟合两头，planned/generated/evaluated=1/1/1，train3117/validation1507/purged349/支持桶5。evaluated SHA=`4b50e12bf15c39cd859c8ca3a83146da64d29b3c97af55883a6dba7401a5cbf5`；100共同日的模型/基线净收益9.6268%/19.1729%，日增量-8.8880bps，实际进入动作不同42日/持仓构成不同92日，有真实模型TAKE但经济增量未通过。本新candidate停止，800不搜索，旧权重/结果不覆盖，不称exact retry/OOS；三臂实际episode日频端点核验未发现额外执行限制，仍不是fill证明。完整结果见v3设计§13。每日身份区分、D法规价和条件节点内核只在Advisory修改，API/UI及生产绑定仍未完成。

2026-09-30历史8日探索结果保留：请求 `advepc_fda8f36ceae1ffe0e77db23c`，T=`2026-09-15..2026-09-24`共160候选，模型/市场可用、未知/停牌/crossing均0，预测167.98秒。连续coverage=0.6125、tick=0.64375、宽度/control=2.104502；IS差=-0.020045，CI95=[-0.021884,-0.013075]，结论INCONCLUSIVE/NAVIGATION_ONLY。该旧回放不再是主动待办，也不触发旧校准frontier回选；新的当前结果为上方29日完整探索。

旧排名模型推理仍需exact包候选及其实际103特征；各新价格模型只能按自己的显式recipe/order/scope消费，不能把103维要求套到D-only12维价值锚或其它9/13/15维family，也不能给旧权重补侧车。遇上游缺失只报告最小依赖给所属窗口；不修改QE/Selection公共代码。ENTRY_PRICE未确认为任何新包/新股票池可用前，其状态保持typed unavailable。

2026-10-05当前唯一主动队列如下。最多一条价格模型主线和一条必要工程辅线；#5324默认未配置运行验证仍有效。H-TIMING-1、H-VALUE-ANCHOR-1及H-CONTEXT-VALUE-1各按冻结方案完整执行一次并停止当前candidate，不列为待训练/确认或新daily family。#5344/#5346/#5347/#5348/#5372/#5383/#5388及M6 #5451、M7 #5453、M8 #5455及M9 #5457、M10 #5459均已合入/自身清理，不再排期。用户新授权§16.6 R2连续多假设任务：M2/M3/M4/M1/M5/M6/M7/M8/M9/M10/M11/M12/M13/M14/M15/M16/M17已完成累计67fit+1index，M10源码7aed83cc6已合入/自身清理，通用日频输入设计#5460/源码#5461已合入并清理，当前M11一次四fit/四臂负向完成、源码#5463已合入/自己清理，当前M12设计#5464已合入/自己清理，一次四fit/四臂负向完成、源码#5465已合入1f85b44ff/自身官方清理；M13自由流通信息已一次4fit/四臂负向完成，源码#5470已合入e86e2ca9c/自己清理；M14/M15/M16工程源码#5478/#5480/#5482均已合入，在各原冻结实施闭包内等待QE空闲后于2026-10-05 15:14～15:19上海时间依次完成一次prepare（M14已准备不重跑）/各四fit/完整四臂，真实累计63fit+1index。三者candidate净收益20.2050%/14.1018%/25.5593%，共同基线21.3220%；相对基线日增量−0.9490/−6.2150/+3.4848bps，均未满足原净增量条件且区间跨零，只停止各自身candidate，不调门槛/回选matched/补证或启用。M16源码#5482已合入7521de2fdd；三研究已完成后才精确官方清理自身冻结源树，清理状态另报；M1六UI/公共BUGsmoke仍独立待交付，不阻断真正新信息研究。当前三个新candidate均不安排再次拟合/确认/日频绑定；继续识别真正有经济解释的新信息或可识别动作，先设计预登记、不为凑时长同族调参。最多一价格研究主线和一必要工程辅线，QE上游alpha/新包仍由QE负责，不并行其训练。 M17同原五session路径一次4fit/四臂负向完成，不再安排重训/补证/绑定，source54定向项通过，当前CI交付另报。

| 优先级 | 直接业务交付 | 禁止绕行及完成边界 |
|---|---|---|
| 已完成 / R2三路线源码交付 | 设计#5395及源码#5404均合入/自身清理；15测试、三次真实四臂及11fit+1索引已完成 | 三candidate均负向停止，不回选matched或补证；经济确认/启用0，QE/DB/runtime不动 |
| 已完成 / R2-M1开发导航 | [动态板块M1 F2设计](advisory_sector_dynamic_price_value_v1_f2_design_20261004.md)#5411交付，源码#5414；原7720键prepare、1795train/765validation、4fit及81D四臂完成；五个冻结导航门通过 | 点估计过门不是收益确认：两个增量区间跨零，93episode中36真TAKE/57 UNKNOWN控制；native/ENTRY_VALUE启用仍0 |
| P1 / M1日频功能接入 | #5442已交付family/classification；#5443详细设计合入。源码#5445已同步main4f7793a0f并push d1ccaae19，新CI37240066029 SUCCESS，43直接测试、真实ASGI200及20日400原候选批量34.719秒通过；六新UI浏览器验证未执行。BUG-1726三层空链局部修复71项通过尚未PR，公共sector-entry-price业务smoke合同待owner | 不借CI绿灯冒充UI；仅这两个交付依赖暂停合入，正常UNKNOWN不阻断其它研发。source适配器LIVE_DB显式canonical key，不改公共数据；代码合入与用户重启/运行配置/收益分开，不重复原实验 |
| 已完成 / M6新信息 | [M6 F2](advisory_moneyflow_price_value_m6_f2_design_20261005.md)#5450设计及#5451源码均合入/自身清理；48直接项及一次7720键prepare/四fit/完整81D四臂完成 | 原candidate减baseline/matched日-3.1318/-1.2873bps、两区间跨零，net/tail失败；只结束本候选，不重跑或追加历史固化。原累计23fit+1index保持，无收益确认/启用 |
| 已完成 / M7新信息 | [M7 F2](advisory_price_path_value_m7_f2_design_20261005.md)#5452设计/#5453源码已合入/自身清理；真实源码61ed666992/30直接项、一次7720键prepare/四fit/完整81D四臂 | candidate相对baseline日+2.0581bps<事前5bps且区间跨零；matched+16.9174bps不能替代基线条件；只STOP本候选，累计27fit+1index，0DB/分钟/sealed/激活 |
| 已完成 / M8新信息 | [M8 F2](advisory_market_risk_price_value_m8_f2_design_20261005.md)#5454设计/#5455源码已合入/自身清理；固定市场时序风险/个股beta的13/16核、39直接项及一次完整研究完成，source producer 6a4589e58；原7720键全保留，单指数SELECT/386行 | candidate/baseline/matched 12.5727%/21.3220%/4.5719%，两个配对增量-7.7890/+7.0702bps均跨零，net_increment=false；只STOP本候选，累计31fit+1index。当前source476537d77已交付，不重跑或绑定；旧Admission的离峰复用、非vintage/native限制仍明确 |
| 已完成 / M9源码交付 | [M9 F2§15](advisory_volume_context_price_value_m9_f2_design_20261005.md)#5456设计已合入/清理；43直接项、7720原键准备/新四fit/完整81D四臂完成；单量SELECT请求38168对/返回38158，不拉全池 | candidate/baseline/matched 25.9681%/21.3220%/4.5719%，两个配对日增量+3.9776/+18.8368bps；基线条件未达5且区间跨零，只STOP本候选、累计35fit+1index。源码#5457已合入4f7793a0f并自己官方清理，不降低原研究判据/重跑/确认/绑定；下一设计须真实不同信息/动作，旧隔夜特征不换名复跑 |
| 已完成 / M10源码交付 | [M10 F2§15](advisory_breadth_state_price_value_m10_f2_design_20261005.md)#5458设计已合入9e39c3c1c并自己清理；源码38直接项/Ruff/F2/L0及真实原M1 bundle兼容通过；一次prepare2.094秒/0SQL/7720原键、7340 AVAILABLE/380预热 | producer296689715，一次4fit/完整四臂21.407秒；candidate/baseline/matched 11.4050%/21.3220%/4.5719%，配对日-8.8603/+5.9990bps均跨零，仅STOP本候选。实际39fit+1index，0经济确认/启用；源码#5459 HEAD696d19bb9/CI37243602139 SUCCESS后合入7aed83cc6并自己清理，不改旧政策/预算/结果 |
| 已完成 / 通用日频价格输入 | [F1详细设计](advisory_generic_price_input_v1_f1_design_20261005.md)设计#5460已合入d78b2e053/自己清理，独立四文件叶已实现；26直接项/Ruff/两L0/原20候选D输入通过，source#5461 HEAD eff5947a6/CI37245983776 SUCCESS后合入0c33faa5c并自身清理；原0～50候选+真实D股票/基准/市场九字段，不需父score或LSTM/FUND腿 | 输入可移植不等于权重/价值标签跨包有效；不改M1～M10、原label/政策，不fit、读收益或连接API/UI/DB，不以未知分类或父资格等待阻断；价值目标后续另立F2 |
| 已完成 / M12源码交付 | [M12 F2](advisory_session_path_m12_f2_design_20261005.md)#5464已合入/自身清理；12文件三轮源码自审/42直接项/Ruff/F2/L0及原M1 bundle兼容通过，producer4fb26cc、一次6.750秒prepare/21.406秒四fit四臂，原7720全保留 | candidate4.8425%/baseline21.3220%，日增量-14.8813bps及对matched-0.0220，两区间跨零，net false其余四项true，仅STOP本candidate。实际47+1，源码#5465 HEAD0f0e8c65a/CI37251660467 SUCCESS后合入1f85b44ff并自身清理，不再fit/确认；继续不同新假设，不默选通用目标 |
| 已完成 / M13新自由流通信息 | [M13 F2](advisory_free_float_turnover_m13_f2_design_20261005.md)：固定自由换手20日均值/波动和D自由股本规模，区别旧量价相对权重。0trial有界聚合检查7330完整/380预热/10正常缺行，1 SELECT/1.438秒，仅键/非空未读值/收益 | 设计#5467已合入7c024171f/自身清理，最新main独立树12文件已实现，三轮源码审核/稳定52直接项/Ruff/L0通过；薄只读来源/13对16核/显式47→51预算；原VALUE_REVIEW_5_V1/支持/成本不改。一次prepare7.453秒/4fit四臂21.782秒完成，candidate12.0020%/baseline21.3220%、配对日−8.1577bps区间跨零、net false其它四项true，仅STOP自身。真实51+1，源码#5470 HEAD1a864561f8/CI37253862090 SUCCESS后合入e86e2ca9c/自己清理，不重复研究；非vintage/native UNPROVEN不升级，不因缺原capture阻断策略包消费 |
| 已完成 / M14估值上下文 | [M14 F2](advisory_valuation_context_m14_f2_design_20261005.md)：一次真实prepare/4fit/完整四臂，源码#5478已合入0146ff7aac | candidate20.2050%/baseline21.3220%、paired日−0.9490bps区间跨零，20真TAKE/69UNKNOWN控制，net与TAKE未过；仅STOP该candidate，不重跑或确认，§16.6.7 |
| 已完成 / M15历史合法界状态 | [M15 F2](advisory_limit_state_m15_f2_design_20261005.md)：一次0SQL prepare/4fit/完整四臂，源码#5480已合入a8ae765ed | candidate14.1018%/baseline21.3220%、paired日−6.2150bps区间跨零，78真TAKE/5UNKNOWN控制，net未过；仅STOP该candidate，不补证或绑定，§16.6.8 |
| 已完成 / M16原候选群体状态 | [M16 F2](advisory_candidate_cohort_m16_f2_design_20261005.md)：一次0SQL prepare/4fit/完整四臂，源码#5482已合入7521de2fdd | candidate25.5593%/baseline21.3220%、paired日+3.4848bps<原5且区间跨零，83真TAKE/6UNKNOWN控制，net未过；仅STOP该candidate，真实全轮63+1，§16.6.9 |
| 已完成 / P2 M5新信息 | #5432已合入716232d3b949d4dd0aecbe9fe210ad6fa8fe506b并完成自身清理；真实run advselectionvalue_8f53ace471987dc7f0b99a00完成7720键prepare/四fit/完整81D四臂，20直接+20reader测试PASS。成本后candidate/baseline/matched为13.2538%/21.3220%/4.5719% | 相对baseline日-6.8073bps、matched+8.0519bps，两CI跨零；原净增量条件失败，仅结束此假设不阻断包消费或项目。真实TAKE82/UNKNOWN控制6，累计19fit+1index；不救活旧模型、不重复研究或固化旧失败 |
| 已完成价格研究 / M17有序资金流路径 | [M17 F2](advisory_flow_path_m17_f2_design_20261005.md)：原M6冻结源0SQL；一次4fit/完整四臂，paired日baseline−4.3050bps/CI跨零，85真TAKE/2UNKNOWN控制 | 本candidate停止/NOT_CONFIRMED，不重跑；源码54项/Ruff/F2/L0通过，source#5485已合入4708dd6a7/自己官方清理，实际67+1，§16.6.10 |
| 当前价格研究 / M18非对称历史风险 | [M18 F2](advisory_asymmetric_risk_m18_f2_design_20261005.md)：56直接项/Ruff/F2/L0通过，独立plan d7d0e967...一次0SQL prepare/7720原键，研究fit0 | 09:21:01UTC QE MA-E42R running；source#5487已合入13b4c4232/冻结树保留未清理，30min复查再一次4fit至71；全轮实际67+1、不重复prepare，§16.6.11 |
| P4 / Exit后置 | 买入价格主线形成完整业务与可验证增量后，推进日级卖出vs继续持有价值 | 已有设计复用，不另开分钟择时或并行Exit训练，不把holding相关性当Exit盈利信号 |

v4.00当前功能进度：#5423以HEAD93ea35f4b/CI37160815000成功交付并自身cleanup_done。唯一已消费2024-08-01原20候选、原core receipt/15D与21D板块行情数值兼容通过；既有M1数学直接输出9条研究买入价格集合、11条UNKNOWN，法律完整tick最多1491，保留支持洞、多段及全部原名单。原D close/除权参考与既存冻结references逐条一致；无T实际行情、市场收益/label、新窗口、fit或角色激活。价格源确实只读DB，canonical历史组件ready不等于实时canonical切换：live仍shsz_st_pit_active_v1、component_is_live=false。首次legacy输入按合同拒绝；没有接受旧路径、改数据/公共模块或启用服务。原父模型时钟仍未知，明确原conf文件只读404不等于不存在模型或必需重训。本次事实/时钟/队列三轮自审只更新两文档及直接功能事实，不改变模型、scope、研究合同或两条跨零增量区间，不形成旧失败固化、新包装平台或人为等日期门禁；此前model-state GET元数据upsert披露保持，不能宣称整轮数据库NOOP。

v4.01历史来源检查：既有Advisory context/板块读取器在原2024-08-01全20候选按KEY对齐，分类和15D与原M1一致（9完整、11 classification_knowledge_time_unverified），未查新市场窗口/收益。该旧context仅EXPLORATORY_SCREEN且受实际release cutoff，正常缺失保留；它不是通用日频实现。v4.02起父/processor/组合时钟不再是缺口或待办，QE包直接接受；当前真正功能工作是消费者接口/来源/family实现，不补原native或确认功效来取得资格。

历史确认框架[§7.1](advisory_sector_price_value_confirmation_v1_f2_design_20261004.md)已完成原100日联合功效规划：500日/假设配对真效应10bps/绝对漂移10bps的两配对代理84.20%，加绝对均值下界仅43.10%；不是实际功效或新样本。原5bps/NAV/模型与结果不改写；v4.02起该规划不形成研发/包使用门、不要求80%或自然等待，M5独立开发研究可继续。未授权sealed不读，不为旧负研究追加证据；本方法计算0新fit/研究run/激活。

2026-10-03计算与输入切片进度：#5313已合入a473e3b502d6cb98f363d6cf73a7953eb931cce0（修复后HEAD343718cc7、CI37099710628通过），提供原8+新4共12个D字段的统一纯计算API，20D候选/指数与2D市场宽度，原名单/实际两腿、OHLC、指数中间缺日以及整日/盘中/复牌缺行情语义；27定向测试、Ruff及F1五项通过。#5319已合入7edd740a82ff91f61ed842a88c615a516a7eff13（HEADab3262320、CI37101259296通过），交付同核单D/至多20D批块只读输入：精确键集、有界5SELECT、单快照rollback；14定向测试和F1四项通过。最小真实SQL smoke只用已消费2024-07-04及两个合成候选投影，证明查询/12D计算兼容，不是原Selection名单、全批性能、native或经济验收。计算core不含query_gap、不读取收益、不拟合、不开DB连接；source显式只读数据库，两者COMPUTATION_ONLY/旧训练parity UNPROVEN，均不改变旧模型、产物或调度。后续[共享内核日频接入详细设计](advisory_economic_common_core_daily_consumer_f2_design_20261003.md)按真实模型recipe接入已有消费者；旧v3/v4缺新recipe身份时不补侧车、不默认重训或新增81D验证。十三字段新模型路由/API/UI仍未完成；旧九字段消费者#5324六UI及必需CI通过并合入68ff7aaaa，用户重启后只读默认未配置语义验证通过，不转交数据准备窗口做业务验证。

H-TIMING-1接续：#5327设计合入725cb1f84e0ad0211b4073a20efac72d2a4787e2；#5331计算/来源37测试与F1验收通过后合入、源树已官方清理。首中末D预检各20/20，完整386D只读准备607.703秒，7720候选/7710完整/10 UNKNOWN，不删日期或股票；新输入successor只规范源码CRLF/LF身份、保留精确Git blob证明和原读取记录，不重查库或覆盖原产物。新13/15共同监督4153/1556以新值为准，不套旧3117/1507缺失mask；实现五叶模块及五测试，三轮不同视角自审修复训练中断隐式再fit、外来候选/法规矛盾，28定向测试/Ruff/F2五项通过，源码PR #5336已合入`5ec8c8e2d1deee16a5587afd604d61481162669e`。其离线run已一次4头完成，详细指标见§1.3；两项MDE描述性代理分别16.5749/22.9235bps，不是确认功效。30个实际进入差异日证明非恒等输出，但新增块相对matched提升小且区间跨零，相对原基线明显负；MDD改善不替代已预注册收益条件。结论只停止精确candidate，不证明全局不可学或包无alpha，不进入新family UI/每日接入，不扩大样本/种子/阈值。旧父identity的历史缺失限制保留；新监督值和mask由本run独立receipt说明，不能混读旧mask。

进度只按工程交付、业务功能验收、经济有效性、正式启用四态记录；目前新收益型价格模型经济确认/正式启用数量均为0，不用PR数量、测试数量或训练完成冒充终极目标完成百分比。#5304/#5305已完成，不再列为下一步。过去有明确日期的回放、资源预检和输入缺口段落仅保存历史结果，不自动生成新任务。

v3.85进度修订审核：本窗口事实轮逐项对照本run的plan、trained metadata和evaluation，区分81个候选决策日/100估值日、真实TAKE/UNKNOWN控制、15减13/15减baseline，不跨实验拼接收益；一致性轮同步页首、§1.2/1.3与§16，去掉H-TIMING-1“待训练”及旧BUG-1640“尚未合入”当前态，早期检查点保留为历史；投入/授权轮确认停止负candidate，不生成确认/激活/补证/UI待办，临时X、持久F、QE及数据库/服务均未改。三轮均为本窗口不同视角审核。DESIGN-COMPLIANCE-001四项分别为状态不冒充功能完成、未知不伪成功、固定合同不结果后放宽、无新治理/未来日期门禁；本次仅更新真实进度及下一设计边界，不扩大现有业务合同。

v3.86设计时计划（历史检查点）：约10小时按设计、标签内核、双头/原子研究、一次导航和多轮审核交付顺序执行；当时仅DESIGN_REVIEWED_NO_EXPERIMENT。该计划的准备/训练/评价现已完成，结果见§1.3，不再作为未执行待办。ADJUSTED_SHADOW_VALUATION边界不变，不升级为实盘成交。

v3.87本轮事实：#5344合入3ee16cac00c69038844de6cbef355981084e6d75，#5346合入776ccfc5a42f23f59d45c9d35dcc2b15b9c4559f，两树均官方cleanup_done。研究源码#5347 HEAD1c8545ab870f5e12b9e128e09b1b13a4bdf0ae2f、17直接测试/Ruff/F2五项通过，CI37115926268 SUCCESS后合入3e20e922d5922838cccc9de99cfa0f4e3df9318b。run完整保留7720候选，新场景21标签UNKNOWN/10 D输入未知及purge296各自报告，不能套旧监督mask；test1521是未跨test_end的监督可用数，实际评价仍保留81D/1620原候选，不删日期/股票。两头fit0.880秒（含源核验），prepare15.163秒、fit+评价12.169秒；前后7项QE状态均0。模型84实际episode中81为TAKE、3为未知控制，胜率61.90%高于基线58.24%，但收益低于基线与常数，两个MDE代理22.5775/24.6641bps都不是确认功效。按原条件停止当前candidate，不关闭全局价格方向，不启用或为其做每日新family。

v3.87三视角审核：事实轮核对new plan/prepared/trained/evaluated，区分81D与100估值日、模型TAKE与未知控制以及新/旧退出场景；一致性轮同步页首、状态表、实验表和本队列，原计划降为历史检查点；投入/合同轮确认不因常数表现较好事后更换candidate，不为负模型补证/扩窗或接UI，不以胜率/MDD替代固定收益目标。DESIGN-COMPLIANCE-001四项分别为源码/研究/经济/启用分报、未知不伪成功、合同不结果后放宽、无新平台/日期/旧固化门禁。

### 16.2 被动观察（零研发排期）

- 基线/P0-D自然observation继续由既有调度形成；价格独立CLI截至2026-09-28仅核验1日20行。EP4每日源码已加载，但ENTRY_PRICE未配置，当前没有该角色的自动价格积累；旧P0-D observation不得算入其确认。
- Position Timing继续自然形成真实可达action cards和outcomes；L2/L4b-1只在冻结人口达到其预注册支持条件后复验，不合成卡、不补历史人口、不以当前不足触发新模型搜索。
- P1-A固定历史R0/R1已完成且selected=0；既存自然记录可由原调度被动形成，但不为旧未达标候选新增收集、结算、复验或证据投入，不作为后续晋级理由。LONG_TREND无真实包输入时也不形成主动待办。

### 16.3 条件性阻塞修复

- 只修阻碍§16.1或每日荐股正确性的BUG；修复范围必须指向可复现错误，不以顺手工程化扩大范围。
- H0默认`CONDITIONAL_DORMANT`。只有业务语义不一致或资源瓶颈已经直接阻塞当前实验时，才执行能解除该阻塞的最小切片；不得主动运行完整H0路线、44日性能工程或历史固化。

### 16.4 零工作约束与已完成状态

- P0-D至P0-L冻结、N1/N2 immutable结论，以及N3固定proposal/overlay/腿间/分钟/generator/margin/event/Score-HMM-v1/causal-Admission-v2.1-R1的selected=0与已消费窗口都只作为约束，不是任务；不得创建P0-M、放宽旧合同、回选旧arm或旧Entry/Exit候选，不能把旧窗口声称为新OOS。§6.3.2是2026-10-02用户批准的新价格条件化经济目标/标签，独立身份、导航证据，不是回选旧候选或抹去负结果。
- 历史实验复盘、历史证据/数据固化、归档、Phase 1R、旧batch/root清理、通用缓存/调度/ModelOps、registry UI和额外治理均分配零主动工时。
- 任何已经未达标的实验（含收益型v1/v3/v4/H-TIMING-1/H-VALUE-ANCHOR-1、旧分布/Admission）不再追加证据收集、扩窗口、补历史身份、自然积累验收或独立确认。最小必要代码正确性检查、通用消费者功能验收与有实质新假设的新实验是不同任务，不能借其名义挽救旧结果。
- 最小PIT、policy hash、成本、窗口、package/descriptor identity继续保留，因为它们防止未来泄漏、跨包误用和结果后改判；不得将这些最小正确性字段扩张为独立数据平台。

固定分钟、QE Alpha generator、融资融券、财务事件、Score/HMM v1和因果Admission v2.1 R1均已完成且未形成资金权重、仓位或交易输入。R1正式结果selected=0并关闭当前辅助frontier，不写因子库或StrategyPackage，也不需要动态资金仓位授权。只有把空槽/现金扩展为动态资金权重、组合仓位或交易执行输入时，才需用户另行扩权。R-PUBLISH若涉及运行时源码生效，可能需要用户重启；离线研究默认不需要后端重启或DDL；如后续详细设计证明产生这些操作，仍须由用户执行或另行授权。

### 16.5 R1历史任务：条件价格新增信息验证（已完成）

状态：`R1_SOURCE_MERGED_CLEANED_CANDIDATE_STOPPED`。G0/#5372、G1/#5383和G2/G3源码#5388已合入，原7720候选完整、严格分类PARTIAL；14项定向测试、四真实fit及完整四臂导航已完成。#5388合入`d18dfb14e996ea7100f843bd7a03d1694e9619ef`，CI37143913405 SUCCESS，自身源树/分支官方清理完成。run=`advctxvalue_a551b5933b0dffc5b3877bd1`按事前净增量/真实TAKE条件停止，经济确认/启用0，不继续确认/挽救。下面G0～G4及“本轮只一次”的措辞仅为R1历史合同，不约束用户随后明确授权的§16.6不同模型R2；R2也不能改判R1。

| 阶段/优先级 | 时间预算 | 具体产出 | 放行或停止条件 |
|---|---:|---|---|
| G0 / P1：复用核对 | 0～2h | 已完成统一source，不重查全量或重建；核对当前身份及新增信息范围 | 可选UNKNOWN保留；必要硬身份漂移只停止相关动作 |
| G1 / P2：详细设计及审核 | 2～8h | 已完成F2详细设计、精确范围、全部事前数值/三视角自审，#5383合入7c146a3e0 | 当时未读评价收益选规格；历史检查点不覆盖当前实际进度 |
| G2 / P3：最小实现 | 8～20h | 已完成五源码叶、五测试/14项；复用标签/回放/registry，多轮修复审核 | 本轮源码PR交付，不建生产family、不改其它模块/旧工件 |
| G2 / P3：一次学习诊断 | 20～28h | 已完成两配置/四物理头/一candidate，857/304共同监督，噪声代理53.7926bps | 实际fit-stage约7.985秒；QE前后running0，未隐式retry/扫参数；代理不是确认功效 |
| G3 / P3：一次组合导航 | 28～34h | 已完成全人口81D/1620、100估值日四臂，评价约28.593秒；candidate负向停止 | matched净增量及真实TAKE门失败；非独立OOS，不用coverage/回撤/名义收益救活 |
| 条件分流 | 34～43h | 正导航仅准备P4独立确认设计；负向停止当前candidate；必要正确性BUG在本模块修复 | 不读sealed、不继续确认/补证/调参，不事后改选控制；下一真正新信息仅设计，不默认第二轮搜索 |
| G4 / 交付 | 43～48h | 多轮审核/定向复测、必要PR/CI/合入、自身精确清理、事实进度更新 | 文档/源码/研究/经济/启用分报；新family生产适配不捆绑本轮 |

总预算最多48小时，包含直接阻塞修复、审核与CI等待；提前完成立即推进，不为达到工时延长失败实验。源/设计不成立可提前结束本候选；必要修复超过余量则保存明确进度与阻断，不砍合同冒称完整交付。预算到期不强制终止正在安全运行的任务，报告剩余耗时，不控制其他窗口进程。

G1必须在拟合前给出明确数字：连续切分边界、实际fit总数、最大迭代和资源预算、最低干预交易日/比例与经济最小效应、风险非劣容忍及固定统计方法。数值依据仅来自允许的训练段/既有开发噪声和业务目标，不能读取新的评价结果后补填。选择GAM/分层收缩仍属设计判断，不要求安装新库；没有必要就不实现完整分布平台。小筛选与完整回放都维持原候选/日期，不将缩减经济人口包装成正式结果。

本计划默认收益主终点：candidate相对原Top5和matched控制均提供事前规定的正净增量，风险按冻结容忍单独评价；简单风险控制用于判断是否只是暴露变化，归因不清只报告限制。通过开发条件仍仅`NAVIGATION_ONLY`；任何新风险优先合同须在该研究前单列，不用结果后权重或降低门槛改判。

权限与资源：只在自身Advisory工作树开发；固定来源只读、无DDL/DML/数据激活/依赖安装、无QE/Selection/HMM/Execution/Paper源码改动。实验临时X盘、正式产物F盘；长运行每半小时读进度，异常即进入定向修复/审核循环，修复改变模型/标签语义时必须新身份，不覆盖旧运行。提交合入及本轮精确清理沿已授权流程，后端重启由用户执行。无新生产接入时通常无需重启；不能因为48小时预算自动操作服务。

#### 16.5.1 G0来源归因与统一消费（2026-10-03）

检查只读取公开profile摘要、外部只读manifest/receipt和原`features.parquet`三个候选键列；没有打开labels/prices收益文件，没有查询数据库、读取sealed holdout、提交QE任务或拟合。原窗口`2024-07-04..2026-02-02`、386D、7720候选、938股票全部保留；活动profile在读前/读后字节一致。所用main已同步到`2bc5f0e09`，该同步不等于修改其他模块。

| 合同/对象 | 本次事实 | 可以证明 / 不可以证明 |
|---|---|---|
| 活动profile | generation=`20260928-v15-unified-moneyflow1`，release=`qe_hmm_full_v2_20260831`；profile文件SHA256=`56b4741044aa98750468b2d5b2b9ae888cf2abf9ba4df00e36019ebc3dd0cc6f` | 已公布六池覆盖；不是新行业特征逐D可知证明，也不是QE进程空闲验证 |
| profile原生sector pins | 公共`require_pinned_sector_context_files`读回code map、market context、membership、quote availability、receipt的文件hash/摘要均通过；membership hash=`959fe44300aa3fcb0c82f730bada5803a51d2f3042597871629d4deb99b97ce5` | 文件/日期身份PASS；不升级为历史知识时钟COMPLETE |
| 行业日期映射 | 原7720候选中7706唯一映射、14未覆盖；14条全部D早于当前canonical eligible_start，与profile股票池sidecar一致且原日线OHLC存在 | 14直接原因是旧冻结名单/当前252交易日准入边界不一致，不是已证明的停牌或日线缺失；保留原键，不重选/删股。原名单没有规则版本证明，不擅自断言当时使用365日规则 |
| profile绑定的原生classification | bundle=`051e2af357703734080ff3ea5b4311926905aa7cbd1f31d926ef5b8575261313`、receipt=`910f6056c5be943116415411d77617154749f670df4efdda0f93e16f70385b13`；公共`IndustryPitResolver`以AS_PUBLISHED_PIT严格求解：3729 resolved、3977 knowledge_time_unverified、14 authority_unavailable；train/validation/已消费test resolved分别2181/835/713 | 部分分类有因果证明；完整dated spans不能将其余未证明行变成已知。不是因分类覆盖不足100%而否定方向 |
| 原生指数成分身份 | 同一bundle只有4股的两段成分区间；原人口仅8/7720 resolved、7712 boundary_unavailable | 这是专门成分证据范围有限，不能解释为全市场行业数据缺口，消费者不以该源阻断普通分类 |
| 生成合同 | `frozen_dated_sector_assignment_then_c013_gap_fill_v1`保留日期化观察；builder绑定可包含历史回投的HMM研究basis，该adapter的historical模式明确为`STABLE_TAXONOMY_BACKCAST/non_as_known_taxonomy=true`；本receipt未报告所用active_mode | 不断言所有行来自backcast，也不单凭日期化生成链路承诺逐D已知。未证明记录不进入因果拟合；3729严格解析类别可以进入有界G1，不因其余UNKNOWN阻断基础设计 |

进一步归因：3977行各有唯一保留分类身份、known_from均为空，source_last_updated均存在；其中3367更新时间不晚于D、610晚于D，均不能据此伪造发布时间。公共builder只对2021-07-30分类切换识别特定知晓边界，其余一般历史记录按设计返回knowledge_time_unverified。这是知晓合同能力有限，不是50%以上行情缺失；共享builder不在Advisory修改范围。

**由本窗口完成消费修正，不设置外部窗口前置：** [统一消费F1设计](advisory_context_consumer_v1_f1_design_20261003.md)绑定同一活动profile及QE兼容六池sidecar；单池/并集始终与canonical股票池交集。原prepared计划/开发窗口、业务/政策、rank/hash及原键完整性严格校验；当前池外仅诊断，旧冻结人口不改变。普通行业分类仅用已证明AS_PUBLISHED_PIT，知晓UNKNOWN保持null；不消费专门指数成分作为全量门禁。core-only准备可不请求行业文件，增强PARTIAL独立报告。请求的身份/hash/歧义冲突仍fail closed；正式自然采集/confirmation/实盘不走该探索入口，不改原严格校验。

本次G0真实prepare已完成：原386D/7720键全部保留；当前池内7706、池外14，日期映射7706、严格分类3729、知晓未知3977、分类不可用14。core输入身份VERIFIED、可选分类PARTIAL、原RECOVERED_LIMITED未升级；此G0检查点无DB/收益/fit/服务操作。后续G1/#5383及G2/G3接续见§16.5.2，不能把当时零fit视为当前状态；行业行情若另行新增仍须审定公开时钟，普通可选缺失不阻断整个研发。不需后端重启。

#### 16.5.2 H-CONTEXT-VALUE-1实际研究及源码交付（2026-10-04）

设计#5383已合入`7c146a3e0d2113762e4a9677e9740e4aac2cf4d6`，必需CI37140950326 SUCCESS。G2真实拟合源码`f89600371567ac95d5053138702cde680241a104`；精确五源码/五测试/两设计文档已由#5388合入`d18dfb14e996ea7100f843bd7a03d1694e9619ef`并清理。最新main同步带入其它窗口已合入代码不等于本窗口修改其它模块。该pipeline只离线登记/准备/拟合/评价，pure price-set允许空集/多段，不接生产family/API，不更改原退出或策略排序。

唯一study `advctxvalue_a551b5933b0dffc5b3877bd1`，plan SHA=`a551b5933b0dffc5b3877bd1cfe9688e81f035b47a132bba7f112a946b40bd4c`；输出根`F:/Dev/AIstock_model_artifacts/advisory_context_price_value_v1_20261004/advctxvalue_a551b5933b0dffc5b3877bd1`，evaluation.json SHA=`45c9ab50b005cfa066568cb2e53f0b176d61ddba9c038ac7fdeda23bf3347e8c`。既有JSONL registry追加PREREGISTERED/PREPARED/FIT_STARTED/TRAINED/EVALUATED，planned/generated/evaluated候选各1、selected0，不将四物理头计为四经济candidate。857/304共同监督；原386D/7720键完整，训练支持10类别/16cells。拟合前后QE公开三运行路径均0，未启动QE实验；训练段噪声代理仅探索分类，未读sealed或新确认窗口。

| 比较/指标 | 事实 | 判定边界 |
|---|---|---|
| 完整四臂 | 81决策日/1620候选、100共同估值日；baseline/rule/matched/candidate名义净收益21.3220%/20.5747%/34.0460%/33.0073% | 同VALUE_REVIEW_5_V1、成本及shadow坐标；非沪深300超额、非真实成交、不是新包迁移验证 |
| 配对增量 | candidate减baseline +9.2802bps/日，block95%[-3.5941,25.1664]；减matched -0.8533bps/日，[-8.3361,5.9988] | 同开发窗口已消费；区间均跨0，matched项<5bps，净增量条件失败 |
| 真实支持 | 相对baseline/matched实际进入变化36/30目标日，原81决策日分母；candidate Top5 TAKE14/SKIP57/UNAVAILABLE334 | 干预门通过；实际模型TAKE4<30，研究控制89笔不算模型TAKE；matched真实TAKE3/控制90 |
| 风险与完整性 | baseline/matched/candidate MDD -10.3314%/-8.0929%/-8.5670%；MDD/尾损非劣门通过；四臂endpoint限制0/held-mark问题0/未退出0 | 风险与完整性不能替代收益/真实TAKE；日级端点非成交证明，不改RECOVERED_LIMITED |
| 结果及后续 | `STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION`，一candidate/四fit/零selected，NAVIGATION_ONLY/deployable=false | 停止本假设，不回选matched/规则，不降低支持/扩大样本/阈值或补证确认；下一新增经济信息仅独立设计 |

审核分三轮本窗口自审，不冒称独立外审；定向14项、Ruff及F2结构检查通过，真实stage链/键/fit/四臂共同净值及registry另一次只读一致性审查通过，没有重复研究。源码、消费输入、负向导航、经济确认和正式启用分别报告。R1 G4源码合入及自身官方清理均完成；DB/DDL、数据激活、依赖安装、服务重启、QE/Selection/HMM/公共数据/Execution/Paper源码修改均0。经济确认/ENTRY_VALUE启用仍0；离线合入不要求后端重启。

### 16.6 当前R2：连续不同条件价格模型（2026-10-04）

用户授权48小时预算`2026-10-04 02:36～2026-10-06 02:36 Asia/Shanghai`，是同一价格主线串行不同假设，不是六条并行项目或无限调参。最新18h工作段2026-10-05 00:08～18:08不重置原48h截止。M2/M3/M4已完成11fit+1索引，M1、M5、M6、M7、M8、M9、M10各完成4fit，整批39fit+1索引。当前M1日频源码已有真实业务但仍待六UI/公共BUG流程；M6/M7已合入/清理，M8 #5454/#5455亦已合入/自己清理，当前M9设计#5456已合入/清理，源码43直接项及一次完整研究完成、#5457已合入4f7793a0f并自身清理；M10设计#5458已合入/清理，38直接项及一次四fit/完整四臂完成，源码#5459已合入7aed83cc6并自己清理，各负候选不调参重跑。下一输入功能与独立价值目标按§16.6.3分离交付，不再同一固定信息只换参数。M8一次prepare/fit/完整四臂实际完成，不把19-return风险改善等同收益；确认可选，不阻断功能，不凑时长搜索同信息参数、不读取sealed或把开发导航当确认。

| 顺序 | 路线 | 唯一比较与阶段边界 |
|---|---|---|
| 0 | 统一设计/公共纯合同 | 原386D/7720键、12D+价格、VALUE_REVIEW_5_V1、费用及完整组合不变；新共同全局支持在结果前冻结，不改旧行业支持结果；先设计多轮审核合入 |
| 已完成1 | M2 H-NONLINEAR-PRICE-2 | 4fit/完整四臂；减baseline/matched日-5.0797/-14.9636bps、真实TAKE81，净增量/MDD门失败；已接续M3 |
| 已完成2 | M3 H-HURDLE-PRICE-2 | 5fit/完整四臂；日-7.2904/-7.8016bps、真实TAKE89，净增量/尾损门失败；已接续M4 |
| 已完成3 | M4 H-LOCAL-DISTRIBUTION-2 | 2fit+1索引/完整四臂；日-5.7956/-0.7159bps、真实TAKE78，净增量门失败；继续下一新信息设计 |
| 已完成4 | M1 H-SECTOR-DYNAMIC-2 | #5411设计已交付，#5414源码及研究；1795成熟train/765validation，4fit/完整81D四臂；两个配对增量6.9758/5.3007bps，36真TAKE，冻结五导航门通过，累计15fit+1索引 |
| 已完成5 | [M5新信息研究](advisory_selection_state_price_value_v1_f2_design_20261004.md) | 一次4fit/完整四臂，82真TAKE/6 UNKNOWN控制；candidate减baseline -6.8073bps，未通过旧净增量指标，仍诚实报告而非包消费门禁 |
| 当前工程接续 | QE包直接消费与M1日频API/UI | #5445实现与43直接项/真实ASGI/20日400候选批量通过，尚未六UI验收或合入；BUG-1726局部71项通过，尚待公共端点smoke合同，不改公共流程或用泛health降级。旧M1结果与两个跨零区间不改，用户重启另报 |
| 已完成6 | M6 H-DAILY-MONEYFLOW-PRICE-1 | 设计#5450/源码#5451已合入/自身清理，48直接项/一次四fit/完整四臂；候选17.4101%<基线21.3220%及matched18.9958%，净增量/尾部条件失败，仅STOP本候选；历史累计23fit+1index，不重跑 |
| 已完成7 | M7 H-DAILY-PRICE-PATH-1 | #5452/#5453设计/源码均合入/自身清理，30直接项及一次四fit/完整四臂；相对baseline日+2.0581bps<5bps且区间跨零，仅STOP本候选；累计27fit+1index，正常缺失原键全保留 |
| 已完成8 | M8 H-DAILY-MARKET-RISK-1 | [M8设计§15](advisory_market_risk_price_value_m8_f2_design_20261005.md)#5454/#5455已合入/自己清理；39直接项/一次4fit/81D四臂，candidate 48真TAKE+8 UNKNOWN控制；配对日-7.7890/+7.0702bps均跨零，收益条件失败，仅STOP本候选。累计31fit+1index，当前source476537d77已交付，不重跑旧研究 |
| 已完成9 | M9 H-DAILY-VOLUME-COST-1 | [M9设计§15](advisory_volume_context_price_value_m9_f2_design_20261005.md)#5456已合入/清理；83真TAKE+4 UNKNOWN控制、配对日+3.9776/+18.8368bps，基线增量<5且区间跨零，净条件失败仅STOP自身。一次4fit后总35fit+1index，旧23/27/31默认不变；源码#5457 CI37238797921 SUCCESS后合入4f7793a0f并自身清理 |
| 已完成10 / M10交付 | M10 H-DAILY-BREADTH-STATE-1 | [M10设计§15](advisory_breadth_state_price_value_m10_f2_design_20261005.md)#5458已合入/清理；原7340宽度历史AVAILABLE/380预热、成熟train3693/195D、78真TAKE/4UNKNOWN控制，一次4fit及81D完整四臂完成。相对baseline日-8.8603bps/区间跨零，仅STOP本候选；总39fit+1index、旧23/27/31/35默认不变，源码#5459合入7aed83cc6/自身清理，不重跑 |

每个run的baseline/±300bps规则/matched/candidate全人口同日历；UNKNOWN研究控制贡献独立，非模型TAKE。停止门及数值由详细设计§8先冻结，未过者仅结束自身lineage，不追加旧失败证据。所有实际fit、工程失败和已消费窗口进入最小现有registry及campaign总账；纯JSON模型parity、完整端点/持仓mark与真实maturity保持fail-closed。不为探索正点估计声称DSR/PBO确认。

整轮停止仅用户停止、48小时到期，或全部预定路线实际完成/真实阻断且无剩余有价值设计；不是首个实验失败、一次CI等待或训练只耗秒级。长实验每30分钟检查，短实验完成后接续下一路线，不凑时间。必要源码先多轮审核修复，沿已授权提交/PR/合入/自身清理；只读固定来源、X临时/F持久，拟合不与QE训练并行。QE/Selection/HMM/公共数据/Execution/Paper源码、DB/DDL、数据激活、依赖安装及服务控制均不授权；后端重启仍用户执行。

本版三轮本窗口自审分别核对方法/标签（修正零原子）、时钟/公平支持、工程/完整证据边界；与R1历史停止及当前队列前后一致，不冒称外审或实现完成。F-266的§16.5历史fit=4保持；当前连续任务另按R2设计F-689～F-696验收，不用旧单次预算否定新用户授权。

#### 16.6.1 已交付三路线与M1接续

#5404最终HEAD80e924298a351d9e16e73d2096ae4ff48a568af1，CI37148272127 SUCCESS后合入5f122f22a5a545f28d2ed80117d52be88e9fc0d2，官方cleanup_done/warnings0。真实fit源码仍1a66ca00067df22eb55a22ad6e8c5673eb61c9d0，后续仅文档更新，不冒称用新HEAD重训。三candidate/11fit+1索引/0selected，原386D/7720键完整，新共同监督4049train/1591val，已消费test完整81D/1620候选/100估值日，不与旧R1监督混用；详细三路线负结果见R2设计§17。源/研发交付不代表收益确认或ENTRY_VALUE启用。

M1来源仍仅结构映射而非原生成员恢复：134个L2显式pair，profile H5 sha575ad57d...；原7720键与50180日/id报价保留，3505 AVAILABLE/3991分类或映射UNKNOWN/224warmup，原未知3977及池外14不升级。05:13 QE三公开running均0后，以源码5afdabd93c75ebc2f52f008d8ff3852fec2a7ee9执行一次4fit（9.734秒）及完整四臂（24.703秒），拟合后QE也均0。候选/基线/匹配/规则的100共同估值日net为29.7444%/21.3220%/23.1821%/20.5747%，非指数超额或真实fill。候选配对减基线/匹配为6.9758/5.3007bps，95%描述性block区间[-9.5384,24.2783]/[-2.1165,13.7297]；MDD为-8.8860%/-10.3314%/-9.0927%，进入差异51/37日。候选93episode中的36真TAKE及57 UNKNOWN控制分账，不能把全组合29.74%归给模型。端点/held-mark/未结算审计均无阻断，五NAV门通过，但确认/启用0。详见动态板块设计§15；不重新拟合、不调整阈值，继续独立确认设计。

#5414最新HEAD e25d920923c269df22b11493a85f1fa4011562d1、新CI37154810577 SUCCESS后合入9ec698502dd91796070d4e6b1e64971c3a5c322a；自身官方清理完成，研究产物保留。确认设计F-713～F-720仅design-only，独立效果研究窗口/推断尚未验收；该可选工作不阻断消费者接入。父训练时钟由QE负责，不再要求Advisory验收，不硬编码1885天或等待自然20D，未授权sealed仍不读。

05:35～05:43元数据spike：父包已有冻结prediction指针、两腿权重和Alpha158DL schema，但无可验证腿训练information_end/完整processor及新窗口候选声明；combined模型字段未绑定和components=0不等于没有legacy模型。A直接消费未证明/B仅需现存模型推理UNPROVEN/C需要新模型也未证明，不自动重训。另发现公开model-state GET会upsert模型状态元数据；此前一次调用触发该写路径，已停止调用并向用户披露，不回滚、不修改StrategyPackage，也未触发训练/激活。后续只读路径先核定无副作用，不用“GET”标签宣称DB noop。此权限偏差不改变旧模型研究数据或给其它模块修复扩权。

本轮48h目标截止仍2026-10-06 02:36，不重计。用户2026-10-04取消重复QE资格与M1正后全局停止：按P1直接消费/日频功能、P2独立M5新信息研究继续，不等待确认或父证明；不以凑时长重跑旧研究，真实研究与业务结果分别报告。产品goal此前BLOCKED是控制面历史状态，不把该标记冒称当前源码/研究已完成。

09:52历史快照：#5428合入fa4b2d9a4、#5429合入26b708ea14且两树官方清理完成，当时运行后端待用户重启，本窗口未控制服务。M5已完成新登记研究，详见[M5设计§17](advisory_selection_state_price_value_v1_f2_design_20261004.md)：3693成熟train/1591诊断validation、4fit/原81D完整四臂、累计19fit+1index，负增量只结束该精确假设；0新窗口/sealed/DB写入/QE提交/激活。合并最新main只接收他人已合入变更，不修改QE或公共数据源码。确认是可选效果研究而非启动条件。

v4.04用户重启后接续事实：#5432和#5434已合入/各自官方清理，后端GET health及静态runtime identity匹配5c51aac49d7cf951f2835a5e68a04c837f803162；只读preflight直接接受现有包。按[family F1](advisory_sector_daily_family_v1_f1_design_20261004.md)七文件精确范围完成D可见公司分类和M1显式日频消费者，27定向测试/三轮本窗口自审通过。真实原2024-08-01全20候选经公开D分类与既存DB 7个行情SELECT，约8.516秒输出9条条件价格集合/11条UNKNOWN，15D及所有区间与原M1数学严格一致、最多1491完整tick，原model/bundle hash不变。无新收益/label/T行情/sealed/fit/数据库写入/QE提交/角色激活，分类证据和原native不升级。不是收益确认或API/UI完成；主线下一步为原daily名单DB适配、显式HTTP/API/UI及同核批量验证，不新增资格/固化项目。

#### 16.6.2 已完成M10：市场宽度历史状态与交付边界

原当日market_up_ratio在386个D一致且已知，固定20原session的均值、线性趋势、低于0.5占比作为唯一新增块。M10设计#5458已合入/清理；实际一次prepare2.094秒保留7720键、7340 AVAILABLE/380 UNKNOWN_20D_HISTORY，无新SQL/数据补齐/股票池扩大。源码296689715、多轮自审/38直接项/Ruff/F2/L0及真实旧M1 bundle数学兼容通过；新run advbreadthstatevalue_5f62ec8b6154418f9c10809e一次四fit/完整四臂21.407秒，相对baseline日-8.8603bps并跨零，风险改善不替代收益，只STOP本candidate。实际累计39fit+1index，source#5459当前HEAD696d19bb9/CI37243602139 SUCCESS后合入7aed83cc6并自己官方清理，旧23/27/31/35默认和旧工件不变。不是HMM/alpha/分钟择时或包使用门禁，无确认/运行启用。

M1日频模型仍有真实Program、LSTM/FUND两腿和原value-label policy作用域；这不是QE包资格，但不能宣称任意策略包通用。[通用输入F1](advisory_generic_price_input_v1_f1_design_20261005.md)已立设计，独立通用价格价值目标需后续F2与明确产品口径，不通过删身份检查/伪造leg/补零或事后改旧退出政策解决。M10保持原价值目标，工程辅线#5445当前d1ccaae19/CI37240066029 SUCCESS，仍与BUG-1726分别等待六UI/公共业务smoke；不借CI排队重跑旧研究或改公共模块。源码、消费者、经济效果和用户重启/配置均分报。

#### 16.6.3 当前通用价格模型：先解除输入依赖，标签与参数另验

唯一主动模型主线接续[通用日频价格输入F1](advisory_generic_price_input_v1_f1_design_20261005.md)。独立原名单/来源身份、九个D股票/市场字段与future模型label_contract分离；0～50原股票保留KEY/rank/order，不进入九字段的父score、rank、LSTM/FUND腿差不再是通用入口必需数据。真实原session、D复权坐标和消费时钟明示；普通停牌/缺bar逐字段UNKNOWN，不删日期或候选、不复制Selection。单指数/index_union等原股票池身份透传，不重建池或修改QE。

本切片仅纯输入计算，零DB/训练/收益/确认/sealed/激活和API/UI；四文件源码已实现并直接验证，源码#5461已合入0c33faa5c并自己清理，不能宣称通用荐股完成。相同股票/行情/市场定义跨包数值一致，只证明输入可移植；原M1权重、复评退出Y、支持/模型scope及旧结果保持不变。独立固定5交易日价格价值目标与原策略复评退出适配尚待产品口径，后续F2必须显式预注册，不能事后改旧lineage或将五有效复评称为五交易日。新研究仍需真正新假设、冻结label/对照/资源与QE fit互斥；本输入设计/实现不增加实际39fit+1index或收集旧负实验补证。未知基准/市场不构成整名单门，后续模型须显式规定自己的字段/缺失策略。 当前新信息研究M11沿原已明确VALUE_REVIEW_5_V1单独立项，不默选通用目标，也不因为该选择待答复阻断其它有价值设计。

设计#5460 HEADbf7e9cb26/CI37244786687 SUCCESS后合入d78b2e053a9375484c18d972948b8a9744540769并自身官方清理；源码随后同base独立树只登记新叶/直接测试/详细设计/蓝图四文件。三轮本窗口自审修复ATR首日前收未消费H/L及Decimal signaling NaN typed失败，稳定26直接项/Ruff/两L0均通过。真实冻结2024-08-01原20候选/400历史bar/20基准行，8字段各20已知、宽度因分母未证明保留20 UNKNOWN，0.125秒/0SQL/0fit；改包声明数学相同，文件前后hash不变。来源D边界非capture/known_from，NV/native UNPROVEN不升级。M1六UI与BUG公共smoke是唯一工程辅线，未交付不冒充完成；原18h/48h截止不重置，后端重启仍user-owned。 source#5461 current eff5947a6/CI37245983776 SUCCESS（5m39s）后合入0c33faa5cd970650263e16cbb8b916f727fa000f、官方cleanup_done；下一研究不把输入功能当模型或激活。

#### 16.6.4 已完成M11：既存成交量在价格轴上的分布

[M11 F2详细设计](advisory_traded_price_distribution_m11_f2_design_20261005.md)与原M9均价/涨跌量/日期量HHI区分：固定20原session、同D价格与量复权坐标，考察成交量在D价格以下的占比、加权价格离散、±1个原D ATR内局部密度；不是持仓成本/真实筹码、支撑保证、QE Alpha因子或分钟执行。既存M9 volume artifact与原frozen raw价格/ATR输入只读，工程几何检查7720/386D全保留，7330完整20session/380预热/10正常缺行、0.860秒、0SQL/fit/收益。

此为现有明确VALUE_REVIEW_5_V1合同的单条新信息研究，不替用户选择独立通用label、不回退generic输入功能或启动第二模型线。12D+本块三字段+g/13D matched同核、原同监督/支持/标签/cost/完整81D四臂保持；先F2重复审核/合入，再最新main独立树登记12文件/重复源码审核/新plan一次4fit。原M10真实39加拟议4上限43，立项时实际仍39fit+1index，随后完成43见下，旧23/27/31/35/39默认不变；不重训M9或为旧负候选补证，未知正常数据保留原股。

模型训练前后QE三running互斥、X临时/F新独立工件、原18h/48h截止不重计；不改公共模块/DB/profile/用户进程，不读sealed/新holdout或自动绑定。源码、真实研究、收益确认和运行态分报，只有本新candidate负向才停止它，不由M1工程依赖或通用label选择阻断全部演进。

后续真实完成：producer49f396e/plan6649fbb9.../run advtradedpricedistribution_6649fbb91b2292276a9634f6，一次7.578秒prepare7330AVAILABLE/380预热/10正常UNKNOWN；一次四fit/四臂21.406秒，QE前00:52:07UTC及后00:53:28UTC三running0，0新SQL/DB写/激活。candidate/baseline/matched/rule成本后100共同日净收益22.5023%/21.3220%/4.5719%/20.5747%；配对baseline日+1.0512bps区间跨零、matched+15.9105bps描述区间为正，但前者低于原5，尾部较baseline恶化46.9917bps>原20，net_increment/tail false，仅停止本candidate。82真TAKE/5UNKNOWN研究控制/87完成episode、无未结算，各53进入干预T映射53原D/81；实际43fit+1index与0经济确认/激活。以上诊断不能冒充独立OOS、真实fill或策略包无Alpha结论；源码#5463 HEAD643ebbfe6/CI37249491174 SUCCESS后已合入f6aef87db4e50e1a2b5a2a407f0128ee8b685708并自己cleanup_done，原39→43是本研究真实完成，不重跑旧M9或补证。

#### 16.6.5 已完成M12：历史隔夜／日内路径信息

[M12 F2详细设计](advisory_session_path_m12_f2_design_20261005.md)固定20原session/19间隔，用已有历史Open将同Close路径拆成隔夜跳空与日内开收：两部分log-return均值及隔夜绝对幅度均值。旧M7只收盘路径、旧12D无此19日Open分解；不是预测T开盘、策略Alpha或分钟执行。第0 Open未消费、gap需前19 Close/全20 AF，日内需后19 Close/不需AF；缺失逐字段UNKNOWN、原日历/候选不压缩或删填，常价/无跳空合法0。

事前工程几何7720/386D/38168请求对保留，7330完整/380warmup/10正常缺行，5.047秒/0SQL/fit/labels/收益数学，非vintage/native UNPROVEN不升级；设计当前从M11 merge f6aef87db同步。先多轮F2设计交付，再最新main独立树精确12文件，实现薄原子阶段与同核13/16监督；现有明确VALUE_REVIEW_5_V1/支持/成本/四臂不改，不默选待答复通用label或复跑旧负候选。

实际43fit+1index，M12拟议4fit/47上限依赖真实M11 evaluated，仅显式session_path_extension且旧23/27/31/35/39/43保持；立项时0M12拟合/prepare/收益，随后一次完整完成47见下。QE三running训练前后互斥、X临时/F新独立、原18h/48h截止不重计、零公共模块/DB/配置/激活/控制。M1六UI和公共BUG是独立工程辅线，无新收据不冒称通过，不导致整个R2停止。

后续一次真实完成：producer4fb26cc/pland7b4e1a0.../run advsessionpathvalue_d7b4e1a0a513fe61c9eacf52，先登记后6.750秒prepare7330AVAILABLE/380预热/10正常UNKNOWN，01:24:25UTC QE三running0后四fit/完整四臂21.406秒，01:26:13UTC后也0。candidate/baseline/matched/rule成本后100共同日净收益4.8425%/21.3220%/4.5719%/20.5747%，日配对-14.8813/-0.0220bps均区间跨零，net false而干预/MDD/TAKE/tail true，STOP仅本candidate。74真TAKE/4UNKNOWN研究控制、78episode全结算，干预T60/41映射原D；跳过34原进入中24盈利/10亏损，不作为放宽旧阈值理由。实际47fit+1index、0DB/控制/激活/sealed/经济确认，源码#5465当前HEAD0f0e8c65a/CI37251660467 SUCCESS后合入1f85b44ff658d1a93ea00940151deef38bfafca7并自己official cleanup_done；不因首个/一批负实验停止整个R2，继续真正不同新信息。

#### 16.6.6 已完成M13：自由流通换手与股本新信息

[M13 F2详细设计](advisory_free_float_turnover_m13_f2_design_20261005.md)考察自由流通换手20原session均值/标准差及D自由股本log规模；不是已有按本股成交量归一的量价权重、实际筹码或QE因子挖掘。唯一新增market.daily_basic/turnover_rate_f百分数与free_share万股，既有源映射/官方单位核实后固定fraction/log股单位，D盘后可消费声明不冒称原生known_from。旧VALUE_REVIEW_5_V1、支持、成本、四臂与原7720/386D全保留；历史换手缺失只UNKNOWN前两量，D股本独立，零换手合法，历史未消费share或未来毒化不阻断。

事前0trial工程聚合spec de29a8d1...仅一条SELECT/1.438秒，38168请求对/38158现存键、7330完整非空/380预热/10正常缺行、D股本非空7720；未读实际源值/标签/收益/fit，不等于数值或模型验收。正式prepare最多单30秒参数化只读请求/rollback/close、F新basic冻结工件，再处理实际非法数。先设计三视角修订/七F2/精确两文档/当前CI交付，再最新main独立树登记12 Advisory叶/测试/文档；纯3字段与同13/16核、真实M12前驱/显式free_float_extension47→51，旧默认不改，无原包资格门。

立项时实际47fit+1index、M13独立12文件source已实现/三轮审核/稳定52直接项/Ruff/L0及原M1 bundle兼容通过，研究前预登记/prepare/fit/收益为0，随后一次完成真实51+1见下。设计#5467已合入/自己清理，源码已ff最新main93c600814，QE三公开running0才单次4fit/完整四臂；负向只停止自身candidate，正NAV也不称独立确认/泛化/激活。X临时/F新独立，无公共模块修改/DB写/服务控制，原18h/48h截止不重计；M1工程辅线等精确六UI/公共BUG语义，目标是价格业务与可验证收益而非历史固化。

一次真实完成M13：producer4f37f85d8/plan ab0773ce.../run advfreefloatvalue_ab0773ce59d14dff25a2c80f，先干净源码/预登记后prepare7.453秒、单只读SELECT及38158新冻结basic行，7720键保留7330AVAILABLE/380预热/10正常UNKNOWN；QE前01:59:08UTC、后02:00:41UTC三running0，一次4fit/完整四臂21.782秒，成熟3693train/195D、1591val仅诊断。100共同日candidate/baseline/matched/rule净12.0020/21.3220/4.5719/20.5747%，配对日−8.1577/+6.7015bps，两区间跨零；net false、干预/MDD/TAKE/tail true。80真TAKE/5UNKNOWN控制、85episode全结算，58/54干预T映射原D；只STOP本candidate，不回选/调参/重跑/确认/绑定。该次完成时累计真实51fit+1原index、0经济确认/sealed/激活/DB写/公共修改/进程控制，源码#5470当前HEAD1a864561f8/CI37253862090 SUCCESS后合入e86e2ca9c48a4a6d7946a5c36d7ef42aa3fafb7e并自己official cleanup_done，继续真正不同路线而非历史补证。

#### 16.6.7 已完成M14：估值条件下的买入价格净价值

[M14 F2详细设计](advisory_valuation_context_m14_f2_design_20261005.md)固定D PE_TTM倒数、PB倒数及过去12月股息百分数/100，提供旧12D及量价相对权重没有的原始盈利/净资产/分红估值上下文；不承诺低估必涨/年度股息就是五复评收益，不研发QE Alpha、分钟执行或重排原候选。PE/PB负值保留有符号倒数，恰0分母局部UNKNOWN、股息0合法；实际坏数或转换/倒数溢出报计算错误，未来/无关宽源先D键投影，不删填原股、无20D预热门。

0trial单有界只读聚合spec8aa0aa63.../1.078秒，原386D/7720候选D/instrument与PE/PB/dv_ttm非空各7720；只键与非空，未读源值/Y/收益/fit，不当原生/数值或经济验收。后续唯一新增market.daily_basic/D三字段，最多7720请求/单30秒SELECT/rollback/close；D盘后声明与非vintage/native UNPROVEN限制保留，不重验QE父资格。

M14设计#5474已合入55d519c1e/自己清理，源码#5478 HEAD34981e815/currentCI37258371053 SUCCESS后合入0146ff7aacafc34f201489b9b3192f3f8851b5f2；稳定58项/Ruff/F2/L0及原M1 bundle兼容完成。原失败plan265ff616保留且0fit，新producerc58f4e6a/plan46e89f537a689f18523151db7937020d36202bfc89da061f8b937f8bbbc7d31d/implementation206c7480.../run advvaluationvalue_46e89f537a689f18523151db已prepare3.328秒/1SELECT，原7720键保留、2952 AVAILABLE/4768 UNKNOWN，NON_VINTAGE/native UNPROVEN。等待QE约半小时只读检查，07:14:38UTC三running归零后仅从原冻结实施闭包一次4fit/四臂26.547秒；train1406/212D，766val仅诊断。原81D/1620候选/100共同NAV日candidate/baseline/matched/rule20.2050/21.3220/14.2643/20.5747%，paired日baseline−0.9490bps CI[−10.7320,8.5428]、matched+5.1142 CI[−3.0655,14.4080]；20真实模型TAKE/69 UNKNOWN控制、89episodes全settled。net与最低30真TAKE不通过，其余三项通过，只STOP当前candidate，不确认/调门槛/回选matched/补证；UNKNOWN控制不冒充模型支持。该次累计55fit+1index，随后M15/M16完成后全轮63+1；源码、研究完成、NOT_CONFIRMED与runtime未启用分别报告。拟合后07:15:39UTCQE三count0，无QE提交/公共改动/DB写/sealed/activation/服务控制；三研究闭包消费完成后才精确自身cleanup，M1工程依赖保留。

#### 16.6.8 已完成M15：历史涨跌停状态与条件进入价值

[M15 F2详细设计](advisory_limit_state_m15_f2_design_20261005.md)固定D向前20session上/下合法界触及比例与D收盘在真实合法界内的位置；原raw_daily schema已有七字段，0trial spike仅metadata、0价格/收益数组/DB/fit。新增信息是法定界与历史触板状态，不是旧price-path调窗口或分钟执行研发；只评估给定可见价格下净价值，不改变原候选、政策/标签、支持、成本或涨跌停制度。

M15设计#5479三轮修订/F2/CI后合入eba96ae43并自己清理；12文件源码57直接项/Ruff/F2/L0/原M1 bundle兼容通过，#5480 HEADbcd05f179/currentCI37260309258 SUCCESS后合入a8ae765ed02422a511df9a31dd7ed08ce9be85b1。工程交付时研究0，原树保留，未制造M14前驱。M14真实55/完整evaluated后，plan27b72172b57b266dddc1476688efdc1ca26dbe2649d4f6617496e9a8f32bc11f/implementation7ed1d3bc.../run advlimitstatevalue_27b72172b57b266dddc14766一次预登记/0SQL prepare10.672秒：7720原键、7330 AVAILABLE/380预热/10正常UNKNOWN。07:17:08UTC拟合前及07:17:39UTC后QE三count0，一次4fit/四臂28.672秒；train3693/195D、1591val仅诊断。原81D/1620候选/100共同NAV日candidate/baseline/matched/rule14.1018/21.3220/4.5719/20.5747%，paired日baseline−6.2150 CI[−26.2633,13.5466]、matched+8.6442 CI[−1.6249,21.2771]bps；78真TAKE/5UNKNOWN控制、83episodes全settled，只有net不通过，只STOP当前candidate。该次真实59+1，随后M16至63+1；缺值不删填/不重跑/不回选matched/不补证或绑定。冻结source闭包已实际消费，清理按精确自身官方流程另报；非原生/非独立OOS/未确认与未启用边界不升级。

#### 16.6.9 已完成M16：原候选群体横截面状态

[M16 F2详细设计](advisory_candidate_cohort_m16_f2_design_20261005.md)固定原D候选群体ret_1均值、population std及上涨比例；这引入其它原候选的D信息，不是同股已有字段单变量变换、M5历史榜单或M10全市场宽度路径，也不是上游QE alpha研发。原群体先于label/成熟度/买入动作筛选定义，正常缺一原成员/值/clock则当天该块UNKNOWN，所有原股保留，禁止删成员后改分母或从Top40/未来/Y补群体。

事前metadata spec c6d4fe38...仅原reusable prepared D表7720行及五字段存在，0实际值/标签/收益数组/fit/DB；不是经济可学或原生证明。设计#5481三轮时钟/数值/人口/预算修订，F2/CI后合入8ffce17015且自己官方清理。独立12文件source的纯三量、0SQL阶段、显式63与三路由保持共同13/16监督/支持/成本/四臂；修复重复rank合成fixture后稳定53项/Ruff/F2/L0/原M1 bundle兼容通过，#5482 HEAD5f611fe25b9ce78ae12272ad258eb1dda01e9615/currentCI37262710010 SUCCESS后于04:30:00UTC合入7521de2fdded0225983b7c4a07da1c52bb8fe298。源码交付时研究0、保留原冻结树，真实M15 evaluated/59后才正式注册/prepare，不把N只同日股票视作N个独立市场状态。

M16 plan99155bf19bf9cf1af6b5fa7e64f0388b4eb147bdb735a46970711c6f49bb3dab/implementation6e97f322.../run advcandidatecohort_99155bf19bf9cf1af6b5fa7e一次prepare2.25秒、0SQL/7720原键全群体AVAILABLE；07:18:22UTC拟合前、07:18:55UTC后QE三count0，一次4fit/完整四臂30.844秒，train4049/214D、1591val仅诊断。原81D/1620候选/100共同NAV日candidate/baseline/matched/rule25.5593/21.3220/15.3803/20.5747%，paired日baseline+3.4848 CI[−19.1286,24.5484]、matched+8.5645 CI[−5.5995,25.6092]bps；83真TAKE/6UNKNOWN控制、89episodes全settled。干预/TAKE/MDD/tail通过，net未通过原5bps且区间跨零，仍STOP本candidate；正名义收益不能冒称增量可复现/经济确认或部署，不调阈值或在同一窗口筛变体。真实累计63研究fit+1旧index，旧23～59身份/cap保留；所有研究仅已消费开发窗口NAVIGATION_ONLY，无sealed/新OOS/生产启用/DB写/公共模块改动或服务控制。三项研究均已消费各原冻结实施闭包，可执行精确自身官方cleanup但不删除正式F模型/输入/结果；M1六UI/BUG公共smoke两个未合入树继续保留，原18h/48h截止不重计。
#### 16.6.10 M17：既存五日资金流的有序持续性（已完成/停止本候选）

[M17 F2详细设计](advisory_flow_path_m17_f2_design_20261005.md)不改变M6的五session窗口，只引入其三汇总量未识别的路径顺序：净流入日占比、D末端连续净流入长度/5、四相邻imbalance绝对变化均值。原M6冻结17013股票日/四侧大单金额已存在，metadata及合成反例已证明同旧摘要可有不同路径，设计时未读价格/收益数组或进行新fit；随后研究按事前冻结合同实际执行；0SQL、不做当前数据回填，不把大单当机构身份。正常NULL/零gross局部UNKNOWN、零净流入中断run但不缺值，future/外股宽源先真实消费投影；不改原候选/标签/支持/成本。

M17已从干净producer f5918b5一次预登记/0SQL prepare8.562秒，run advflowpath_e91edf35d6c73780264a4817/plan e91edf35.../impl d9bd5f11...；7720原键保留，7640AVAILABLE/80零gross UNKNOWN，原M6冻结17013源行只读复用。08:20:55UTC拟合前及08:22:42UTC后QE三running0，一次4fit/完整四臂28.094秒，train3980/214D、1591val仅诊断。原81D/1620候选/100共同NAV日candidate/baseline/matched/rule16.2467/21.3220/19.6497/20.5747%，paired日baseline−4.3050bps CI[−20.9002,10.9491]、matched−3.0266 CI[−18.0195,7.3037]；85真TAKE/2UNKNOWN控制、87episodes全settled，只有net不通过，仅STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION。真实累计67fit+1旧index，旧caps不变；新研究仅NAVIGATION_ONLY，0sealed/确认/activation/DB访问写入/QE公共修改/服务控制。源码54直接项/Ruff/F2/两L0/旧M1 bundle兼容通过，源码#5485当前HEAD2d4fcf096/currentCI37283603330 SUCCESS后08:35:05UTC合入4708dd6a74447e1b9a8a6e44a0dfa4bf084d8f96；自身official cleanup_done/18.438秒，无blocking/warnings，F正式产物保留；M1六UI/BUG公共smoke两树继续保留，原18h/48h时钟不重计。

#### 16.6.11 下一M18：历史收益非对称幅度与下跌聚集

[M18 F2详细设计](advisory_asymmetric_risk_m18_f2_design_20261005.md)检验固定原20session的19log-return两侧RMS与18相邻下跌能量，区分同末端涨幅/趋势但幅度分布不同的历史风险。不改窗口、seed/loss/标签/成本；先log原close+log同日adj_factor避免乘积溢出，正常缺源逐股UNKNOWN，平坦或全上涨时合法0，不做QE alpha/分钟执行或重新Selection。

M18源码已在latestmain e4c9f7771独立12文件范围实现，clean producer397aaa83f624a2670cef2642adf89f1635d51a96；固定三量/共同13-16核/显式71及旧caps，正常UNKNOWN和原价值锚不改。三轮自审修复后56直接项/Ruff/F2/两L0及原M1 bundle只读兼容通过，尚非模型有效。一次新预登记run advasymrisk_d7d0e9676f7177bb1fbc2cc8/plan d7d0e9676f7177bb1fbc2cc8c0c52ac056c780c864060c23725a45b6f86d17f4/impl a5022095...，0SQL prepare8.594秒/7720原键/原380828价格源：7330AVAILABLE、380不足20session、10正常缺源UNKNOWN，无删填或重选股。09:21:01UTC复查公开QE三running0/1/0，MA-E42R qe_20261005_162928_e86b占用，未启动M18四fit或读取评估收益；全轮实际仍67fit+1旧index，source#5487 HEADb71f0aef7/currentCI37286496082 attempt2 SUCCESS后09:17:32UTC合入13b4c4232da82d9d0abc1cc0c5f20251da92d23b，root main clean且与origin/main一致；原attempt1仅checkout远端读取失败，重试后检出及业务检查全部通过，未改公共CI/网络配置。冻结源码树暂保留至实际消费，下一QE约09:51UTC复查，fit不并行。M1六UI/BUG公共smoke仍外部等待，sealed/confirmation/activation/DB写/服务控制0，原18h/48h不重计。
