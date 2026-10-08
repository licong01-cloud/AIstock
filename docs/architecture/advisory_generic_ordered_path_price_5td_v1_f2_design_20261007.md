# Advisory GP5-ORDERED-PATH-1：D分钟时序新增信息 F2详细设计

2026-10-07，SOURCE_VERIFIED_RESEARCH_EVALUATED_NOT_CONFIRMED。设计#5655已合入81d43fe8202f9e09e43f62f7cf5adad157ca7622/自身官方清理完成21.672秒；新九文件源码多轮本窗口自审与修复、最终7定向/Ruff/L0/F2八项通过。一次研究相对原Top5/冻结joint为−52.0708/−9.5296bps每5TD cohort，仅停止本candidate、不调阈值/seed补救。累计113研究fit＋1历史index；源码PR、合入与清理另报告，未激活。

## Background / Goal

GP5-JOINT-DISTRIBUTION-1一次研究相对原Top5−42.5412、相对冻结39D GBDT＋5.0023bps每5TD cohort；已知拒买−44.5856，UNKNOWN现金＋2.0444不是模型增量。只停止该candidate。前三版D日频、分钟全天汇总、量价全天汇总与联合结构均未实现相对原Top5的净收益目标。现有分钟数据可产，但汇总没有保存固定时段的走势/成交量顺序。

本次假设：只在同一联合学习器加入D的固定16时段收益及成交量份额，是否降低误拒盈利、产生相对原Top5与冻结joint控制的成本后净增量？保持原19量、gap场景、学习器参数、诚实分区、成熟训练人口、5交易日和政策不变。原GP5已有D-only matched，不重复删gap实验；gap模型是观察开盘场景的条件关联，不宣称因果挂单fill，也不是预测未来开盘价。

首版固定T..T+4五交易日，买.95/sell5.95bps各一次，expected_net>0、downside_q90<=800bps；原Top5五槽不补Top6、不重排、不改候选策略。允许无可买价格或UNKNOWN；利润/幅度与拒买净贡献是研究目标，胜率、覆盖率或现金不能替代。只研究价格区间，不开发分钟执行/最优分钟买卖算法。

方法保留[RandomForestRegressor官方API](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestRegressor.html)与[Quantile Regression Forests原论文](https://www.jmlr.org/papers/v7/meinshausen06a.html)支持的森林邻域分布思想；诚实时间分离和联合成对目标仍是本地定义，并非论文原样实现或因果识别。本次不换神经网络家族、不安装依赖，不将外部方法当收益保证。

## Scope / Non-goals

设计PR只本文件、蓝图与上一个joint设计的当前结果段。源码从最新main独立树开发，严格九文件：

- backend/services/advisory_model_first/generic_ordered_path_price_5td_contracts_v1.py
- backend/services/advisory_model_first/generic_ordered_path_price_5td_source_v1.py
- backend/services/advisory_model_first/generic_ordered_path_price_5td_model_v1.py
- backend/services/advisory_model_first/generic_ordered_path_price_5td_pipeline_v1.py
- backend/tests/advisory_model_first/test_generic_ordered_path_price_5td_source_v1.py
- backend/tests/advisory_model_first/test_generic_ordered_path_price_5td_model_v1.py
- backend/tests/advisory_model_first/test_generic_ordered_path_price_5td_pipeline_v1.py
- 本文件与advisory_strategy_conditioned_model_blueprint_v1_20260710.md。

旧joint/volume/GP5源及权重不修改或重训。禁止QE/Selection/HMM/StrategyPackage/公共数据/行业/Execution/Paper/CI/workflow/AGENTS修改；不加父包资格门、不扩UI/ModelOps平台。包/单指数/多指数并集仅元数据，输入数学不依赖父alpha/rank分数。正常缺失保留原股票，不压缩停牌时钟或删日期。

临时/test/cache全X，正式root F:/Dev/AIstock_model_artifacts/advisory_generic_ordered_path_price_5td_v1_20261007。0DB写/DDL/DML/数据或模型激活/安装/服务及其它进程控制。离线切片不需后端重启。只消费已使用开发窗口，sealed不读。

## Architecture / Contracts

### 1. 显式来源与时序表示

沿用消费者公开active_profile_path，只读解析当前节点minute_root与generation/calendar/instruments/meta pins；不回退旧硬编码路径或修改profile。2026-10-07只读spike当前generation20260928-v15-unified-moneyflow1，三只原train候选在D=2024-07-04均241slot OHLC/volume已知；这不是全人口coverage或收益结论。已冻结19量的原身份保留，新路径源身份额外绑定，非vintage来源不提升为原生捕获证据。

只解码每KEY的D open/high/low/close/volume，T及之后解码0；按instrument复用stream、386D/7720原KEY批量，不能每天重建工作区。使用既有OHLC验证，known非正价格、负volume、矛盾区间/非法key/未来时点显式失败；缺文件/NaN/停牌正常UNKNOWN。源/元数据若读取时变化不能混代。来源校验只是计算一致性，不是策略包资格门。

固定交易所clock16bin：第一收益bin09:30～09:45（含09:30），其余上午09:46～11:30每15min，共8；下午收益13:01～15:00每15min，共8。不按观察bar序号等分，不跨午休，不把缺失或停牌bar挤成邻接。09:30和13:00是允许存在的边界anchor，13:00只在原calendar存在时计入下午第一个volume bin，绝不替代13:01收益端点或缺失09:30。每个bin输出：

1. return_bps=10000*(固定bin末close/固定bin首open−1)，只需这两端及其known OHLC成立；端点缺则此量UNKNOWN，不找最近可用端点。
2. volume_share=该bin volume和/全D原声明calendar volume和，边界anchor也计入对应bin；只有240核心交易分钟（09:31～11:30、13:01～15:00）全部存在、原D所有声明clock的volume已知且全天总volume>0才测量。已知零成交bin份额为0，全天0、缺核心分钟或声明bar volume未知则所有份额UNKNOWN，不以部分和当完整分母。未声明的可选anchor不伪造volume为0，也不认定停牌。

32列按bin0..15的return/share交错排列。正常未知的mask显式保存，不全行淘汰；元数据另报原calendar_slots、两anchor是否存在、各bin端点/volume已知及缺失原因。D价格量和无量纲份额不需要amount/VWAP、跨raw/adjusted绝对价格映射或未来复权锚。缺少09:30不是用13:00隐式替换，只使第一bin收益UNKNOWN；无新stock或包准入条件。

### 2. 同信息外扩、同成熟人口与编码

新prepared只读引用joint已冻结rows＋trained model；完整保留原19量、supervision、KEY/order/date、policy。新32列按KEY一对一左对齐，外来/重复/缺失KEY必须计算错误，不能用inner join删股票。原观察gap仅训练监督和T评估时使用，D查询仍传假设价节点。严格D/T与原H=T+4，不能延长H补停牌。

原joint19 medians/support不变，新32 medians仅结构池D已有特征计算，缺列全UNKNOWN编码0＋missing=1；估计池/test/validation不参与median。输入51raw＋51mask＋gap/100=103维。原日频可用判定沿用任一九量已知，新时序缺失不改变原成熟population、不另过滤股票。

先train时钟、再解析数值，test/validation非法毒值不影响fit校验。原mature pool与时间split不变：结构H严格早于估计首D、估计H<=train_end；结构/估计KEY SHA与冻结joint diagnostics逐项一致。固定128tree/depth6/leaf30/max_features1/bootstrapTrue/seed20261007/n_jobs2，仅新candidate一次fit，128树不计128trial，不试其它bin数/encoding/seed/窗口/loss。

### 3. 联合数学与价格区间

保存非执行JSON树/估计leaf成员/成对terminal与path，矩阵float32 walk对原生apply精确验证；维度103不能假标原39schema。分块<=128，原样累加每树leaf等权质量，再按原估计样本归一，重复不是独立样本。同质量计算mean terminal、path q10、正净收益概率，零质量UNKNOWN，不填global mean，不把mask当已知AVOID。

price query/完整legal tick仍成本各一次、expected_net/risk同原合同；精确Decimal tick不越法律边界、不跨UNKNOWN洞合区间。price basis D_ANCHORED_CNY，原schema/model/policy/hash、5TD及不激活字段完整。支持外正常UNKNOWN，不因为假设买得更低就无限外推。概率/权重ESS只是解释，不新增概率/ESS阈值。

### 4. 原子一次研究及对照

四stage/preregister/prepare/train/evaluate，单独plan/schema/implementation/root，只追加registry EXPLORATORY_SCREEN、NAVIGATION_ONLY、RISK_MANAGED_ADVISORY，唯一变量ORDERED_D_PATH_INFORMATION。parent明确joint与volume及原GP5；new dataset identity额外绑定32列时序来源，控制仍原来源，不假称原生/vintage。

四臂：new103D candidate、冻结joint39D candidate为matched、原Top5 baseline、固定±300bps rule。控制直接读权重，不重fit或根据结果换control；新模型原19量/政策/成熟pool/分区/参数与控制一致，只有32列及相应结构池编码扩展。原Top5五槽、81testD、1正常未结算null不删；不跨洞计算CI、不把重叠cohort复利成NAV/MDD。

报告两配对增量candidate−base/matched、共同四臂分母、实际known干预/UNKNOWN-only、TAKE胜率及盈亏幅度、不可执行/未结算、避免亏损−错过上涨和UNKNOWN现金分账并精确对账。结果负只停该candidate；正也仅开发导航，不激活、不证明跨包盈利或关闭全局方向。新journal独占写STARTED，partial不可隐式再fit；完成只读回原stage不重复评价。

## Implementation Plan

设计多轮自审/F2/diff通过按授权合入→latestmain独立九文件→新D时序source/103D编码与JSON分布→父输入/冻结控制stage与完整四臂→PIT/数学、业务/缺失、来源/预算/边界多轮审核修复→失败只定向节点→最终小矩阵/Ruff/L0/F2、clean producer→fresh公共QE single/custom_evo/multi-alpha全部0后唯一1fit及评估→真实当前结果/源码当前CI合入/自身官方清理。忙只等，不终止或提交QE。超过30min才半小时检查，不密集监控。目标约3～6h工作量，不凑时长、不用旧负研究证据归档代替研发。

## Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-761 | 当前显式minute源/pins、只D/原KEY、只读0未来/0数据修改 |
| F-762 | 固定clock16bin、午休/缺失不压缩、端点收益及完整volume分母 |
| F-763 | 51raw/103dim、旧19编码不改、新32只结构池、未来毒化不影响 |
| F-764 | 同成熟KEY/诚实role、固定1fit、JSON float32 leaf parity/模型hash |
| F-765 | 同成对样本分布、零质量UNKNOWN、原成本/risk/fulltick及洞 |
| F-766 | 原候选与日期、冻结joint控制不重fit、四臂/未结算/非NAV |
| F-767 | 两配对增量、known干预与UNKNOWN分开、拒买贡献精确对账 |
| F-768 | 精确scope/多轮小矩阵/CI、真实fit数、QE串行/生产NOOP |

## Design Acceptance Matrix

设计PR已合入；当前源码切片只验收完整离线模型/来源/编排。真实prepare/研究fit与收益另报告，不把unit fixture计入研究；API/UI/经济确认/模型启用不冒称已验收。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-761 | backend/services/advisory_model_first/generic_ordered_path_price_5td_source_v1.py | backend/tests/advisory_model_first/test_generic_ordered_path_price_5td_source_v1.py | SOURCE_VERIFIED | none |
| F-762 | backend/services/advisory_model_first/generic_ordered_path_price_5td_source_v1.py | backend/tests/advisory_model_first/test_generic_ordered_path_price_5td_source_v1.py | SOURCE_VERIFIED | none |
| F-763 | backend/services/advisory_model_first/generic_ordered_path_price_5td_model_v1.py | backend/tests/advisory_model_first/test_generic_ordered_path_price_5td_model_v1.py | SOURCE_VERIFIED | none |
| F-764 | backend/services/advisory_model_first/generic_ordered_path_price_5td_model_v1.py | backend/tests/advisory_model_first/test_generic_ordered_path_price_5td_model_v1.py | SOURCE_VERIFIED | none |
| F-765 | backend/services/advisory_model_first/generic_ordered_path_price_5td_model_v1.py | backend/tests/advisory_model_first/test_generic_ordered_path_price_5td_model_v1.py | SOURCE_VERIFIED | none |
| F-766 | backend/services/advisory_model_first/generic_ordered_path_price_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_generic_ordered_path_price_5td_pipeline_v1.py | SOURCE_VERIFIED | none |
| F-767 | backend/services/advisory_model_first/generic_ordered_path_price_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_generic_ordered_path_price_5td_pipeline_v1.py | SOURCE_VERIFIED | none |
| F-768 | backend/services/advisory_model_first/generic_ordered_path_price_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_generic_ordered_path_price_5td_pipeline_v1.py | SOURCE_VERIFIED | none |

最小直接测试：一份固定clock fixture共享，时段手算/午休/正常缺bar/0volume、两anchor变体、known矛盾、无D文件保KEY；一份成熟监督fixture一次forest fit，test毒化、role/编码/hash/leaf手算、zero质量/成本/价洞、frozencontrol不重fit；一份stage fixture验证原人口、partial、四臂归因/未知/未结算。无重复参数快照、实现明细锁定、整QE/UI或泛化邻接套件。

## Risks / Rollout / Rollback / Production Gates

新增32列在约1700结构样本上可能增加方差，16bin是单次事前选择、负后不调。全天volume未知会使新份额缺失，mask不是收益信号证明；原optional_UNKNOWN不能晋级为候选不合格。估计池来源非vintage且已消费开发窗口，多重研究选择偏差保留；分布关联不能当实际限价成交价值，5TD cohort不是资金曲线。

backend_restart_required=false；0DB写/依赖/profile/模型/角色激活/后台进程控制，回滚停止新离线消费者不改旧产物。M1 NOT_CONFIGURED/旧close-sync不阻本研究；DB辅线源独立待BUG-1778公共交付，不在本PR顺手修复。新模型不加入生产consumer family或UI，若后续要消费另列精确设计。

## Review / 多轮自审

第一轮业务/新颖性：原D-only控制已存在，不重复删gap或换模型；保留joint39D为固定控制，只有D时序信息外扩，原Top5与known拒买净贡献两条收益验收不变。第二轮PIT/统计：固定交易所clock而非观察bar分箱；端点不邻填、volume完整分母、结构池medians/test毒化、两role SHA相同、1fit/已消费窗/非NAV。第三轮边界/预算：不改旧任何源/权重或共享数据，没有QE Alpha/父包门/amount数据修复/平台/UI任务；只有9自身文件、X临时/F正式、QE互斥和user重启保留。以上是本窗口自审，不冒称独立外审。

源码第一轮：新32列/103维、原19编码/诚实role哈希不变、固定clock手算/午休/缺失不压缩、JSON apply parity、成本和完整tick洞，5项最小直接节点通过。第二轮：新增自身stage/原人口/只读父控制/partial不可第二fit/四臂未知及未结算；一个fixture误取EvidenceReferenceV1.path，按既有_verify_reference修正，失败节点单独复测通过，未修改公共合同。第三轮：补齐新source/contracts/pipeline的implementation hash，避免只hash模型却漏新reader；known high/low矛盾即使其它价格UNKNOWN仍失败，新增同节点定向复测通过。没有因正常缺失删股票或改变成熟训练人口；不存在新研究fit、数据库写/服务操作。最终稳定小矩阵/Ruff/L0/F2及真实一次研究另报告。

上述三轮是首次实际prepare/fit之前的审核快照。第四轮clock修复后最终稳定7项通过，Ruff无问题、官方L0 quality0finding/guardrails0blocking（1既存非阻断项）、F2八项0warnings；并执行一次真实研究和必要四stage/数学读回，未重复评价或扩大旧负实验审计。

完整prepare前第四轮：3个原D（2025-11-27、12-08、12-12）calendar有13:00、没有09:30；初版硬模板把合法边界anchor误报非法。只读metadata定位，没有读收益或fit；修订合同后，原D/股票不删、固定收益端点不邻填，volume保全部声明clock并要求核心240分钟完备。新增同一手算节点的变体验证通过，不修改基础数据或资格门。首个preregistered advgp5ordered_f858f556c1d8ef5390cf142a仅准备失败、0fit且0trained/evaluated，保留不覆盖；修复producer以新implementation/独立run登记，不将准备错误计作负研究或第二次研究fit。

## 当前实际一次研究结果

clean producer f2887c579667e8ae41f978634ed07ade3fb10ebf，implementation SHA f9976d76f62c6d8bea5e69c3b070ec4858cc214c7f8f1d0ea70a2a97e1b49e3f。run advgp5ordered_316536901e41a77dfcf39517，plan SHA 316536901e41a77dfcf39517aab6c81321a969dd93f54f6951773be2d7482d99。首次成功prepare46.172秒，386原D/7720原KEY全保留，只D解码37,154,800bytes，0未来；4880行新32量全部已知，其余逐字段UNKNOWN/mask不删股、不另过滤训练人口。来源CURRENT_RELEASE_NON_VINTAGE_D_PRICE_VALUES不提升为原生。

2026-10-07T03:27:24.669133+00:00及03:27:29.816319+00:00、公共QE三入口都0；唯一1候选fit/128内部树，train阶段2.438秒、train至完整四臂5.141秒。原结构1738/估计1848与两role KEY SHA和冻结joint相同，结构最新H2024-12-23<估计首D2024-12-24，test/validation未参与训练/编码。model SHA 01e3250a0836db22fa2c5b4fecc977421960e4fb70fb5775b3e59fa1edab5dda；trained/evaluated stage SHA 416521064b7cc21950f59f8fc03bfc94367498c55ce56f26648659c02bbe60da/7d7735c89af7e988c438f8046965e4a2c4caeac4b492a9e465a57f3c43a74282。0旧控制重fit/数据库写/QE提交/服务或进程控制。

81原testD，80四臂共同完整组＋1未结算null。baseline/rule/matched/candidate平均129.0145/131.5724/86.4733/76.9437bps每5TD cohort；candidate−baseline−52.0708、−matched−9.5296。280已结算TAKE胜率57.50%，base403/58.3127%、matched293/58.0205%，不能用胜率冒充期望。已知拒买111次/54D，避免亏损33.6043−错过上涨87.7194=−54.1152bps，UNKNOWN空槽＋2.0444另列，合计−52.0708；相对matched21次已知干预/17D，非恒等候选。1不可执行、12UNKNOWN、1未结算分别保留，区间/NAV/MDD null、经济确认和deployable=false。

一次新时序信息在本固定模型/窗口下没有收益增量，仅停止这个candidate；不调bin数/参数/seed/loss/阈值、回选rule控制或重跑旧研究。不把正常缺失/3个anchor变体解释为基础数据故障或跨包不合格，也不证明全局不可学。下一研究必须先提出不同业务目标/信息的事前设计，当前必要日频DB输入交付保留为独立辅线、UI后置。源码完整可交付与盈利/生产启用是不同状态。
