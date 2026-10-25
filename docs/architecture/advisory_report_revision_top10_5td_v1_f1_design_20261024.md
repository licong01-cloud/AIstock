# 下一年度同机构EPS修正代理 Top10 固定五交易日研究（F1）

版本v1.1；2026-10-24；Advisory / model_first。设计PR#6670合入424265b65cb22a4dfd4ab9d75e68f73a05badfb6。24直接合同节点和一次真实六fit研究完成，结果INNER_NOT_SUPPORTED：原生增量−7.644913bps、matched增量−2.156612bps。累计231实际研究fit＋1旧index；无outer、经济确认或生产接入。源码PR/CI/合入另报，后端重启由用户执行，本次离线源码无需重启。

## Background / Goal / Feature Card

唯一假设`P10_NEXT_FY_EPS_REVISION_PROXY_TOP10_5TD_1`：同机构、同下一年度年报预测期的EPS数值变化，可能在原日频/native信息之上改善原Top50内Top10排序。目标固定T开盘至T+4收盘、原费用后的candidate−原生Top10净cohort增量。研究的是带明确时间/口径限制的数值代理，方向由训练决定，不预设“上调必涨”。

既有基础八字段为日收益、波动、日内收盘位置、量比、父分数/排名；没有直接EPS修正列。研报记录与这些日量价字段定义不同，但父模型可能间接吸收基本面信息，不能声称统计独立或天然新增Alpha。matched只衡量在相同支持、模型、训练和原八字段之上的增量；不提升旧P9控制或重新打开已结束研究。

## Input / 来源、时钟与UNKNOWN

只消费已验收delivery-v2：`X:/AIstock_dataset_candidates/research_source_candidates/report_rc/20230701-20250930-capture-20261023-v1/delivery-v2`。manifest SHA256 `4d1bd7f24ef29295eae450f47c664a84b2e36efbc4e0ebdc141619e0acbc4222`；panel SHA256 `dfcd2b9b2b36d47c6810337caf58fb8bd98515d7079564f4cbc7e7eb18a42e9f`，548,991行。原始文件验收已完成，不重采、不写库，不要求生产持久化作为本轮前置。

[Tushare官方字段说明](https://tushare.pro/document/2?doc_id=292)将quarter定义为预测报告期、create_time定义为TS数据更新时间，未证明逐条首次公开时间。真实captured_at为2026-10-23，不倒填。探索时间假设为`max(report_date, naive_provider_create_time的日期)+1自然日`，仅假设日期<=D的记录进入配对；时区/首次公开/历史修订未知，滞后不能恢复PIT。

原人口来源`X:/AIstock_temp/ADVISORY/multiperiod-top10-5td-20261021/study/advmultitop10_6c632adf532818cfcd059113`；binding `589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`。两包LSTM `pkg_cc7eccb6202e4816b9d7c7d6b748bd37`、TCN `pkg_8dbd396d27184de6aa5fe288011220f6`；原52,400六列身份SHA256 `a53daa66a623b9cbcb684e1750ff618efd04035f801932bdd5b6e7df181a7eae`。开发2024-07-04..2025-09-30的305D/30,500行全部保留；其余21,900行在P10输入交付中保留UNKNOWN，实验不解码其金融值。

每个股票D仅取`str(D.year+1)+'Q4'`，不回退其它预测期。每个字面机构取假设可见的最新报告日和紧邻前一不同报告日；在假设时钟筛选后重建前值，不直接用交付prior_eps。当前报告年龄<=90自然日，前后报告间隔<=365日。最新缺失/同日EPS冲突不跳过；机构别名不猜测合并，EPS缺失不填零。

唯一新增量`eps_revision_proxy`为有效机构对的`2*(当前EPS−前EPS)/(abs(当前EPS)+abs(前EPS))`的中位数；两个已知零为0，负EPS按同公式，先缩放避免溢出。至少一对有效；未知机构数量、可用机构数量、年龄、变化对数仅诊断，不能事后成为模型字段。股本口径/币种UNKNOWN保留；不是已验证同口径经济修正。真实捕获覆盖与探索可算覆盖分别报告。

## Feasibility / 已冻结的无标签审查

证据根`X:/AIstock_temp/ADVISORY/p10-report-revision-20261024`。signal_contract SHA256 `99992ed7de9ad750c3981cd84dad393f07cea1afa5a46fb4af6a23e4042aef84`，2026-10-24T05:09:01.761236+00:00冻结，早于本候选数值读取。screen_rows SHA256 `ff88c5976452678cfdefd5b5991b9c0a06e223ada5690c442ee968042c35a4b9`；screen源SHA256 `91dae5a5e38b7830899ab3f084cd96daafaad886ebd8d8b1a2ab30c2977c1122`。规则不因结果调整。

事前要求每包首训练窗>=200可用行、>=40非恒定D、>=20跨原Top10边界机会D；每包每验证折>=100可用行、>=10边界机会D。机会指原Top10与其余40中分别有已知、且可找到数值差>1e-8的一对，仅潜在参与，不是实际swap或统计功效。

| 输入检查 | TCN | LSTM |
|---|---:|---:|
| 开发期可算/原行 | 3,195/15,250 | 3,636/15,250 |
| 首训练窗可算/非恒定D/边界机会D | 1,971/173/126 | 2,232/179/130 |
| 三折可算行 | 305/514/405 | 272/572/560 |
| 三折边界机会D | 28/37/35 | 26/42/40 |

18项全通过，合计6,831/30,500（22.3967%）可算；28,119唯一stock-D，历史捕获覆盖0。执行134.2707秒、采样峰值997,376,000bytes，0fit/收益/DB/API。12合成合同测试通过。另用不同实现重算全部支持统计，按stock-D SHA固定抽64已知+64未知，250有效机构对的时钟、最新/前值及引用通过；仅同窗口自审，非独立外审。审计脚本首次symbol格式错误已修复，不影响原screen或规则。

## Model / 唯一有界实验

原八字段为matched，candidate只加`eps_revision_proxy`，共9字段；各列附missing bit。两臂共同资格为EPS代理有限且至少一个基础字段有限。推理未知保留原槽，只有共同支持的股票在其原槽集合中重排，固定十槽，不删原Top50、用下一名填空或只评价有研报的子人口。

新预算明确为3折×2臂=最多6真实监督fit，不继承P9授权。三折train_end=2025-03-31/05-30/07-31，验证2025-04-01..05-30、06-01..07-31、08-01..09-30。先仅投影D及label_information_end(H)，只解码D<=cutoff且H<=cutoff的成熟监督值。AVAILABLE与合法CASH_ENTRY_NOT_EXECUTABLE可训练，现金真实目标0；UNKNOWN不伪零。两臂训练KEY、共同字段编码及权重一致。

固定Ridge(alpha=100,solver=lsqr,tol=1e-6,max_iter=2000,fit_intercept=False)。训练内逐列median/IQR；IQR为0且非恒定用population std，否则scale1；训练全缺列停用，missing bit显式保留。编码后包/D中心化X/Y；D等权、D内包等权、包内监督行等权。平手沿原rank，不搜索参数、窗口、符号、预测期、时钟或包。

每折两臂预测原子发布后，才读取该折成熟评价值。独占STARTED claim后fit；中断未发布fit不自动重拟合。源码commit、实现闭包、环境、父stage/binding、screen/规则/源身份固定。只读prepared/train_finance，拒绝outer及任何end/cutoff>2025-09-30；不提供final/sealed路径。本次六fit已完成，累计231实际研究fit＋1旧index；设计冻结时为225＋1。

## Evaluation / 经济终点及停止规则

主终点candidate−原生Top10的5TD十槽净cohort。费用buy0.95/sell5.95bps各一次；合法不可入场现金0，真缺价/未成熟UNKNOWN。原126评价D、两包全部保留；三臂两包共同完整日先包等权再D等权，不把股票、机构对或跨包同股当独立样本。matched增量、系数与支持率仅解释，不另设事后门、不回选matched或较好包。

复用native三分类：均值>0、至少2/3折正且三折可评价、共同成熟D覆盖>=80%、原生干预>=20D且>=20%成熟D并跨两折、尾部恶化<=100bps、额外UNKNOWN<=2pp才INNER_SUPPORTED；均值<=0或风险超限为INNER_NOT_SUPPORTED，其余INNER_INCONCLUSIVE。描述性95%区间跨零不新增否决。固定同步10D circular block、10000bootstrap、seed20261021，报告区间/MDE、三折/两包、实际原生/matched干预、UNKNOWN、尾部及系数。

所有终态均结束本候选的六fit预算，不追加第七fit、备用模型、翻符号、outer或数据补采。SUPPORTED仅支持讨论确认价值：本305D已反复消费、NON_VINTAGE和股本口径未知均未消除；本轮不得独立OOS/盈利确认、生产入库/角色激活。确认评审只判断是否有未消费、时点可证且兼容的输入与成熟标签；没有则明确暂不推进，不以时间等待或复用已消费outer补证。

## Scope / Implementation Plan

设计从main `861bf6ccafe36b8c5335c988290f03c45bdee6b9`独立PR先合入，源码从随后最新main另建工作树。精确六文件：

- backend/services/advisory_model_first/report_revision_top10_5td_inputs_v1.py
- backend/services/advisory_model_first/report_revision_top10_5td_v1.py
- backend/services/advisory_model_first/report_revision_top10_5td_pipeline_v1.py
- backend/tests/advisory_model_first/test_report_revision_top10_5td_v1.py
- 本文
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

输入叶内实现冻结配对，不导入scratch脚本；WSL重新计算开发期代理、状态/机构计数/trace，与screen逐身份校验，浮点事前rtol=1e-12/atol=1e-12并另报逐bit；身份/UNKNOWN/引用必须精确相同。源manifest/投影schema、panel及screen内容SHA固定，stat租约防执行期漂移；hash核对不是重复采集或完整文件语义验收。

复用multiperiod原输入八字段、peer_residual population/group_center/panel、peer_lead_lag原生评价、economic_entry原子stage、projected_finance及research_control registry/lock。只复用函数，不调用P9等旧研究入口；不改共享源或数据owner文件。恢复验证已发布stage及消费身份，不恢复或重复已做fit。

## Operations / Test Plan / Verification Plan

使用既有WSL Conda Python，2CPU线程、无GPU、RSS<=2GiB、一次执行<=1800秒、研究输出<=200MiB。文件写入仅本轮X证据根与唯一study目录，源数据/旧研究只读；训练前只读公开QE running及WSL资源，资源充足可并行，不控制其它任务。

测试覆盖时钟筛选重建前值、同股票/机构/预测期、最新缺失/冲突不跳过、零/负/溢出、别名/股本未知、年龄/间隔、全人口和跨节点screen一致；成熟H投影/未来毒化、共同监督/编码、未知原槽/平手、六claims/恢复/拒绝outer、主端点不由matched或系数改判。行为稳定后一次直接小矩阵、Ruff/changed L0/ownership/diff、F1及蓝图F2；大范围回归交必需CI。至少两轮同窗口自审及逐条设计符合性审查，不冒称外审。

## Design Acceptance Index

| ID | 验收目标 |
|---|---|
| F-501 | 单一新输入、P10冻结检查及证据等级 |
| F-502 | 同预测期配对/假设时钟/UNKNOWN与原人口身份 |
| F-503 | 共同成熟训练、8/9字段和新六fit预算 |
| F-504 | 原生5TD净终点、三分类及确认边界 |
| F-505 | 不可变身份、原子stage/claims/恢复与资源限制 |
| F-506 | 真实结果、精确范围、多轮审核及生产分态 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-501 | 本文Background/Feasibility | artifact: signal_contract、screen_receipt、screen_tests、audit_receipt位于本轮X证据根 | DESIGN_AND_INPUT_PASS | none |
| F-502 | inputs.stock_context/read_panel/verify_screen_alignment | backend/tests/advisory_model_first/test_report_revision_top10_5td_v1.py；30,500原行/5数值列/4精确列跨节点逐bit对齐 | SOURCE_AND_REAL_INPUT_PASS | none |
| F-503 | core.training/fit/predict | backend/tests/advisory_model_first/test_report_revision_top10_5td_v1.py；六真实fit、三折两臂共同KEY/编码/监督数审计 | SOURCE_AND_SIX_FITS_PASS | none |
| F-504 | pipeline.finance/fit_pair及core.summarize | backend/tests/advisory_model_first/test_report_revision_top10_5td_v1.py；文末结果与独立算术读回 | RESEARCH_COMPLETE_INNER_NOT_SUPPORTED | none |
| F-505 | pipeline.Study/preregister/run及inputs.sources | backend/tests/advisory_model_first/test_report_revision_top10_5td_v1.py；十stage及6STARTED/6FINISHED、源SHA/lease读回 | SOURCE_AND_REAL_STAGE_PASS | none |
| F-506 | 本文Scope/Execution Result/Production Gates | artifact: 本轮X根validation_receipt.json、research_audit_receipt.json、review_receipt.md；24直接节点/三轮源码与结果自审 | OFFLINE_RESEARCH_COMPLETE_NOT_ACTIVATED | none |

## Non-goals / Risks / Production Gates / DESIGN-COMPLIANCE-001

0生产DB/DDL/DML、0采集/数据集切换/服务控制/激活；production_ddl_gate=noop、dependency_gate=noop、backend_restart_required=false。仅Advisory离线；不改QE/HMM/Selection/StrategyPackage/LocalSIM、价格/Exit/仓位、LLM研报正文分析、流水线或启动脚本。用户另行确认生产持久化与业务接入。

设计符合性逐项：设计、源码及一次真实六fit研究完整完成，阴性按预声明停止分支交付，未用局部/mock替代模型交付；未知和异常不伪成功；原人口/5TD/费用/经济标准不改；不新增设计外审批或阻断。六fit失败或阴性均可真实结束；稀疏覆盖、机构选择、供应商修订、未验证股本/时区及反复消费窗口限制须随结果保留。

## Execution Result / 唯一六fit研究及确认价值评审

clean producer `d9df06412e9d8529ff1c57595f4acff2969cded0`；实现闭包SHA256 `28baf19d84dd22e67b65c563541fc20705e17a7e6e8a91379afd99b2c9a56c4f`。唯一run `advreportrevisiontop10_24550edf4d65fadf24da1cb7`，位于本轮X根/study/。result SHA256 `6af4833aa1a6ef16f45f9395a08847bc718a5a97a2b3d8293c9a161b3f07fb73`。WSL fresh import来自本树；运行前两项公开QE running查询各0，WSL可用59,713,118,208bytes/28CPU，本次2线程/noGPU，不控制其它任务。

原305D/30,500行完整保留，28,119唯一stock-D，6,831可算/23,669 UNKNOWN。WSL重算与P10全开发期5个数值列、4个状态/trace/预测期列完全对齐；实际逐bit相同，不依赖事前1e-12容差掩盖偏差。prepare105.074377秒；全研究110.879157秒、峰值1,005,666,304bytes、终态目录30,846,507bytes。源/规则/screen身份终态回查通过。

三折两臂成熟共同训练4,153/4,695/5,729行、174/214/257D，训练KEY与共同列median/scale一致；matched迭代10/10/10，candidate11/11/11。真实6STARTED/6FINISHED，十个stage链及每折原预测/未知槽/固定十槽全部读回通过；没有中断重拟合。直接测试24节点通过：首轮19通过，C盘临时路径相关失败移到明确X盘目录验证，reader超窗负例修正D/T一致性后通过；新增同stat内容篡改测试通过。不放宽研究路径约束，不重复全套测试。

| 预声明评价项 | 实际结果 |
|---|---|
| 原评价 / 成熟 / 三臂两包共同完整 | 126D、252包D、12,600原行；111成熟D，101共同完整D，90.9910%覆盖 |
| 原生 / matched / candidate均值 | 103.774553 / 98.286253 / 96.129640bps/5TD cohort |
| candidate−原生主终点 | **−7.644913bps**；描述性95%[-23.560736,+6.395927]，MDE80=21.529874bps |
| 三折原生增量 | −17.846256 / +9.354378 / −17.932795bps，仅一折正 |
| candidate−matched解释项 | **−2.156612bps**；95%[-8.210800,+3.383289]；三折−5.498560/−0.820782/−1.293793 |
| 两包原生增量 | TCN−9.797404、LSTM−5.492423bps，均负，未择包 |
| 真实原生 / matched干预 | 90/111成熟D（81.0811%）/64/111成熟D（57.6577%），原生跨两季度 |
| 尾部 / UNKNOWN | 最差10%cohort差+8.758772bps；原生与candidate未知槽各326，额外UNKNOWN=0 |
| 编码后EPS代理系数 | −25.916002 / −25.734043 / −16.631537，仅描述，不翻符号重试 |

`research_audit_receipt.json`用已发布cohort按两包等权、共同日等权另算主均值/三折/干预及分类，和运行回执一致，0新fit/新收益源读取。它是同窗口不同算术实现复核，不是独立外审或独立OOS。Ruff、语法、ownership4/4、两L0通过；一P2监督merge复杂度警告已核对精确KEY、one_to_one和<=30,500行边界。设计两轮、源码两轮、实际结果第三轮逐项自审，未改冻结规则。

按事前合同 **INNER_NOT_SUPPORTED**，`supports_future_confirmation_design=false`、`proceed_outer=false`。主均值非正且仅一折正是停止原因；区间跨零不是新增门槛，matched负和系数负只作解释。该结果不能简单归因于“缺数据或没有发生重排”：支持与干预均够用，成本后增量未实现；也不证明全部研报信息或机器学习排序无效。

本精确候选停止，不做final/outer/第七fit，不换预测期/滞后/模型、挑包或要求数据owner为阴性重采同版本。文件消费PASS、NON_VINTAGE探索READY保持，无新的数据交付缺陷。确认阶段不推进：经济支持本已未达标，且该305D重复消费，文件历史首次公开/股本口径未知，不能用已消费outer或新目录换取独立性。生产持久化不是本次问题的解决办法；不生成需要用户重启的部署。

后续仍以重排序为唯一研究主线，但本轮没有自动备用候选。下一次立项须先提出真正不同、可追溯信息的单一业务假设，并与这次EPS修正及既有量价/同群研究查重；本实验不转为同族参数搜索或并行价格/Exit研究。保留可复用的原人口、时钟/配对验证、未知原槽、成熟训练与评价能力；源码可合入不代表模型应接入荐股。
