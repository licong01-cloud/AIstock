# 研报出现频率变化 Top10 固定五交易日研究（F1）

版本 v1.0；2026-10-24；Advisory / model_first。**P13 只交付设计；实现与真实研究尚未启动。** P12 无标签输入审查已完成，3,773/30,500 行支持，18 项检查通过。本设计冻结一个候选、一次最多六 fit 的后续实验，不将设计预算记作实际拟合；当前累计仍为231实际研究fit＋1旧index、0新增经济确认/激活。

## Background / Goal / Feature Card

唯一假设 `REPORT_ORG_DAY_ARRIVAL_ACCELERATION_PROXY_TOP10_5TD_1`：近期出具研报的频率，相对该股票前一段时间发生变化，可能在原日频/native 信息之上改善原 Top50 内 Top10 排序。固定 T 开盘至 T+4 收盘、既有费用后的 candidate−原生 Top10 净 cohort 增量，不预设“更多研报必涨”。

P11 的 EPS 修正候选已完成并按原合同停止。本候选使用报告出现的时间与计数，不使用 EPS 修正幅度、评级或标题情绪；不重开 P11 的参数、符号、预测期、模型或滞后搜索。P12 的构造反例保持 EPS 数值不变、增加不同报告日后得到不同计数代理，说明不是确定性改名；不证明对父模型、价量或财报季的信息独立性。

原八字段为 `ret_1/ret_5/ret_10/atr14_close/close_location_in_day/volume_ratio_5_to_20/parent_score_z/parent_rank_fraction`。没有显式研报计数；父分数可能已经吸收相关信息。两臂 matched 消融只解释新增输入贡献，不提升旧 matched 为生产策略，也不继承 P9 或 P11 的预算。

## Scope / 本阶段与后续执行范围

本阶段只新增本文、同步[蓝图](advisory_strategy_conditioned_model_blueprint_v1_20260710.md) v5.44，并执行设计审查、验证与独立文档 PR。P12 输入不重算、不重采；本轮0新fit、0收益标签读取、0模型源码修改。当前输入结果与 P11 阴性结果完整保留。

本文同时定义下一实现阶段必须完整交付的输入、模型、评价、身份恢复和测试合同；不是以设计代替已实现功能。设计合入不自动启动研究进程或改变生产行为，源码及一次真实研究作为下一阶段交付。后端重启仍归用户，本候选仅离线，设计与后续离线叶均不要求重启。

## Input / 来源身份与无标签准备

只消费已验收 `X:/AIstock_dataset_candidates/research_source_candidates/report_rc/20230701-20250930-capture-20261023-v1/delivery-v2`，548,991 条原预测记录。manifest SHA256 `4d1bd7f24ef29295eae450f47c664a84b2e36efbc4e0ebdc141619e0acbc4222`；panel SHA256 `dfcd2b9b2b36d47c6810337caf58fb8bd98515d7079564f4cbc7e7eb18a42e9f`。不以生产持久化作为文件研究的前置。

原人口来源 `X:/AIstock_temp/ADVISORY/multiperiod-top10-5td-20261021/study/advmultitop10_6c632adf532818cfcd059113`，binding `589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`。两包为 LSTM `pkg_cc7eccb6202e4816b9d7c7d6b748bd37`、TCN `pkg_8dbd396d27184de6aa5fe288011220f6`；52,400 行六列身份 SHA256 `a53daa66a623b9cbcb684e1750ff618efd04035f801932bdd5b6e7df181a7eae`。P12 中全部身份和顺序保留；开发2024-07-04..2025-09-30，共305D/30,500行，范围外21,900行UNKNOWN，后续研究不解码其金融值。

原六列身份为 package_id、manifest_sha256、run_id、decision_as_of_trade_date、target_trade_date、instrument；另核对 selection_effective_rank 和 candidate_group_size。每包D完整50行、原rank1..50、D/T一致，不能重新生成或按研报覆盖筛选人口。

P12证据根 `X:/AIstock_temp/ADVISORY/p12-report-information-inventory-20261024`，冻结于 `2026-10-24T06:12:59.950166+00:00`。下列摘要进入Git设计，X盘原文件仍是本地研究工件，不冒称可跨机器自动访问的生产数据集。

| 工件 | SHA256 |
|---|---|
| inventory_contract.json | 026e80c0153fb56657ce31cc25787db28ae556815e796ad0b933a0fbaa0181d6 |
| inventory.py | 13bcbd96c34bb910ac4d27dd21f50f205488f00c6474a8f280fcf0b8899bfc27 |
| inventory_rows.parquet | 6eed971c22df4d2e62c3d29f963b43838d7eb997df405d18d472f0a62ebf33d8 |
| event_ledger.parquet | 47cfbb7e7eb64956de6ea01677abba324dbcd2d6e3a3b48dd472029ed902f076 |
| inventory_receipt.json | c7f630429e4ab61d7146e2ba5b3589934d5cfacae94ac3f32fcd4f84f4327fbb |
| audit_receipt.json | 8e45ab97d1cda1c7730508c2c207ac7c2b8c882a88550a12b662b6ce83101adc |
| delivery_receipt.json | 40c92fca59cf5470c5c474b45ebdec735901bb31b02a8bb094d3369b2ca6d50c |

重建代理的panel白名单为 `ts_code/org_key/report_date/provider_create_time/captured_at/capture_id/row_ordinal/source_row_sha256/vintage_status`；原人口只投影上述六列身份及rank/group_size，旧八字段按既有输入叶读取。无需 EPS、评级、目标价、预测利润或标题内容；其余字段仅在P12盘点过，不能成为此次模型的新列。事件账本留存capture_id/row_ordinal，源panel保留source_row_sha256，可逐层追溯。原人口KEY缺失或重复、来源引用无法匹配或身份漂移显式失败；源里不存在某股票研报则正常UNKNOWN，不误当身份错误。

## Contracts / 单一信号、时钟与 UNKNOWN

1. 计数单位是 `(ts_code, 原文字面org_key, report_date自然日)`。不同预测年度、同日多个标题、多个原始行最多贡献一次。它是机构日报告出现代理，不是唯一报告份数；不能凭空补供应商report_id、合并机构别名或声称真实首次覆盖。
2. 对每个原始行计算 `assumed_available_date=max(report_date日期, provider_create_time日期)+1自然日`，仅假设日期<=D的行可支持计数。机构日只需至少一条原始行满足该规则，即有一次可见存在；后续追加不可见预测行不能改变此前数值。报告日/更新时间缺失不采用skipna或默认日期。
3. 近期窗口为报告日期 D−29..D，基期为 D−119..D−30，均含端点且互不重叠。先按D筛假设可见性，再计数。窗口基于报告日期，不能把供应商晚更新日期当成新出具报告日期；不另试交易日、7/20/60日窗口。
4. 唯一新增列 `arrival_acceleration_proxy=log1p(recent_org_days)-log1p(baseline_org_days/3)`，float64。90/30天比率固定，方向由训练决定；不裁剪、翻符号或改变分母。
5. recent和baseline计数均>0才AVAILABLE；缺任一窗口记录为UNKNOWN，而不是负关注或已知零。某股票相关120日窗口内原行缺机构、时钟或日期时，该股票D为UNKNOWN；日期缺失不能证明落在窗口外。无法定位股票的无效源行显式失败。P12这类无效源行实际为0，不以零出现免除负例测试。
6. 同日EPS/评级冲突不改变本代理的“机构日报告出现”定义。所有源记录保留，数值内容不读取；机构字面身份、报告日期或来源引用不一致才按本合同拒绝/UNKNOWN。不要照搬P11的EPS缺失判据制造与本信号无关的缺失。
7. 计数、机构数量、事件ID和 `recent_snapshot_source_rows_diagnostic_only` 只用于复核；后者包含完整快照的原始行数，不能冒充D可见记录数或作为模型特征。P12的EPS重叠统计也不进入本次输入资格或模型。

[Tushare字段文档](https://tushare.pro/document/2?doc_id=292)的create_time为TS数据更新时间，不能证明首次公开。真实captured_at为2026-10-23；历史捕获覆盖0。provider时区、首发/修订/删除历史与机构实体身份仍未知，始终 `NON_VINTAGE / EXPLORATORY_SCREEN / NAVIGATION_ONLY`。人为滞后没有恢复PIT。

## Feasibility / 已完成的 P12 支持与残余问题

548,991行归并为189,973个机构日，186,052个含多个预测期，原始预测行约为机构日的2.89倍。不能直接用预测行数作为研报频率。有效覆盖3,773/30,500（12.3705%），其余26,727开发行UNKNOWN；并不提升P11覆盖。与旧EPS共同支持3,539行、独有234行，Pearson−0.0322/Spearman−0.0133只是按原行的描述性关系，不证明条件独立或预测能力。

| 输入检查 | TCN | LSTM |
|---|---:|---:|
| 开发可算/原行 | 1,583/15,250 | 2,190/15,250 |
| 首训练窗可算/非恒定D/边界机会D | 995/142/79 | 1,326/167/91 |
| 三折可算行 | 194/190/204 | 199/307/358 |
| 三折边界机会D | 17/20/17 | 20/36/26 |

读取本轮数值前已冻结：每包首窗>=200行、>=40非恒定D、>=20边界机会D；每包每后续折>=100行、>=10边界机会D。18检查全通过。边界机会指原Top10内外有可算且差异>1e-8的一对，不是真实改序、成熟训练数量或统计功效。

13合成检查通过；另实现复核全部身份/数值/18统计，并固定SHA抽64已知＋64未知stock-D，核验2,534来源引用，均通过。执行104.01秒、采样峰值1,522,266,112bytes；本轮只核对其文件哈希，没有重跑P12。

评级盘点发现大量“无”及缺跨机构尺度；目标价两列覆盖不对称，标题不等于正文，imp_dg/首发时间/供应商report_id全空。上述信息族没有被证明无价值，但本次不入实验队列，不同时下发新的数据需求。

## Model / 单候选、对照、成熟训练与预算

原生Top10为无需拟合的业务基线；matched使用原八字段，candidate仅增上述一列，总9字段。每个输入列附missing bit，训练全缺列按原编码协议停用。两模型臂共同资格为代理有限且至少一个基础字段有限；即使matched不使用代理数值，也在同一支持行训练/预测，以免把覆盖差异误认作信息增量。

只在可预测股票的原槽集合中重排，UNKNOWN保留原rank；平手沿原rank稳定排序。原50候选与十个选中槽全部保留，不把不支持股票删除、填零打分、顺延补位或只评价有报告股票。

| fold | 扩展训练截止 | 评价D区间 |
|---|---|---|
| 1 | 2025-03-31 | 2025-04-01..2025-05-30 |
| 2 | 2025-05-30 | 2025-06-01..2025-07-31 |
| 3 | 2025-07-31 | 2025-08-01..2025-09-30 |

预留3折×2臂，**最多6真实监督fit**，没有候选/超参搜索或final模型。此设计阶段实际新增0；后续实现阶段需以该cap独立登记，不继承P11六fit。若另有Advisory研究改变累计数，登记时如实更新起点并记录原因，不能重置历史搜索记录。

仅投影D和label_information_end(H)，筛D<=cutoff且H<=cutoff后才解码训练监督值。只接收AVAILABLE或合法CASH_ENTRY_NOT_EXECUTABLE，后者真实目标为0；UNKNOWN不伪零。同股D跨包的金融标签必须一致，两臂训练KEY、共同列编码、权重和日期完全一致。最小技术支持沿用现有训练合同（至少2行、至少2个D）；实际成熟数量在prepare/fit回执中报告，P12输入数量不能代替它。

固定 `Ridge(alpha=100,solver=lsqr,tol=1e-6,max_iter=2000,fit_intercept=False)`。只用训练行计算median/IQR；IQR零且非恒定用population std，否则scale1；均值或缺失状态不使用未来数据。编码后按包/D中心化X和Y，D等权、D内包等权、包内监督行等权。非有限系数/未收敛显式失败，不以另一模型救结果。

每折先冻结两臂预测并原子发布，再读取该折成熟评价标签。后续扩展训练可使用已在其cutoff之前成熟的前折数据，这是预定时间协议，不是按前折表现改变规则。共享源码只读复用，不回写旧stage、原包或旧研究。

## Evaluation / 主终点与停止条件

主终点是candidate−原生Top10的5TD十槽净cohort。费用buy0.95/sell5.95bps各一次，合法不可入场现金0，真缺价/未成熟UNKNOWN。保留126原评价D、两包原人口，三臂两包共同完整日先包等权再D等权。不是账户NAV、可成交实盘收益，也不把机构日、股票行或跨包同股当独立时间样本。

完全沿用P11的native三分类：主均值>0，至少2/3折正且三折可评价，共同成熟D覆盖>=80%，相对原生的干预>=20D且>=20%成熟D并跨至少两折，尾部恶化<=100bps，额外UNKNOWN<=2pp，全部满足才INNER_SUPPORTED；主均值<=0或风险超限为INNER_NOT_SUPPORTED，其余INNER_INCONCLUSIVE。不足以形成有效评价或合同被破坏时记录失败/不可评价，不伪造成功。

同步10D circular block、10000 bootstrap、seed20261021，只给描述性95%区间与MDE；区间跨零不是事后新增否决。全量报告每折/每包、支持/UNKNOWN、相对原生和matched的实际干预、尾部及代理系数。matched增量、低相关或系数符号只解释，不更换主终点、回选matched或好包，不要求特征单独通过第二套产品门。

SUPPORTED、NOT_SUPPORTED、INCONCLUSIVE或执行失败均结束本次预算。无第七fit、翻符号、换窗口、备用模型或自动外层确认。SUPPORTED至多支持讨论是否值得另立确认合同；已消费305D、旧outer及新目录都不能变成独立样本。不得由本实验直接宣称盈利、历史PIT、生产持久化或角色启用。阴性只停止此精确候选，不关闭所有研报信息。

## Implementation Plan / 身份、原子阶段与资源

后续实现精确叶建议为 `backend/services/advisory_model_first/report_arrival_top10_5td_inputs_v1.py`、`report_arrival_top10_5td_v1.py`、`report_arrival_top10_5td_pipeline_v1.py`，直接测试 `backend/tests/advisory_model_first/test_report_arrival_top10_5td_v1.py`。本文只登记方案，四文件不属于本轮写入范围。复用原八字段输入、成熟金融读取、同人口评分/评价及已有原子工件协议；不修改公共模块来迁就本候选。

输入叶只投影必需元数据列，在WSL训练环境独立重建计数并与P12的30,500开发行对齐：原身份、状态、整数计数、机构数量与事件ID集合精确一致，float64代理绝对容差<=1e-12且记录实际最大误差。未来追加不可见行只能改变完整快照诊断，不得改变当日数值、资格或排序。P12代理parquet不能不验来源就直接当训练真值。

固定新schema/hypothesis/experiment_id、implementation闭包SHA、clean producer commit、环境、父binding/stage、原人口、信号合同和源文件内容哈希。复用P11代码能力不等于复用其EPS列、225旧计数、claim目录或CONTRACT身份；本候选基准起点231，发生新的真实研究后才累加。只按mtime/size不足，需拒绝同stat内容篡改。stage读取验证上游manifest及文件SHA，生成后重新核对源租约。源变化、中断、错误和资源超限保留失败回执，不能换目录掩盖同一候选的尝试。

唯一study使用preregistered、prepared、每折fit/predict、每折evaluated及final汇总的既有原子发布合同；汇总不等于final模型。预登记3×2个排他STARTED/FINISHED fit claim，进入实际fit前独占领取。已有完整且同身份的阶段只读恢复，不追加fit；STARTED无完整可验证发布不自动重拟合，不能以缺模型当作尚未消耗预算。不得提供final训练、outer、sealed、end/cutoff>2025-09-30或非注册arm入口。

下一阶段临时/研究产物仅在明确登记的X盘任务根及唯一study目录，源数据与旧研究只读。固定WSL训练环境、最多2线程、无GPU、RSS<=2GiB、总时限1800秒、研究输出<=200MiB。P12已有资源证据不代替执行时检查；执行前仅只读核对公开QE running及WSL资源，不控制其它任务或服务。当前设计不调用这些运行接口。

## Verification Plan / 可合入标准

实现阶段直接测试覆盖：同org-day跨预测期/标题封顶；原文字面别名不合并；未来更新/追加、日期缺失、未知机构/股票、窗口边界和双方有记录条件；数值字段改变不影响计数；全人口顺序/身份及跨节点P12一致；成熟H先投影、未来标签毒化；两臂共同训练KEY/编码；UNKNOWN原槽、平手和固定十槽；六claims独占、中断不重fit、hash篡改、源租约漂移和越界拒绝；native三分类不被matched/符号/区间事后改写。

行为稳定后只运行直接小矩阵、changed-file lint/compile、ownership、必要L0、diff和F1/F2验证；大范围回归交必需CI。真实实验一次执行后，使用已发布cohort另实现重算主均值、折/包、干预、UNKNOWN与分类，核对fit记账和stage闭包；不因审计再读新收益源或重fit。至少两轮同窗口审核，不冒称独立外审。

本阶段文档合入标准：P12哈希读回、下列设计验收项无未批准设计缺口、F1和蓝图F2校验、`git diff --check`、必需CI通过。设计阶段不执行实现测试，不将计划中的测试名当成通过证据；经济阴性也可按完整合同交付实现及结果，不以强求盈利无限续试。

## Design Acceptance Index

| ID | 设计验收目标 |
|---|---|
| F-521 | 单一信息问题、P12不可变证据、与P11查重及经济未知边界 |
| F-522 | 机构日计数、固定30/90窗、假设时钟、UNKNOWN与原人口完整性 |
| F-523 | 同支持8/9字段、共同成熟训练、固定模型与最多六fit |
| F-524 | 原生5TD净主终点、原三分类和所有终态停止 |
| F-525 | 源/阶段身份、原子发布、claim恢复和资源约束 |
| F-526 | 最小实现范围、直接验证及真实研究算术审计方案 |
| F-527 | 设计/实现/研究/生产分态与蓝图唯一队列一致 |

## Design Acceptance Matrix

本矩阵验收P13设计合同，不验收尚未实施的模型能力。下一阶段必须用真实代码/测试/运行结果更新相应行，不能复用DESIGN状态宣称功能完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-521 | 本文Background/Input/Feasibility | artifact: X:/AIstock_temp/ADVISORY/p12-report-information-inventory-20261024/delivery_receipt.json；11文件SHA读回 | DESIGN_AND_INPUT_PASS | none |
| F-522 | 本文Contracts及原人口合同 | artifact: P12 input_validation_receipt.json、audit_receipt.json；13合成项、128stock-D/2534引用通过 | DESIGN_PASS | approved_by_user: 本轮仅设计，离线输入叶与跨节点验证尚未执行 |
| F-523 | 本文Model | artifact: X:/AIstock_temp/ADVISORY/p13-report-arrival-design-20261024/review_receipt.md | DESIGN_PASS | approved_by_user: 预留最多六fit，实际0；源码/成熟训练数据尚未执行，不宣称模型完成 |
| F-524 | 本文Evaluation | artifact: X:/AIstock_temp/ADVISORY/p13-report-arrival-design-20261024/review_receipt.md | DESIGN_PASS | approved_by_user: 评价未执行，经济NOT_TESTED，不允许预算外续试 |
| F-525 | 本文Implementation Plan | artifact: X:/AIstock_temp/ADVISORY/p13-report-arrival-design-20261024/review_receipt.md | DESIGN_PASS | approved_by_user: stage/claim测试与真实运行由下一阶段实现 |
| F-526 | 本文Verification Plan及精确四叶 | artifact: X:/AIstock_temp/ADVISORY/p13-report-arrival-design-20261024/validation_receipt.json | DESIGN_PASS | approved_by_user: 本轮文档校验，未运行模型测试或真实研究 |
| F-527 | 蓝图§6.3.31/§16.0及本文Scope/Production Gates | artifact: X:/AIstock_temp/ADVISORY/p13-report-arrival-design-20261024/review_receipt.md | DESIGN_PASS | approved_by_user: 用户本次执行下一设计阶段；仅文档交付，不把既有合入授权扩大为生产写入/服务操作 |

## Non-goals / Risks / Production Gates

仅Advisory离线重排序设计；不改QE/HMM/Selection/StrategyPackage/LocalSIM、价格/Exit/仓位、LLM正文、公告、流水线或启动脚本。0DB/DDL/DML、0数据采集/切换、0模型激活、0服务控制；production_ddl_gate=noop、dependency_gate=noop、backend_restart_required=false，BUG close-sync不适用。生产持久化和业务接入不在本轮。

主要风险是12.37%稀疏支持、供应商历史重构/补录/删除、字面机构漂移、报告日集中财报季、计数未包含方向以及父模型已经吸收信息。三折是同一反复消费开发窗，支持数量不是独立样本量；低相关不证明新Alpha；历史首发时钟未知使正向探索也不能直接确认生产价值。保留负结果和失败，不要求数据owner为阴性重采同版。

## DESIGN-COMPLIANCE-001 / 四项逐条审查

1. 禁止简化交付：当前交付完整F1设计和蓝图同步，明确源码/真实fit/收益验收未执行；不把设计预算或P12输入PASS当模型成果。
2. 禁止静默错误：缺记录、缺时钟、输入资格、未成熟、原子阶段与内容身份均有明确UNKNOWN/失败分支；未运行不写成功。
3. 禁止改变业务逻辑：原Top50、固定Top10/5TD、成本、native主终点和三分类保持；P11阴性不改判，不并行评级/正文/价格方向。
4. 禁止私增门禁审批：只用已有输入身份、成熟监督、不可变fit预算与CI流程；不要求先生产入库、补PIT或另造平台才允许有限探索。设计合入与下一实施阶段分开是本次任务范围，不增加额外审批流程。
