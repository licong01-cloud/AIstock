# 原生锚定的固定5交易日Top10组合研究（F1）

版本v1.1；2026-10-21；归属Advisory/model_first。设计PR #6527已合入cb29720f01c68bb202c930cb324bf83d09f9e540；
源码及一次真实inner/outer已完成，结果NOT_WORTH_CONTINUING_THIS_CANDIDATE。源码PR/CI/合入单独交付，0确认/激活。
依据：蓝图§6.12、§16.0及[训练期OOF诊断](../analysis/advisory_top10_oof_complementarity_20261021.md)。

## Background / Goal / Feature Card

唯一假设`NATIVE_ANCHORED_TOP20_TOP10_FIXED_3_TO_1_V1`：在原Top20中保留父排序主导，
是否比完整模型替代排序更好地利用其局部互补信号，并相对原Top10获得五日净增量。
区别于已失败的完整Top50模型重排、20D更新与旧P0-J拟合残差：本候选不重新训练模型、不拟合残差或权重、
不沿用旧P0五槽policy，而是在固定五交易日、完整候选身份下检验一个限定动作域的排名组合政策。
原失败候选不改判。本候选新lineage为EXPLORATORY_SCREEN / ALPHA_RANKING / NAVIGATION_ONLY。
训练OOF诊断只是导航；旧outer已消费，不能恢复独立性，也不把同一数据的重复评价宣称独立确认。

## Inputs / Scope

只读`advmultitop10_6c632adf532818cfcd059113`既存阶段：prepared/selected/predicted/evaluated。
继承source plan、阶段hash、输入/政策、Ridge100的唯一选定身份、环境版本和源码摘要。
输入全Top50、两包、524D/52,400行保持；inner126D，outer219D，各自原轴保留。
不重读active profile，不查询DB，不重算行情/标签，不读取sealed（2026-08-31起）金融值。
原prepared与evaluated的历史身份限制不升级为native/vintage COMPLETE。
来源包ID仅作身份与分组，不进模型特征，不按包调整政策或增加白名单。

## Non-goals

不训练新模型、研究其它权重或选股数量，不预测买卖价格，不建设API/UI、生产角色、自动调仓、
通用ModelOps或共享组合平台；不恢复旧候选，不重复QE上游研究。

## Ranking / 唯一组合政策

对每包、D，原Top20里`no_rank_advice=false`的股构成可重排集合，原rank槽为S。
按冻结Ridge100预测净相对收益降序、原rank稳定破同分，将S从小到大分配，得到局部模型排名m。
组合距离`3 * native_rank + m`从小到大排序，平手保留native顺序，再分配同一S。
没有模型建议的股保留自己的原槽；Top21～50原槽完全不变。
输出派生排名≤10的股票；原生rank、完整评分、候选、来源和UNKNOWN不覆盖、不补位、不重新Selection。
父和子均是同一集合的顺序尺度；3:1是一次保守设计选择，不是统计最优估计，也不宣称减少风险有保证。
不搜索1:1/2:1/4:1、不改变候选Top20边界、不加swap预算、不反转模型或选择表现好的包。
这只是Advisory离线排序候选；不修改共享内核、真实选股、成交、账户、现金或仓位。

## Clocks / Label

输入仅对应D可见信息；父研究原三fold模型的训练截止与label_information_end成熟约束保持。
每fold只对H≤本fold结束的标签计量，未来值不解码；未成熟/UNKNOWN保留原状态与分母。
固定T开盘至T+4收盘，沿用已冻结成本、合法不可入场现金及真实缺价规则，不是H5六会话或五次复评。
只从已经冻结的OOF和outer预测导出新排名，无新fit、无任何目标驱动校准。
outer先冻结全部派生预测，再解码其评价金融值；不能把此排序屏障等同新的未消费holdout。

## Evaluation / 单一候选与分支

一个组合候选，原生Top10为唯一经济主基线，原Ridge100完整重排为机制对照。
inner共同D要求三臂两包结果均完整；包先等权、再D等权，不按股票数或stock-D重复计独立样本。
继续outer的事前规则：组合相对原生平均>0、至少2/3fold为正、共同成熟日覆盖≥80%，
至少20干预D且占成熟D≥20%、至少两个fold干预。每fold必须有配对日。
不要求相对完整模型控制更高才计算outer，但完整模型差值必须报告，不能隐去。
未满足为INNER_NOT_SUPPORTED，研究完整结束，不读outer收益、不调参重选。

outer使用219原D、两包和完整人口，沿用原评估：原轴10D block/10,000次bootstrap，
平均增量≥10bps、95%下界>0，至少20干预D/20%成熟D/两个时间块、共同覆盖≥80%、
UNKNOWN增加≤2百分点、最差10%cohort恶化≤100bps，才值得另行设计独立确认。
平均≤0、95%上界<0或尾部恶化>100bps时停止该候选；其余INCONCLUSIVE，不自动追试。
三臂需在相同共同D比较，原native/candidate完整独立可测D也单列，避免挑不同人口作差。
分别报告收益、避亏/漏盈/新增盈亏、包/时间块、干预、UNKNOWN、MDE与资源；cohort不是NAV或实际成交收益。
此研究不能激活模型。新确认须有真正未消费的数据与预注册合同，日频原基线不受研究结论阻断。

## Operations / Implementation Plan

先合入本设计，再从最新main独立源码树实现。
精确源码：`backend/services/advisory_model_first/native_anchored_top10_5td_v1.py`；
直接测试：`backend/tests/advisory_model_first/test_native_anchored_top10_5td_v1.py`。
更新本文及蓝图实际状态；不修改旧多时段/更新/共享算法或其它owner源码。
复用已有cohort/evaluate、projected_finance、publisher、read_stage、registry与来源Study，
不另写收益算法。新plan绑定全部政策/源stage/实现；派生输入、inner、outer预测、结果原子保存。
恢复复用同一plan已完成stage，检查hash漂移，不覆盖旧输入/研究；0fit不意味着0候选，registry登记1个候选。
X盘新run，原小Parquet就地消费；CPU≤2线程，无GPU/训练；RSS≤2GiB、一次计算≤30分钟、输出≤100MiB。
不运行QE、生产回放，不写库/激活数据/控制服务；离线源码不需用户重启。

## Verification Plan / Test Plan / Design Acceptance Index

| ID | 验收目标 |
| --- | --- |
| F-421 | 原Top50身份、唯一Ridge来源及政策绑定；未知/原槽保持 |
| F-422 | 唯一Top20内3:1排序、稳定破同分、Top10、21～50不变 |
| F-423 | OOF折叠与H截止、先预测后outer评价；sealed隔离 |
| F-424 | 单候选/0fit/共同D/原生及完整重排控制；条件停止 |
| F-425 | 原子阶段、exact retry、漂移/缺候选报错 |
| F-426 | 完整实际结果、资源及模块/生产边界 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-421 | native_anchored_top10_5td_v1.Study / same_population | artifact: anchored-study/fresh_process_verification.json；原始身份与评分完全不变 | SOURCE_AND_REAL_PASS | none |
| F-422 | native_anchored_top10_5td_v1.combine | backend/tests/advisory_model_first/test_native_anchored_top10_5td_v1.py；真实Top20外/无建议槽不变 | SOURCE_AND_REAL_PASS | none |
| F-423 | Study.finance / predictions；run | backend/tests/advisory_model_first/test_native_anchored_top10_5td_v1.py；outer/prepared先于金融评价 | SOURCE_AND_REAL_PASS | none |
| F-424 | comparison_panel / summarize / inner / run | artifact: evaluated/result.json；100共同innerD、205共同outerD，一次候选0fit | RESEARCH_COMPLETE_NEGATIVE_STOP | none |
| F-425 | Study.read / publish / preregister | artifact: fresh_process_verification.json；exact_retry=true、0新增计算；16定向项 | SOURCE_AND_RECOVERY_PASS | none |
| F-426 | run资源/registry；Research Result | artifact: source-ownership.json、source-l0.json、source-junit16.xml；无其它owner/DB/服务操作 | SCOPE_AND_REAL_PASS | none |

直接测试覆盖确定性组合、完全相同排名、未知建议原槽、Top20边界/完整人口、真缺收益保留、
同日不同包/重复键/来源漂移、未来H、inner失败不读outer、成功分支与exact retry。
多轮自审修复、精确测试、Ruff/diff、F1校验及必需CI通过后依已有授权交付。

## Risks

OOF被模型选择和先前研究多次消费，导航选择偏差不可消除；当前区间跨零且包间分化，组合可能无效。
3:1与Top20是冻结政策而非最优性结论；未知使共同D减少时必须报告覆盖和MDE，不能删洞提高显著性。
父/子排名非收益概率或可比较绝对分数；组合也不是概率或收益量纲。不能把机制对照改善替代原生增量。

## Production Gates

源码合入与离线研究分报；本切片不增加backend运行路径，restart/client/dependencies/DDL/data activation=noop。
0DB写/生产回放/真实下单/跨owner修改；后端重启仍用户所有，不以研究/合入授权控制服务。
研究通过也仅支持另定确认设计，不自动绑定bundle或更新原日频程序。

## DESIGN-COMPLIANCE-001

1. 设计交付不冒充源码/运行/经济完成；预定inner停止为合法完整研究终态。
2. 不吞错误、不填收益、不删未知、不把配对诊断当可交易收益。
3. 新组合仅本新lineage；旧模型/政策/历史负结果不改判。
4. 无新增包准入或审批；科学导航/确认与用户日频业务分离。

## Research Result / 实际一次结果

根：`X:/AIstock_temp/ADVISORY/top10-oof-complementarity-20261021/anchored-study`；
run=`advanchortop10_52251cc72d96cc07c933ce67`；producer=`74724f0aa67ed3f3fb48e7ede97c07b2f77662b6`。
evaluated stage=`234197f6fe885011aab4633efb654837835d422ad1ac74d662096677b641e1f7`。

inner原126D/12,600候选保留，111成熟D/100共同D；组合−原生+2.313599bps，
三fold为+3.806802/+6.286243/−2.602120bps；83干预D、覆盖90.0901%，满足事前继续条件。
组合−完整模型为−13.192779bps，未隐去该机制对照；这一对照不是本设计的outer必要条件。

outer原219D/21,900候选、214成熟D、205三臂两包共同D完整报告：

| 指标 | 结果 |
| --- | --- |
| 组合 / 原生 / 完整模型5TD净cohort均值 | 93.207944 / 93.583451 / 69.431135 bps |
| 组合−原生 | **−0.375507bps**；95%CI[−8.756906,+8.720444]；MDE80=12.531971bps |
| 组合−完整模型 | +23.776809bps；95%CI[−9.802254,+58.326019] |
| 真实干预 | 173D/214成熟D=80.8411%；4季度 |
| 共同成熟覆盖 / UNKNOWN额外比例 | 95.7944% / 0；组合与原生均109未知槽（含未成熟） |
| 最差10%cohort均值变化 | +27.157844bps；不是资金组合回撤 |
| 四时间块净增量 | −4.774317 / +0.654706 / +13.548919 / −15.565031bps |
| 两包共同205D净增量 | pkg_8dbd…：−2.501475；pkg_cc7e…：+1.750462bps |

主端点点值≤0，按事前规则为NOT_WORTH_CONTINUING_THIS_CANDIDATE；区间跨零，
不能宣称统计证明组合有害或所有组合无效。相对完整替换模型的损失缩小、尾部改善及略高cohort胜率，
均不能替代超过原生Top10的经济证据；不调权重、挑包/季度或继续确认。
现有事实只支持“父排序应保留为主基线”，不证明通过组合创造了新Alpha。

新增模型fit=0，新增政策候选=1，累计189研究fit＋1旧index不变；零拟合不隐去这次候选评价。
一次研究5.465343秒，峰值333,873,152bytes，全部研究产物837,764bytes；0SQL/新H5/Bin/资产复制。
fresh-process exact_retry返回同一结果，原inner/outer所有身份与原始评分不变，
每组仍50原候选/10派生选择，Top21～50及无建议槽不变，旧产物未覆盖。
只读消费已验证历史来源，不升级vintage/native完整性；未读sealed金融值，未确认/激活或改变日频基线。

## Source Review / 多轮修复

第一轮检查固定排名及UNKNOWN边界，直接反例覆盖零干预、单挑战与Top20外保留。
第二轮检查共同日期、三臂机制比较和真实缺数；修正测试异常类引用并增加未知挑战不填收益测试。
真实预登记发现元组/JSON列表合同规范化缺陷，RED复现后在生成plan时规范化；
该失败在研究计算前发生，没有目标读取、候选变更、额外fit或旧plan覆盖。
第三轮核对真实完整人口、固定得分、排名、预测屏障及fresh-process恢复；最终16定向项/Ruff通过。
ownership两文件均Advisory；L0仅一项有界按日/两包groupby的P2，已按原人口与实测资源检查，0阻断。
本轮为本窗口自审，不伪称独立外部评审；源码CI/合入与上述研究完成分别报告。
