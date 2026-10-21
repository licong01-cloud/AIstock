# 原生锚定的固定5交易日Top10组合研究（F1）

版本v1.0；2026-10-21；归属Advisory/model_first；阶段DESIGN_READY，未实现/运行。
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
| F-421 | Inputs / Ranking | artifact: 父阶段身份及OOF诊断 | DESIGN_READY | approved_by_user: 设计先交付，源码后实施 |
| F-422 | Ranking | artifact: 固定代数政策及测试方案 | DESIGN_READY | approved_by_user: 设计先交付，源码后实施 |
| F-423 | Clocks / Label | artifact: 继承成熟截止及outer屏障 | DESIGN_READY | approved_by_user: 设计先交付，源码后实施 |
| F-424 | Evaluation | artifact: 一次候选及分支终态 | DESIGN_READY | approved_by_user: 设计先交付，源码后实施 |
| F-425 | Operations | artifact: 现有publisher/read_stage复用合同 | DESIGN_READY | approved_by_user: 设计先交付，源码后实施 |
| F-426 | Operations / Boundary | artifact: X/CPU/无服务操作边界 | DESIGN_READY | approved_by_user: 设计先交付，源码后实施 |

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
