# Advisory 同群先导—滞后 Top10 固定五交易日研究 F1 v1.0

日期：2026-10-22。依据蓝图v5.32 §6.3.22/16.0与用户继续实施授权。单模块离线研究，先设计交付，再源码及一次研究；后端无需重启。

## Background / 目标与语义查重

唯一假设 `CONDITIONAL_PEER_LEAD_LAG_TOP10_5TD_1`：在控制本股自身一日延续后，以过去估计的同群领先响应乘D最新同群冲击，能否改善原Top50内固定五交易日Top10的成本后价值。

原生Top10为产品主比较；matched用于区分有限表示增量。系数正负、matched收益和消融单列，不作为第二个产品收益门，不改E2旧合同。可能的经济动机是同群信息传递不同步；D收盘或T开盘已经吸收信息时，本候选应在原固定五日终点失败，不能改成日内或一日终点救结果。

Lo/MacKinlay [原论文](https://www.nber.org/papers/w2977)区分个股反转和跨股交叉自协方差；纽约联储[原研究](https://www.newyorkfed.org/research/staff_reports/sr303.html)讨论信息在股票间传播。它们仅提供机制动机。本研究使用当前候选同群等权参照，不声称复现“大股领先小股”或保证A股可交易收益。

查重：E2是同刻beta与五日残差，group_centered只作同日监督中心化，joint_risk是历史下行二阶矩，serial/volume_pressure是本股分钟顺序。当前两父包50份冻结动态因子源码hash重读一致（LSTM26/TCN24），有行业动量差、行业中性反转和量价相关等已有信息；这些不等于当前D原50股留一同群、条件化本股lag后的响应。20个Alpha158及LSTM/TCN同样包含自身时序，不能宣称新表示与全部父包预测统计独立，亦不要求重新审核父包。无公告/行业新数据前置。

## Scope / 精确归属与产物

仅允许：

- `backend/services/advisory_model_first/peer_lead_lag_top10_5td_v1.py`
- `backend/services/advisory_model_first/peer_lead_lag_top10_5td_pipeline_v1.py`
- `backend/tests/advisory_model_first/test_peer_lead_lag_top10_5td_v1.py`
- 本F1与既有策略条件化蓝图。

只消费原生候选/既存预测和冻结文件，不改父包、QE、Selection、StrategyPackage、HMM、LocalSIM、数据/公共CI。原日频规则、原TopK/原排序不变，新排序仅自身研究产物。无DB写入/数据激活/服务控制/生产回放；X临时及输出，WSL原有Conda拟合，无安装和大资产复制。

## Inputs / 人口、来源与时钟

复用多时段原study `advmultitop10_6c632adf532818cfcd059113` 的冻结身份/文件reader与5TD估值；两包只是当前验证人口，不加入包ID、模型或股票白名单。原package/manifest/run/source/policy/D/T与Top50、原parent score/rank/dtype、UNKNOWN和所有日期保留。source commit、源stage、binding、实际消费片段及环境进入新计划。

原开发人口524D/52,400项，inner只用2024-07-04..2025-09-30的305D/30,500项。outer开发2025-10-01..2026-08-28已经被旧研究消费，条件推进时仍仅NAVIGATION_ONLY；sealed自2026-08-31起收益不读。不得以新release或新模型名字恢复独立性，NON_VINTAGE和旧历史身份限制不升级，不据此阻断父包日频。

每个D使用D可知的当前50股为历史参照群。对这些股票读取过去价格是合法条件化；不能声称该名单在历史校准日已被选中。日历/复权沿源binding，不解析新active。所有数值先投影<=D再解码，未来坏值也不得提前接触。

## Model / 唯一表示与匹配控制

读取至D的67个原交易日价格，形成66个日对数收益r，单位bps；close沿原生复权，factor仅验证价格观测合同，不再乘一次。缺少价格不补零、不跨缺口拼收益；已知无穷、非正价格/factor明确失败。

对每个股票i、每个收益日t，p_i,t为当前50股中除i外当日**实际可观测**收益等权均值，至少40个其它股票。其它股票缺失只缩小合法参照分母，不填经济值。自己不进入自身peer。

校准使用60对：y=r_i,t，u=r_i,t−1，v=p_i,t−1；响应t的最后一天为D−5，最近五个收益日不进入估计。至少48个共同有效(y,u,v)。带截距OLS：`y = a + phi*u + gamma*v + epsilon`，以去均值2×2协方差闭式求解；无参数搜索、正则或lag选择。

协方差行列式相对`var(u)*var(v)`<=1e−10、任何解释方差为零，均typed `DEGENERATE_CALIBRATION`，不伪造gamma或默认参数。残差sigma允许合法0：本表示不除以sigma，不能照搬E2的零sigma拒绝条件。足够正常观测仍发生非有限算术则报错。原日历/唯一50股/日期不符报错。历史不足、有效对不足、D端点或peer不足分别typed UNKNOWN；原股票和原rank槽保留。

共享组件为D的本股r、peer p，以及a/phi/gamma/残差sigma和`a+phi*r_i,D`本股响应。matched：原B八字段＋这七组件。candidate仅多 `gamma*p_i,D` 同群响应。两臂共同支持要求这八个新增量均有限且至少一个原B字段可测。该乘积是组件的交互表示，不是独立外部信息或直接五日收益预测；第二层监督标签仍严格是T开盘至T+4收盘净收益。

固定Ridge(alpha=100,solver=lsqr,tol=1e−6,max_iter=2000,fit_intercept=false)，训练中位数/IQR与缺失标记，y/X同包D中心化，包/D等权。label_information_end必须<=每折训练截止，先选成熟键再读金融值。合法不可入场现金标签0、真未知null；买0.95bps、卖5.95bps各一次，不二次扣成本。

三折训练截止2025-03-31/05-30/07-31，预测窗04-01..05-30、06-01..07-31、08-01..09-30按原日历。每折两臂共6监督fit；满足inner条件才固定2025-09-30最终两fit及一次outer评价，最多8。输入闭式OLS实际估计数另报，不隐瞒成0计算，不算作新增独立假设。历史起点201预测fit＋1旧index，全部实际fit累加。

预测稳定降序、原rank破同分；共同可建议槽内重排，UNKNOWN保留原slot，不补新股票、不生成资金权重。两臂完整预测均原子冻结后才读对应评价金融值，金融估值/原生比较沿唯一既存实现。

## Evaluation / 产品主终点与三类结论

唯一产品主比较candidate−原生Top10；两个包先等权再D等权，共同成熟D完整保留，缺洞不删、重叠5TD按10D block/10,000次/固定seed20261021报告95%描述性区间及MDE80。胜率是cohort胜率，尾部是cohort最差10%均值，不冒充单笔胜率、资金NAV或MDD。

inner仅导航：原生净均值>0、三折均可评价且至少2折>0、共同成熟D覆盖>=80%，原生干预>=20D且>=20%成熟D、跨至少两折；尾部恶化不超过100bps、额外未知槽<=2百分点。全部满足才`INNER_SUPPORTED`；原生均值<=0或超风险约束为`INNER_NOT_SUPPORTED`；其余`INNER_INCONCLUSIVE`，均不自动追加样本/outer。区间跨零本身不额外否决inner探索。

matched增量、matched干预、逐折gamma及交互系数完整报告，正负均不决定产品推进。所有原包、时间段结果同时显示，不挑最优包、D、seed、阈值或lag。

outer若执行：主均值>=10bps、95%下界>0、覆盖>=80%、原生干预>=20D/20%/两季度、额外未知槽<=2百分点、最差10%cohort均值恶化<=100bps才`WORTH_NEW_CONFIRMATION_DESIGN`。主均值<=0、上界<0或超尾风险为`NOT_WORTH_CONTINUING_THIS_CANDIDATE`；其余`INCONCLUSIVE`。这是新合同事前标准，不重判E2。缺少独立窗口不伪称确认；本轮不激活、不开sealed。

## Feasibility / 已完成的真实准备

六D在训练端事前固定：2024-07-04/08-01/09-02/10-08、2025-01-02/03-31。600原行保留，597满足lag观测数量、3最新端点/peer不足UNKNOWN；各组最少有效对48～60。4.308453秒、消费854,112bytes、结束RSS212,717,568bytes（非OS峰值），0数值收益计算/参数估计/监督fit/标签/DB。

`X:/AIstock_temp/ADVISORY/lead-lag-20261022/input_availability_receipt.json`：binding `589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`，消费片段hash `1b3b2b1859916d74eac95bffa1a9cff741f4612c3b63249ac9731b787d61de27`。不是全数据内容hash或完整特征可测/盈利证明；全窗、数值退化和真实研究仍待实施。

## Implementation Plan / 登记、恢复与预算

复用FrozenDataset/file_dataset/现有有界Bin读取和economic_entry_pipeline原子stage、research_control登记/锁、prequential成熟金融投影、native_anchored估值/统计；不修改这些已有合同。新pipeline冻结自身代码、直接依赖、环境和源stage身份，真实消费片段hash+stat lease进入input receipt，重试验证漂移；不覆盖旧输入或用新active改变边界。

监督fit先写唯一STARTED claim，发布模型/预测后完成；发布前中断不自动refit。已完成阶段只读恢复；原子stage身份/金融成熟时钟/两臂支持错配必须失败。新experiment/run独立登记；累计研究轮次不清零，ORACLE/合成检查不冒充模型trial。闭式统计数与监督fit分账。

已有WSL环境、2CPU线程、无GPU；2GiB RSS、单进程30分钟、200MiB输出、原52,400行与2M批读单元上限。只读核对QE公开运行状态和资源，遵守既有并发授权；不提交/控制QE。Linux临时指向验证可用/mnt/x/AIstock_temp/ADVISORY/lead-lag-20261022，Windows全X。

## Non-goals

本候选不预测开盘价、买卖区间、资金仓位或分钟执行，不研究新父包，不给其它消费者复用研究结果。公告及新UI仍待后续优先级；不恢复已停止E2或旧frontier。

## Verification Plan / 验证方案与多轮审修

直接合同覆盖：精确leave-one-out/条件OLS与自身延续分离、67日/60对/D−5边界、48/47及40/39阈值、未来数值隔离、单位缩放/行列身份；真缺/零方差/退化typed分支；成熟训练与JSON预测同支持、原slot/Top10；产品推进不被matched/系数符号替代；原子身份、fit次数/中断/恢复、实质代码及换行身份、数据漂移。

至少两轮同窗口不同关注点审核/修复：先PIT/数学/经济时钟，再身份/恢复/主终点；不称独立外审。changed-file Ruff、直接测试最终小矩阵一次、L0/ownership及F1校验、必需CI。经济完整研究只在源码稳定且commit可绑定后启动，终态回写设计/蓝图；负向停止本候选，不补旧E2证据。

DESIGN-COMPLIANCE-001四项：完整来源/实现/研究/盈利分态；未知/失败不默认成功；原政策和E2结果不变；无新增包准入、人工审批或运行激活。设计PR通过不是源码或收益通过，源码合入也不等于生产角色启用。

## Risks

同群信号可能只是共同冲击/自身延续，匹配控制和条件OLS不能证明因果；当前D名单条件化不能当历史成员。统计估计有噪声，需完整报告退化/UNKNOWN。T开盘后可能无剩余收益，固定5TD原生终点直接检验；不换持有期。多轮开发消费与五日重叠使CI偏乐观，保持探索等级，不用股票行数替代独立日期。当前MDE约50bps量级，10bps目标未必有功效，三类结论透明而非保证产出winner。

## Production Gates

`production_ddl_gate=noop`、`production_db_writes=false`、`production_8001_touched=false`；依赖安装/数据激活/服务重启/生产角色激活均noop。本离线源码不要求后端重启；未来集成若需重启由用户执行。源码/经济/运行/其它消费者验收分报。

## Design Acceptance Index

| ID | Requirement |
|---|---|
| F-451 | 查重、真实六D准备、唯一有限表示与产品主终点 |
| F-452 | past-only条件lag1估计、留一peer、共同支持与typed缺失 |
| F-453 | 原人口/成熟标签、固定六至八fit、训练编码与slot排序 |
| F-454 | 原生主比较、matched解释、三类结论及收益屏障 |
| F-455 | 原子登记/阶段/claim/代码与消费内容/恢复/资源身份 |
| F-456 | 精确Advisory归属、直接回归、多轮审修与零生产变更 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-451 | Background/Inputs/Feasibility | artifact: X:/AIstock_temp/ADVISORY/lead-lag-20261022/input_availability_receipt.json | DESIGN_REVIEW_PASS | none |
| F-452 | Model/Inputs | target: backend/tests/advisory_model_first/test_peer_lead_lag_top10_5td_v1.py | DESIGN_REVIEW_PASS | approved_by_user: 设计先交付，后续源码与全窗按本F1实施，未称SOURCE_PASS |
| F-453 | Model/Scope | target: backend/tests/advisory_model_first/test_peer_lead_lag_top10_5td_v1.py | DESIGN_REVIEW_PASS | approved_by_user: 同上，当前0fit |
| F-454 | Evaluation | target: backend/tests/advisory_model_first/test_peer_lead_lag_top10_5td_v1.py | DESIGN_REVIEW_PASS | approved_by_user: 收益屏障与完整评价待源码实施，不称经济通过 |
| F-455 | Implementation Plan | target: backend/tests/advisory_model_first/test_peer_lead_lag_top10_5td_v1.py | DESIGN_REVIEW_PASS | approved_by_user: 登记恢复待实现，现有输入收据仅六D |
| F-456 | Scope/Verification | artifact: 本F1精确范围和验证方案 | DESIGN_REVIEW_PASS | approved_by_user: SOURCE/真实研究/合入及清理后分别回报 |
