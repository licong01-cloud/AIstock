# HMM Evolution Phase 2：L2轮动P0完整详细设计

> 版本：v1.6；修订日期：2026-10-06；tier：F2；owner：HMM。
> 父蓝图：`hmm_evolution_and_risk_management_system_design_20260716.md` v2.59及其后保持本L2方向的版本。
> P0基线：`9640cf67c5c40884e0f99074224cff0f0270e1f6`。用户随后明确要求按本文开始P1完整实现，D1～D6因此作为本版本获准实施的精确合同；生产DDL/DML、runtime activation、服务重启、QE实验及tail读取仍未授权。
> 当前状态：原P1资金流基线已完成历史评价并持久化/API可读，mean Rank IC=`0.02973212294698707`（原10D/358日口径），研究页面当前验证尚未闭合；2026-09-22的输入阻断只作历史。原冻结HMM效果IC=`0.009072776767716752 < 0.02`。§8.2设计#5433及源码/结果#5444均已合入，#5444 merge=`2fa41eab6b7efc76173ae3a0ced4143006b1671c`；R1双process零fit终态`BELOW_BINDING_MBE`，IC=`-0.009341302842128188`。独立risk双process共2fit达到development效果要求：precision lift=`0.1174666556015582`、recall=`0.4121112481161803`，仍`FORWARD_UNCONFIRMED`；其完整产品#5449已合入并完成DEV/生产55,544行和用户重启后真实API/UI，详见直接risk产品设计v1.3，不能代替本轮动run的surface验收。原标签经单独批准只重建一次并严格匹配`61600e85…`。两候选已停止，模型、§8.2公式和阈值不改；当前任务复用已有资金流产品，不重训/重导。
> 完成定义：源码合入、模型评价、产品表面及生产状态分别核算。当前用户将QE验证后置，近期按父蓝图推进轮动改进与独立风险，不要求为模型结论执行DDL/DML或发布；真实API/UI仍不得由离线结果代报。

## 1. Background、目标与非目标

终极目标是预测申万二级行业未来相对强弱及独立风险，并经场景验证后支持QE、荐股和模拟盘。本设计首先交付一个完整L2轮动纵切：正式共享输入→L2因果分数→完整截面评价→真实存储/API/可配置排名。不是让131个行业分别通过三态HMM普查。

保留历史L1 v1.6、risk_L1、旧QE L2 HMM及全部实验记录。L1 v1.6的Rank IC `0.039580909571655214`是其旧数据/目标合同下的历史结果，不是L2效果，不将L1分数广播给L2。v1.6为零fit确定性资金流信号，迁移的是可解释假设而非训练权重。

### 1.1 当前源码核对与最小复用

| 当前位置 | 已核对行为 | P1处理，不代表本次已实现 |
|---|---|---|
| `rotation_l1_gbdt.py::_moneyflow_intensity_from_window`、`build_v16_single_date_feature_frame` | 两个20日资金流比值之差，窗口相隔5日；单日入口固定31行业 | 抽取必要纯计算到中立HMM模块，L1通过回归保持原数值；L2显式新合同，不复制实现或简单改常量 |
| `rotation_l1_input_bundle.py::_append_g2a_l1_daily_inputs` | 按L1聚合，count≥5且有效成员覆盖≥90% | 按正式L2 PIT身份聚合；L2小行业规则单独提案，不能偷改L1门 |
| `scripts/precompute_hmm_coefficients.py` | 共享稀疏code map、membership、quote authority、quote/moneyflow分离 | 复用已受审读取规则；不调用旧系数生成、模型恢复、数据库入口 |
| `rotation_l1_prediction.py` | writer/validator/readback绑定L1、31行及旧模型身份 | 新L2专用薄repository和明确表；只提取实际共享纯校验，不放宽L1约束 |
| `backend/routers/hmm_risk.py`、`frontend/src/lib/hmm-risk/api.ts` | `/overview`、`/rotation-l1`等L1接口 | 保留旧端点；新增L2明确端点和类型，在现有HMM页面增L2视图 |
| `industry_pit_adapter.py`与共享release reader | C-013因果分类与index-membership具有各自权威及不可用原因 | 不用事后成员表替代历史available-at；只消费与request闭合的正式权威 |

### 1.2 Scope与Non-goals

- 包含P1模型/数据/回放/产品、B消费者输出接口和风险边界，一份设计不拆adapter、测试、API、UI微阶段。
- 原P1不研发新GBDT/HMM/jump，不复活B3/P6，不搜索horizon/窗口/seed；§8.2新增共享逻辑回归风险及唯一score提案须独立批准，未获批不实现/运行。不新增训练平台、feature store、registry、调度器。
- 不修改QE/Selection/Advisory/Paper、行业黑名单或数据生产；QE正式实验由QE窗口执行。
- 不用描述性页面代替预测目标；可诚实交付未达效果门的研究回放，但不得升级为预测能力。
- 本轮不读取`2026-04-01`起的sealed tail outcome，不以改用L2恢复旧数据的untouched身份；不执行依赖安装、DDL/DML、activation、进程控制或cleanup。

## 2. Architecture与身份

1. 显式解析active profile并冻结request；正式reader按列/日期读取必要共享文件。
2. 逐日PIT归属与股票资金流/成交额生成L2汇总；同release官方L2行情只用于报价资格和独立outcome。
3. 唯一确定性模型产生完整目录状态及全部合格L2分数；两fresh processes重算，parent核对输入权威。
4. 先封闭分数payload，再读取development成熟标签评价；tail reader不可达。
5. 同一已验证payload写入L2表、readback、API及UI；UI截断不改变计算、评价或消费者输出。

推荐版本：`hmm_risk_rotation_l2_moneyflow_delta_v1`。`model_hash`是包含公式、数值、因果时点、eligibility、rank/tie和state语义的canonical contract SHA256，不伪装为booster；`evaluation_contract_hash`另绑定窗口、指标和效果规则。`run_id`绑定二者及input identity，不以model_hash代替数据身份。

request至少包含：source commit、model/evaluation contract hash、profile generation与SHA、release_id/cutoff/manifest identity、组件文件SHA、code-map digest、PIT/available-at authority、quote authority、security identity、suspend/provider-absence authority、calendar hash、运行环境和明确输出根。组件同release，code map不得与旧模型key顺序混用。开始前解析一次，运行中不追随active profile变化；已冻结合法request继续使用其原root，不自动替换。

## 3. D1：共享数据、PIT与逐日资格（已批准精确合同）

### 3.1 来源、范围与读取边界

执行时使用经正式resolver验证的最新统一QE/HMM release，当前要求cutoff为`2026-08-31`。不硬编码R7/R8/hotfix路径或旧manifest；实际generation/root/hash由P1首次request解析后报告，不在本文虚构实时身份。

| 输入 | 必需用途与校验 |
|---|---|
| canonical open calendar、PIT instruments | 交易日偏移、当日股票分母；禁止当前上市列表替代PIT |
| 共享code map | `l2_code_id→canonical_l2_code`一一对应；官方文本代码为行业身份；稀疏ID及>130合法 |
| C-013及共享membership spans | 解析历史有效归属与当时可得性；release成员span是闭合资产，不是绕过C-013 unavailable的fallback |
| 日频股票moneyflow/amount、security identity、suspend、provider absence | 按既有正式reader单位规范化为CNY；不混用元/千元/万元，不填缺失净流入为0 |
| 共享`sector_data.h5`及quote availability | 官方L2 `sw2_pct_change`用于标签，报价资格有精确生效日；不以L1或股票收益聚合代替官方L2行情 |
| 同release CSI300指数close | 只用于评价相对收益；不进入此模型score，不加载market estimator |

131是本版canonical目录目标，不是每天必须131个有限预测。正式preflight检查目录精确集合及对应hash；未来taxonomy变化不能只改计数。119 active/113有报价/6停发只是历史窗口统计，不写成全时间常量。

HDF若按股票重复存储行业行情，先按`date,canonical_l2_code`检查所有重复承载行的同字段一致性（含NaN模式），再生成唯一行业日值；不得求和、平均、任取首个有限值或把重复股票行当独立行业。moneyflow不得把这类重复行业字段重复相加。

源文件内容hash可使用既有reader/manifest校验，禁止为本设计另建全历史冻结。语义读取只到development允许末日；对table HDF做日期/列选择，若格式只能整表解码且会访问tail outcome，则明确阻断，不以“读后过滤”声称tail未读。日历、schema、hash和availability元数据读取不等于读取tail收益。

### 3.2 人口、缺失与coverage的三个分母

对source day `u`，目录`D`、当日有PIT成员且当时有正式报价的结构集合`S(u)`、具备完整合法25日feature历史的集合`E(t)`分别持久化。`as_of(t)=prev_open(t)`；`E(t)`只取决于截至as-of的信息，不看score、标签或是否效果通过。

严格定义`E(t)⊆S(as_of(t))`：成员是否存在以已解析的因果归属为准，不能把unresolved股票猜配到行业；所有unresolved股票仍在全PIT分母单列，报告不能声称其已覆盖。25日完整性针对资金流/成交额和逐日贡献资格，不额外要求25日全有官方指数报价；当日结构报价资格取as-of，标签报价路径另按§5检查，二者不得互相替代。

股票集合先由当日PIT instruments限定，再由正式因果分类映射到L2；悬空、矛盾、未知代码或未知reason是数据错误。正式`classification_authority_unavailable`不被猜成行业，其股票保留在全股票coverage报告，不从目录或消费者股票面板消失。若共享有效归属与因果authority不一致，停止；不能择一迎合结果。

这里的“不一致”指两者均明确resolved却给出冲突身份/生效范围；C-013正式unavailable不是可用span的反证，也不是允许回退的理由。一个行业没有resolved成员但存在归属不确定股票时，显示`no_resolved_members`而不是断言这些股票不属于该行业；不使用未知分类证明全市场完整。

每个行业日的期望contributors是有权威归属的PIT成员减去正式停牌成员。停牌同时从分子/分母排除；停牌不得被当作缺数，不影响其他行业。非停牌provider-absence仅在既有authority明确证明时允许作为typed NA：留在期望分母、不进入净流入或成交额求和。未解释缺数/非法数值不能套provider-absence。

每日有效contributors至少1只且`valid/expected≥0.90`，有效成交额sum有限且>0；该规则适用于25个source日中的每一天。不沿用L1固定最少5只：真实L2可能少于5只，改用比例并显著报告最小成员数、最大成员成交额占比，不增加集中度效果门。允许单成员不代表统计可靠，是D1已批准的显式取舍。

- `expected=0`、全部停牌、当时尚未发布/正式停发或新行业尚无完整25日历史：行业typed unavailable，其他行业继续。
- 有权威provider-absence导致coverage不足：typed unavailable，计入结构覆盖缺口，不临时改比例。
- 正式应有字段缺失、未知非有限值、重复冲突、identity/hash漂移：batch fail closed；不得缩小`S(u)`或`E(t)`绕过。
- quote unavailable时，仅报价字段遵守停发合同；真实moneyflow允许有限值，但不能据此生成“有报价”的L2预测。membership保留，不要求数据窗口清空真实资金流。
- 全部目录行业每日均有状态。`|E(t)|/|S(as_of(t))|`、全股票因果mapping coverage、逐行业feature coverage分别报告；目录完整不等于行情完整，不用131硬填分数。

P1先完成一次列/日期受限file-only预检并冻结已解释不可用集合，两个process复用结果；不能为每次评分重扫全量股票历史。当前P0不宣称这些新窗口的输入已预检通过。

只复用必要IO、security/PIT与moneyflow字段规范化，不调用旧C-010/B3/full-family全量builder来“顺便”生成L2，不引入旧train-only contributor筛选、circ_mv或L1 index close依赖。没有需要的共享字段/合法权威就给出准确缺口；不暗中新增数据准备流程。

## 4. D2：唯一零fit公式与状态（已批准精确合同）

对行业`s`和每个source day `u`，使用当日PIT有效contributors同一集合求和：

```text
F(s,u) = fsum(net_mf_amount_cny(i,u))
A(s,u) = fsum(amount_cny(i,u))
I20(s,t) = sum[u=t-20..t-1] F(s,u) / sum[u=t-20..t-1] A(s,u)
delta(s,t) = I20(s,t) - I20(s,t-5)
```

偏移均为canonical open sessions，因此需要`t-25..t-1`恰25个source日。每一天按其当时成员归属，不能拿`t-1`成分回填25天；不能拿当前行业归属处理历史。只使用D1已批准的CNY金额与coverage，不额外加入circ_mv、breadth、market state、L1 score或资金流权重搜索。

在`E(t)`内对delta升序做精确float64相等值average rank：

```text
N = |E(t)|
score(s,t) = (average_rank(delta(s,t)) - 1)/(N - 1) - 0.5
```

`N<2`时分数不可定义，整日typed unavailable；不得除零或填neutral。有限score范围`[-0.5,0.5]`，不是预期收益率、概率或置信度。成员金额先按canonical股票码排序，再用`fsum`，行业和日期也canonical排序，输入行顺序不能改变结果。

state用于连续分数的解释，不是HMM hidden state。推荐`q=min(floor(N/2),ceil(0.20*N))`，从低到高的前q为fading、后q为trending，其余neutral；跨任一分位边界的精确同分组整体neutral，不以行业代码打破模型tie。与L1最少5的规则不同，禁止机械套用。UI行次序可用代码作为同分稳定排序，但必须标明同分、不能宣称严格优胜。

contribution只存`moneyflow_intensity_delta_5d_rank=score`，说明算术来源；不保留十列伪0贡献，不要求SHAP。不输出market regime、risk warning或posterior。合法不可用行score/state/contribution均null并有reason；所有delta相等时score为真实0、state为neutral，评价Rank IC因零方差另报不可定义，不能用这个事实伪造缺失默认值。

`model_hash`包含上述完整算法；两次process为`planned_fits=started_fits=completed_fits=failed_fits=0`，不需要seed、504日fit窗口、hmmlearn或LightGBM。seed字段写not_applicable，不伪造训练完成。

## 5. D3：一次历史回放、标签和比较（已批准精确合同）

### 5.1 明确窗口与可执行性

推荐只运行一个10D候选，decision范围`2024-09-19..2026-03-31`，分三个不重叠报告块：`2024-09-19..2025-03-31`、`2025-04-01..2025-09-30`、`2025-10-01..2026-03-31`。这是现有已消费development的后三区间，不是新holdout，也不以L1全620日的结果冒充同窗口比较。

选择该起点是覆盖完整可审计L2/PIT输入的保守提案，兼顾约一年半历史与避免恢复早期数据工程；本次未读取新面板证明充分性。P1必须按冻结calendar列出起点前25日、每块实际交易日数、末10日标签不成熟数与总体计数。任何所需PIT/输入范围不覆盖则停止为INPUT_RANGE_INCOMPLETE，不能自动往后缩窗；向用户报告能否复用现有authority，不恢复历史数据工程。

这三个块是报告切片，不是三个开发阶段/拟合fold；模型没有训练、预处理fit、early stopping或选参，所以训练purge/embargo不适用。全部评分仍严格t-1；旧L2 HMM若将来用于对照，必须另核训练截止，不能用零fit规则豁免它。

前三段边界不截断标签：例如2025-03-31的10D标签可以使用下一报告块的行情，只要全部outcome仍在总development末日以内；否则会无依据地每段删10日。仅总体末端按成熟度剔除评价，保留评分。三个报告块及总体分别报告指标，不以挑选最好块决定合同通过。

本候选是2026年研发后选定的假设，`selection_basis=RETROSPECTIVE_DEVELOPMENT_SELECTED`；“因果回放”只证明每个score的输入截至当时可得，不证明2024年的用户已拥有该假设，也不消除开发期选择偏差。不能对QE宣称此窗口是无选择偏差的正式独立收益证明。

### 5.2 官方L2标签与成熟度

冻结`h=10`，采用官方close-to-close收益语义。若共享资产只有`sw2_pct_change`，按官方百分数除100形成`r_s(u)`，不创造指数close，不写合成指数文件：

```text
R_s(t,10) = product[u=t+1..t+10](1 + sw2_pct_change(s,u)/100) - 1
R_m(t,10) = close_CSI300(t+10)/close_CSI300(t) - 1
y_s(t,10) = R_s(t,10) - R_m(t,10)
```

这等价于同一官方指数close(t+10)/close(t)-1的期间收益定义；不是从t-1开始、不包含t日涨跌、不代表真实可成交组合收益。单位必须经正式schema/reader验证，不能猜测decimal还是percent；单日gross return必须有限且>0。CSI300分母有限且>0。

score及`E(t)`先封闭；只有`t+10≤2026-03-31`且标签路径合法时才评价，不为补末端标签读取4月tail。末10个decision仍生成研究分数并标`outcome_not_mature`，不进入metric分母。已知在未来路径中官方停止报价的行记`outcome_unavailable_quote_discontinued`，保留当时真实预测；不回溯删除预测、不填收益0。应有但漏采的标签是输入错误，不冒充未成熟。

实现必须先由日历判断成熟度，未成熟行不请求其未来行情；不得先读到4月后再打未成熟标签。quote authority只能按对应source日状态判断，不能因行业在后来停发，就把停发前的合法样本从历史剔除。

每个可评价日的`M(t)`为已有分数且完整成熟label的行业；报告`|M|/|E|`及不可评价原因，不隐藏幸存者偏差。主评价使用完整`M(t)`，绝不只评价展示的前后N。没有把候选失败行业事后移出人口的通路。

### 5.3 预算、旧对照与责任

正式回放为两个fresh processes，共0 fits，无battery、无参数搜索；parent从同一输入独立核对score/state身份，比较canonical评分与指标payload，不包含PID/绝对路径/时间戳。Windows/WSL只按已支持固定数值环境分别做consumer smoke；不要求跨host bitwise，不能把同机复现宣称跨机保证。

已有L1结果只作层级不同的历史参考，不以L1与L2 IC数值差宣称相对改善。旧L2 HMM是场景辅助系数，并非同尺度rotation score；其模型/参数/数据/训练截止/527日资产适用性在P2逐项核对，不把风险系数强行当未来收益标签或逆序排序。旧臂不可执行时如实报告，不换模型、重训、缩窗或要求重建历史证据。本P1不等待旧臂通过，也不跑QE实验。

## 6. D4：效果、coverage、状态与停止（已批准精确合同）

### 6.1 指标公式与数值建议

唯一binding development效果推荐`mean_daily_RankIC≥0.02`。0.02来源是与旧研究量级一致的**先验约定**，不是从L2数据得出，不声称由产品收益价值严格推导；必须经本次L2精确合同批准，不能视作L1授权自动继承。

每日Rank IC为`M(t)`内score与`y`各自average-rank的Pearson相关；任一rank零方差则该日IC不可定义，记录原因。每日等权，不把股票数或行业数当独立日期数。推荐metric日要求`|M(t)|≥max(2,ceil(0.90*|E(t)|))`；可成熟日期中有效IC日比例≥90%，不足为EVIDENCE_INSUFFICIENT，不自动宣布模型被证伪。

推荐coverage规则：每个报告块及全部development中，`|E|/|S(as_of)|≥90%`的结构非空日比例≥90%；逐行业另报告feature可用日/结构合格日，但不增加“每行业均≥90%”的AND门。未知缺失不能当合法NA获得预算。小成员和短历史行业单列有效日期/成员数，不因缺乏全目录长期数据要求全部行业一起有数值。

任一coverage分母为0时返回null/not_applicable，禁止用0/1伪造失败/成功；整体没有结构非空日、没有成熟日或没有可定义IC时直接EVIDENCE_INSUFFICIENT。未成熟总体末端不进入metric-date比例，但仍计入feature/产品覆盖。`E(t)`与模型可打分集合在N<2时分别报告，不能用特征有限冒充已有预测。

上述0.90规则服务已解释provider-absence及历史可用性，不是新增数值失败豁免；整体coverage不足允许真实研究结果展示但不升级capability，也不消费tail。单行业coverage不足不删除其结果、不阻止其他行业或全批资格；它按实际日期贡献总体coverage并单列局限，不能增加私有逐行业验收门，也不能把局部稀疏误报为全模块故障。

HAC只作为区间与不确定性诊断，不增设t统计量promotion AND门。推荐Bartlett Newey-West lag9：在完整可成熟calendar上，以实际open-session距离匹配有效IC对；缺失日不压缩成相邻交易日、不用0替代其观测值。设有效日数n、`e_t=IC_t-mean`，

```text
Var(mean) = [sum(e_t²) + 2*sum(k=1..9)(1-k/10)*sum(valid t,t-k)(e_t*e_(t-k))] / n²
95% CI = mean ± 1.96*sqrt(Var(mean))
```

缺失日的贡献缺位而非虚造IC观测；n为有效日数。n<2、负/非有限variance或无法计算则HAC_UNAVAILABLE，不clamp，不自动失败模型，不产生确认成功。输出覆盖掩码，解释非随机缺失可能影响区间。没有MDE前置门，forward_power_status未实测时为UNAVAILABLE。

spread为按D2 state分组的真实`y`均值差；任一极端组为空则该日spread unavailable。另报各报告块IC、IC分布、覆盖、组样本数及集中度；IC/spread符号不一致记录诊断，不自动加第二效果门。spread不含组合、换手、费用，不能宣称QE收益。

### 6.2 正交状态与停止

| 情况 | 可做什么 | 不允许什么 |
|---|---|---|
| 输入/身份/数值/因果或两process复现失败 | durable typed failure，停止本批 | 不写成功prediction，不调参重跑、fallback或开第二候选 |
| 可计算但coverage/评价证据不足 | 保留真实回放，展示EVIDENCE_INSUFFICIENT与具体分母 | 不宣称模型无效或有效，不读取tail |
| 证据充分但mean IC<0.02 | 标BELOW_BINDING_MBE，真实研究产品可验收 | capability/advisory仍NOT_AVAILABLE，不自动追加候选 |
| 证据充分且mean IC≥0.02 | RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED资格 | 不等于独立确认、QE有效或生产采用 |
| writer/readback/API/UI未闭合 | 只报告离线结果 | research_surface_status不得为AVAILABLE_EXPERIMENTAL |

所有本批终态`tail_accessed=false`、`forward_confirmation=NOT_STARTED`、`advisory_status=NOT_AVAILABLE`。不把未做功效核算标为PENDING_INSUFFICIENT_POWER。下一独立确认的窗口/统计方案不在本轮授权内；不把展示研发结果变成“必须等新日行情”阻断。

若整个计划窗口没有任何可用预测，记`NO_USABLE_PREDICTIONS`；可展示真实不可用原因，但不得把全null表/空研究页标为research prediction surface验收通过。身份与执行错误也不能借“允许效果不足展示”绕过writer拒绝。

## 7. D5：真实产品、持久化与展示（已批准精确合同）

### 7.1 完整行与最小表

采用一张新的`hmm_risk.rotation_l2_prediction`表，保留L1表与旧端点不变；这是L2明确身份边界，不新建通用registry或第二套写队列。字段：run_id、model_hash、evaluation_contract_hash、input_hash、mapping_hash、quote_authority_hash、trade_date、as_of_date、sector_level固定L2、sector_code/name、score/state/contribution、availability/reason、structural_eligible、feature_eligible、outcome_status、research/capability/forward/advisory状态、validation_basis、revision、created_at及supersedes_id。

唯一键`(run_id,trade_date,sector_code,revision)`；run绑定所有身份。目录行数/集合必须与request code-map一致，同日所有行使用同一run/revision，不能从各行业最大revision拼装“完整日”。一次完整日期批原子写入；可用行score有限，不可用行score/state/contribution=null且reason非空；分类不可用股票的汇总留在run摘要。same-key相同payload幂等，任何不同payload冲突拒绝；修正使用新revision并完整重闭合，不覆盖旧行。

writer接受parent已核对的prediction payload，事务内回读canonical逻辑行hash；不在事务外伪报成功。DB异常回滚；多日提交逐日原子，整个运行只有全部计划日期readback闭合才completed，重启可按同一身份幂等继续，不把部分日期成功写成完整产品。

run摘要（指标、日期计数、目录/资格统计、身份、effect与工程状态）用一个compact manifest供repository读取，与行hash一致，不另建数据资产平台。研究表`validation_basis=HISTORICAL_CAUSAL_REPLAY_ZERO_FIT`，不冒称训练fold OOF。本P1历史模式不支持新增实时写入；未来单日可复用同一feature函数但需明确执行范围。

运行有效性`execution_status`、`effect_status`、`research_surface_status`与`rotation_l2_capability_status`为独立字段；production表不使用占位L1字段。run摘要在全部日期提交且API/UI验收前只能是PENDING_PRODUCT_VALIDATION。surface验证可通过外部紧凑验证记录绑定run及行hash，不回写旧预测来循环证明自己已通过；读端缺该记录时保持surface NOT_AVAILABLE。不得将数据库行中自报的AVAILABLE作为唯一验收依据。

BUG-1753（2026-10-06用户明确要求修复）：实验/产品验证结果是业务记录，不是部署配置；禁止用`AISTOCK_HMM_*_PRODUCT_VALIDATION_RECEIPT`或其他环境变量选择某份记录。默认文件存储为后端服务账户的`~/.aistock/hmm/product_validation`，不在源码或数据集目录内。已有验证结果由`scripts/hmm_risk/register_product_validation.py --receipt <已验证记录>`登记并原子读回；同identity同内容幂等，不同内容冲突拒绝。registration不运行验收、不伪造通过、不重新训练或改写预测。L2轮动按产品域/run_id/完整行hash定位，L2风险另保留部署commit身份；L1历史产品按model_hash/日期/行hash定位，不通过记录自动选择最新模型。读端每次请求查找，不缓存“无记录”；同一进程的新记录下次请求生效，不改`.env`、不需要重启。记录缺失只保留表面未验收状态，不阻断模型研发、回放或既有预测读回；存在却损坏/间接路径/读取漂移/身份不符报typed错误。不改变任何effect/capability/forward/advisory晋升合同。首次加载这项持久化源码修复与以后登记业务结果是两回事，服务启停仍归用户。

状态字段域按§6冻结：`execution_status=COMPLETED|FAILED`；`effect_status=EVIDENCE_INSUFFICIENT|BELOW_BINDING_MBE|DEVELOPMENT_EFFECT_QUALIFIED|NO_USABLE_PREDICTIONS`；`research_surface_status=NOT_AVAILABLE|AVAILABLE_EXPERIMENTAL`；`rotation_l2_capability_status=NOT_AVAILABLE|RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED`。只有完整执行、coverage/效果达标才允许后一个capability值；有真实工程展示但效果不达标仍是前一个值。产品验证阶段PENDING_PRODUCT_VALIDATION属于产品验证记录，不混写execution_status。本版不实现forward/advisory晋升分支。

### 7.2 API与UI

- 新增`GET /api/v1/hmm-risk/rotation-l2/overview?run_id=...`与`GET /api/v1/hmm-risk/rotation-l2?run_id=...&trade_date=...`，返回完整目录状态与全部合格预测。run_id必填；前端使用显式产品配置，未配置显示NOT_CONFIGURED，不以max(created_at)猜当前模型。
- overview包含可用历史日期、真实覆盖/指标/状态、run身份；detail包含expected/actual目录数、member-backed/quote/feature/metric/display分母的原始字段。后端不把页面数量当数据过滤条件。
- malformed参数422、不存在run/date404、身份冲突409、真实存储/校验错误500且typed reason；不得空200冒充完整日期，不吞异常。
- 新增L2主视图，旧L1视图保留显式版本入口。默认top_count=10、bottom_count=10；整数≥0，`1≤top_count+bottom_count≤30`，越界明确错误，不静默clamp。
- 前区按score降序、后区按score升序，同分按代码升序稳定显示。先取top_count，再从未显示集合取bottom_count；有重叠只保留一行，数量不足提示实际值。该规则不改变score或state；前后同分需明确提示无严格差异，不能用代码次序伪造预测差。
- 当前共享release仅冻结正式申万L2文本代码而没有独立名称authority；P1以`sector_display_name_authority=canonical_sw_l2_code_only`明确记录并只显示代码，不从数据库、旧字典或私有映射猜名称。以后补充正式名称必须作为共享authority演进，不能静默改变既有run。
- 完整分母和unavailable摘要一直可见，目录详情可查询但不把unavailable塞进“最弱”列表；UI只限制主排名≤30，不截断API/消费者。切换日期/配置不重算模型，不写DB。
- 显著展示历史as-of、L2、模型版本、research-only、IC及区间/不可计算原因、coverage及forward未确认；fading不是risk warning，不加买卖按钮。
- 真实writer→API→UI闭环采用真实历史payload与无mock浏览器验收；loading/error/empty/not-configured/stale均有终态。只通过单测不算产品验收。

### 7.3 部署与回滚

未来DDL先既有DEV验证，生产迁移/写入与runtime activation各按明确目标授权；用户操作后端重启。本设计合入不执行任何这些动作。L2失败只禁用其配置/入口，不篡改L1旧数据，不自动fallback。新schema回滚只按已批准migration执行，不删除历史预测或用户资产。

## 8. D6：消费者与L2风险边界、一个完整实施包

### 8.1 HMM交给QE的合同，不越界运行

输出所有目录状态及合格L2分数，不是UI前后名单。至少包含run/model/input/mapping/quote identity、数据release identity、日期/as-of、horizon、level/code、score/state、availability/reason及因果声明。`schema_version=hmm_rotation_l2_consumer_v1`与content hash绑定payload，生成动作只在正式P1运行后。

股票映射使用消费decision日当时已知、与模型同源的PIT identity；不得用回放结束时归属。若要求t日开盘使用t-1输入，消费端也只能使用其当时已知的生效映射，不读取t日收盘事实。行业停发/正式分类不可得时，传递`overlay_applicable=false`和明确原因；不是伪造neutral、coefficient=1.0或删除股票。如何保留base Alpha属于待批准B消费公式，不在HMM这里私改策略。

连续score不能直接塞入旧`risk_coefficient`或HMM posterior，不能自动替换旧preset_A资产。旧版本维持原config/snapshot/窗口/SHA。P2按同Alpha、同股票面板、同release/日期、执行成本比较无辅助/旧L2 HMM/新L2选项；若旧模型训练时间晚于测试日，不能作为因果对照，需要用户决策是否另列重训版本，P1不等待此动作。

QE owner负责compose、策略消费实现和正式实验；HMM owner只提供资产、HMM侧验证和明确错误。Advisory/荐股、模拟盘由各自owner验证，QE收益不替它们验收。P1不承诺其接入已经完成。

### 8.2 Contracts：轮动唯一改进与独立L2风险（精确合同已批准）

2026-10-04用户暂不执行QE验证，改为继续HMM侧模型研发与验证。下面在同一设计中一次收敛两个业务包，不另设“数据预检/adapter/评价器/API”阶段。**用户在上一回复明确列出两份D1～D6需要批准后答复“批准”，本节精确公式、参数、阈值与停止条件由此全部获准。批准模型合同不等于PR合入、cleanup、数据库或服务授权；按DESIGN-MAIN-001，先将设计PR合入main，再实施源码。** 原P1、原HMM效果D1～D6及旧生产资产不被修改。

#### 8.2.1 R1：行业内neutral utility中心化，唯一零fit轮动候选

动机仅为待检验假设：当前HMM分数是各行业各自校准的3档utility，描述性诊断中行业间均值占合并分数方差59.1502%，可能将行业固定差异混入当日轮动。相邻状态变化25.3366%，不能诊断为模型“不动”；也不能据此证明中心化必然提高效果。

| 决策 | 推荐精确合同 | 状态 |
|---|---|---|
| L2-R1-D1 | 唯一版本`hmm_risk_l2_neutral_centered_effect_v1`；复用2026-10-04正式封闭request/acceptance、原v17/full-v3及seed47；不重新构造输入、不重新过滤HMM、不fit/D5 | APPROVED_FOR_IMPLEMENTATION |
| L2-R1-D2 | 对正式mapping确定的neutral隐状态n(s)，`raw_score_centered(s,t)=mu(s,k(s,t))-mu(s,n(s))`；mu和hard k均取原冻结结果，禁按hidden index指定neutral、除标准差、截断或重新校准 | APPROVED_FOR_IMPLEMENTATION |
| L2-R1-D3 | 原221日决策/201日复合成熟目标、131目录、原4项mapping不足和其他行mask不变；任何原不可用行仍不可用。daily average-rank、状态投影和展示ties沿原规则；neutral为0是公式结果，不是缺失补0 | APPROVED_FOR_IMPLEMENTATION |
| L2-R1-D4 | 仍以同一复合标签、coverage/有效IC日90%、mean IC>=0.02验收；HAC lag19、固定块、原score/基线及paired差异只诊断，不新增必须击败基线门；全同分日IC不可定义，不能用0充当有效IC | APPROVED_FOR_IMPLEMENTATION |
| L2-R1-D5 | 两fresh-process仅重评分/evaluate、0 fits；pin原参数/输入/封闭预测/标签hash及新score合同hash。原semantic_state不变，daily_rank_group可变化；新contract version独立，不修改旧acceptance/writer约束 | APPROVED_FOR_IMPLEMENTATION |
| L2-R1-D6 | 一次终态即停；低于MBE/证据不足不再试其他中心或标准化，不开新训练。不读tail、不写DB、不部署、不调用QE；达到效果也只形成新的development结论 | APPROVED_FOR_IMPLEMENTATION |

已消费development用于一个明示的新假设，不能冒充独立确认。当前9,573/27,980可用行是neutral（34.21%）；中心化会增加跨行业0分ties，既可能去掉固定偏移也可能损失有用截面信号，必须真实报告全部同分/IC不可定义日及coverage，不能只保留trending/fading。这就是选择零fit、单版本、失败即停而不开展模型网格的取舍。标签读取只复用封闭request中2026-03-31以内的批准结果字段，不重算股票历史。

#### 8.2.2 L2-RISK-D1：独立绝对回撤事件与时间边界

唯一候选推荐`hmm_risk_l2_absolute_drawdown_logistic_v1`，不是HMM状态合格证、相对弱势或由fading推导的warning。每个decision t使用`as_of=prev_open(t)`，标签从t+1开始，固定h=10。

对同一release/C-010股票事实行业收益r_s（decimal），`P_0=1; P_j=product[m=1..j](1+r_s(t+m))`；`DD_min=min[j=1..10](P_j/max[i=0..j](P_i)-1)`；`event=1[DD_min<=-0.08]`。这描述从决策后路径高点发生的绝对回撤，不减CSI300，不把弱于市场但上涨当作损失。-8%是用户已批准的先验业务刻度，并非从已读风险标签寻优；每日行业组合成分由PIT规则决定，不声称可执行持仓收益。

任一未来收益合法NA则标签`OUTCOME_LEGAL_NA`，预测仍保留；未来停牌/membership/停发不能回写t的输入资格。任一r<=-1或非有限未知值是源错误，不补0/插值。正常下跌可以成为真实正例，但不是结构失败。v1.2提案阶段仅做day.txt日历算术；批准后正式标签与执行结果见§8.4。

#### 8.2.3 L2-RISK-D2：冻结输入、训练/开发分离与人口

推荐沿现有v17 manifest=`97df6acdbe43dc20f577e85f8cceb2814d73fca13b0f12cda7aefd90cfb62f2c`、full-v3 PIT bundle=`051e2af357703734080ff3ea5b4311926905aa7cbd1f31d926ef5b8575261313`及原C-010/A5 **20D原列序**。不用9D产品输入或官方L2指数代替股票事实模型，也不用新active身份覆盖旧run；不重导/写数据或数据库。

推荐固定训练calendar为2022-01-04..2024-06-28（原601日）；监督标签必须在训练末日成熟，purge10后实际fit decision为2022-01-04..2024-06-14（591日）。所有训练feature只读各自t-1历史，训练第一个as-of及既有250日warmup由正式builder校验。训练label最晚到2024-06-28，不能为最后10日借7月数据拟合。

唯一开发评价calendar为2024-07-01..2026-03-31（424日），首日as-of=2024-06-28；10D完整成熟414日，末成熟decision=2026-03-17。后10日预测保留但不记有效负例。数值由同冻结day.txt算得，SHA=`ce017cfbf1d9dde630c0d7f39e33b767e95293acd5258104f80491239826207a`；601/591/424/414不替代逐日资格核验，正式完整文件面板核验结果见§8.4。

131目录全保留。逐日S为原C-010/A5有合法L2观测者、P为有真实概率者、M为P中10D标签合法成熟者，分母/合法NA与真实漏采分开；HMM的4个semantic mapping不足不是此新监督模型的排除理由。不得额外删单成员、停牌或无官方指数行业，不要求逐行业三态普查。历史stable-taxonomy-backcast不等于当时as-published，结论限retrospective development；不读2026-04-01起tail。

#### 8.2.4 L2-RISK-D3：一个共享监督模型、全部先验参数

所有合法L2训练行拟合**同一个**正则化逻辑回归，不为131行业分别调参或增设HMM；也不使用行业整数ID作为特征。输入仅原20D，按train期合法fit行计算全局均值/总体标准差z-score，不做截面rank、类别平衡、oversampling、winsorization、缺失填补或概率再校准。严格零方差维度冻结inactive；之后有限非零仍用固定mask并诊断，不动态扩维。其他低方差不得随意clamp；变换或分数非有限fail closed。

固定scikit-learn现存版本，`LogisticRegression(penalty='l2', C=1.0, solver='lbfgs', fit_intercept=True, tol=1e-8, max_iter=1000, class_weight=None, random_state=42, warm_start=False)`；不做CV/超参或seed搜索。同日n_t合法训练行，每行`sample_weight=N_rows/(N_dates*n_t)`，使各日期总权重相同且总权重=N_rows。日期相关性不因行数增加而消失，效果不使用iid行级显著性。

训练没有0/1两类则`TRAIN_LABEL_EVIDENCE_INSUFFICIENT`，不能使用常数概率补位；optimizer未收敛警告或参数非有限则typed fit failed，不加迭代数重试。风险概率取明确class=1列，范围[0,1]；seed只用于新classifier，不修改原HMM seed47。模型及标准化仅train fit一次，整个development不滚动重训。

此选择是低成本监督风险基线而非最终架构保证；若它失败，不据此证明所有非线性/风险方向无效。实现参数和train-only变换依据[scikit-learn 1.8 LogisticRegression](https://scikit-learn.org/1.8/modules/generated/sklearn.linear_model.LogisticRegression.html)及[官方泄漏边界说明](https://scikit-learn.org/1.8/common_pitfalls.html)，不安装/升级依赖。

#### 8.2.5 L2-RISK-D4/D5：报警、效果与不确定性，不追加统计AND门

已批准`warning=1[p_event>=0.20]`，不强制每日报满Top20%，因此全体低风险时可以合法无warning，全体高风险时可以全部warning；UI最多30只是显示限制，不影响计算。0.20对应先验漏报/误报损失4:1时的二分类阈值`1/(1+4)`，该代价是已批准研究刻度，不是用户真实交易损失估计，风险概率尚未被forward确认。

在整体M上`base_rate=sum(event)/|M|`；在封闭warning集合与M的交集计算`precision=TP/(TP+FP)`，`recall=TP/(TP+FN)`，`precision_lift=precision-base_rate`。已批准唯一效果合同为precision_lift>=0.05 **且** recall>=0.25，明确这是本次独立批准的L2先验刻度，不继承L1批准。TP/FP/FN、实际warning覆盖、错过事件严重度、错误报警日的上涨机会、概率Brier score及日期分块不确定性均报告，不作为额外晋升门；不假装已经降低组合回撤。

推荐证据合同：总体sum|P|/sum|S|>=90%；一个成熟日有效需`|M(t)|>=max(2,ceil(.90*|P(t)|))`，414个成熟日中有效日期比例>=90%；至少有一个真实正例与一个标签合法warning才能定义recall/precision，否则`EVIDENCE_INSUFFICIENT`而非假0/假成功。所有标签合法行均计整体指标，未达单日证据比例不得被选择性删除；这也防止只有一两个行业有标签却被算作整日有效。没有按行业/块显著性、最少股票、资源或额外power门。分母为0给null及明确原因，不默认0。

不确定性也一次冻结，禁止实现自行选bootstrap或iid行级区间。逐有效M非空的成熟日记录T_t=TP、W_t=已知标签warning数、E_t=正例数、N_t=|M|；整体precision=sumT/sumW、recall=sumT/sumE、base=sumE/sumN。各指标的日影响量分别为`(T_t-precision*W_t)/mean(W)`、`(T_t-recall*E_t)/mean(E)`、`(E_t-base*N_t)/mean(N)`；lift影响量为precision减base。对日影响量按真实成熟open-calendar距离计算Bartlett Newey-West lag9的LRV，再给SE=sqrt(LRV/n)及点估计±1.96SE。n<2、空分母、负/非有限LRV则诊断HAC_UNAVAILABLE，不clamp或追加效果失败；近边界正态区间不保证[0,1]，不伪造置信度。固定报告块为2024-07-01..2024-12-31、2025-01-01..2025-06-30、2025-07-01..2026-03-31；跨块标签仍按全局成熟上界，不人为删块末10日。

唯一零fit参照是同20D输入的`volatility_Nd`（原3日人口标准差）风险排序。只诊断：每个decision在完整P内按该值由高到低选择与模型warning相同数量、跨入选边界tie整体排除，再在各自合法M报告指标与实际报警预算差。预算为0时不选，预算等于|P|时全选，没有边界tie；P为空时参照也为空并标明原因。不是强制相同未来有标签人口，不以结果挑日期/行业。也不是原rotation moneyflow的收益排名。另报告无筛选base_rate；不私增“必须击败参照”门。

#### 8.2.6 L2-RISK-D6：一个实施/实验包、双process2 fits、停止

推荐一次HMM-owned源码+CLI+直接测试+正式file-only监督面板+评价包；借用当前C-010/PIT/reader和数值纯函数，不创建通用训练平台、scheduler或数据库表。模型预处理属于新风险合同，不套用HMM covariance/MAP/D5/D6普查。两fresh-process各拟合同一唯一model一次，共2 fits；同host固定Conda base/单线程、不改Conda AIstock、不安装依赖，实际environment写入一个结果身份。

两次参数/封闭概率/指标canonical payload一致；先封闭424日×131目录预测，再评价414日成熟目标。结果只有必要预测行、一个compact结论及运行必要参数/输入identity；不重建旧全grid证据、复制源数据或记录逐股票历史账本。`validation_basis=HISTORICAL_CAUSAL_FIXED_TRAIN_DEVELOPMENT`，不是walk-forward OOF、untouched或实时确认。

终态分离：身份/未知源/数值/执行错误FAILED；合法覆盖/标签/概率证据不足EVIDENCE_INSUFFICIENT；证据充分但两效果值任一不足BELOW_BINDING_RISK_MBE；均通过则DEVELOPMENT_RISK_EFFECT_REACHED_FORWARD_UNCONFIRMED。research surface和advisory仍NOT_AVAILABLE，实际writer/API/UI另行验证；不建占位页或补默认warning。本轮不DDL/DML、runtime、QE或新实时推理。

任何终态即停止，失败后不换阈值/label/训练窗/模型，不开第二风险候选。源码最多三轮审修；需模型合同变更时返回用户。本R1与risk分别验收，不要求同时成功，不把风险模型的新参数授权扩展到原HMM。

#### 8.2.7 验证矩阵与批准索引

R1直接测试：hidden index重排不改neutral身份；mu/hash漂移拒绝；真实neutral=0和缺失null分离；原4行业/mask/目标保持；全同分不伪造IC；复合标签与parent authority；旧版本不变；fit/D5/DB/tail poison；两process一致。不得在未知新合同下预跑重评分结果。

Risk直接测试：10日复利绝对路径MDD、无未来feature/标签访问、purge及未成熟NA；原20D/代码和当前逐日资格、单成员/正常停牌；train-only scaler、严格零方差与有限未来变化、日期权重；一类train/不收敛/非有限typed失败；class=1概率与0.20边界；precision/base_rate/recall空分母；无warning是合法输出但效果证据不足；完整人口/预算对照不按outcome重分组；双process2fit计数和DB/tail poison。必要聚焦测试与HMM slice/registry/L0按实际changed files选，不复制旧矩阵或降低现有保护。

`L2-R1-D1～D6`及`L2-RISK-D1～D6`均为`APPROVED_FOR_IMPLEMENTATION`。本轮批准覆盖新score公式、10D/-8%绝对事件、classifier/训练窗、0.20/4:1报警代价、0.05/0.25效果值、证据合同和各自停止条件；数值全部沿v1.2提案，不调整模型、特征、窗口或阈值。§8.2.2～8.2.6依次为L2-RISK-D1、D2、D3、D4/D5、D6的完整定义。设计PR #5433已合入，源码/实验状态单独记录；不拆多个微设计。QE消费公式及验证全部后置。

### 8.3 Implementation Plan与allowed scope

P1同一完整任务包：正式reader/公式和回放实现→定向测试及多轮审核修复→固定commit双process零fit回放→效果分析和真实产品验证。文档、CLI、数据库及UI验证是包内动作，不拆产品阶段。若效果不足，保留诚实研究产品及结果，本批停止，不自动搜索下一模型。

预计新增HMM-owned `backend/services/hmm_risk/rotation_l2.py`、`rotation_l2_input.py`、`rotation_l2_prediction.py`、`scripts/hmm_risk/run_rotation_l2.py`及直接tests；允许对`rotation_l1_gbdt.py`/`rotation_l1_input_bundle.py`做经回归证明行为不变的最小纯函数抽取。共享文件用明确level/contract参数，不弱化31行L1约束、不建立兼容代理复制实现。

以上是原P1已实现的范围，不重新创建模块。§8.2的设计已由PR #5433 / `e10ba033`进入main。2026-10-05实施文件钉住：`backend/services/hmm_risk/formal_state_effect.py`、`scripts/hmm_risk/run_formal_state_effect.py`、`backend/tests/hmm_risk/test_formal_state_effect.py`；risk新增`backend/services/hmm_risk/risk_l2.py`、`scripts/hmm_risk/run_risk_l2.py`、`backend/tests/hmm_risk/test_risk_l2.py`，必要L2文件构造复用/抽取`backend/services/hmm_risk/formal_state_input.py`，直接回归`backend/tests/hmm_risk/test_formal_state_input.py`。本详细设计仅同步实施状态；不改CI/nox/test plan，不扩大模型合同。

产品范围为`backend/routers/hmm_risk.py`、HMM migration、`frontend/src/lib/hmm-risk/api.ts`、`frontend/src/components/hmm-risk/`及`frontend/src/app/hmm-risk/page.tsx`和直接测试；具体文件在实施前按changed-files→ownership→module registry→test plans确认。不得修改全局CI、noxfile、数据生产、QE/Advisory/Paper源码以使门禁或消费者通过。

原P1零fit CLI设计建议为`preflight`与`development`，显式request、输出根及mode必填，无默认latest/model/date；其离线development拒绝tail/fit/数据库writer，不能将这条旧零fit限制误套到已批准的§8.2 risk。本轮R1实际CLI为`score/run/child`，均0fit且不重建标签；risk为`prepare/run/child`，prepare只做文件制备，run只执行同一固定模型的双process共2fit，无搜索/重试或writer/serve。产品导入另沿既有HMM管理入口实施显式run写入，不能用参数默认值触发生产。

原P1 typed reason采用`hmm_risk_rotation_l2_`前缀加固定后缀；复用已有底层具体cause，不只记录异常字符串。最小映射为：identity/schema/hash/未知码/映射冲突→`input_identity_invalid`；未知漏采/冲突行情/非有限输入→`source_invalid`；合法无resolved成员、停发、全停牌、历史不足、provider-absence覆盖不足→对应`no_resolved_members|quote_unavailable|all_members_suspended|history_unavailable|provider_coverage_insufficient`；N<2→`cross_section_insufficient`；分数/复现/parent权威不一致→`score_authority_failed`；writer/readback失败→`product_integrity_failed`。本轮R1/risk各自保留`hmm_risk_l2_effect_`/`hmm_risk_l2_risk_`版本域及底层cause，不将旧P1前缀当通用替代。日期、行业、字段、底层cause和失败stage进入context；合法NA是行状态，其他错误是batch失败，不共享“catch后继续”路径。标签未成熟/官方停发造成的不可评价与源错误分开。

仅需一个最小输入bundle、两份紧凑child结果、一份parent结果及真实prediction rows；失败保存reason/阶段/计数，不保留逐股票大日志、全量源拷贝或重建历史账本。

### 8.4 2026-10-05正式执行结果：候选独立终止，产品状态不外推

执行源码冻结为`02d15a1f899cf399182f45b0c85e99644a043173`，独立validation worktree、现存Conda base、同host单线程。输入仍为§8.2的v17/full-v3 identity；未切换active profile、未改数据或模型合同、未访问市场数据库。源码提交`de8d36a0a`及主线同步后的8文件PR #5444尚未合入；结果回填只改文档，不为文档HEAD变化重复fit。

**R1**：原request/process/acceptance确实只有标签hash。用户单独批准一次文件标签重建，canonical hash严格等于原`61600e85df8fc2fe77fdf520d7695206b388fd4450d9e3485ea04ae5dc24e214`；重建过程DB connection/HMM fit poison有效，未重过滤HMM。两fresh-process零fit重评分一致，28,951目录预测行及原971条不可用mask完整保留，coverage=`0.9664605713101447`，201/201成熟日有有效IC。mean Rank IC=`-0.009341302842128188`，HAC诊断区间`[-0.03792539584645551, 0.019242790162199132]`，终态`BELOW_BINDING_MBE`。

原评分同口径IC=`0.009072776767716752`；配对差点估计`-0.01841407960984494`、区间跨零，因此没有neutral-centering改善证据，不宣称“显著更差”或所有HMM不可行。R1 canonical acceptance=`4b699237a5b73b632dded70e7d7cf83f9f6148c2d6474d97fc97d7860ceab6e9`；该唯一候选已停止，不返回D5、不补4个semantic不足、不尝试其他score。

**Risk file-only**：131 L2、原20D和601/591/424/414日历逐行核验PASS。train finite=75,856、合法不可用=2,875；development finite=55,388、合法不可用=156，保留全部55,544目录行。实际fit 591日×合法样本共74,144行，事件11,201/非事件62,943。feature receipt=`2e9911a5fd2a83c15803e120b1e3c9a21a1a9ffed7f336e53e156c9acdde1c70`；原4个HMM semantic不足不作为新模型排除项。

**Risk正式2-fit**：两fresh-process参数、封闭概率和指标bitwise一致；M=54,012，真实事件7,299，已知标签报警11,908，TP=3,008、FP=8,900、FN=4,291。base_rate=`0.13513663630304376`，precision=`0.25260329190460196`，precision_lift=`0.1174666556015582 >= 0.05`，recall=`0.4121112481161803 >= 0.25`；Brier=`0.11224067478689662`。P/S coverage=100%（S是当日合法20D观测人口，不是整个目录），目录预测覆盖55,388/55,544；414/414成熟日满足证据比例。全部预测报警12,444，其中最后未成熟日报警保留但不冒充已知结果。

| 固定报告块 | precision lift | recall |
|---|---:|---:|
| 2024-07-01..2024-12-31 | 0.07568123069101235 | 0.3129496402877698 |
| 2025-01-01..2025-06-30 | 0.10098961418652926 | 0.47099447513812154 |
| 2025-07-01..2026-03-31 | 0.160658554143138 | 0.509741550695825 |

各块只是预注册诊断，不改为新增AND门。整体lift HAC区间`[0.06747164189488156,0.16746166930823483]`、recall区间`[0.315783326339333,0.5084391698930275]`也只诊断；同报警预算volatility参照precision=`0.214603441040705`、recall=`0.35032196191259074`，模型点估计较高，但不是已证明统计显著的增量或交易收益。

Risk终态为`DEVELOPMENT_RISK_EFFECT_REACHED_FORWARD_UNCONFIRMED`，canonical acceptance=`88341607f8772bcb97d1832cd1941f92971f35d62c1f0c8ed90261a8c8df7d26`，model SHA=`37259b5e9ca2c6eee2845cf0f1f02932a8cfd6cf21ad29080d6570d274b8038d`。这提供可继续验证的L2风险研究模型，不是forward-confirmed、实时或QE收益证据。已知报警中约74.74%没有目标事件，错误报警未来平均收益约+3.39%，漏报事件平均回撤约-10.80%；不得把warning直接当禁买或降仓结论。截至原模型评价时surface/advisory均NOT_AVAILABLE，writer/API/UI/DDL/DML未执行；后续risk产品#5449及真实生产/runtime验证已完成，当前surface=AVAILABLE_EXPERIMENTAL、advisory仍NOT_AVAILABLE，详见risk产品设计v1.3。两候选已终止，不为填满时长开新候选，不借risk产品验收升级本rotation surface。

源码三轮审修完成；最小HMM直接矩阵99 passed，最后风险修订及主线同步后16 passed；registry 8 passed/14映射、L0无blocking、Ruff/compile/diff与F2通过。源码实际runtime分类仍backend/backend-main，fresh-process router/health及HMM依赖导入通过；合入、用户重启、运行态验证与模型/产品状态独立。下一业务优先是已有risk成果的真实研究产品闭环及误报成本验证，而非继续模型合法性普查、历史证据工程或自动扩展参数搜索；本轮没有批准这些生产动作。

## 9. Verification Plan、结果验证与合入标准

### 9.1 实施时直接测试（均为计划，P0没有运行）

| 合同 | 必须覆盖的反例/结果 |
|---|---|
| release/PIT | 稀疏ID>130、801783.SI映射、未知/重复code失败；同release pins；C-013正式unavailable不猜行业；DB poison；旧路径fallback设失败桩 |
| quote与moneyflow | 停发quote=NaN且moneyflow finite合法；停发后报价值、有效span漏报失败；停牌不阻断别的行业；25日历史不足显式不可用 |
| stock汇总 | 1/4/5成员、90%边界、全停牌、provider-absence和未知缺失区别；CNY单位；PIT变更不回填；重复行业承载行一致性及NaN模式 |
| 公式/rank/state | 25日边界、两个20日相隔5日、手算例、行重排不变；N=0/1/2，全tie及跨边界tie；无复制L1、无任何fit调用 |
| 因果与标签 | t日及未来feature变更不改t分数；t+1..t+10百分数复利；不含t日收益；未来label/报价停止不改变既有分数；未成熟保留预测；tail reader poison |
| 评价 | 完整M分母、缺IC日期不压缩HAC距离、零方差/非有限/少样本；coverage分母与UI选择无关；IC效果不足不冒充能力 |
| 双process/identity | child自哈希共同篡改不通过parent重算；code-map/input漂移、environment漂移、输出碰撞、finalization失败均typed；结果计数0 fits |
| writer/API | 131目录请求精确全量、同日原子revision、重复幂等/不同payload冲突、回滚/readback；L1旧路径零行为变化；真实404/409/422/500 |
| UI/消费者 | 10+10、仅前/仅后、总数30/31、无重复/不足/同分提示；全量API；版本日期真实、无mock验收；新score不写旧coefficient字段 |

建议直接文件：`backend/tests/hmm_risk/test_rotation_l2.py`、`test_rotation_l2_input.py`、`test_rotation_l2_prediction.py`与HMM前端直接测试。这些是待建位置，不是已通过证据。测试不整批复制旧862/297项历史矩阵，局部失败只重跑失败节点；完整现存矩阵交CI按实际ownership执行。

### 9.2 可合入与可交付不是同一状态

原v1.1设计PR已完成F2、diff check、逐条DESIGN-COMPLIANCE及多轮语义审核，当时用户启动P1使§10原D1～D6进入实施授权。v1.2提出§8.2新R1/risk合同，现由用户独立批准成为v1.3；不能借用原授权代替此次批准。未来源码PR按实际变更运行精确pytest、Ruff、py_compile、HMM slice、registry/L0和必要前端检查，CI无阻断；不得标记未跑项通过。

实验验收：request/date/人口闭合、无tail/fit/DB副作用、两process与parent一致、coverage/effect实算、停止条件正确。产品验收：真实DEV writer/readback/API/UI，无mock；生产单独授权及用户重启后readback。服务端源码修改预期runtime_impact=backend/target=backend-main，前端按实际影响分类，禁止套用本P0的none。

不通过时最多按用户既定三轮代码审修，在scope内修复真实BUG；仍有阻断则如实暂停，不能改阈值、成员资格或数据口径来通过。本轮P0文档亦执行多轮自审，审核记录见§13。

## 10. Decision Index：已批准实施的新增精确合同

用户已明确要求按本详细设计开始P1完整实现，下列D1～D6由此进入`APPROVED_FOR_P1_IMPLEMENTATION`。该授权不扩展到生产DDL/DML、runtime activation、进程控制、QE正式实验、tail或P2风险合同。

| decision | 推荐精确合同与取舍 | 状态 |
|---|---|---|
| L2-P0-D1 | §3共享PIT及报价资格；25日逐日contributors≥1、coverage≥90%；小行业不套L1最少5，保留集中度/全股票mapping覆盖；不清空真实moneyflow | APPROVED_FOR_P1_IMPLEMENTATION |
| L2-P0-D2 | §4唯一20日比值相隔5日差，L2 average-rank；精确tie；q=min(floor(N/2),ceil(.2N))；无market/fit/seed | APPROVED_FOR_P1_IMPLEMENTATION |
| L2-P0-D3 | §5固定10D、2024-09-19..2026-03-31三个报告块、25日warmup；outcome≤3月31日；双process0 fits；无新holdout或旧臂重训 | APPROVED_FOR_P1_IMPLEMENTATION |
| L2-P0-D4 | §6 Rank IC≥0.02先验约定，90%coverage与metric规则；HAC lag9按真实日历缺口；无显著性AND门，效果/工程独立，tail禁止 | APPROVED_FOR_P1_IMPLEMENTATION |
| L2-P0-D5 | §7独立L2表/run identity与两个明确API，完整目录，原子revision；真实研究页面，旧L1隔离；生产操作另行授权 | APPROVED_FOR_P1_IMPLEMENTATION |
| L2-P0-D6 | §8完整P1包、L2全量消费者artifact、旧L2同场景对照边界；QE由QE窗口执行，风险独立不阻塞；不提前批准score→coefficient公式 | APPROVED_FOR_P1_IMPLEMENTATION |

主要取舍：相比原L1保持可解释信号、减少到0 fits；小行业coverage规则改变需批准；一年半开发窗口功效有限且已消费，不保证效果；新增一张L2表是最小隔离成本，不把重构整套L1作为前置。若D1/D3预检不具备输入，停止并交付明确缺口，不以新数据工程自动扩大本任务。

## 11. Design Acceptance Index

- **F-011**：L2因果计算、完整人口、历史效果、零fit复现，历史L1成绩不算L2完成。
- **F-012**：共享数据/PIT/identity、typed不可用和跨owner隔离，旧QE模型行为不变。
- **F-013**：真实L2 writer/readback/API/UI、全量消费与最多30展示，版本/效果状态真实。

## 12. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | formal_state_effect.neutral_rescore/neutral_effect_repeat；risk_l2.fit_predict/evaluate；两个显式CLI | backend/tests/hmm_risk/test_formal_state_effect.py；backend/tests/hmm_risk/test_risk_l2.py；§8.4真实双process结果 | APPROVED_BY_USER_SOURCE_READY_R1_BELOW_MBE_RISK_DEVELOPMENT_EFFECT_REACHED_UNMERGED | R1独立终态IC=-0.00934，不发布；risk lift=0.11747/recall=0.41211达development要求，未forward确认；源码PR #5444待合入，按用户已批准实验与产品后置边界，不推导API/UI完成 |
| F-012 | formal_state_input._l2_stock_facts复用当前C-010/A5；risk_l2.prepare_file_inputs | backend/tests/hmm_risk/test_formal_state_input.py；test_risk_l2.py；§8.4完整file-only preflight和标签hash匹配 | APPROVED_BY_USER_FILE_ONLY_PREFLIGHT_PASS_V17_FROZEN | 131/20D及601/591/424/414完整；真实fit 74,144行；合法NA保留，原4个semantic不足不排除；DB/active/data写入=0，冻结identity未改 |
| F-013 | 已有rotation_l2_prediction/router/dashboard；§8.2明确不发布新版本 | backend/tests/hmm_risk/test_rotation_l2_prediction.py；父蓝图§1.5现有L2 API/run_id读回；旧4 passed/1 live skipped仅为2026-09-22测试历史 | APPROVED_BY_USER_L2_API_READBACK_SURFACE_PENDING | 用户批准已有L2版本；当前surface=NOT_AVAILABLE，不代报浏览器或新风险产品通过，不要求生产部署才能得到模型结论 |

## 13. DESIGN-COMPLIANCE-001与审核记录

| 检查 | 本设计约束 | 本次边界 |
|---|---|---|
| 禁止简化交付 | 完整合格L2计算、完整目录状态、真实产品链；不将UI子集当训练人口 | 源码完整实现但真实输入/产品验收阻断，不标P1/P2完成 |
| 禁止静默错误 | PIT/身份/未知缺失fail closed，停发与moneyflow分离，无neutral/1.0替代 | 不放宽旧模型，不要求数据造价/补零 |
| 禁止业务逻辑迁移 | 用户批准L2方向，L1数据/接口及旧QE契约保留 | D1～D6仅用于L2 P1；未激活交易行为或跨模块消费 |
| 禁止私增门禁审批 | MBE/coverage提案显式列D条款；HAC/集中度诊断不另设AND门 | 不新增学习性/资源/全family合取审批；沿既有模型授权边界 |

已完成三轮文档自审、修订及回读（不是独立第三方审核）：

1. 源码/因果审核：核实旧31行业writer和最少5成员不能机械复用；修订共享span与C-013优先级、resolved冲突与合法unavailable区别、完整股票分母；补充retrospective selection声明，禁止全null页面伪验收和调用旧全family builder。
2. 数学/产品审核：核对25日的两个20日窗口、官方百分数复利、t+1..t+10与总体末10日成熟度；修复报告块边界重复删标签风险；补齐零分母、完整日期原子revision、surface外部验证与状态分域。合成算术检查：每日net依次1..25、amount=100时当前比值0.155、滞后0.105、delta=0.05；连续10日1%收益为0.1046221254；N=2..131的q均无前后状态重叠。这些是公式检查，不是源码测试或实际数据实验。
3. 目标/授权复审：删除草案逐行业coverage≥90%的AND门，仅保留总体coverage和逐行业诊断；复核停发与真实缺数边界、全量计算/≤30展示、L1历史保留和QE owner边界。用户开始P1实施后，D1～D6更新为已批准；生产与P2权限不随之扩大。
4. P1源码复审：修复未固定因子清单、物理路径进入canonical hash、amount=0误作缺失、revision直接前序未校验、UI越界静默忽略和旧L1入口丢失；定向backend、TypeScript与Playwright mock均通过。真实preflight保持fail closed，未以代码绕过数据权威缺口。
5. P1持久化/产品复审：修复模型合同字符串漏写`rank-1`、可用行业不足时前后榜重复，并把既有L1风险明确隔离为“L1历史风险独立能力”而非L2风险；迁移增加事务锁与非空回滚保护，并在Python/DDL同时钉住contribution等于score、zero-fit/no-tail摘要及effect/capability耦合。共享release无正式行业名时显式记录code-only authority，不猜测名称。

上述五条、上方合规表的“本次边界”及定向pytest/TypeScript/mock Playwright为v1.1在2026-09-22的历史记录，当时file-only preflight失败、正式双process/writer尚未运行。之后原P1基线已形成真实结果并持久化/API可读，不得沿用历史失败当今日阻断。v1.2提案时新R1/risk只有设计；当前源码与正式离线结果由本v1.4头、§8.4及矩阵记录，仍无本轮产品发布或经济收益改善结论。

### 13.1 v1.2本轮三轮设计自审（2026-10-04）

第一轮核对当前acceptance与两种基线标签，修正旧输入阻断/未评价状态；只对封闭结果执行零fit评分结构诊断。第二轮补齐风险每日标签证据、日期相关ratio区间、训练purge和固定20D共享模型身份，明确所有新数值仍待批准。第三轮修复参照报警预算0/全部/tie边界及旧P1授权表述，核对旧批准effect D1～D6逐字未改、三份文档仅本模块、历史版本与验收行保留。本轮为作者自审，不声称第三方审核或源码测试；风险标签、模型及R1新分数均未生成。

| DESIGN-COMPLIANCE-001 | v1.2当前设计结论 |
|---|---|
| 禁止简化交付 | 两个完整模型包，全131目录保留；不能以最多30展示或2-fit预算冒充完整业务验收 |
| 禁止静默错误 | 合法NA、mapping不足、标签不成熟与源错误分离；不补分数/收益/概率，空分母有明确原因 |
| 禁止业务逻辑迁移 | 原模型/seed/效果合同及旧QE资产不变；新score与监督风险版本独立且待批准，QE验证后置 |
| 禁止私增门禁审批 | 新效果/证据值显式列为待批准合同；HAC和参照仅诊断，无全行业三态、显著性或击败基线AND门 |

三份文档F2、UTF-8、历史/范围和diff检查用于设计合入，不替代新模型实验或产品验收；最后结果以PR最终HEAD为准。

### 13.2 v1.3批准后复核

2026-10-04用户批准上述两份D1～D6，当时仅同步头、Contracts、索引/矩阵与父蓝图。v1.2审核记录中“待批准”保留当时事实，批准状态由§8.2给出。两轮复核确认批准前后的精确公式/数值、原effect Contracts D1～D6及旧历史记录不变；合入、cleanup、生产/服务动作不从模型批准推导。该审核时PR #5433仍OPEN，之后已合入`e10ba033`，再进入2026-10-05源码与实验；设计批准审核本身没有源码/实验/数据变更。DESIGN-COMPLIANCE-001四项沿上方约束逐项复核，无新增简化交付、fallback、业务漂移或未批准门禁。

## 14. Production gates、参考与变更

v1.1源码交付时runtime_impact=backend+frontend及DEV/生产门禁按当时状态记录，后来已授权动作不由本轮重做或撤销；当时“DEV不存在”不作为2026-10-04数据库现状。v1.3设计批准批次仅三份文档及零fit只读诊断，runtime_impact=none；当时无新fit/tail/runtime/cleanup，backend_restart_required=false。2026-10-05源码实际分类为backend、target_ids=[backend-main]，不继承文档none；2个新risk fit是离线研究而非runtime activation。当前production_ddl_gate/noop、production_dml_gate/noop、dependency gates/noop；database_write/dataset_write/active_profile_write/runtime_action=false；源合入、用户重启、运行态及产品验证仍独立。

参考：父蓝图§1/§4/§7；`hmm_evolution_phase2_rotation_l1_g2a_detailed_design_20260903.md` §24（旧L1公式与历史结果）；`hmm_phase2_qe_assistance_three_arm_f2_detailed_design_20260916.md`（历史consumer边界与C-013不可用语义）。旧合同只为各自版本负责，不自动授权本L2实验。

| 版本 | 日期 | 变化 |
|---|---|---|
| v1.4 | 2026-10-05 | 回填源码三轮审修、R1零fit未达标与risk双process2fit达到development效果；一次标签重建严格匹配原hash；不改精确合同，不发布产品，不推导经济改善或forward确认 |
| v1.3 | 2026-10-04 | 用户批准L2-R1/L2-RISK全部D1～D6；只同步批准状态，不改变公式/数值；设计PR合入、源码/实验、生产/进程动作独立，QE继续后置 |
| v1.2 | 2026-10-04 | 同步已完成基线/HMM效果，QE后置；同一设计补齐唯一neutral-center零fit与独立L2绝对回撤2-fit合同提案，全部待批准；保留v1.1历史，不运行新模型/标签或部署 |
| v1.1 | 2026-09-22 | 用户启动P1完整实现；D1～D6更新为实施授权，回填源码/测试/产品链实现与真实preflight的精确数据权威阻断；不把source-ready写成模型或产品完成 |
| v1.0 | 2026-09-22 | P0一次闭合L2数据/公式/历史评价/真实产品及消费接口；明确D1～D6精确提案和执行边界，不拆微阶段，不继承L1成功 |
