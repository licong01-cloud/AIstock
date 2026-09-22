# HMM Evolution Phase 2：L2轮动P0完整详细设计

> 版本：v1.0；日期：2026-09-22；tier：F2；owner：HMM。
> 父蓝图：`hmm_evolution_and_risk_management_system_design_20260716.md` v2.59及其后保持本L2方向的版本。
> 基线：`9640cf67c5c40884e0f99074224cff0f0270e1f6`。本次仅交付设计，未运行数据预检、模型、回放、数据库或服务。
> 授权：用户已批准P0文档编写、审核修订、提交和合入，以及L2主线和默认前10/后10、总数≤30的展示方向。§10的新增精确模型合同是明确的推荐方案，状态为`PROPOSED_PENDING_USER_APPROVAL`；文档合入不等于数值批准、源码实施或实验授权。
> 完成定义：P0闭合可审阅设计和执行边界；P1才交付L2计算、历史效果与真实产品。设计通过不得报告L2模型有效、API可用或QE已接入。

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
- 不研发新GBDT/HMM/jump，不复活B3/P6，不搜索horizon/窗口/seed，不新增训练平台、feature store、registry、调度器。
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

## 3. D1：共享数据、PIT与逐日资格（推荐精确合同，待批准）

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

推荐每日有效contributors至少1只且`valid/expected≥0.90`，有效成交额sum有限且>0；该规则适用于25个source日中的每一天。不沿用L1固定最少5只：真实L2可能少于5只，改用比例并显著报告最小成员数、最大成员成交额占比，不增加集中度效果门。允许单成员不代表统计可靠，属D1待批准取舍。

- `expected=0`、全部停牌、当时尚未发布/正式停发或新行业尚无完整25日历史：行业typed unavailable，其他行业继续。
- 有权威provider-absence导致coverage不足：typed unavailable，计入结构覆盖缺口，不临时改比例。
- 正式应有字段缺失、未知非有限值、重复冲突、identity/hash漂移：batch fail closed；不得缩小`S(u)`或`E(t)`绕过。
- quote unavailable时，仅报价字段遵守停发合同；真实moneyflow允许有限值，但不能据此生成“有报价”的L2预测。membership保留，不要求数据窗口清空真实资金流。
- 全部目录行业每日均有状态。`|E(t)|/|S(as_of(t))|`、全股票因果mapping coverage、逐行业feature coverage分别报告；目录完整不等于行情完整，不用131硬填分数。

P1先完成一次列/日期受限file-only预检并冻结已解释不可用集合，两个process复用结果；不能为每次评分重扫全量股票历史。当前P0不宣称这些新窗口的输入已预检通过。

只复用必要IO、security/PIT与moneyflow字段规范化，不调用旧C-010/B3/full-family全量builder来“顺便”生成L2，不引入旧train-only contributor筛选、circ_mv或L1 index close依赖。没有需要的共享字段/合法权威就给出准确缺口；不暗中新增数据准备流程。

## 4. D2：唯一零fit公式与状态（推荐精确合同，待批准）

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

## 5. D3：一次历史回放、标签和比较（推荐精确合同，待批准）

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

## 6. D4：效果、coverage、状态与停止（推荐精确合同，待批准）

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

## 7. D5：真实产品、持久化与展示（方向已批准，精确接口提案）

### 7.1 完整行与最小表

采用一张新的`hmm_risk.rotation_l2_prediction`表，保留L1表与旧端点不变；这是L2明确身份边界，不新建通用registry或第二套写队列。字段：run_id、model_hash、evaluation_contract_hash、input_hash、mapping_hash、quote_authority_hash、trade_date、as_of_date、sector_level固定L2、sector_code/name、score/state/contribution、availability/reason、structural_eligible、feature_eligible、outcome_status、research/capability/forward/advisory状态、validation_basis、revision、created_at及supersedes_id。

唯一键`(run_id,trade_date,sector_code,revision)`；run绑定所有身份。目录行数/集合必须与request code-map一致，同日所有行使用同一run/revision，不能从各行业最大revision拼装“完整日”。一次完整日期批原子写入；可用行score有限，不可用行score/state/contribution=null且reason非空；分类不可用股票的汇总留在run摘要。same-key相同payload幂等，任何不同payload冲突拒绝；修正使用新revision并完整重闭合，不覆盖旧行。

writer接受parent已核对的prediction payload，事务内回读canonical逻辑行hash；不在事务外伪报成功。DB异常回滚；多日提交逐日原子，整个运行只有全部计划日期readback闭合才completed，重启可按同一身份幂等继续，不把部分日期成功写成完整产品。

run摘要（指标、日期计数、目录/资格统计、身份、effect与工程状态）用一个compact manifest供repository读取，与行hash一致，不另建数据资产平台。研究表`validation_basis=HISTORICAL_CAUSAL_REPLAY_ZERO_FIT`，不冒称训练fold OOF。本P1历史模式不支持新增实时写入；未来单日可复用同一feature函数但需明确执行范围。

运行有效性`execution_status`、`effect_status`、`research_surface_status`与`rotation_l2_capability_status`为独立字段；production表不使用占位L1字段。run摘要在全部日期提交且API/UI验收前只能是PENDING_PRODUCT_VALIDATION。surface验证可通过外部紧凑验证记录绑定run及行hash，不回写旧预测来循环证明自己已通过；读端缺该记录时保持surface NOT_AVAILABLE。不得将数据库行中自报的AVAILABLE作为唯一验收依据。

状态字段域按§6冻结：`execution_status=COMPLETED|FAILED`；`effect_status=EVIDENCE_INSUFFICIENT|BELOW_BINDING_MBE|DEVELOPMENT_EFFECT_QUALIFIED|NO_USABLE_PREDICTIONS`；`research_surface_status=NOT_AVAILABLE|AVAILABLE_EXPERIMENTAL`；`rotation_l2_capability_status=NOT_AVAILABLE|RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED`。只有完整执行、coverage/效果达标才允许后一个capability值；有真实工程展示但效果不达标仍是前一个值。产品验证阶段PENDING_PRODUCT_VALIDATION属于产品验证记录，不混写execution_status。本版不实现forward/advisory晋升分支。

### 7.2 API与UI

- 新增`GET /api/v1/hmm-risk/rotation-l2/overview?run_id=...`与`GET /api/v1/hmm-risk/rotation-l2?run_id=...&trade_date=...`，返回完整目录状态与全部合格预测。run_id必填；前端使用显式产品配置，未配置显示NOT_CONFIGURED，不以max(created_at)猜当前模型。
- overview包含可用历史日期、真实覆盖/指标/状态、run身份；detail包含expected/actual目录数、member-backed/quote/feature/metric/display分母的原始字段。后端不把页面数量当数据过滤条件。
- malformed参数422、不存在run/date404、身份冲突409、真实存储/校验错误500且typed reason；不得空200冒充完整日期，不吞异常。
- 新增L2主视图，旧L1视图保留显式版本入口。默认top_count=10、bottom_count=10；整数≥0，`1≤top_count+bottom_count≤30`，越界明确错误，不静默clamp。
- 前区按score降序、后区按score升序，同分按代码升序稳定显示。先取top_count，再从未显示集合取bottom_count；有重叠只保留一行，数量不足提示实际值。该规则不改变score或state；前后同分需明确提示无严格差异，不能用代码次序伪造预测差。
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

### 8.2 L2风险的明确非阻塞接口

L2风险是另一目标，后续直接在本设计中按独立D条款补齐风险事件、预测周期、标签、precision/recall及代价，不再开微阶段设计。当前不批准新分类器、不继承risk_L1阈值、不将fading当预警。预留的业务边界是同L2 code/as-of/身份和独立`risk_score/warning/effect_status`，当前不创建占位列、空路由或假开关。

风险和B精确合同只约束自身执行；不阻断P1研究排序完成。本次P0完成的是轮动可实施设计及下游接口范围，不是风险模型或QE消费公式已获批准。

### 8.3 Implementation Plan与allowed scope

P1同一完整任务包：正式reader/公式和回放实现→定向测试及多轮审核修复→固定commit双process零fit回放→效果分析和真实产品验证。文档、CLI、数据库及UI验证是包内动作，不拆产品阶段。若效果不足，保留诚实研究产品及结果，本批停止，不自动搜索下一模型。

预计新增HMM-owned `backend/services/hmm_risk/rotation_l2.py`、`rotation_l2_input.py`、`rotation_l2_prediction.py`、`scripts/hmm_risk/run_rotation_l2.py`及直接tests；允许对`rotation_l1_gbdt.py`/`rotation_l1_input_bundle.py`做经回归证明行为不变的最小纯函数抽取。共享文件用明确level/contract参数，不弱化31行L1约束、不建立兼容代理复制实现。

产品范围为`backend/routers/hmm_risk.py`、HMM migration、`frontend/src/lib/hmm-risk/api.ts`、`frontend/src/components/hmm-risk/`及`frontend/src/app/hmm-risk/page.tsx`和直接测试；具体文件在实施前按changed-files→ownership→module registry→test plans确认。不得修改全局CI、noxfile、数据生产、QE/Advisory/Paper源码以使门禁或消费者通过。

CLI建议两个明确模式：`preflight`与`development`。显式request、输出根及mode必填，无默认latest/model/date；任何tail/fit/数据库writer选项在离线development入口拒绝。产品导入沿既有HMM管理入口实施显式run写入，不能用参数默认值触发生产。首次功能实现不增加调度/serve模式。

typed reason采用`hmm_risk_rotation_l2_`前缀加固定后缀；复用已有底层具体cause，不只记录异常字符串。最小映射为：identity/schema/hash/未知码/映射冲突→`input_identity_invalid`；未知漏采/冲突行情/非有限输入→`source_invalid`；合法无resolved成员、停发、全停牌、历史不足、provider-absence覆盖不足→对应`no_resolved_members|quote_unavailable|all_members_suspended|history_unavailable|provider_coverage_insufficient`；N<2→`cross_section_insufficient`；分数/复现/parent权威不一致→`score_authority_failed`；writer/readback失败→`product_integrity_failed`。日期、行业、字段、底层cause和失败stage进入context；合法NA是行状态，其他错误是batch失败，不共享“catch后继续”路径。标签未成熟/官方停发造成的不可评价与源错误分开。

仅需一个最小输入bundle、两份紧凑child结果、一份parent结果及真实prediction rows；失败保存reason/阶段/计数，不保留逐股票大日志、全量源拷贝或重建历史账本。

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

设计PR：F2、diff check、逐条DESIGN-COMPLIANCE及多轮语义审核通过；D1～D6待批准标记不得为了validator改成模型已批准。源码PR：按实际变更运行精确pytest、Ruff、py_compile、HMM slice、registry/L0和必要前端检查，CI无阻断；不得标记未跑项通过。

实验验收：request/date/人口闭合、无tail/fit/DB副作用、两process与parent一致、coverage/effect实算、停止条件正确。产品验收：真实DEV writer/readback/API/UI，无mock；生产单独授权及用户重启后readback。服务端源码修改预期runtime_impact=backend/target=backend-main，前端按实际影响分类，禁止套用本P0的none。

不通过时最多按用户既定三轮代码审修，在scope内修复真实BUG；仍有阻断则如实暂停，不能改阈值、成员资格或数据口径来通过。本轮P0文档亦执行多轮自审，审核记录见§13。

## 10. Decision Index：需一次性确认的新增精确合同

下列是推荐值而非已批准实验合同；用户对P0文档提交/合入的授权不替代对这些新数值的确认。无需再次审批已经明确的L2方向、展示上限或既有安全规则。

| decision | 推荐精确合同与取舍 | 状态 |
|---|---|---|
| L2-P0-D1 | §3共享PIT及报价资格；25日逐日contributors≥1、coverage≥90%；小行业不套L1最少5，保留集中度/全股票mapping覆盖；不清空真实moneyflow | PROPOSED_PENDING_USER_APPROVAL |
| L2-P0-D2 | §4唯一20日比值相隔5日差，L2 average-rank；精确tie；q=min(floor(N/2),ceil(.2N))；无market/fit/seed | PROPOSED_PENDING_USER_APPROVAL |
| L2-P0-D3 | §5固定10D、2024-09-19..2026-03-31三个报告块、25日warmup；outcome≤3月31日；双process0 fits；无新holdout或旧臂重训 | PROPOSED_PENDING_USER_APPROVAL |
| L2-P0-D4 | §6 Rank IC≥0.02先验约定，90%coverage与metric规则；HAC lag9按真实日历缺口；无显著性AND门，效果/工程独立，tail禁止 | PROPOSED_PENDING_USER_APPROVAL |
| L2-P0-D5 | §7独立L2表/run identity与两个明确API，完整目录，原子revision；真实研究页面，旧L1隔离；生产操作另行授权 | PROPOSED_PENDING_USER_APPROVAL |
| L2-P0-D6 | §8完整P1包、L2全量消费者artifact、旧L2同场景对照边界；QE由QE窗口执行，风险独立不阻塞；不提前批准score→coefficient公式 | PROPOSED_PENDING_USER_APPROVAL |

主要取舍：相比原L1保持可解释信号、减少到0 fits；小行业coverage规则改变需批准；一年半开发窗口功效有限且已消费，不保证效果；新增一张L2表是最小隔离成本，不把重构整套L1作为前置。若D1/D3预检不具备输入，停止并交付明确缺口，不以新数据工程自动扩大本任务。

## 11. Design Acceptance Index

- **F-011**：L2因果计算、完整人口、历史效果、零fit复现，历史L1成绩不算L2完成。
- **F-012**：共享数据/PIT/identity、typed不可用和跨owner隔离，旧QE模型行为不变。
- **F-013**：真实L2 writer/readback/API/UI、全量消费与最多30展示，版本/效果状态真实。

## 12. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | §3～§6/§10，拟建rotation_l2.py与input/CLI；旧算法参考rotation_l1_gbdt.py | 计划`backend/tests/hmm_risk/test_rotation_l2.py`；本文§9手算、因果、双process测试，尚未运行 | APPROVED_BY_USER_DOCS_ONLY_PENDING_EXACT_CONTRACT_AND_IMPLEMENTATION | 用户授权P0设计合入；D1～D4数值待确认，无L2效果证据，不以F2结构通过替代 |
| F-012 | §2/§3/§8；正式共享reader和industry_pit_adapter.py，未来消费者artifact | 计划`backend/tests/hmm_risk/test_rotation_l2_input.py`；DB/tail poison、PIT及共享码测试未实施 | APPROVED_BY_USER_DOCS_ONLY_PENDING_EXACT_CONTRACT_AND_IMPLEMENTATION | 用户批准方向，旧运行兼容保留；输入预检/P2精确消费与各owner验证尚缺 |
| F-013 | §7及拟建L2 repository、hmm_risk router和HMM页面 | 计划`backend/tests/hmm_risk/test_rotation_l2_prediction.py`及真实浏览器测试，尚未执行 | APPROVED_BY_USER_DOCS_ONLY_PENDING_EXACT_CONTRACT_AND_IMPLEMENTATION | 用户批准展示方向和设计合入，不等于源码/DDL/DML/产品已批准或完成；D5～D6仍待确认 |

## 13. DESIGN-COMPLIANCE-001与审核记录

| 检查 | 本设计约束 | 本次边界 |
|---|---|---|
| 禁止简化交付 | 完整合格L2计算、完整目录状态、真实产品链；不将UI子集当训练人口 | P0文档交付，不标P1/P2完成 |
| 禁止静默错误 | PIT/身份/未知缺失fail closed，停发与moneyflow分离，无neutral/1.0替代 | 不放宽旧模型，不要求数据造价/补零 |
| 禁止业务逻辑迁移 | 用户批准L2方向，L1数据/接口及旧QE契约保留 | 新数值推荐待批准，文档不激活交易行为 |
| 禁止私增门禁审批 | MBE/coverage提案显式列D条款；HAC/集中度诊断不另设AND门 | 不新增学习性/资源/全family合取审批；沿既有模型授权边界 |

已完成三轮文档自审、修订及回读（不是独立第三方审核）：

1. 源码/因果审核：核实旧31行业writer和最少5成员不能机械复用；修订共享span与C-013优先级、resolved冲突与合法unavailable区别、完整股票分母；补充retrospective selection声明，禁止全null页面伪验收和调用旧全family builder。
2. 数学/产品审核：核对25日的两个20日窗口、官方百分数复利、t+1..t+10与总体末10日成熟度；修复报告块边界重复删标签风险；补齐零分母、完整日期原子revision、surface外部验证与状态分域。合成算术检查：每日net依次1..25、amount=100时当前比值0.155、滞后0.105、delta=0.05；连续10日1%收益为0.1046221254；N=2..131的q均无前后状态重叠。这些是公式检查，不是源码测试或实际数据实验。
3. 目标/授权复审：删除草案逐行业coverage≥90%的AND门，仅保留总体coverage和逐行业诊断；复核停发与真实缺数边界、全量计算/≤30展示、L1历史保留、QE owner边界及P0仅文档状态。父蓝图链接与本设计相符，未发现剩余文档合入阻断项；新增D1～D6精确合同仍待用户确认，真实数据可执行性和源码验收不得提前报通过。

F2和`git diff --check`通过；F2只是结构检查，不替代上述语义审核。未运行pytest、正式数据preflight、模型、writer/API/UI或生产操作，未生成模型/预测资产。

## 14. Production gates、参考与变更

本次`production_ddl_gate=noop`、`production_dml_gate=noop`、前后端dependency gates=noop、`runtime_impact=none`、backend_restart_required=false；源码/实验/tail/runtime/database/dataset写入均未执行。任务只改本设计和父蓝图链接/设计进度，不修改标准、memory、workflow或其他模块。

参考：父蓝图§1/§4/§7；`hmm_evolution_phase2_rotation_l1_g2a_detailed_design_20260903.md` §24（旧L1公式与历史结果）；`hmm_phase2_qe_assistance_three_arm_f2_detailed_design_20260916.md`（历史consumer边界与C-013不可用语义）。旧合同只为各自版本负责，不自动授权本L2实验。

| 版本 | 日期 | 变化 |
|---|---|---|
| v1.0 | 2026-09-22 | P0一次闭合L2数据/公式/历史评价/真实产品及消费接口；明确D1～D6精确提案和执行边界，不拆微阶段，不继承L1成功 |
