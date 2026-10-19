# Advisory 公告标题增量价值单一研究 F1 详细设计 v1.0

> 2026-10-19用户后续决定：**DEFERRED_BY_USER / 待执行**。保留本设计及基础数据需求，不再作为当前主线的前置或继续寻找/补采任务；输入未验证、源码与研究仍未开始。先执行[现有输入的独立排序研究](advisory_group_centered_rank_5td_v1_f1_design_20261019.md)。恢复公告使用时再按本文核对文件/版本，不自动降级或启动。

日期2026-10-19；当前交付为设计，**没有源码实现、数据就绪、训练或收益结果**。状态`DESIGN_DEFINED_INPUT_UNVERIFIED`。唯一假设`ANNOUNCEMENT_TITLE_VALUE_1`；`RISK_MANAGED_ADVISORY / EXPLORATORY_SCREEN / NAVIGATION_ONLY`。源文件可用性未证明，不等于数据库没有公告；不以此限制原策略包使用或日频荐股。

## Background / 业务问题与依据

R1发现最新同业模型AUC约0.507，日内IC约0.025且区间跨零；随后的成熟组合状态研究仅+2.198630bps、7/81日干预，未达继续确认条件。这不足以支持再调整旧模型阈值。下一问题是：**D前已公开的公司公告标题内容，能否在同一数值/披露元数据对照之外，改善固定五交易日未来标价的预测，并改善原候选的价格建议？**

这是增加公司事件信息，不是选择QE模型、生成上游因子、重跑旧财务事件或训练一个通用LLM。首个可交付研究输出仅为五日条件期望价值和指示性盈亏平衡价上限；不把单一均值上限称为完整买卖区间、下单指令或最佳成交价。

文献只是立项动机：[Tetlock等作者页面](https://business.columbia.edu/faculty/research/more-words-quantifying-language-measure-firms-fundamentals)研究公司新闻语言与盈利/收益的关系；其市场、文本和年代不同，不证明本项目A股标题有增量。[Loughran–McDonald原论文](https://doi.org/10.1111/j.1540-6261.2010.01625.x)提示金融语境与通用情感词不同，因此不翻译英文词典后直接硬过滤股票。

### 与已做研究的差异

| 已做内容 | 本设计不重复的部分 | 唯一待检验差异 |
|---|---|---|
| N3 financial-event | forecast/express/fina indicator的数值方向、强度与披露计数；旧H20合同不改 | 原始标题字符内容，不用旧signed rules冒充文本 |
| D-clock population / 父分数 | 已经用过Top50监督及D锚目标、价格函数 | Top50、D-only和价格代数均只是复用，不称新发现 |
| 因果Admission / 成熟cohort状态 | 个股Ridge更新、过去收益状态和拒买阈值均已测试 | 不增加收益窗口、不使用当前或未来结算值作事件输入 |
| 公共announcement/event_signal | 已有公告分类、issuer绑定与风险引擎，属于其它owner | 仅消费基础文件，研究数值回归；不复制公共分类、风险过滤或写库适配器 |

查重范围为上述Advisory设计/直接源码和当前公告公共设计，不声称项目中从未研究过文本。发布前若发现已有同来源/时钟/目标的正式研究，先判等价，不通过改名继续。

## Scope / 非目标及数据现状

本次只改本文与主蓝图；不改Advisory/QE/LocalSIM/HMM/数据/事件服务源码。未来消费者实现仍限`backend/services/advisory_model_first/`及其直接测试，实施前另登记精确文件，不借设计授权修改公共模块。

已核对：原N3投影`211b8db1.../event_source_projection.parquet`为84,272行、7,061,459bytes，schema只有source/type/severity/confidence/日期等16列，**没有title/text**。读取仅Parquet元数据，未读经济标签。已检查Advisory artifact顶层、财务bundle和local_data目录，未定位到可交接的公告标题文件；没有遍历全机或查询数据库，故实际标题覆盖、版本与缺失仍未知。

`backend/services/event_signal/announcement_adapter.py`包含schema初始化/写库，ownership为`local_data`；不得将它当只读prepare入口调用。本设计不要求补齐PDF、OCR、LLM、事件平台或全量历史归档。数据owner只需已有基础公告元数据的版本化只读交付，业务匹配/验证/模型归Advisory。

## Contracts / 输入与时间边界

### 1. 两类输入各自保持来源

1. 原包候选/日历/D特征/标价标签沿已有D-clock研究公共格式，只读新请求指定的来源，不改旧run或label。研究对象仍原两包开发305D、原Top50训练、原Top5评价；包ID只在请求中，不写生产白名单。
2. 新标题文件由基础数据owner给出真实路径、manifest/SHA、schema、导出时间和覆盖摘要。至少包含稳定公告ID、原股票代码及issuer绑定来源、原始标题、公告日、源发布时间及时间质量、可获得的first_seen/修订时间/原文hash。没有的字段保留null，不伪造历史捕获时间。coverage表区分某源日期/标的域已完整、部分、未知及明确零条。
3. 固定开发请求：公告源日期2024-06-20..2025-09-30；训练D=2024-07-04..2025-05-30、评价D=2025-06-03..2025-09-30；标签H必须在相应cutoff以内。日期仅本研究参数，不修改QE全局窗口。sealed及其它窗口不读取。
4. manifest身份、单位/字符编码、自然日覆盖、股票发行人绑定和原候选键验证失败时停止本新研究；不是增加策略包门禁，不重建Selection、不填当前成员、不删原日期/股票。

### 2. 公告可见性与修订

- 固定保守时钟：不论源是否有精确时分，公告在源公告日之后的首个交易日才生效；修订同理按其自身公开日。D_CLOSE仅使用生效日<=D的版本，不能将后来更正后的标题放回原公告日。
- 精确timestamp、date-only和本地first_seen分别保存；同日午夜不自动升级EXACT。若原公共合同给出的生效时间更晚，采用更晚者；未知/矛盾不得变早。
- 能证明原公开版本及公开日、但本地后来获取的，标注历史重建，不伪称自然采集。只有当前最终标题、无原版本依据的，单独标`BACKFILLED_VERSION_UNPROVEN`，不能混入本设计的PIT训练；不据此判断公司无事件。若整个源只有此类版本，本实验不运行；可另提出非vintage探索，但不是本任务默许降级。
- `NO_DISCLOSURE_OBSERVED`仅在窗口对应源覆盖已证明且无合法标题时成立；缺源/缺issuer/空标题/未证明修订为`SOURCE_UNKNOWN`。未知不置为“正常公司”、负事件或0收益；本模型NO_ADVICE，原包继续。
- 只用含D在内的五个交易日窗口内合法公告：age为0..4；不试其它窗口，不用当天收盘后标题服务当天D，也不读取T新闻。周末公告按冻结日历映射。

## Model / 预测目标与两次fit预算

### 3. 先预测价值，后做价格映射

沿既有D-clock标价标签：`y=10000*(Q_H/Q_D-1)`，H为T..T+4末日，Q为同一D锚坐标。D只读取D及此前信息；未来复权桥/停牌标价只在标签端使用。原生成标签核、UNKNOWN、IMMATURE及停牌持有标记不改，标价不是可卖出的报价。

train监督使用原Top50、H<=train_end的唯一stock/D可测簇；同股票跨包总质量1，不把重复业务槽当独立训练样本。不新增收益筛选、去极值或按事件正负选训练集；原始所有行/理由继续保留。

对照为新训练的`numeric_disclosure_control`：原D-clock 9个数值字段及其缺失指示，加标题存在、5日数量`log1p`、最近age（无公告固定5并保留存在位）。缺失数值采用训练期中位数，全部缺失的训练字段显式UNKNOWN，不默认0；控制均值/标准差仅训练内学习。

一次控制Ridge拟合后冻结，文本candidate为`mu_candidate=mu_control+delta_text`。第二个Ridge只拟合**训练内控制残差**，`fit_intercept=False`；不能把这些in-sample残差称为OOF。无标题或词汇全未见时文本向量为0，delta精确0、与control恒等；SOURCE_UNKNOWN时两模型均NO_ADVICE，不能把未知当文本0。这一结构不改变control系数来制造无事件日期上的“文本增量”。

两Ridge均固定alpha=10、solver=lsqr、tol=1e-6、max_iter=2000；控制fit_intercept=True。未收敛为明确技术失败，不加迭代/改参数默默生成新模型。不选正则/seed/模型族；2个预测器fit、1个训练词表/IDF估计、1个candidate，不计成3个独立模型假设。按蓝图使用既有WSL Conda训练环境，Windows仅编排与读回，不安装依赖、不启动QE任务；此设计阶段也不探测或操作其它窗口进程。

### 4. 文本表示与防泄漏

只用原公告标题，不下载正文/PDF，不使用事件分类器输出或LLM情绪标签。按稳定公告ID与源版本去重；不同ID相同标题不自行合并。规范化NFKC、统一空白、移除明确源代码及同版本原source_name（若存在）的字面前缀；缺原name不查询当前名称表补齐，不以事后更名表或后验风险词表补充特征。标题余下内容和数字保留，记录规范化版本。

固定字符TF-IDF，analyzer=char、ngram_range=(2,4)、min_df=5、max_features=8192、sublinear_tf=True、smooth_idf=True、norm=l2、lowercase=False。词表/IDF只拟合**参与合法训练输入的唯一公告版本标题**，不在重复stock/D行上拟合词频，不看评价标题。参考[官方TF-IDF接口](https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html)及[训练外预处理泄漏说明](https://scikit-learn.org/stable/common_pitfalls.html)。不使用带训练期后世界知识的通用预训练模型。

每stock/D取合法窗口各标题向量的等权均值，之后不再按未来信息归一；无标题为全零。有标题但无训练词汇明确`TEXT_OOV_CONTROL_IDENTITY`，不能伪装“已识别事件无风险”。稀疏矩阵保持稀疏，禁止全部文档拼接造成跨公告字符片段。

## Price / 动作与评价

### 5. 指示性上限，不承诺完整安全区间

`V_hat=Q_D*(1+mu/10000)`；沿原费用cb/cs，`p_break_even=V_hat*(1-cs)/(1+cb)`。查询价p只代入`expected_net_bps(p)=10000*(V_hat*(1-cs)/(p*(1+cb))-1)`，不得改变D预测值。非有限或V_hat<=0为显式预测无效，不能夹为0伪造上限。

在标价期望不随支付价变化的限定情景下，查询净值随p单调下降；这不是“价格下跌本身没有新消息”的因果假设，也不是给定T实际开盘后的条件期望。**不发布(0,p_break_even]为建议买入区间**：模型没有给出暴跌时的安全下限，也没有识别T出现的新消息。首轮只评价原T实际入场参考价的TAKE/SKIP；不能模拟一个任意更低价必然成交，不能把T价格提前作为D输入。均值模型不输出盈利概率、风险置信区间或止盈价；这些角色明确未实现，不用规则值冒充。

实际研究四臂：原包baseline、原固定规则、新numeric/disclosure control、control+text candidate。原生T可交易约束/现金状态保持；模型已知且expected_net>0才TAKE，否则SKIP，NO_ADVICE沿原包。没有重排、递补、仓位、订单、分钟策略或卖出规则；H/费用相同，原Top5固定五槽不重归一。

### 6. 先技能、再收益，两者不能互相替代

- 预测：同一共同已知stock/D上报告terminal目标RMSE/MAE、平均偏差及日内RankIC，对照训练期常数及新control；常数IC未定义。全人口、事件子集、OOV、无标题与未知分别报告，不事后择包/择子集晋升。
- 动作：保留原86评价D/860槽、成熟/未知真实分母；主端点candidate−baseline与candidate−control，原日期block5/2000次/两端点同时95%区间。空洞不压缩，金融未知不填0；固定5TD重叠cohort均值不是daily return/NAV/成交。
- 归因：避亏、漏盈、已知动作净变化、UNKNOWN对账；唯一stockD及干预日、两包和各月全量分报；交易费用只扣一次。风险报告终值<-800bps的超额损失、原路径不利幅度和退出不可执行率，不能将无观察尾损解释为没有风险。
- 本轮值得继续条件：两个主净增量均>5bps且同时下界>0；共同口径预测RMSE低于control；相对两个对照各>=30唯一stockD、>=20日且>=25%成熟D干预；已知动作归因正，组终值尾损不劣于两对照。MDE与不确定性单列，不因功效不足否定整个文本路线。未满足只停本exact候选。
- 正结果也仅`WORTH_CONFIRMING`，以后另设计未消费历史确认，不能自动激活。源码通过、数据消费、探索经济结果、确认及角色启用分别报告。

## Implementation Plan / 实施步骤与资源

1. 本轮：完成查重、基础schema核对、本文及蓝图设计审核交付；0源码/训练/收益读取。
2. 输入交付后：Advisory只读prepare，先验源版本/时钟、原键、coverage/OOV与数据量，不调用写入型事件adapter。没有完整文件时给精确需求，不造一个空输入模型。
3. 固定前置可计算性：成熟训练事件>=100唯一stockD且>=20D、词表非空；这是避免空模型计算，不是有效性/策略包门禁。不足只记录尚不可识别，不扩大窗口或换源。评价文本/收益在模型冻结前均不用于拟合、词表选择或立项筛选。
4. 复用Advisory输入、stage、registry及配对统计，只在确有新消费/模型能力需求时开发精确叶模块；不复制finance/选股/事件引擎。实现前登记文件；单位测试和真实prepare通过后，先注册新请求/source/policy/text-encoding身份，再顺序两fit、冻结D预测、一次评价。
5. 上限2CPU线程、8GB RSS、20,000成熟训练簇/8192稀疏词项；超限明确报告，不截断日期/股票、降词数或覆盖已发布文件。主体准备/fit/评价预算4小时上限（不是承诺运行4小时）；GPU与QE均不使用。所有临时文件X，基础输入就地只读，新小产物沿独立Advisory F根，零大文件复制。
6. 阴性/无支持终止该候选，无窗口/词表/阈值/包搜索；值得继续才进入确认设计。无生产API/UI、调度、绑定、数据库或重启步骤。

## Verification Plan / 未来实施验收

合同测试必须覆盖：源时间/版本错配、未来标题与未来Y毒化、date-only/周末/修订、无公告vs源未知、issuer错误、重复公告/跨包同股、空/OOV词表、训练外IDF禁止、零文本delta恒等、价函数单调及非安全下限、原空槽/停牌/未知、费用一次、stage exact retry。真实文件消费验收不能由合成fixture替代。

设计审核只验本设计完整一致，不声称上述测试已实现。源码阶段本地直接小矩阵/changed-file Ruff及scope，广测交CI，执行DESIGN-COMPLIANCE-001后才交付。日频生效及独立收益确认不在本F1设计交付内。

## Risks / 有限缺口与owner交接

最大风险是历史标题版本无法还原、标题文本太模板化/稀疏、稀疏拟合对小样本过拟合、公开消息已反映在D价、源coverage与标签质量相关。两次fit不意味着只有两个历史尝试；沿已有Advisory研究族登记开发窗口消费与此前负结果，不清零搜索史。

**数据准备提示词（仅需求，未代为执行）**：

> 请提供2024-06-20至2025-09-30已有A股公告标题基础数据的只读版本文件及manifest/hash/路径：公告ID、股票代码/issuer依据、原始title、ann_date、源发布时间/时间质量、现有first_seen及修订/版本信息，并附源日期覆盖摘要（完整/部分/未知/已确认零条）。缺字段保持null，不伪造历史版本，不要求PDF/LLM/事件分类/收益标签；不要执行Advisory业务验证或训练。不改active profile。若已有正式文件，仅交付其路径与现有限制；若需补采/写库，由数据owner按自身授权处理，不能把本提示当新增写入授权。Advisory负责候选匹配、PIT消费、支持度及全部业务验证。

## Production Gates

本轮docs-only，DB/DDL/DML/network数据采集/训练/激活/服务控制/依赖安装=noop。文献检索不是基础数据采集。PR #6330已合入d3e1dd7ed0f59b04c924455e6e6c94faaaf72cac并自身清理；它的阴性结果不重跑。本设计源码状态NOT_IMPLEMENTED、输入INPUT_UNVERIFIED、研究NOT_STARTED、经济/角色未确认，不需要重启。

## Design Acceptance Index

| ID | Requirement |
|---|---|
| F-311 | 单一新标题信息假设、旧数值事件/人口/时钟查重，不称文本已有效 |
| F-312 | 原人口/标签/费用复用，源标题版本/issuer/coverage和NO_ADVICE语义明确 |
| F-313 | D前可见时钟、修订/日期质量、训练内词表/IDF及禁止LLM历史知识泄漏 |
| F-314 | 两fit预算、共享control与无截距文本残差，零文本恒等及稀疏训练 |
| F-315 | 未来价值到价格映射，不声称完整安全区间、分钟成交或卖出策略 |
| F-316 | 同口径预测技能、原五槽双经济端点、支持/MDE/风险及条件停止 |
| F-317 | 数据owner最小交接、Advisory独立范围、设计/源码/研究/激活状态分离 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-311 | 本文Background及差异表 | artifact: docs/analysis/advisory_prediction_action_diagnostic_20261019.md；旧N3财务设计/本次schema读回 | DESIGN_REVIEW_PASS | none |
| F-312 | 本文Scope/Contracts §1～2 | artifact: docs/architecture/advisory_announcement_title_value_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |
| F-313 | 本文Contracts §2/Model §4 | artifact: docs/architecture/advisory_announcement_title_value_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |
| F-314 | 本文Model §3～4 | artifact: docs/architecture/advisory_announcement_title_value_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |
| F-315 | 本文Price §5 | artifact: docs/architecture/advisory_announcement_title_value_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |
| F-316 | 本文Price §6/Verification Plan | artifact: docs/architecture/advisory_announcement_title_value_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |
| F-317 | 本文Risks/Production Gates | artifact: docs/architecture/advisory_announcement_title_value_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |

矩阵只表示设计验收。源码和数据尚未完成不是被隐藏的功能缺口；不得把设计交付报告为F1实现已完成。

## 多轮设计自审与DESIGN-COMPLIANCE-001

第一轮：查重旧Top50、D-only价格代数和数值财务事件；纠正“未找到文件”等同于数据库缺数的说法，明确只是已检查路径无交付。第二轮：修订五日窗口为age0..4，禁止当前更名/修订标题回灌；明确训练残差非OOF，0/OOV与未知不同。第三轮：明确均值价格上限非完整安全区间/条件开盘预测，训练限WSL、0源码/0fit现状及owner最小范围。

四项符合性：①仅交付设计，不冒称实现/盈利或完整价格功能；②未知输入、NO_ADVICE、OOV与未收敛均显式，不吞错/补价；③原包/公共事件逻辑、旧研究结果/政策不变，新实验仅文本残差一变量；④不增加包使用门禁，不要求数据平台/服务/生产审批来阻断既有业务。以上为同窗口自审，不声称独立外部审核。
