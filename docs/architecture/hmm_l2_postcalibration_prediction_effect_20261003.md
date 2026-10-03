# L2冻结HMM：校准后历史轮动效果与产品消费详细设计

版本：v0.1。Feature tier：F2。当前状态：PROPOSED_PENDING_USER_APPROVAL。

本文件属于父蓝图P1的一个完整业务任务包，不另设预检、adapter、指标、CLI或页面小阶段。终极目标仍是**申万二级行业轮动预测与风险预警**；本包直接回答已训练HMM能否在校准完成之后提供有用的L2轮动排序，不以结构合格、文档完成或历史证据整理替代预测效果。

## Background（背景与真实进度）

父权威为[HMM总体蓝图](hmm_evolution_and_risk_management_system_design_20260716.md)。[当前正式executor设计](hmm_formal_state_executor_20260930.md)中的C-008-L2-D6-PERSISTENT-RC-A及C-008-L2-INDEPENDENT-A已获批准；其文档PR #5323、源码PR #5325在本文件起草时尚未合入。本文不更改这两份合同，不把本文的新评价规则宣称已批准。

- 最终v17原正式实验5184 fits已完成，原两family×两level合同未接受，原acceptance不变。
- 已批准的全131行业zero-refit研究回读：autocycle_all_core:L2仍为D5 seed47，127个行业具有语义mapping，4个为证据不足；原121个通过行业无回退。模型参数hash未改变。
- 4个不足项为801033.SI、801045.SI、801204.SI、801743.SI的1～3日稀有状态证据，不是漏采、正常停牌错误或需要数据窗口补数的事实。
- mapping使用2024-07-01..2025-03-31的5/10/20日future excess utility校准，结果水位为2025-04-30。该窗口只能说明开发期语义校准，不能再作为该mapping的历史预测验收。

本轮只实际读取原compact研究结果、冻结交易日历和必要源码；未读取后续窗口收益/特征、tail outcome或两个大型child文件，没有运行新训练或预测实验。

## Scope（范围）

提出并一次闭合D1～D6：冻结模型恢复、L2文件输入延续、模拟available-at、硬状态到轮动分数、全目录覆盖与同日期基线比较、结果/产品消费边界。获精确合同批准后，才实施源码及两次fresh-process零fit历史推断。

现有代码复用位置：`formal_state_input.py`的C-010/A5构造、`stock_fact_observation.py`的L2 feature-domain panel、`formal_state_model.py`的preprocess/projection/restore/causal_filter、`formal_state_executor.py`的identity/readback；产品复用既有Rotation L2 repository/API/UI。不新建训练平台、feature store、通用registry或平行writer。

## Non-goals（不做什么）

不训练、不调用D5、不换seed、不改变20D特征、训练窗口、数值参数或已批准D6；不恢复退役B3/P6链，不生成原两family READY。不新增L1候选、per-sector stitching、soft semantic mapping、收益驱动缺失过滤或参数搜索。不重建/修改数据集、active profile或历史产物，不进行证据归档。正式QE实验交QE窗口；不改QE、荐股、Selection、Paper或Advisory。

本文仅提出新的评价及score合同。设计审核/F2通过只代表可提交设计，不代表合同批准、源码实现、效果通过或生产权限。

## Architecture（同一P1闭环）

冻结v17与full-v3 authority → 验证原request/acceptance/研究mapping → 恢复seed47原模型、train-only预处理和projection → 构造仅L2的新日期C-010观察 → 前缀因果过滤 → 封闭全部131目录的预测与可用性 → 读取批准的成熟开发期标签 → 同日期效果/覆盖/基线比较 → 输出一个compact结果及已有产品合同可消费的显式研究版本。

预测与标签是两个访问阶段；预测器禁止获取结果标签。先封闭预测，再计算指标；两进程复现相同预测/指标payload。工程完成、开发效果、forward确认、QE增量及生产采用分别报告，不互相推导。

## Contracts（待批准精确合同D1～D6）

### D1：原模型与冻结文件身份，不改训练

推荐版本：`hmm_risk_l2_postcalibration_effect_v1`。对象仅`autocycle_all_core:L2`、seed47；目录仍是131个官方申万文本代码，整数仅为共享release连接键。L1及legacy旧终态保留，不成为本包的合取条件。

固定身份：

- generation=`20261002-v17-unified-basic-history1`，revision=`20261002-r8-unified-basic-history1`，release=`qe_hmm_full_v2_20260831`，cutoff=`2026-08-31`。
- manifest identity=`97df6acdbe43dc20f577e85f8cceb2814d73fca13b0f12cda7aefd90cfb62f2c`；manifest文件SHA=`eb193a03fedeb0aa4c825daed492f50331d4f1581b59b750203501b95694ad76`。
- full-v3 PIT bundle=`051e2af357703734080ff3ea5b4311926905aa7cbd1f31d926ef5b8575261313`；保留stable-taxonomy-backcast的non-as-known局限，不冒充as-published forward结果。
- 原request canonical SHA=`94b35f9c8b5767b1a5cb3ade009fcada1fe5811c2486404a9c0e1796b03084f7`；原acceptance canonical SHA=`fa42b6982f2ff5d8b5e7c257ad727ba66eb0ee9ac9a18e045847e573704a01ac`。
- 原repeat canonical SHA=`e23d7c36fec13aa6a678b3780475e048ad217d764ccce31000bb21b6d2bd89c4`；批准研究readback canonical SHA=`7f0d2376dd955069444077a3c7821e2aac761b3a4f139577a69d1ec8450a8f2d`。

原request位于`F:/Dev/AIstock_runtime/hmm_formal_state/20261003-v17-file-construction/request.json`，原acceptance位于`F:/Dev/AIstock_runtime/hmm_formal_state/20261003-v17-formal-5184/run/acceptance.json`，研究结果位于`F:/Dev/AIstock_runtime/hmm_formal_state/20261003-l2-persistent-readback/research-final.json`。数据root显式传入并验证为上述manifest；不解析active来替代、不自动找latest。

恢复原repeat的选中131个模型及原train-only preprocessing/projection；参数hash必须与原acceptance、研究结果逐项一致。不得使用只存hash而无参数的研究JSON假装模型可推断。只需读取一个原child恢复必要参数，不为保留历史重读所有seeds/两个child；复用既有hash校验，不建立全量历史复制工具。参数无法认证则typed input失败，不另训一个相似模型。

新增观察必须生成独立`evaluation_input_identity`：绑定同v17 manifest/component pins、实际读取的source日期集合、C-010定义/列序、原train-only A5资格hash、full-v3 authority、完整calendar/观察NA lineage以及原模型/preprocess/projection/mapping hash。原request的`source_window`仍止于2025-04-30，原`source_inventory_sha256`/policy receipt不能直接冒充后续观察receipt。新source receipt按真实日期及新观察计算，旧请求不改字段、不仅重哈希延长window；产品input_hash引用新identity而非原request hash单独充当新数据证明。

### D2：available-at、冻结日历与无tail区间

以下为本轮**实际零fit日历算术**，不是效果结果：

| 项目 | 推荐冻结值 |
|---|---|
| train模型/预处理数据末日 | 2024-06-28，原601日训练不变 |
| semantic校准窗口 | 2024-07-01..2025-03-31，原182日不变 |
| semantic outcome水位 | 2025-04-30收盘后；不声称真实历史工程发布日期 |
| 新决策窗口 | 2025-05-06..2026-03-31，221个open days |
| 首个decision的as_of | 2025-04-30 |
| 新source及outcome读取上界 | 2026-03-31；不读取2026-04-01起tail outcome |
| 5D完整成熟decision | 216日，最后2026-03-24 |
| 10D完整成熟decision | 211日，最后2026-03-17 |
| 20D与加权utility完整成熟decision | 201日，最后2026-03-03 |
| 两个仅用于报告的固定块 | 2025-05-06..2025-09-30：105/105日；2025-10-01..2026-03-31：116/96日（decision/20D成熟） |

日历路径为冻结root下`components/daily_bin_candidate/calendars/day.txt`，文件SHA=`ce017cfbf1d9dde630c0d7f39e33b767e95293acd5258104f80491239826207a`。实施时复验实际日期集，不用手写日数替代读取；日历漂移拒绝，不静默缩窗。

对decision t，信息仅至`as_of(t)=prev_open(t)`收盘；文件是回溯冻结release，不证明当时真实供应商发布时间。模型、预处理、mapping的**模拟信息水位**与2026年的工程创建时间分别记录。本假设/规则已在研发期提出，该区间属已消费development的校准后回放，`validation_basis=POST_CALIBRATION_RETROSPECTIVE_DEVELOPMENT`，不是untouched或forward confirmation。

过滤从原2024-07-01验证calendar开始，使用原startprob，不携带train末posterior；原182日过滤必须与原receipt闭合，再因果延续2025-04-01之后的观察直至各as_of。内部合法NA只按现有transition-only规则推进，输出预测必须有当个as_of的有效观察，不能把仅递推posterior伪装成有数据预测。不得每天重置startprob，不在未来日期重新校准mapping，不使用backward smoothing。完整前缀一次递推即可，不重复读取训练数据拟合。

### D3：仅L2完整20D构造与自然NA

新日期输入仍由同release股票事实、security resolver、严格前置circ_mv、PIT成分、suspend/provider authority及CSI300构造，沿用C-010/A5公式、coverage、feature domain、列序与train exclusion。不用九维产品输入、SW L1、L2官方指数或个股简易平均替代原20D定义。

观察阶段只能使用截至as_of的当前及历史事实构造features；评价标签阶段另调用同源行业daily_return读取。未来已知成员变更、停牌或观察有效性不得回填到之前的资格/分母，原full-v3 stable-taxonomy-backcast本身的已披露局限除外，不用该局限允许其他类型未来泄漏。

原A5 train-only contributor资格保持原训练期和逐股票identity，不根据新评价期资金流完整性重新选择contributors；cross-section仍按原131目录及批准C-010逐feature资格计算，不只用127个mapping通过行业计算reference。新入池/无原资格键必须按现有批准规则得到明确状态，不能静默删除或由本包私增排除。若当前构造不能表达其合法状态，作为精确合同缺口停下报告。

优先复用原已认证观察与构造函数，仅补校准后所需源窗口及必要lookback；预热长度依原250日等滚动公式和calendar确定，不截短为25日，不重造原训练request。原`prepare_file_request/prepare_request`固定SOURCE_END=2025-04-30和两level范围，**不能直接传新end绕过它**；实施须在HMM现有模块抽出参数化L2观察构造，旧训练入口默认语义不变，不复制C-010/A5算法或恢复旧模式。

所有源/标签日期选择限制在批准上界；使用已有分区/按日期读取，不为了新验证遍历完整数据根或加载未来tail数据帧。manifest身份/组件SHA校验不等于允许读取未来outcome。必要原warmup文件仅供当前观察的历史滚动上下文，不进入标签。

正常停牌、无官方指数、持续下跌、单成员行业不自动判整个模型失败。按原聚合合同区分合法观察NA、provider absence、quote停止发布及真实未知缺失；官方quote不可用不等于membership消失，真实moneyflow不清空。当前C-010股票事实版与官方指数版各自独立：官方指数停发不自行制造指数，也不一概判定股票事实版无观察。若真实源应有却缺失、hash/schema/mapping漂移，则typed fail closed，不能补零、前填、市值同日替代、填1.0或fallback数据库。

### D4：唯一硬状态分数与标签，不搜索horizon

推荐沿用原D6已校准的经济utility，不引入soft semantic authority：

`k(s,t)=argmax posterior(s,as_of(t))`，top1-top2必须严格>1e-12。

`raw_score(s,t)=frozen_utility_mean(s,k(s,t))`。

`y(s,t)=0.35*sum[j=1..5](r_s(t+j)-r_300(t+j)) + 0.35*sum[j=1..10](...) + 0.30*sum[j=1..20](...)`。

r_s为与原C-007/C-010相同的股票事实行业daily_return，r_300为同release benchmark_return，decimal单位；不是复利收益、指数近似替代或可执行组合收益。目标从decision之后开始，不把as_of或decision当日收益混入未来。较长horizon已包含较短日期贡献，此重复权重是原批准utility定义，不另调权重。

所有模型posterior可以保留诊断，但不以posterior加权utility代替硬状态，不利用validation/评价结果改mapping/均值或组合行业模型。fading/neutral/trending始终为各行业内部冻结mapping；**不是**每日Top/Bottom组。跨行业raw_score再按当日可预测人口average-rank归一化到[-0.5,0.5]用于排行榜；状态不因排名重命名。行业内utility均值存在校准误差和尺度差异，本包就是检验其跨行业可用性，不宣称此score已可靠。

精确相等值average rank，N<2或全分数相等时保留真实状态/分数及不可定义IC原因，不默认neutral，不按文本码制造统计上的排名差异。UI同分可按文本码稳定展示，但不能改变指标rank或创造预测差别。无正式mapping的4项始终score/state=null，不补到末位。

N<2时normalized rank_score为null并带`cross_section_rank_unavailable`，但有限raw_score与合法行业内state可作为非排名诊断保留；禁止把rank_score=null说成已有可用排行榜。N>=2而raw_score全部相等时rank_score是真实0，IC仍不可定义。这两种情况分别记录，不以默认0混同。

末20个decision仍输出真实预测，复合目标标`outcome_not_mature`，不读tail补齐。5D/10D/20D分量在各自成熟日期分别诊断，**唯一binding效果使用复合目标的201个计划成熟日**，不事后择最好的horizon。合法未来NA保留原预测并记录outcome不足；应有而漏采不能冒充合法NA。风险precision及消费者收益未在本包定义，不由负utility或spread自动推导。

### D5：全人口、固定对照与效果/证据分离

目录D=131。结构人口S(t)依模拟as_of的membership及本股票事实版批准结构资格冻结，不以20D特征是否有限缩小S，不能由D6通过或后来收益决定。原始分数集合R(t)为S(t)中有冻结mapping、完整20D有效观察及合法hard posterior者；N=|R(t)|>=2时可排名预测P(t)=R(t)，否则P(t)为空，有限raw_score仅为非排名诊断。4个mapping不足项属于模型不可预测，不从D/S分母删除。逐日所有131项都保留状态和reason；raw候选覆盖与可排名覆盖分别报告。

metric集合M(t)为P(t)中有完整合法复合目标者。推荐沿用已有L2效果方案的**先验约定**：有效IC日要求`|M(t)|>=max(2,ceil(0.90*|P(t)|))`；201个计划成熟decision中有效IC日比例>=90%；总体预测覆盖`sum|P(t)|/sum|S(t)|>=90%`。上述为待批准的本包证据充分性规则，不是股票最少数量/资源门禁，不把自然NA诊断为业务数据错误；同时报告D、S、P、M、逐行业缺失率和全131目录覆盖，不能只报127行业为全人口。分母0则null/NOT_APPLICABLE；无可用预测或无可定义IC均EVIDENCE_INSUFFICIENT，不伪造0或成功。

每日Rank IC为M(t)内raw_score与y各自average-rank后的Pearson；有效日等权。唯一binding development效果推荐`mean_daily_RankIC>=0.02`，来源`CONVENTIONAL_PRIOR_MAGNITUDE_NOT_VALUE_DERIVED`，是本包新增推荐值，需批准，不能靠L1或旧L2授权自动继承。HAC只诊断区间，不附加t-stat/power/每行业每报告块AND门；正常行情变化不是失败reason，真实效果不足仍如实报告。

HAC推荐Bartlett Newey-West lag19，按完整201日成熟calendar的真实open-session距离计算，不压缩NA成邻日、不用0充当IC。n为有效IC日，e_t=IC_t-mean，`gamma_k=(1/n)*sum[calendar_distance(i,j)=k](e_i*e_j)`，`LRV=gamma_0+2*sum[k=1..19](1-k/20)*gamma_k`，`SE=sqrt(LRV/n)`，诊断区间mean±1.96SE。n<2、variance负/非有限则HAC_UNAVAILABLE，不clamp、不同意据此确认或额外阻断研究产品；保留观测mask和非随机缺失局限。

唯一对照为既有L2零fit资金流delta公式版本，固定其原参数，不调用新的训练或其他owner实验。从同v17和相同decision/as_of计算；**重新用本D4复合标签**做公平比较，不拿原10D IC与本复合IC相减。两臂先分别封闭预测/可用性，再在交集J(t)有合法目标者做paired评价；交集coverage、两臂各自全可用人口的绝对效果均报告，不能只报有利交集。paired差值及不确定性仅诊断，不追加“必须击败基线”的promotion门，不因基线强弱自动换模型。原基线历史数值/旧identity不改。

spread只按每日score Top/Bottom组的真实y均值差，`q=min(floor(N/2),ceil(0.2*N))`，边界ties整体排除，两端空组则不可定义；不以hidden-state index或trending/fading人数替代。固定两报告块、IC、spread、覆盖与小行业集中度为诊断，不新增循环候选或逐块通过要求。

终态：输入/身份/执行错误为FAILED；合法证据不足为EVIDENCE_INSUFFICIENT；证据充分且IC<0.02为BELOW_BINDING_MBE；充分且达到0.02为DEVELOPMENT_EFFECT_REACHED_FORWARD_UNCONFIRMED。后两者都可报告真实研究结果，但任何一种都不等于forward、QE增益或两family READY。`tail_accessed=false`、`forward_confirmation=NOT_STARTED`、`advisory_status=NOT_AVAILABLE`；没有实际功效核算不得写PENDING_INSUFFICIENT_POWER。

### D6：一个交付包、真实消费与停止

实施预算推荐两次fresh-process、0 fits、0 D5 selection；环境沿用现存Conda base固定单线程版本，不装依赖、不修改Conda AIstock。两个进程输出必要预测/结果canonical payload一致，PID/host/path/时间戳不参与数值payload。原模型/preprocess/projection/mapping hash前后均不变；不增加跨host bitwise承诺。

在现有formal模块/CLI内增加明确的L2效果模式，原prepare/train/preflight/l2-readback保持默认合同。预测输出逐日全131目录与可用性，结果只保留compact指标/identity及必要产品行，不输出冗余child grid、逐股票证据仓库或归档副本。必须使用独立新输出，不能覆盖原request/acceptance/research-final或数据集。

产品消费复用既有Rotation L2链，新增明确model/evaluation版本绑定：原零fitdelta的contribution==score、model_hash等校验不能删除或被HMM状态强套。当前表/API若只支持原delta版本，应在同一HMM任务内显式按批准版本验证HMM字段及来源，不做兼容代理/静默改旧行；真实无可用预测时只能typed NOT_AVAILABLE，不把空页验收为预测成果。排名全P(t)计算，默认Top10/Bottom10，自定义总数<=30；评估不缩成展示榜单。不修改其他模块或现用QE系数资产。

HMM行业内semantic_state与daily_rank_group是不同字段/展示含义；现有delta行的state语义不得重定义。必须以明确版本投影保留两者，页面区分“该行业内状态”与“当日相对排名”。分数归一化后不伪称收益或posterior confidence；贡献说明引用硬状态、冻结utility_mean及average-rank算式，不构造delta contribution或SHAP。两个极端榜单不重叠，同分的展示顺序不改变统计分组。

本包先完成源码、直接矩阵、多轮审核和正式零fit效果结果；是否持久化/激活按实际schema与用户对应授权执行，不为演示另建表或新审批。源码合入、DEV/生产写入、runtime activation及用户重启独立报告。缺少这些权限不抹去已经完成的效果结果，也不能伪造产品验证完成。

本轮不自动读取tail或开新候选。不论结果达到/不足/失败，都停止，交付原因及下一步建议；需新模型、再校准、窗口/目标/人口变化时必须提出具体新合同，不自行提高预算。QE正式回放仍由QE窗口决定并执行；本包未批准任何score→QE风险系数转换。

## Design Acceptance Index

- F-001：原模型、train-only变换、mapping与v17身份保持。
- F-002：available-at及冻结calendar保证校准后、无tail因果回放。
- F-003：完整20D L2 C-010/A5观察与自然NA，不用九维替代。
- F-004：硬状态utility分数、固定复合目标、全人口效果与同日期基线。
- F-005：零fit双process及既有产品版本化消费；无伪预测完成。
- F-006：授权、失败终态与模块隔离，遵守DESIGN-COMPLIANCE-001。

## Implementation Plan（同一任务包，不新增开发阶段）

当前先审核本文及父蓝图增量，提交D1～D6推荐决策；未批准不实施。批准后以已合入的A/B源码为基线，在一个HMM feature任务中完成必要模型读取、观察延续、推断/指标、产品版本适配及直接测试；代码最多三轮审核修复，零阻断可提前结束，否则报告真实阻断。源码/CI与合入授权闭合后，在指定validation worktree运行双process零fit，报告效果及是否值得进入已有L2产品/消费者验证，不再重跑5184 fits。不把本次“开始下一步”解释为合入现有PR或批准本文新增score/日期/阈值。

本次已完成日历算术和源码可复用性核对；源码缺少后续窗口构造/预测效果入口，仍待实施，不能把现有l2-readback当作本设计已实现。数据窗口仅负责真正共享源缺口，所有HMM特征/因果/效果验证由本窗口负责。

## Verification Plan

设计：F2 validator、git diff --check、三轮以内针对性自审；校准前后角色、日期算术、预测封闭先于标签、原训练不变、人口分母、产品旧版本隔离逐条检查。设计中的新增90%、0.02、lag19、硬状态均值排序均标待批准，不暗中运行。

实施直接矩阵限`backend/tests/hmm_risk`：冻结模型hash恢复、seed/D5/fit poison；预处理仅train/旧projection；未来source/target poison与prefix不变；calendar20D成熟末端及内部NA；131目录/4项mapping缺失；停牌/停发保留moneyflow/单成员；合法NA与未知漏采；同分/全同分/空分母/HAC缺口；基线同源同标签同日期及交集覆盖；旧product版本不变/新版本writer-readback/身份冲突。只增加实际缺失合同节点，不复制整套矩阵。changed-files→ownership→HMM slice、Ruff、py_compile、registry/L0、fresh-process import；完整矩阵优先CI，不重复跑历史训练测试。

## Design Acceptance Matrix

下表只验收**本次设计文档的完整性与边界**，不是实施/效果矩阵。测试路径仅为实施后的计划矩阵，尚未运行；D1～D6仍为PROPOSED_PENDING_USER_APPROVAL，F2格式PASS不能改变该状态。实施后必须用真实代码/运行证据另更新对应状态，不能沿用design review作为功能完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §D1；formal_state_executor/model | artifact: F:/Dev/AIstock_runtime/hmm_formal_state/20261003-l2-persistent-readback/research-final.json；核对已有身份与新source身份边界，未实施恢复入口 | DESIGN_REVIEW_VERIFIED_ONLY | 无 |
| F-002 | §D2；冻结day calendar | artifact: X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260831-qe_hmm_full_v2-direct-20261002-r8-unified-basic-history1-candidate/components/daily_bin_candidate/calendars/day.txt；实际日历算术，非预测回放 | DESIGN_REVIEW_VERIFIED_ONLY | 无 |
| F-003 | §D3；formal_state_input/stock_fact_observation | backend/tests/hmm_risk/test_formal_state_input.py：计划验证后续L2构造、旧入口不变，本次仅对照源码职责 | DESIGN_REVIEW_VERIFIED_ONLY | 无 |
| F-004 | §D4/D5；formal_state_model与现有L2基线 | backend/tests/hmm_risk/test_formal_state_model.py：计划hard utility与因果矩阵；本文明确score/效果/coverage全部待批准 | DESIGN_REVIEW_VERIFIED_ONLY | 无 |
| F-005 | §D6；现有Rotation L2 repository/API/UI | backend/tests/hmm_risk/test_rotation_l2_prediction.py：计划版本/语义隔离测试；本文未报告实施/运行通过 | DESIGN_REVIEW_VERIFIED_ONLY | 无 |
| F-006 | §Non-goals/Production Gates/审核记录 | backend/tests/hmm_risk/test_formal_state_executor.py：计划fit/D5/DB poison；本次逐项核对未执行/未授权边界 | DESIGN_REVIEW_VERIFIED_ONLY | 无 |

## Rollout / Rollback

新研究版本独立run/model/evaluation identity；不替换已有L1、L2delta、旧QE系数或运行receipt。没有上线即没有需回滚的生产状态；未来授权发布只按既有HMM产品revision和manifest事务边界执行，失败不覆盖旧版本。不以重新激活旧数据集作为本实验回滚。

## Risks / Failure Modes

127/131语义有效不是经济有效；硬状态均值可能排序很粗、跨行业尺度不同、参数随时间变陈旧。选定研究模型/持续规则都使用过开发信息，虽分离校准日期也不是独立证明。stable-taxonomy-backcast与历史实际发布不同；同一股票事实可因停牌/缺字段出现合法稀疏性，coverage必须如实展示。201个重叠utility日期不等于201个独立样本，L2行业多也不按比例增加时间功效。证据不足、能力不足和数据错误不能互换；不承诺本候选一定通过。

## Production Gates

本次文档：production_ddl_gate=noop；production_dml_gate=noop；backend/frontend dependency gates=noop；runtime_activation=noop；backend_restart_authority=false；database_write=false；dataset_write=false；active_profile_write=false；fit/selection/tail_access=0/false/false；cleanup未执行。未来任何写库、激活或重启不由本文自动授权。

## 正式审核记录（设计，不代替实现/模型验收）

第一轮自审修复：新增日期不能借原SOURCE_END=2025-04-30的request/source inventory作身份；明确独立evaluation_input_identity、原参数认证和旧请求不变。区分industry semantic_state与daily_rank_group，不能迁移现有delta版本语义。首次F2检查发现`proposed`矩阵状态不受支持，改为仅验收文档完整性的矩阵并明确所有实施/效果未执行，不把待批准合同伪装已批准。

第二轮自审修复：原先P(t)混合“有raw分数”与“可排名”的口径，现明确R/P及N<2的null排序、全同分真实0、IC不可定义分别记录。复核源观察与标签分离、train-only A5/新source receipts、250日lookback、原startprob连续prefix、201日复合成熟分母及基线同标签；不新设最少股票数、HAC显著性AND门或资源门。

第三轮自审核对：父蓝图目标/P0完成/P1待批准与本文一致；核对原19条版本历史和11条verified矩阵行逐字保留。221个decision、201个复合成熟日、105/96报告块计数由冻结calendar实际核算；源/标签上界不跨tail，模型及4个不可用行业不被事后改变。两份文档F2 PASS、warnings=0，git diff --check通过。未发现设计范围内未描述的阻断；D1～D6仍待用户批准，实施及预测效果没有运行。

另有真实交付依赖：本次实时查询PR #5323 CI verdict为SUCCESS；PR #5325的run `37105506032`为FAILURE，verdict明确`backend=failure`。可读取的GitHub jobs/log只返回verdict，尚未取得底层backend失败断言，因此不臆断根因、重跑训练、重试CI冒充修复或修改模型阈值。该源码PR在解除真实CI阻断及获合入授权前不可作为已交付基线；不把本文文档F2通过报告为源码CI通过。

DESIGN-COMPLIANCE-001：

| 检查项 | 设计结论与直接依据 |
|---|---|
| 禁止简化交付 | §D1/D3/D6保持原20D模型及全131目录，产品/效果未执行如实标记；0 fits是冻结模型推断，不是替代训练的mock |
| 禁止静默错误 | §D2～D5区分合法NA、未知缺失、未成熟、空分母、mapping缺失及真实效果不足；不默认neutral/1.0/0收益 |
| 禁止改变业务逻辑 | 新score/utility评价/日期是明确待批准合同，旧D3～D6、seed/model hash、delta产品及QE资产保持原语义；仅L2研究 |
| 禁止私增门禁审批 | §D5数值全部为提案；无新HAC/power、每行业、最少股票数、资源/审批AND门；授权按现有动作边界处理 |

以上只说明设计审核，不宣称实现、经济效果、QE或产品已通过。F2 PASS仅是文档格式与验收索引一致性检查。
