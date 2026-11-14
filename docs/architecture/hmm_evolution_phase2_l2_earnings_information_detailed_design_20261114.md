# HMM Phase 2：L2盈利披露信息单候选详细设计

> 版本：v1.2；日期：2026-11-14；tier：F2；owner：HMM。
> 状态：`STOP_NO_INCREMENTAL_VALUE_REFERENCE`。用户批准的同包任务已完整完成：源码#7258合入、完整预检PASS、12/12 fits、双process模型/142×131预测及事前分组一致封存、零fit效果评价及算术复核完成。132成熟日主J=−0.5939238bp/10日参考，按原合同停止该精确组合，不追加fit或QE、不替换旧模型。完整结果与边界见§11。
> 父权威：[HMM总体蓝图](hmm_evolution_and_risk_management_system_design_20260716.md)§1.41/§7。原N/C/S/W/M/VIX终态、正式资产及消费者全部保留。

## 1. Background / Purpose / Scope / 非目标

终极目标仍是在**申万L2**证明可用于减少损失且不过度错过上涨的场景增量，最终由对应业务窗口验证账户收益/风险；不以行业结构、合法性、文件或训练数量作为成果。此前原20维的目标变换、融资余额和浅树组合已经结束。本轮只问一个新问题：**最新已披露盈利增长的行业相对分布，能否在原20维之外改善同预算风险识别的经济参考？** 不预设它有效，也不将净利润同比称为一致预期、预期差、单季度增长或未来盈利预测。

唯一候选E21=原20D+1个`earnings_growth_rank`；匹配C20=同一事前共同TRAIN集合上的原20D。保留原成员损失目标、10D持有期、三季度滚动及Ridge数值内核，只隔离新增信息。选择10D是匹配现有场景/标签/控制的归因决定，不声称它是财务信息的最优horizon。forecast/express已审计但**不进入本候选**，不因E21失败再自动切换来源、字段、horizon或算法。

不是新latent-state HMM、概率校准、买入禁令、仓位公式或股票重排产品；仍属于HMM模块的L2辅助研究线。不修改QE/荐股/LocalSIM、数据生产、候选集或active profile，不建DB/API/UI/调度器/记录服务，不把实验记录绑定环境变量或重启。

## 2. Source readiness：已完成与尚未完成

基线main=`1a10f11e838b3956a7ba634c295da0e9a848b2f9`。共享raw导出已完成，HMM已逐行核验原文hash/日期/版本身份并用既有C-013/PIT reader核验来源人口；11月14日再次核对manifest全部11个成员hash/size及选定字段。机器回读：`F:/Dev/AIstock_runtime/hmm_l2_earnings_information/20261114/source_qualification_v2.json`，文件SHA=`43ee5e93514c9f60f6581e2d923a528dfb22dc9bcb298fecde4cd61b7da72d9d`。该回执只证明来源资格与明示算术例证，不是21维TRAIN预检或效果验收。

| 事实 | 已知结果 / 不允许的外推 |
|---|---|
| 全部三类原文 | forecast32,604、express6,509、fina_indicator182,385，共221,498条；无丢行或重复版本，不取latest |
| 业务投影预检 | 原next-open诊断下，目标PIT记录的C-013/L2解析失败0；fina来源触及131行业。此为原始公告投影，不是本设计更保守时钟/252-session状态或共同TRAIN全部通过 |
| 选定`netprofit_yoy` | 6,590条null；747个原source key有该字段内容变化；192个key在最早first_seen时间仍有字段冲突。均为原始人口审计数，不能当成最终有效模型行数 |
| 正常源稀疏 | BJ、PIT外、正常停牌、源公告日无记录、无官方行业报价均不自动解释成缺数，不增加全行业每天有公告的门 |
| 真实语义边界 | 财务指标一条603400.SH/2026-04-22的报告期为2026-06-30；不能作为提前披露的正式报表。forecast中报告期在未来可合法，不能共享此过滤规则 |
| 知识时间 | published_at均未知，NON_VINTAGE；本地时间均起于2026，旧链可能更新observed_at，部分first_seen平手。新增lag不能修复历史版本/首次发布时间证明 |

数据owner的原始导出责任已闭合，不要求重导、补采、清空真实值、发布successor或激活数据。单条报告期异常若需纠正，只核验精确source key；本设计先显式隔离而不改原件。HMM自行实现下面的读取、身份、版本、状态、聚合与训练配对，不再交给数据窗口完成业务预检。

## 3. Contracts D1 / L2-EARNINGS-INFORMATION-D1：任务与唯一候选

批准模型版本`hmm_risk_l2_earnings_growth_member_loss_ridge_v1`。E21预测成员固定权重的10D末端损失C；对照C20在相同样本、标签、第一20列scaler和日期权重上重拟合。输出`raw_expected_member_loss`与沿原C的`clip(raw,0,1)`投影，不伪装事件概率、不套1−p或warning0.20。完整131行业输出availability/reason，TopK只供日后展示，总数≤30，不裁业务分母。

decision=t，as_of=a=prev_open(t)。M[t,g]及固定权重w[t,g,i]原样复用C：a时点PIT、正式L2成员，排除a已知停牌；真实严格前置流通市值固定权重，不因未来停牌/亏损/行业无官方报价/单成员改组。成员收益R为原C-010未复权integer-li的decision close至t+10 close；C=Σw·max(0,−R)，U=Σw·max(0,R)，S=U−C。对自然端点NA沿原合同保留，不能补价、重新归一权重或将未知收益写0。

## 4. Architecture / Contracts D2 / L2-EARNINGS-INFORMATION-D2：输入、版本与身份

复用现有文件身份/C-013/PIT/成员C与Ridge数值内核；新增的只有HMM财务reader和一列聚合。数据流是显式共享raw→证券/事件版本状态→a时点报告状态→同一M的增长rank聚合→共同K上的E21/C20→先封存再配对经济评价。所有输出独立版本化文件；API/DB/UI合同本轮均不变，没有新的跨模块运行依赖。

### 4.1 显式双输入，不追随active

基础release保持10月底原研究版本；新增信息是独立通用共享包，不把它伪装成candidate内原有组件，也不修改旧manifest或模型hash。

| 固定输入 | 身份 |
|---|---|
| release root | `X:/AIstock_dataset_candidates/backtest_dataset_candidates/20261030-qe_hmm_full_v2-direct-monthly-v2-unified-candidate-correction-4388f6c6d166` |
| release canonical / manifest file | `772112c58f959748787b939e33ec874fb0766ca921d88eddbd49c131c3b3c43d` / `2242c44261dc0eaaf54cda3416a260cf6712ec960cff8bfbb46a527f6587b958`；cutoff2026-10-30 |
| source request | `F:/Dev/AIstock_runtime/hmm_l2_signed_member_return/20261109/source_request_v2.json`，文件SHA`5470f7f803ef801fa0023199ef282621e592b9ca72b0072fbf8b85827a566c02`；包含原C-013及10月release L2延伸的完整pins，不以本页简称替换 |
| C-013/release binding | request内receipt `d53b033790108c851d15b3cdc7cfb48aff45d2c70c6af63f2f9ddf101b29ffb7`；原C-013/release组合身份`505ede3b457014b0b125cad557b1404fd1d8237b96481494a55c5c15895247de`，仅historical replay |
| calendar / PIT pool | `components/daily_bin_candidate/calendars/day.txt` SHA`7ea1bb6e038cce8ff792e6ecbc13a7dd1a1168fc5ca23fc24c995bff195a08ed`；`stock_pools/stock_universe.txt` SHA`f242acb266d585306e63533c52c478d743b9526c2405bd7e1180f5a784531af5`，不是instruments/all的指数/股票混合人口 |
| financial package root | `X:/AIstock_shared_inputs/tushare/20261113-financial-ann-20220321-20261030-v1` |
| package manifest / schema | 文件SHA`bbdaa3542d89fbbf8558a446da574914860babaa1e7e59dd62589257965d6f1c` / `aistock_shared_financial_raw_observations_v1`；公告闭区间2022-03-21..2026-10-30 |
| selected raw file | `tushare_fina_indicator_raw.parquet`，39,534,010bytes，SHA`64302167dd4c0445e2b273cde9c6fefa3f8f3c4bf3f06ff3d04df069e9e59ea7` |
| original 20D/C carrier | 原S请求的prepared/feature/TRAIN/mask/definition完整pins及Q4 prepared；只可在原source identity、分区、数值及mask匹配后显式复用，不重建旧标签作为本轮证据任务 |

20D/C载体的显式入口是`F:/Dev/AIstock_runtime/hmm_l2_signed_member_return/20261109/request_v2.json`（文件SHA`b537a156c4d36ef3561df1cabe0121ebb538b9cfccdfbc64f71d93892e90339c`），其source_pins含原feature definition `c9c197c189c580cf203332944d160b5c88fe414ca1145ef1e6d5a8221a25dccd`、TRAIN、mask及Q4路径。仅借用该原C数学输入闭包，不恢复S目标/预算，也不以仓库后续同名函数内容替换冻结定义。

每次request显式传root、manifest及source request pins，所有读取前后核对文件身份、路径与size/hash，拒绝越界、symlink/junction、未知schema或混release。原文是JSONB保留的语义内容，不是HTTP传输字节；source_row_hash沿现有`json.dumps(payload,ensure_ascii=False,sort_keys=True,default=str)`的UTF-8 SHA256（含默认空格），不得混用紧凑receipt canonical。保留原shared security/provider absence/suspend/价格pins。禁止latest搜索、旧release/数据库/其他模块投影fallback。

### 4.2 证券身份、版本与记录级NA

整数行业ID只作release连接键，输出使用原131个申万文本代码；不建HMM私有行业编号。财务原文ts_code保持原样。该合同另显式定义HMM request中的`financial_source_projection`：沿用既有证券身份`szse_300114_302132`，300114.SZ在2025-02-16以前对应canonical302132.SZ；canonical回溯代码自身仍有效。依据是共享`security_source_identity/20261112-margin-detail-v2/security_source_identity.json`（文件SHA`e6b984ccaadec5bddc8ada28c4a57d2331f1cc5739b958629b946ef547a2c84e`）中既有正式改码证据，官方2025-02-17切换。不修改该authority，也**不把moneyflow/margin限定row直接当作财务resolver规则**；本提案显式批准新的财务消费投影后，request同时钉住证据row、源namespace及投影hash，实施时验证全部实际旧码。其他未知alias不按名字、位置或整数猜测。

来源审计已发现同一证券在新/旧文本代码下的12组财务指标公告、2组预告，数值版本集合一致；canonical证券+ann_date+report_period是经济事件分组，不能双计。实际模型只消费fina源；不将公告日或报告期相同的不同公司合并。

本轮仅为回溯研究冻结以下版本规则，不冒称真实历史可得：先排除first_seen_at晚于2026-10-30上海23:59:59.999999的内容，标`POST_SOURCE_CUTOFF_OBSERVATION`；对每个经济事件取最早first_seen时间的**全部并列行**，只在netprofit_yoy一致时形成字段值。不同hash但本字段完全一致可共用字段身份；null与有限值不一致视为`AMBIGUOUS_EARNINGS_VERSION`，不得按hash/ID/数值完整程度/update_flag挑选、拼接或按未来是否修订筛行。保留所有原行引用/原因，后续修订不回填旧公告。不把最早本地观察说成最早公开版本；该规则仍可能受旧链元数据继承与回溯修订影响。

fina的report_period>ann_date记录标`INVALID_FINANCIAL_REPORT_TIMING`永久隔离，603400.SH该行预期命中；不自动改成下一报告期公告日或把它加入forecast。隔离不是从收益不好的行业删样本，原记录/原因仍在审计中；其他真实有效的已披露报表状态仍按D3选择。未知字段/源覆盖保留typed NA，不能把无观察写成盈利0或安全状态。hash/唯一性/身份冲突是source fail-closed；已定义的局部字段缺失、版本歧义及自然NA不升级为全行业全通过合取门。

## 5. Contracts D3 / L2-EARNINGS-INFORMATION-D3：唯一特征与时钟

只使用fina_indicator的`netprofit_yoy`，按[官方字段定义](https://tushare.pro/document/2?doc_id=79)为归母净利润同比增长率（%），不是q_netprofit_yoy单季度指标、预告利润金额或卖方预期。选它是较直接、较广覆盖且不新增金额换算的可解释信息假设，不是按收益或IC选出的字段。无clip/winsor、盈利正负筛选或字段替补。

在a的原C贡献股票人口中，只允许ann_date **严格早于a的日历日期**的事件进入；公告仅有日期时，不把a当天盘后/未知时刻信息加入T−1收盘事实。例：周五公告→周二decision最早使用，周末公告也至少经过完整周一session。此是保守**回溯假设**，不能证明当时供应商已提供，更不能修复NON_VINTAGE。真实capture/first_seen只保留版本溯源，不能倒填为历史公告时点。

每只股票先从合资格事件选最大report_period，再选该报告期最大ann_date；若被选事件字段null/歧义则该股票NA，**不能跳过它去挑较旧有限值**。已知报告状态最大有效期252个交易session，年龄按**报告期**计：`count_open(report_period, a] <= 252`，超过为`STALE_EARNINGS_DISCLOSURE`；重新公告很老的报表不能重置年龄。选择最新已披露报告状态不是为缺失行情前填；不制造每日新公告，也不把后续修订提前。报告期异常原行从合法报表集合隔离并单独报告，不因此伪造新的公告。

令O[t]为原C当前贡献人口并集内、有上述唯一有限值的股票；不得加入BJ、当时不在PIT或未来股票，不能按未来行情可估性筛选。按股票原始增长值在O[t]中做average-rank，n=|O[t]|：

```text
z[i,t] = (average_rank(netprofit_yoy[i,t]) - 1) / (n - 1) - 0.5
E[g,t] = sum(v[a,i] * z[i,t], i in M[t,g] intersect O[t])
         / sum(v[a,i], i in M[t,g] intersect O[t])
```

n<2或行业O为空则特征NA；真实全同值有确定rank0，不伪造NA或反转分数。v严格复用原C的真实前置市值，份额/币种一致时比率不需新增换算。单成员行业可合法贡献，不要求该行业有官方指数或全部三态。保存观察股票数、其占原M权重比例、事件龄、报告期/版本reason，不把coverage当第二学习特征或设置最低比例门。

共同TRAIN集合K=原C有效TRAIN mask∩事前可定义E；每季度E21/C20在完全相同K上训练。前20列scaler相同，第21列仅E21的TRAIN拟合行等权mean/population std；常量列沿原内核active=false。两臂使用日期等权权重N/(D*n_date)。不以validation标签/收益/缺失结果重选K或rank人口；完整预检必须实际报告每季K与日期/131覆盖，不预先编造未来mask hash或宣称全部可训练。

## 6. Contracts D4 / L2-EARNINGS-INFORMATION-D4：滚动、数值与12-fit预算

| vintage | 原TRAIN闭区间 / 原始session数 | 标签在train_end前成熟后的最多decision数 | 完整预测窗口 / session数 |
|---|---|---|---|
| Q2 | 2023-04-01..2026-03-31 / 724（首日2023-04-03） | 714，末日2026-03-17 | 2026-04-01..2026-06-30 / 60 |
| Q3 | 2023-07-01..2026-06-30 / 725（首日2023-07-03） | 715，末日2026-06-15 | 2026-07-01..2026-09-30 / 65 |
| Q4 | 2023-10-01..2026-09-30 / 726（首日2023-10-09） | 716，末日2026-09-15 | 2026-10-08..2026-10-30 / 17 |

252-session事件预热只从固定源窗口/日历取，不改变上述TRAIN起止。持有10session，标签末端必须≤该季度train_end；不是把train_end后标签purge为可读。完整预测142日×131=18,602行，每臂每process均保留，10月底最后10日未成熟不裁掉；最多132成熟日/截至2026-10-16，真实可估值J日期与该上限分开。Q4只有7个成熟日，不装作充分独立验证。

沿现有固定Ridge：alpha=1.0、fit_intercept=true、copy_X=true、max_iter=null、tol=1e-4、solver=svd、positive=false、random_state=42。只新增一列，不更换目标/窗口/参数。数值环境复用既有Conda base：Python3.13.5/numpy2.3.3/scipy1.16.3/sklearn1.8.0/hmmlearn0.3.3/threadpoolctl3.6.0及原五项数值线程变量=1；仅child process数值设置，不是实验记录/后台环境绑定。不安装依赖或改AIstock环境。

固定总上限12 fits=E21/C20×Q2/Q3/Q4×双fresh-process，失败attempt也占预算。完整21维文件/共同TRAIN预检通过后，冻结实际12项attempt清单，再从已交付clean HEAD执行；失败不换目录重试、换seed/字段/参数或补候选。旧C可作背景，不以不同mask的旧模型冒充配对控制；本预算未授权复跑旧N/M或新增QE。若全无可用TRAIN则停止输入不足而非伪造模型；合法少行业缺特征仍局部NA。

两process模型、scaler、mask、完整预测与**事前风险分组**封存/一致后，才读取原已消费评价事实。不读取新tail、不因为9/30/10/30未成熟缩短预测，不覆盖旧模型、QE资产或封存控制。

## 7. Contracts D5 / L2-EARNINGS-INFORMATION-D5：经济主量与停止

复用已有同预算风险参考，不新增账户/消费公式。在两臂事前共同可用集合A_t，k=ceil(0.20*|A_t|)，按风险从高到低取k；边界tie均分剩余名额，h∈[0,1]、Σh=k。|A_t|<2或全部同分无法排序则该日typed不可定义。分组与成员必须先于未来标签冻结，不因未来缺价/停牌删除后重排。

```text
avoided_loss_increment_t = sum((h_E21-h_C20) * C[g,t]) / 131
missed_upside_increment_t = sum((h_E21-h_C20) * U[g,t]) / 131
J_t = avoided_loss_increment_t - missed_upside_increment_t
    = sum((h_C20-h_E21) * S[g,t]) / 131
```

J日期等权均值是**唯一资源筛选量**：≤0即`STOP_NO_INCREMENTAL_VALUE_REFERENCE`；没有可估值配对证据为`INSUFFICIENT_EVALUABLE_EVIDENCE`；>0仅`RESEARCH_LEAD_REQUIRES_VALUE_AND_VINTAGE_REVIEW`。不增加显著性、IC0.02、全季度/行业/seed都赢的AND门；不按单一好季度、损失IC或coverage取代J。重叠10日gross参考不是日收益/NAV/净收益，不把132日当132个独立10D样本。

差系数不为0的任一行业缺原C/U/S，该日J typed NA；差系数为0时允许配对代数抵消，但不将未知资产收益写0、不声称两臂绝对水平可估值。分母仍131，不重归一到可用行业。HAC按实际session距离、Bartlett lag9、双侧95%区间，只描述不作通过门。完整报告两项分解、季度/覆盖/NA、loss Rank IC/MSE/MAE及固定原新买队列诊断，后者不改变主量或自动触发QE。

额外报告与同日相同k的无排序敞口X（所有A_t取h_X=k/|A_t|）的配对差，检验是否仅减少敞口而非选对行业。X是归因参照，不是另一可择优候选。原生价格为主，复权敏感性使用同一冻结分组，仅诊断，不挑较好口径。没有订单/持仓路径不生成交易净值、换手或声称成本后收益；同日名义预算相同也不能据此声称费用或成交影响完全抵消。可用的固定预算参考与真实账户评估分开。

E21正也只支持进一步核对，**不能消除回溯财务修订/分类与已消费历史的偏差**。交QE前需核对其AF是否已有同类盈利信息以及可接受研究边界；正式QE实验、后续荐股/LocalSIM由owner执行，本窗不提交账户/擅改消费。失败结束该字段/rank/状态/目标/季度/Ridge组合，不自动改为q_netprofit_yoy、forecast/express、预期差、其他TTL或非线性；其他路线须有独立新依据，不机械重复。

## 8. Contracts D6 / L2-EARNINGS-INFORMATION-D6：Implementation Plan / Verification Plan

一个连续业务包完成：正式财务reader/身份与版本状态→唯一特征及matched TRAIN→最小直接测试/作者审修/CI交付→完整预检与固定预算研究→一次价值分析和父蓝图同步。不将这些拆为多个独立小阶段或为完成历史证据另开任务。精确合同现已批准；源码交付后先完整预检，PASS后才运行既定fit，不扩预算。

本合同批准的源码范围：新增`backend/services/hmm_risk/earnings_information_input.py`、`risk_l2_earnings_information.py`、`scripts/hmm_risk/run_risk_l2_earnings_information.py`、`backend/tests/hmm_risk/test_earnings_information.py`及本两份设计状态，实际已按此实施；必要时允许仅在现有`risk_l2_loss_severity.py`抽取可复用的Ridge/scaler纯函数并保持旧行为，本轮未改该文件。不复制整套margin研究、不新增通用研究框架。新增脚本位于既有catalog路径，实际ownership/计划以实现diff核定；HMM不修改全局CI/nox/test plan，映射缺口由流水线owner独立修复。范围外改动先说明，不改其他业务来迁就本模型。

直接测试矩阵：显式双root及SHA/相对路径、未知schema/漂移/越界失败；原20D source未改；财务源namespace投影与其他源隔离、old/new同事件不双计、未知alias；first_seen并列一致/冲突/null、后续修订及post-cutoff排除；fina未来报告期NA而forecast合法事前预告不被共用规则误伤；ann<a与周末/节假日、252年龄边界及原有报告状态而非行情前填；raw增长百分比/negative/large/0、average ties、n<2、无观察不补0、单成员合法；原C成员/前置市值/权重/自然停牌NA；两臂共同mask/前20列scaler/日期等权、标签成熟边界；131×142完整预测、12-fit失败计数、双process一致、分组先封存再标签、J代数与未知值抵消、实际session HAC；DB/network/额外fit poison、旧资产不变。

本地仅changed lint/compile、该直接矩阵、F2/scope/diff及既有最小模块计划；广域矩阵交现有CI/Nightly，不盲跑全量。至少两轮当前作者正确性/完整性审修，绑定最终HEAD，不冒充独立第三方。正式预检以真实mask/scaler/输入/日期回执证明，不能用本设计的来源统计或合成测试代替。

### Design Acceptance Index

F-011：唯一新信息/经济目标；F-012：D2完整输入与身份；F-013：D2版本/时钟局限；F-014：D3唯一特征/NA；F-015：D1/D4配对TRAIN/数值/预算；F-016：D5事前经济主量与停止；F-017：D6单包实施及权限。

### Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | risk_l2_earnings_information.CONTRACT/evaluate；父蓝图§1.41/§7 | backend/tests/hmm_risk/test_earnings_information.py；J代数/固定预算 | SOURCE_VERIFIED | 无 |
| F-012 | earnings_information_input.load；risk_l2_earnings_information.load_source | artifact: F:/Dev/AIstock_runtime/hmm_l2_earnings_information/20261114/prepare_v2/preflight.json；完整文件及共同TRAIN预检 | SOURCE_AND_RESEARCH_VERIFIED | 无 |
| F-013 | earnings_information_input.choose_version/Disclosures/canonical_symbol | backend/tests/hmm_risk/test_earnings_information.py；并列/未来版本/别名/报告期/lag/TTL | SOURCE_VERIFIED | 无 |
| F-014 | risk_l2_earnings_information.aggregate/design | backend/tests/hmm_risk/test_earnings_information.py；负值/0/ties/单成员/无观察/权重 | SOURCE_VERIFIED | 无 |
| F-015 | risk_l2_earnings_information.design/fit_packet；scripts/hmm_risk/run_risk_l2_earnings_information.py | artifact: F:/Dev/AIstock_runtime/hmm_l2_earnings_information/20261114/run_v1/acceptance.json；12 fits/配对scaler/双process一致 | SOURCE_AND_RESEARCH_VERIFIED | 无 |
| F-016 | risk_l2_earnings_information.freeze_groups/paired/evaluate/statistics | artifact: F:/Dev/AIstock_runtime/hmm_l2_earnings_information/20261114/evaluation_v1.json；预测先封存、J/HAC复核及按负J停止 | VERIFIED_NO_PROMOTION | 无 |
| F-017 | 本页§8/§9/§10；单一离线CLI | backend/tests/hmm_risk/test_earnings_information.py；DB/network/fit poison/禁止覆盖；F2及scope | SOURCE_VERIFIED | 无 |

初版16项算术例证不替代代码测试；源码31项直接测试、两轮作者审修及精确HEAD CI均通过。当前完整输入、拟合和经济终态分别由§11所列真实回执证明；研究流程完成不等于模型晋升，负J是按合同完成的停止结论。

## 9. Risks / Production Gates / Rollout / Rollback

盈利增长可能已被价格/QE因子反映，也可能只反映行业结构或幸存者/修订偏差；历史负基数、累计报告季节性、观察子集和大成员权重影响解释。rank仅降低幅度极端影响，不证明信号。NON_VINTAGE的历史版本风险无法靠first_seen排序/lag消除；本轮只能是明示限制下的**backcast exploratory**，不是历史可交易证据、未消费holdout或实盘验证。旧研究负结果不证明本信息无效，来源完整也不保证它有效。

production_ddl_gate=noop；production_dml_gate=noop；dependency_gates=noop；database_access=false；dataset_write=false；active_profile_write=false；runtime_activation=false；service_control=false；QE_tasks=0；正式fits=12/12，预检/评价fits=0。Rollout仅独立离线源码与研究，负J后停止，不替换旧C或其他消费者。canonical源码分类为backend-main，不擅自降为none；无线上消费者接线或运行激活，不为离线研究记录要求重启，不宣称线上运行态已验收。未授权清理，源/设计工作树保留。

## 10. 精确批准与作者审核

### 10.1 已批准的一次性整包

用户2026-11-14回复“批准执行”，批准L2-EARNINGS-INFORMATION-D1～D6：单一netprofit_yoy/rank/252-session状态；ann<a回溯时钟；财务源显式证券投影；最早本地时间并列冲突NA及post-cutoff规则；E21/匹配C20、原季度/C目标/Ridge、最多12 fits；J筛选和失败停止。此次批准独立于#7253文档合入，允许同包源码、完整预检及固定预算研究；不授权生产、QE、数据改写、依赖安装或服务控制。原v1.0提案/两轮设计审核作为历史保留，不把它们改称当时已获批准。

### 10.2 源码实施与审修

实施：`backend/services/hmm_risk/earnings_information_input.py`负责共享包/原文hash、独立财务namespace、并列版本和报告状态；`risk_l2_earnings_information.py`负责唯一增长rank、共同TRAIN、固定Ridge及事前分组/J；`scripts/hmm_risk/run_risk_l2_earnings_information.py`固定request级12项预算、双fresh-process、模型/完整预测/分组封存后再评价；直接测试为`backend/tests/hmm_risk/test_earnings_information.py`。原业务源码零修改，只复用原20D/C载体和现成纯数值/文件核验函数。

源码交付时的真实财务file-only读取182,385条PASS；25,882条BJ/非正式沪深A股代码属于原C人口之外，按范围保留审计，不错误升级为全局身份故障；进入沪深代码域后107,349个有效时序事件、117个并列字段歧义事件（不同于全源192原key），只按批准规则局部NA。未增加覆盖阈值/字段或模型。当时完整原C源/共同TRAIN预检及fit尚未运行，此后均已完成，见§11。

第一轮当前作者审核：修复源范围、分组重新核验和C/U/B合成夹具不自洽，原文hash保持默认空格算法；新旧别名只在独立财务namespace解析。第二轮完整性审核：固定request级12项attempt ledger，失败attempt不可重试或换输出目录清零；补充显式availability、真实session HAC及单一可估日仍报告点均值（HAC不能变相增加显著性门）。两轮不冒充独立审核；最终源码门禁/CI绑定实际HEAD，正式输入与效果另由回执验收。

DESIGN-COMPLIANCE-001源码四项：完整正式reader/配对训练/执行/评价而非简化POC；未知版本/缺失/自然NA不静默填值；原20D、模型/窗口/seed/阈值/旧资产及其他模块行为不变；不增加coverage、显著性、资源或人工审批门。未执行项明确保留，不提前声明研究完成。

### 10.3 初版设计作者审修（历史）

第一轮核对真实源/字段和原数值内核：单字段冲突为192而非多字段2,488；版本并列值不靠hash打破、fina/forecast报告期规则不同；现有scoped alias不跨来源偷用，显式财务request投影单独属于提案；ann<a与252边界、quarter标签成熟/142完整日和J代数完成16项零fit例证。补齐F2 Background/Architecture/Implementation Plan锚点，并将矩阵“无”与未实施状态分开，不把设计例证冒充代码回归。

第二轮核对完整性、父蓝图/本设计/源回执一致、旧STOP与批准边界；将252-session年龄明确按报告期而非重新公告日期，避免多年旧报表重置有效性；补齐原C数学载体入口/definition pins，明确同预算不证明交易费用抵消。最终两份F2/scope/diff为文档交付门禁。两轮均为当前作者审修，不冒充独立第三方；合入前结论绑定最终文档版本，后续改动复核受影响项。

初版DESIGN-COMPLIANCE-001逐项：当时只交完整设计而非把未实施模型算完成；来源未知/局部NA/歧义不静默；旧C/N及跨owner业务/数据保持；新数值在初版标提案，不私增显著性、资源或人工审批门。后续用户批准及真实执行状态分别以§10.1/§11为准。

## 11. 正式执行与价值结论（2026-11-14）

### 11.1 交付、预检与预算

源码PR #7258 merge=`8ad642719332f490ac3d9bb46ddfa6b430545c03`；正式clean源HEAD=`5e03af2c558bd0baa32913a3224eaf5a2e227dcb`，已包含在main，精确HEAD CI run43560620975成功。先前CI失败发生于收集前缺direct-neighbor映射，由流水线#7261补齐三个精确映射后复验通过，未降低coverage或改HMM业务。当前31项直接测试、F2、Ruff/compile/L0/ownership已通过；两轮当前作者源码审修不冒充独立第三方。

原预检`prepare_v1`保留；同步主线后重新执行`prepare_v2`，未只改旧回执hash。新旧特征值、共同mask、完整预测网格和12项attempt清单逐项一致。新预检PASS，特征866日；季度配对TRAIN为Q2=88,929行/714日、Q3=89,569/715、Q4=89,871/716，新增特征未减少原C有效TRAIN。原mask保留正常成员端点NA，不补价或因单成员/无官方行业指数阻断全部业务。

正式完成12/12 fits，没有失败重试、参数/字段/窗口搜索或额外fit。两process模型/scaler/mask一致，三季度C20参数与原C逐项完全一致，新增信息是唯一变化。两臂各142日×131=18,602行全部AVAILABLE；预测及事前风险分组文件逐字节一致封存，之后才启动零fit评价。Q2/Q3/Q4分组改变10/45/8日，这不是收益证据。

| 正式回执（同一runtime根下） | canonical SHA256 |
|---|---|
| `F:/Dev/AIstock_runtime/hmm_l2_earnings_information/20261114/prepare_v2/preflight.json` | `e7f0315726426b5a35cb2bdbfa19f0661a533ce19a3184130f193b42ffcab68e` |
| `run_v1/acceptance.json` | `6212a9639feeaa854ee8891f06a5159003e33856f0026223a560a0186a9eba21` |
| `run_v1/prediction_acceptance.json` | `2884a375217b70860b7a4ad3a77158976a87684696c3035d0092a349fff8c717` |
| `run_v1/predictions_1.json`（两process字节一致） | `0c9839c746eb1881f596e284977f2125612cbb0308c7841492f2c42031875f5b` |
| `evaluation_v1.json` | `618b80c4af35700f11a471580bda74ec7d9a7095e632b77d4a48bfc769ca5638` |

预测文件字节SHA=`1b3238f3321bb8334bb854b71f36190bb74f0afd2dc954d0714678fb5065d88e`；评价文件字节SHA=`b0b650ca86043105cf3a49d7db99aa67a1bd861ea49a8bbf21d19b7fdc8b6446`。canonical回执hash与文件字节hash不混用。完整执行前审核/实际状态复用同根`execution_review_v1.json`，不建实验记录服务或环境变量。

### 11.2 经济主量与解释

下表单位均为**bp/10日重叠gross固定预算参考**，不是日收益、账户净值或成本后收益。原生价格为主，HAC实际session距/Bartlett lag9只描述，不是门禁。

| E21−C20 | 可估日数 | 新增避损 | 新增错过上涨 | J=避损−错过上涨 |
|---|---:|---:|---:|---:|
| 全期 | 132 | −0.6060276 | −0.0121038 | **−0.5939238** |
| Q2 | 60 | +0.0583474 | +0.0665167 | −0.0081693 |
| Q3 | 65 | −1.2196764 | −0.1091460 | −1.1105304 |
| Q4 | 7 | −0.6025023 | +0.2151126 | −0.8176149 |

全期J的95% HAC区间[−1.4017496,+0.2139020]bp；复权、相同冻结分组的J=−0.5816599bp，未扭转结论。新增盈利信息主要**降低避损能力**，并未靠足够的上涨捕获补偿；不是收益被保守门槛拦住。季度点估计均负只是描述，不新增季度全通过门；区间跨零不证明所有盈利信息永远无效，但原唯一J>0资源条件未达到，故按合同停止。

原生损失Rank IC：C20=0.2770878、E21=0.2748026、差=−0.0022852；MSE=0.0018225992→0.0018242474，MAE=0.0293108821→0.0293252611。固定新买队列99日IC=0.0757674→0.0776147，局部+0.0018473不能替代主J或触发新QE；2,053原队列行去跨seed重复后1,269个唯一stock-date，不把它们算独立seed实验/账户利润。

两臂对同日相同预算X：C20的J=+0.5485645bp、E21=−0.0274611bp，各仅130可估日且区间均宽跨零。不能用与主配对不同的日期数直接相减来冒充132日主J。2个正式成员端点NA保留；这些键在E21−C20差系数为0时依法代数抵消，因此主配对仍132日，绝非把缺价收益写0。10月末10个OUTCOME_NOT_MATURE日期的预测仍保留，最后成熟decision为2026-10-16；Q4只有7日，不夸大为独立样本外确认。

### 11.3 终态及后续边界

独立算术复核（同一作者，不冒充独立第三方）确认：评价receipt canonical hash、132日J与分解、季度加权均值、按真实session距离直接双重求和的HAC核、12个attempt及每日完整131/固定27行业预算均一致；未重新fit或为记录重建旧历史证据。

**终态STOP_NO_INCREMENTAL_VALUE_REFERENCE**。结束本netprofit_yoy/rank/252-session状态/原季度成员C目标/10D/Ridge精确组合，不追加fit、改符号、字段、TTL、horizon或容量，也不切换forecast/express。原C/S/W/N/M/VIX终态、资产及QE/荐股/LocalSIM消费者不变；不交新QE、不做AF冗余后继或生产采用。后继若有真正不同的信息/场景依据，再形成一次清晰的价值假设；不因已经具备reader就继续搜模型或把SOURCE/CI PASS当可用Alpha。

本轮仍为NON_VINTAGE财务/分类回溯研究，不能声称严格历史as-known、未消费holdout、因果增益或实际净收益。收益和风险的最终L2业务目标不变；来源闭合、工程交付与流程完成是已完成能力，不包装成已证实经济成果。两轮结果文档作者审修核对数值/单位/NA/历史与父蓝图一致；不改任何阈值/停止条件来制造成功。
