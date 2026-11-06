# Advisory 资金流—日内价格协动 Top10 / 固定5TD F1 v1

2026-10-22。唯一假设 `FLOW_PRICE_COUPLING_5D_TOP10_5TD_1`。F1单模块离线研究。设计#6590、源码直接验证与真实六fit已完成，INNER_SUPPORTED仅支持未来确认设计，0经济确认/生产角色；完整执行证据见Execution Result。

## Background / 目标与修正

用户在方向复核后明确要求按调整建议执行。上轮资金流—价格响应只完成零fit提案筛选，未定义公式。本次将同一未成形问题明确为唯一预测表示，保留其自适应研究血缘；不伪称独立发现，不改旧筛选收据，也不恢复任何已完成研究的训练。

原两包Top50→Top10，T开盘至T+4收盘成本后相对原生Top10为产品主终点。可检验问题：控制资金流强度/持续性及价格水平和波动后，D以前五日的有符号主动成交金额失衡与日内收益的同日协动，是否提供额外排序价值？

同样的流量强度和价格边际分布，可以有不同的同日配对关系。这是可观察的条件状态，可能刻画不同的供需/价格响应环境；持续性是否延伸到T开盘后由未来固定目标检验。本轮不要求事前排除吸收、反向供给或信息交易等解释，也不预测固定符号。它不是机构身份、盘口OFI、因果冲击或已经可盈利的信号。

## 唯一测量与匹配控制

窗口为原正式交易日历中截至D的紧邻五session，缺日不压缩。选择五日是一个固定交易周的短期联合状态尺度，与已有M6/M17控制共窗，未查看数值/收益后挑选；不测试其它窗口。

每日B=大单和特大单买入金额，S=大单和特大单卖出金额，A=八项买卖金额和；I=(B−S)/(B+S)，r=10000×[log(close)−log(open)]。金额按canonical CNY原样消费，同一日的复权因子在开收比中相消；不跨日混价，不做第二次万元转换。

matched包含原B八字段，并同时加入：
D的I、D的(B+S)/A、五日Σ(B−S)/Σ(B+S)、I的五日均值/总体标准差、I>0占比、从D向前连续I>0的日数/5、四步abs(diff(I))均值，以及r的D值/五日均值/总体标准差。共19个数值字段，训练编码的缺失指示另计。

candidate仅新增 **q5=mean[(I−mean(I))×(r−mean(r))]**，共20个字段。q5单位bps。不是除以流量/收益方差的冲击斜率；恒流或恒价时q5=0为合法可测，不因为零方差拒绝。

正常缺日/NULL/NaN或任一日B+S=0令两臂共同UNKNOWN，保留原行和排名槽；不填零/前填、不靠标签决定支持。观察到负金额、非正价格、bool/Inf/非数、重复身份等是合同错误。稳定缩放避免求和溢出，不clip结果掩盖坏数。

## 与既有工作区别及限制

复用前轮八设计与七份父源码查重，不再次重审50份。M6/M17的强度和有序持续性已经放入共同控制；M19和GP5资金流使用价格状态与资金汇总，未保留本式五日流价配对矩。GP5日量lag和D1用总成交量及收益顺序，本式使用主动买卖金额桶的有符号失衡且同日配对；E2/同群lag没有本输入。它不因此成为全项目从未见过的信息，父LSTM/TCN可能已部分编码该关系；原生Top10比较专门检验最终是否仍有产品增量。

预定合成对照：五日I依次为(.1,.2,.4,.3,.5)或(.1,.3,.2,.4,.5)，两者各日大单gross相等、总side amount相等，价格路径固定，r=(-200,-100,100,200,0)bps。上述全部11共同控制相同，而q5分别为12和10bps。证明仅针对选定摘要的非等价，不证明父模型独立、真实输入完整、可学习或盈利。

## 输入检查：只读六D，不读取标签

读取边界和机器定义见同目录input_spec.json，必须在首次市场数值读取前落盘。六D、600原包行沿旧计划；仅moneyflow八金额与daily_pv的open/close，五日窗口、对应原股票。来源固定父binding，不解析当前active。预算2M源数值单元/180秒/2GiB；不借CGO端点覆盖作本检查PASS。文件pin、尺寸和读前后stamp及实际消费片段hash分别记录；未重新核算大H5全文件hash则明示。

输出完整原行、可算/UNKNOWN原因、零分母、恒流/恒价、q5分布、逐日期/包支持。没有依据收益选择覆盖阈值、符号或剔除股票；全UNKNOWN或q5全零时不进入本测量的训练。六D可产只允许准备F1，不能冒称305D完整可学。

## Research / 唯一研究合同

必要输入检查已通过；按本F1实施唯一离线研究。沿原三inner折：训练截止2025-03-31/05-30/07-31，对应04–05/06–07/08–09预测窗。Ridge(alpha=100, solver=lsqr, tol=1e−6, max_iter=2000, fit_intercept=False)，不搜索模型/窗口/符号。共同成熟训练集合、train-only编码、同日同包中心化及等组权重；原排名破同分、UNKNOWN保槽。价格/成本沿原唯一估值，买.95bps、卖5.95bps只计一次。

只允许三折×两臂=6研究fit；五日确定性矩计算另报，不算监督fit。每折两臂预测冻结后才读该折成熟评价标签；不读旧outer/sealed，不做final。2CPU无GPU、RSS<=2GiB、总计算<=1800秒、输出<=200MiB；拟合前只读QE公开状态及实际资源，允许足够资源时并行，不控制QE或服务。

inner沿最新产品规则：对原生净均值>0、三折可评价且至少两折>0、共同成熟D覆盖>=80%、原生干预>=20D且>=20%成熟D并跨两折，尾部恶化不超过100bps、额外未知槽<=2百分点才SUPPORTED；均值<=0或超风险为NOT_SUPPORTED，其余INCONCLUSIVE。区间跨零本身不新增inner否决；matched及系数仅解释。报告完整每包/每折/UNKNOWN/干预/描述性区间和MDE，不选择最好子组。SUPPORTED仅支持后续确认设计，不自动加fit或激活。

## Verification Plan / 验证及退出

当前检查器验证合成非等价、统一金额单位缩放不变、同日价格共同比例缩放不变、正常缺失、零分母、常数、坏数拒绝及D后的源毒化不进入投影。正式实现另验证训练成熟时钟/完整人口/共同支持/预测冻结/六fit限制/无隐式重fit及sealed拒读。

全inner准备或研究不足时报告真实原因，结束精确候选，不自动替换窗口/字段。本轮不扩大QE/HMM/LocalSIM/Selection/StrategyPackage/数据发布ownership，不恢复公告、流水线或启动脚本任务，不进行DB/DDL/依赖安装/服务控制。

## Scope / 精确范围与实现接口

设计从origin/main `380456ddc31a31442d93e4f6c257af28b32f847b`的独立树 `F:/Dev/AIstock_worktrees/advisory-flow-price-coupling-design-20261022`交付，本PR只写本文。设计合入后再从最新main建独立源码树，精确六文件：

- backend/services/advisory_model_first/flow_price_coupling_top10_5td_inputs_v1.py
- backend/services/advisory_model_first/flow_price_coupling_top10_5td_v1.py
- backend/services/advisory_model_first/flow_price_coupling_top10_5td_pipeline_v1.py
- backend/tests/advisory_model_first/test_flow_price_coupling_top10_5td_v1.py
- 本文
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

inputs叶负责上述唯一纯测量、按D投影、H5日期/列/成员边界和消费收据；core叶复用已有原人口校验、成熟金融验证、等组权重/训练编码与原生配对逻辑，只绑定19/20字段；pipeline叶复用immutable stage及claim范式，新身份与新目录，明确只注册六个fold fit。

精确复用：`frozen_dataset.py::inside/stamp`和H5只读投影模式；`multiperiod_top10_5td_inputs_v1.py::INPUT_FEATURES/IDENTITY/file_dataset`；`peer_residual_top10_5td_v1.py::validate_population/group_center/panel`；`economic_entry_pipeline.py::read_stage/publish_stage`；`peer_lead_lag_top10_5td_v1.py::summarize`的原生产品内层分类。复制训练/预测模式时绑定本合同，不修改旧模块全局变量、不运行旧run。

finance显式拒绝end或cutoff超过2025-09-30、任何outer参数；只从父prepared/train_finance.parquet先投影KEY/label_information_end，再按label_information_end<=cutoff解码数值。新runner无final/outer路径；旧shared.Study里的八fit/global模型窗口不能继承为本研究权限。原研究旧evaluated stage仅做byte/hash身份核定，不解码旧评价金融值。

完整inner从2024-07-04至2025-09-30的原30,500行准备，source窗口向前四session；数值读取上限20M单元（与六D筛选2M单元上限分开）、源只moneyflow八金额/open/close。按范围/成员分块，每个D仍只取自己的五session；所有实际解码行计读取预算，即使随后过滤也不漏记。原input列投影仅身份、原八字段、parent_score/source_id；没有金融字段。临时全X，不复制模型/大源文件。

## Feasibility / 已执行输入检查

事前spec SHA256 `9c7b37cdf6ceeae20c15d662239004b19db1dea8210120a4dda2ba06b0681f27`；
收据 `X:/AIstock_temp/ADVISORY/p4-flow-price-coupling-20261022/screen_receipt.json`，
SHA256 `fb93b70dac833b2d19d90861d8aa7add644da82f746b2a9697cba231edf6cf8b`。

600原包行/530唯一股票日全部保留，579包行/512唯一股票日可算；16包行零大单金额分母、5包行必需来源缺失，均UNKNOWN。各D可算97/96/95/95/96/100。全部可算q5非零，不按值大小/符号选择股票或修改公式。26,410源数值单元，36.950958秒，峰值RSS542,810,112bytes；0标签数组/SQL/参数拟合/监督fit。530个确定性五日窗口与监督fit分报。

源码/合成检查证明共同控制一致而q5=12/10bps；单位缩放、同日价格共同比例、正常缺失/零分母/常数/坏数及未来源投影通过。这不是全inner准备、训练成熟支持、父模型独立或收益证明。冻结moneyflow单位收据指向同一文件SHA，原API转换不再次执行；H5无历史发布/known_from证据，NON_VINTAGE保持。

## Non-goals / 模块边界

无API/UI/生产bundle/价格区间/Exit/资金权重/订单执行交付。QE上游训练与因子选择、Selection、StrategyPackage、HMM、LocalSIM及数据发布均不改；无新父包准入。公告仍暂缓，旧CGO/资金流提案筛选不伪称模型实验，旧E2/先导lag等结论与fit不重判/重跑。不写DB/DDL/DML、装依赖、控制服务或恢复流水线/启动脚本任务。

## Implementation Plan / 实施方案

1. 设计两轮同窗口自审、F1 validator及diff检查；设计PR必需CI成功后按既有授权合入、官方清理。
2. 最新main独立源码树登记上述六文件，输入层严格复用事前定义；完整input contract、同成熟监督、预测冻结、六fit及无outer约束直接测试。
3. 首轮审时钟/人口/数学，第二轮审单位/支持/恢复/预算；稳定后一次定向矩阵、changed Ruff/L0/ownership与必需CI，不把自审称独立外审。
4. 清洁源码commit绑定新plan、已读父stage身份和来源；拟合前只读资源判断，单次六fit。中断已claim未发布不得自动重fit，成功恢复只能读取已冻结模型/预测。
5. 三折完整报告，按事前产品合同结束，回写本设计/蓝图并完成源码PR/合入/自己的官方清理。即使INNER_SUPPORTED也只支持未来确认设计，本轮0final/outer/sealed/activation。

## Risks / 证据与失败模式

五日协方差可能噪声大，父序列模型可能已编码它；控制变多也可能稀释读出器，须看相对原生与matched的完整结果。不能以合成不同或六D可产推导有效。600包行有重叠股票，不是600独立观察；现有开发窗口多次消费，新增6fit也不恢复独立OOS。变量尺度与Ridge惩罚可能影响结果，固定训练IQR编码，不搜索变换/符号。

零分母或停牌缺口保留原股票和原槽；缺价使经济结果未知时不以现金0掩盖。只有合法不能入场按既有现金合同。任何实现/输入错误显式报错，不降级日线或填默认；技术修正保留attempt血缘，不根据收益挑重试。PIT身份、日期与单位错误是计算约束，机制系数符号和matched胜负不增加产品门禁。

## Production Gates / 运行边界

源码合入、离线研究、运行加载、经济确认分开。production_ddl_gate=noop，dependency=noop，activation=noop，backend_restart=noop。当前不需要用户重启，不启用模型或修改现有原生日频业务。

## Review / 设计自审记录

第一轮：为避免重开流量强度/持续性，已将M6/M17对应控制同窗纳入matched；固定单一协方差、无方差分母，常数为合法0。明确新输入是预测表示而非可识别因果冲击，未增加符号条件。
第二轮：将六D可产和完整inner/经济验证分开，固定六fit和2025-09-30金融边界，明确旧shared runner的outer/八fit不能沿用；生产与其它ownership均noop。四项DESIGN-COMPLIANCE-001：设计交付范围明确；错误/UNKNOWN不隐去；原生主终点/成本/人口不改；无额外审批或父包门禁。

## Execution Result / 单次研究已完成

设计PR #6590先合入；研究源码commit `b626786732b3d0c78d78d680dcdb6f251a06bf29`，唯一run `advflowpricetop10_64b5c7a379fcb9792ca07c9b`。本节的源码直接验证和研究状态覆盖前文设计时态；源码PR/合入读回另报，不把已研究等同生产已加载。

两个本窗口自审分别核对数学/单位/成熟时钟/原人口与恢复/资源/只读边界，不冒称独立外审。11直接测试，最终含19模块smoke/证据边界的30项矩阵全部通过；Ruff、ownership（4/4，无歧义）、两个L0及F1六项索引通过。L0两项P2复杂度提醒保留：输入按D最多5×100成员投影、30,500行上限，训练KEY一对一连接禁止行膨胀。WSL既有环境缺pytest，直接测试使用既有Windows环境，未安装依赖；真实研究仍在冻结WSL解释器运行。

拟合前公开QE running接口ok=true/total=0/has_more=false；WSL 28核、1分钟load1.11、可用59,667,664,896bytes，Windows观测可用10,499,817,472bytes；实际限制2线程/无GPU，无服务控制。

完整输入305D/30,500原行/28,119唯一股票日，30,044共同可算、369零分母UNKNOWN、87必需来源缺失UNKNOWN；所有行/槽保留。消费6,608,794源数值单元，prepare21.616235秒，0参数拟合；原六D的600行全部12测量值与事前筛选逐值一致（含NaN）。大H5仍仅既有pin/尺寸/stamp及消费片段hash，并未重新计算全文件hash。三折两臂共同成熟训练行17,038/20,975/25,223（174/214/257D），均为19/20字段。

| 事前完整评价项 | 实际结果 |
|---|---|
| 原人口/成熟/共同可估值 | 126D、252包D、12,600原行；111成熟D、100共同D；90.0901%共同成熟覆盖 |
| 原生 / matched / candidate固定5TD Top10净组均值 | 105.045200 / 115.519671 / 109.901749 bps |
| candidate−原生主终点 | +4.856549 bps；描述性95%区间[-26.234364,33.171700]；MDE80 42.286734 bps |
| 三折原生增量 | +1.962842 / −1.279703 / +12.820406 bps；两折正 |
| candidate−matched解释项 | −5.617921 bps；95%[-16.636180,4.409546]；三折−20.239689/+3.243594/−5.244635 |
| 两包原生增量 | LSTM +54.011907；TCN −44.298809 bps；不据此择包/回选 |
| 原生/matched干预 | 111/111成熟D、109/111成熟D；原生跨三折 |
| 尾部/UNKNOWN | 尾差+27.592235 bps；原生326、candidate333未知槽；额外0.315315百分点 |
| 协方差系数 | −13.323372/−12.553279/−12.510108；编码尺度系数，仅描述，不作因果或固定符号证明 |

事前原生产品合同给出 **INNER_SUPPORTED**，`supports_future_confirmation_design=true`，但`proceed_outer=false`。跨零区间、低于MDE的点估计、两包强烈异向及matched负增量均完整保留：它没有证明盈利，也没有证明新增协方差的独立价值。组均值不是账户NAV/实际成交；既消费开发窗口保持NAVIGATION_ONLY/NON_VINTAGE，不是独立OOS。不得事后升格matched、只选LSTM、换符号或重判E2。

真实恰好6fit，累计213实际fit＋1旧index；28.210226秒、峰值730,648,576bytes，结束前输出约5.35MB，未超预算。10个immutable stage链及六个started/finished身份通过；无隐式重fit。0final/outer/sealed、0SQL/数据激活/确认/生产角色/服务控制。本轮结束，下一步仅评审是否值得为同一冻结候选做确认设计，另行界定未消费数据、经济目标和资源；不自动执行确认或再筛新候选。

工件根 `X:/AIstock_temp/ADVISORY/p4-flow-price-coupling-20261022/`：
- `validation-receipt.json`、`resource-preflight.json`、`audit-receipt.json`；
- `study/advflowpricetop10_64b5c7a379fcb9792ca07c9b/evaluated/result.json`，SHA256 `318beaca003cc9d4b229eca09a6575ba7a091d685995a7b3a8ed3fa243240981`；
- `inner/prepared/receipt.json` SHA256 `55831c2f06a693b3d65c4b6a360d4d897b696096fe135c43aa1071728fe77c7d`；terminal stage `b30e4312275ba0ab75111bb44891a079d09b6249d894b71d8b68d2b608d56f81`。

## Design Acceptance Index

- F-461：唯一五日流价协方差及19/20匹配控制，常数/缺失/单位/未来边界。
- F-462：冻结父来源、精确原人口、日期投影、消费hash和资源预算。
- F-463：共同成熟训练、train-only编码、原slot/破同分及固定Ridge。
- F-464：两臂预测先冻结、三fold六claim、无隐式重fit及无outer/sealed路径。
- F-465：原生主终点、完整未知/成本/风险/干预/分折及机制解释分离。
- F-466：六文件交付、自审/直接验证、研究和生产状态分报。

## Design Acceptance Matrix

本表验收精确源码与单次离线研究；SOURCE_VERIFIED不代表PR已合入、生产已加载或经济已确认。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-461 | backend/services/advisory_model_first/flow_price_coupling_top10_5td_inputs_v1.py::measure/attach | artifact: X:/AIstock_temp/ADVISORY/p4-flow-price-coupling-20261022/validation-receipt.json | SOURCE_VERIFIED | none |
| F-462 | backend/services/advisory_model_first/flow_price_coupling_top10_5td_inputs_v1.py::sources/read_panel | artifact: X:/AIstock_temp/ADVISORY/p4-flow-price-coupling-20261022/audit-receipt.json | SOURCE_VERIFIED | none |
| F-463 | backend/services/advisory_model_first/flow_price_coupling_top10_5td_v1.py::training/fit/predict | artifact: X:/AIstock_temp/ADVISORY/p4-flow-price-coupling-20261022/validation-receipt.json | SOURCE_VERIFIED | none |
| F-464 | backend/services/advisory_model_first/flow_price_coupling_top10_5td_pipeline_v1.py::finance/claim/fit_pair/run | artifact: X:/AIstock_temp/ADVISORY/p4-flow-price-coupling-20261022/audit-receipt.json | SOURCE_VERIFIED | none |
| F-465 | backend/services/advisory_model_first/flow_price_coupling_top10_5td_v1.py::summarize及Execution Result | artifact: X:/AIstock_temp/ADVISORY/p4-flow-price-coupling-20261022/validation-receipt.json | SOURCE_VERIFIED | none |
| F-466 | Execution Result/Production Gates | artifact: X:/AIstock_temp/ADVISORY/p4-flow-price-coupling-20261022/validation-receipt.json | SOURCE_VERIFIED | none |
