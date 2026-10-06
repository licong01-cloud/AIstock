# Advisory GP5-VOLUME-PATH-1：D量价路径与固定5交易日收益型买价 F2详细设计

2026-10-06设计、2026-10-07源码接续；SOURCE_VERIFIED_RESEARCH_NEGATIVE_NOT_CONFIRMED。设计#5623合入bb7c7514并自身清理。用户要求UI后置、先重启验收后新模型长任务。新候选源码、prepare及计划内一次四fit/完整四臂已完成，最新累计111研究fit+1旧index。本文不以源码或研究胜率承诺实际利润。

## Background / Goal

GP5-MINUTE相对19维matched有+8.4748bps/5TD cohort点增量，但相对原Top5−46.9091bps，未确认且该candidate结束，不重新拟合/调阈值救结果。其train八量全已知，test1617/1620行仅VWAP未知。新只读六D样本发现train的amount/volume≈分钟close，后期中位比仍约1但部分bar隐含价在OHLC外；这不是已证明的停牌/全行缺数或统一100倍单位错，根因不能由六样本扩称已闭合。

新假设：D日成交量沿价格路径的分布，是否能改善次日观察价买入后固定5交易日净价值与下行风险？只依赖相同D的OHLC与volume，新增三项稳定定义的量价统计，不消费amount货币坐标、不研发分钟买卖时点/执行算法、不重复QE Alpha。对照保留九日频与七个非VWAP分钟量，隔离三量新信息块；新版本不替换/改判旧模型或旧实验。

用户重启后identity/health通过，M1业务GET200 NOT_CONFIGURED；#5597保持OPEN且不能擅自配置/启用M1以闭环。这条辅线、UI、浏览器收据及自然交易日积累均不阻本离线主线。价格模型输出可以为空/未知，目标是净收益和下行风险，不预测开盘覆盖。

## Scope / Non-goals

设计阶段只本文和蓝图。设计多轮修订/F2通过并合入后从最新main新建独立源码树，精确九文件：

- backend/services/advisory_model_first/generic_volume_path_price_5td_contracts_v1.py
- backend/services/advisory_model_first/generic_volume_path_price_5td_source_v1.py
- backend/services/advisory_model_first/generic_volume_path_price_5td_model_v1.py
- backend/services/advisory_model_first/generic_volume_path_price_5td_pipeline_v1.py
- backend/tests/advisory_model_first/test_generic_volume_path_price_5td_source_v1.py
- backend/tests/advisory_model_first/test_generic_volume_path_price_5td_model_v1.py
- backend/tests/advisory_model_first/test_generic_volume_path_price_5td_pipeline_v1.py
- 本文与advisory_strategy_conditioned_model_blueprint_v1_20260710.md。

只读复用GP5已有prepared原KEY/标签/九字段、source纯validator、JSON export/predict、stage/registry。新原生分钟reader复用已有profile、D-only float32切片与校验模式，旧source/model/pipeline不改，不改变其implementation身份。不要建设通用平台或新公共registry/UI。

禁止修改QE/Selection/HMM/StrategyPackage/行业/基础数据/Execution/Paper/CI/公共workflow/AGENTS；禁止DB/DDL/DML、数据补齐/激活、重建名单/池、安装、sealed读取、模型/角色自动激活及用户服务/其它进程控制。临时全X，正式新root为F:/Dev/AIstock_model_artifacts/advisory_generic_volume_path_price_5td_v1_20261006。用户后端重启仍user-owned；本离线切片不需重启。

## Architecture / Contracts

### 1. 同一5TD业务目标与原人口

只读消费GP5 plan及prepared manifest/rows，验证同configuration、原完整decision_dates、KEY/rank/group及标签/政策身份。GENERIC_ENTRY_FIXED_5TD_V1，D输入冻结，T为下一原交易session、H=T+4收盘；停牌/未知不压缩或延长H，10/20与旧五有效review不混用。buy .95bps/sell5.95bps各一次，risk800bps及net>0判断不结果后变更。已有标签仅监督/结算，不能成为D feature；不重新生成GP5标签/日频数据或读旧收益结果。

原386D/7720候选（含正常未知、零候选日定义）不筛，包/stock_universe/single_index/index_union只是元数据，同D输入/价格数学不依赖父alpha、rank/腿或退出政策。不重审QE包，不以效果/MDE/native/父时钟阻使用；真身份矛盾/损坏是正常计算错误而不是新增资格门。

### 2. 新分钟来源

plan显式绑定当时活动profile路径、generation、controller minute_root及全部minute_pins。prepare解析当前profile，校验三个元文件完整hash及rule/snapshot/universe语义，来源实际路径与声明一致；后续stage只消费不可变prepared，不读取当前活动指针。只在原名单内按D的calendar positions seek float32，不解码T/H分钟，不硬编码旧provider或回退旧路径；若调用RD-Agent身份另显式传节点data_root_uri并要求complete=true，本Windowsreader不需调用。

一次同D读取原八字段，调用旧aggregate_d_minute_features_v1验证已知OHLC/limit/activity矛盾并计算七个非VWAP分钟量，再计算三项新量；同一批raw bars产生matched/candidate的分钟信息。旧close_to_vwap_bps/其状态可仅作source解释，不进入模型。继承原calendar覆盖分母/含09:30实际slot、午休相邻定义与80%OHLC可测定义；未观测不填、正常缺file/NaN/停牌保留原行。文件越界/坏头/截断/Inf/已知非法价量/变化报告计算错误。保存每D切片hash/size/mtime而非冒称整bin/vintage身份，源仍CURRENT_RELEASE_NON_VINTAGE_D_PRICE_VALUES。

每股文件一次open、按D合并seek，calendar positions一次计算，原始<=7720KEY/5000股/300slots/片<=500000值；2线程/RSS<=2GiB/30min测量预算。profile/meta读取前后一致与slice读取前后稳定不需要重建工作区或全量日复核。源是同一观测时刻的关联性数据，不宣称回到D原生捕获。

### 3. 三项新的D量价定义

原calendar OHLC有效率>=80%且OHLC与非负已知volume联合有效率>=80%才测量三量，否则三项UNKNOWN，仍保留九日频及其它可用字段。已知volume=0是已知零，不是缺失；活动全0没有可定义加权价格/份额，应UNKNOWN，不伪造0。

1. close_to_volume_weighted_bar_close_bps：在D联合已知且volume>0的原bar集合计算weighted_bar_close=sum(close*volume)/sum(volume)，并用原D最后slot已知close比较，10000*(D_last_close/weighted_bar_close−1)。这是成交量加权bar收盘价代理，明确不是逐笔成交VWAP；不读取amount、不自动反推复权/手股倍数。volume在D内乘统一正尺度及所有D价格乘统一正尺度不改变该量。
2. signed_volume_pressure：只用原calendar相邻恰一分钟、两bar OHLC有效且后bar volume已知的pair。sum(sign(log(close_t/close_t-1))*volume_t)/sum(volume_t)，午休及任何缺bar断点不桥接；正常平价且volume>0时为真实0，分母0/无pair为UNKNOWN，范围[-1,1]。
3. closing_30m_volume_share：D>=14:30的联合已知volume之和/D联合已知volume之和。尾窗口原slot任一OHLC/volume未知则该量UNKNOWN，不取更早/更晚窗口顶替；D少量其它缺bar允许部分观察但明确partial和原calendar分母。非正分母UNKNOWN，范围[0,1]。

返回original_calendar_slots/ohlc_coverage/volume_coverage/partial/reasons与逐字段UNKNOWN。二/三量不使用旧amount份额冒充volume，限价flag仍是原非VWAP七量之一；训练不能按三量完整筛原监督。

### 4. 模型与价集

matched=九日频+七非VWAP分钟值（16）+16missing+scenario_gap_bps/100，共33维。candidate新增三量及三missing，共39维。只增这一块，不逐特征筛选为多候选；两臂同成熟train标签、训练keys/价格支持、成本与固定GradientBoostingRegressor200/lr.05/depth3/min_leaf30/subsample1/seed20261006，两臂mean/path q.1各一头，共4物理fit/1candidate/0index。不用旧35维权重或回选33维控制作为事后候选。

先筛train/validation时钟再解析数值；median/missing/support只用train，test毒化不改变训练或其输入校验。19原始字段train-only median，train全未知编码0+missing=1不写回源。相同成熟监督、H<=train_end、100bps gap桶30观察/5D及2.5～97.5%域与旧GP5一致；正常股票字段不足为逐行UNKNOWN，不删原日期/包。validation只诊断，不调loss/阈值/seed/支持。

新独立fitted schema、recipe/JSON4头内容hash、逐头sklearn parity；不能冒充旧bundle。纯query_volume_path_nodes_v1返回KEY/metadata/order/逐字段未知、expected_net_bps/downside_q90与同5TD状态；volume_path_price_set_5td_v1保留同D_ANCHORED_CNY完整legal tick、多段/未知洞、全UNKNOWN/空合法集合。数学用mean_ratio及path_q10折算假设价、成本各一次；不预测开盘、最佳分钟/卖点或任意限价fill因果。正常跌8%/涨8%不是固定判胜，支持外未知，不无限外推低价更好。

### 5. 一次研究及业务收益归因

preregister→prepare→trained→evaluated四原子stage，新run身份/source SHA和研究registry独立lineage；STARTED/逐头physical journal，partial不隐式retry，完成stage可读回但不重复fit。fresh QE single/custom_evo/multi-alpha三公开running0才fit，拟合后读回；QE忙继续开发而非停止QE/提交其任务。>30min每半小时检查，无持续轮询。

首次只已消费开发窗EXPLORATORY_SCREEN/NAVIGATION_ONLY，不读sealed，不以这轮关闭全分钟方向或支持经济激活。四臂candidate/33维matched/original Top5/fixed±300bps rule，每D原Top5独立5槽5TD cohort、SKIP空槽、不补Top6，不将重叠cohort复利成NAV/MDD；81testD与正常unknown/未结算null保留，区间只有完整连续配对板才block5，不压缩未知洞。

输出两项关键增量candidate−matched（新量价信息）和candidate−base（最终业务）；TAKE胜率与盈亏幅度单独报告，不能替代成本后增量。已知模型干预数量/日、UNKNOWN-only动作差异、不可执行、未结算分别计数。新增对baseline拒买归因：在可结算、两臂已知动作的同原槽中，避免亏损=被AVOID且真实净收益<0的负收益绝对值，错过上涨=被AVOID且真实净收益>0的净收益；各D按五槽除5。unknown空槽影响另报、不归模型功劳；已知拒买净贡献必须等于避免亏损−错过上涨，包含在candidate−baseline配对差额中，不把空仓本身称成功。

点增量可导航；区间、单父人口/非vintage、真实未结算和配置/工程分别如实报。负只停自身、无旧研究补证/归档，下一候选须新信息或结构事前设计，不只换seed/loss/阈值救本candidate。软件完整离线交付不以研究盈利为提交门。

## Implementation Plan

本F2三视角自审修订/F2/当前HEAD CI合入→latestmain独立九文件源码→纯量价定义/原native reader→共享训练/纯价集→小原子pipeline及四臂/收益归因→多轮修复定向节点/稳定一次矩阵/Ruff/L0/F2→clean producer/事前新plan→一次prepare/fresh QE空闲一次4fit与完整评估→最新事实/当前CI合入与自己清理。不能为构建通用平台延期真实研究。

优先P8～P10约6～11h工作量，P11不同假设与P12必要消费者按结果推进；任务总12～18h只是估计，不强制运行或靠重复fit凑时长。所有tmp/cache/pytest在X，正式新root在F；不建source tmp→X junction使清理跨reparse，静态输出按官方既有最小路径或离线准备明确X配置。源码/研究/经济/运行态分开，UI不是验收前置。

## Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-721 | 同固定5TD/成本/原人口与新schema/lineage，不改旧包/标签/权重 |
| F-722 | 当前profile/pins、D-only seek、源稳定/路径/float错误，正常未知不删 |
| F-723 | 三量手算、尺度不变、午休/端点/零量/缺bar/原calendar分母 |
| F-724 | 33/39同监督、train-only median/support、未来毒化/跨包/指数解耦 |
| F-725 | 四固定真实fit、独立JSON parity及STARTED/partial不可隐式重复 |
| F-726 | 成本一次/全legal tick、多段unknown不桥、纯D价集非open/fill |
| F-727 | 四臂原日/槽、UNKNOWN/未结算null/非NAV及真实干预 |
| F-728 | 避免亏损−错过上涨=已知拒买贡献，unknown不归功，单位5TD |
| F-729 | 多轮自审/精确范围/currentCI交付、QE串行及生产NOOP/UI后置 |

## Design Acceptance Matrix

设计提交时只验收详细设计；当前精确九文件离线源码已实装，矩阵只验收该切片。真实一次研究如下，经济确认、日频运行和激活未发生。单元测试fixture拟合不是研究试验。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-721 | backend/services/advisory_model_first/generic_volume_path_price_5td_contracts_v1.py | backend/tests/advisory_model_first/test_generic_volume_path_price_5td_pipeline_v1.py | SOURCE_VERIFIED | none |
| F-722 | backend/services/advisory_model_first/generic_volume_path_price_5td_source_v1.py | backend/tests/advisory_model_first/test_generic_volume_path_price_5td_source_v1.py | SOURCE_VERIFIED | none |
| F-723 | backend/services/advisory_model_first/generic_volume_path_price_5td_source_v1.py | backend/tests/advisory_model_first/test_generic_volume_path_price_5td_source_v1.py | SOURCE_VERIFIED | none |
| F-724 | backend/services/advisory_model_first/generic_volume_path_price_5td_model_v1.py | backend/tests/advisory_model_first/test_generic_volume_path_price_5td_model_v1.py | SOURCE_VERIFIED | none |
| F-725 | backend/services/advisory_model_first/generic_volume_path_price_5td_model_v1.py | backend/tests/advisory_model_first/test_generic_volume_path_price_5td_pipeline_v1.py | SOURCE_VERIFIED | none |
| F-726 | backend/services/advisory_model_first/generic_volume_path_price_5td_model_v1.py | backend/tests/advisory_model_first/test_generic_volume_path_price_5td_model_v1.py | SOURCE_VERIFIED | none |
| F-727 | backend/services/advisory_model_first/generic_volume_path_price_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_generic_volume_path_price_5td_pipeline_v1.py | SOURCE_VERIFIED | none |
| F-728 | backend/services/advisory_model_first/generic_volume_path_price_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_generic_volume_path_price_5td_pipeline_v1.py | SOURCE_VERIFIED | none |
| F-729 | backend/services/advisory_model_first/generic_volume_path_price_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_generic_volume_path_price_5td_pipeline_v1.py | SOURCE_VERIFIED | none |

最小测试复用同叶fixture：少量手工D数组覆盖三量/正尺度/午休/缺bar/零量，原native float32 header/truncated/未来毒化/路径变化；同33/39及medians/support/test Inf不影响train、metadata变化数值一致；四头JSON parity/旧schema拒绝、全tick未知洞/成本手算；空原日/未知结算/不补Top6/拒买收益代数/partial stage。无实现快照或重复fixture，不扩大QE/全modeling套件。

## Risks / Rollout / Rollback / Production Gates

该三量是bar代理而非真实订单流/逐笔VWAP；既存七量可能有来源阶段差异，matched同源吸收但不证明跨源经济泛化。部分观察/missing encoding不代表源已知；同一已消费窗不独立OOS，条件gap不是挂单因果，未知price不等于便宜可以买。所有限制直接输出，不新增资格/审批/数据准备平台。

本离线切片backend_restart_required=false；database/DDL/DML/dependency/profile/runtime/model activation/process control均NOOP。旧产物与包不动，回滚仅停止新离线调用。日频接入需要后续精确scope而非UI平台，用户决定任何重启/运行配置。本任务未经授权不设置AISTOCK_ADVISORY_SECTOR_PRICE_CONFIG。

DESIGN-COMPLIANCE-001：完整离线合同不得简化/假完成；正常未知与矛盾显式、不制造成功；首版五TD/价格/成本与最新UI后置要求一致，不改旧研究；不加QE门禁或让runtime/经济证明成为研发前置。当前设计有完整实装计划，不冒充实现或盈利。

## Review / 多轮自审

第一轮业务/研究：matched改为九日频+七非VWAP分钟量33维，candidate三量增至39，避免“有分钟 vs 无分钟”的旧混杂；所有三量是一个事前新信息块而非三轮后验搜索。

第二轮PIT/数学：声明VW-bar-close非真实VWAP、尺度相消/已知零不等未知；sign pressure排午休与缺bar、不把首bar量配未来方向；尾窗口原端点/未知定义及全calendar分母不随候选缺slot缩小。

第三轮收益/授权：拒买收益归因与unknown控制分开，完整日/槽/未结算null、重叠非NAV；旧107研究fit不重置。NOT_CONFIGURED语义验收未过只保留工程辅线，不擅自配置/改公共smoke、不阻本研究。上述为本窗口不同视角自审，不冒称独立外审。

源码第一轮17项定向PASS：同33/39维、共享监督、test Inf毒化不影响训练、完整D读取不解码T、正常未知保留行、原子stage/partial不能隐式重fit、成本与原Top5槽保持。第二轮复核补齐最多386原D预算、合法tick支持空洞与读取期间源变化的2项定向PASS；新增三量范围矛盾显式失败而非伪未知。第三轮核对拒买归因：相对原Top5的已知拒绝收益严格等于避免亏损−错过上涨，UNKNOWN空槽另项且完整组总贡献与配对差额逐组代数对账；不将未结算获利猜为零。旧四类GP5/GP5-MINUTE代码、权重与正式产物未改。本切片是完整离线新模型，不包含API/UI挂载；最终最小矩阵与生产门分别报告。

## 实际一次研究与结果驱动分流

clean producer a55f0a6270025b4ee91ef0b404f445a846e4477a；implementation SHA 60d1d984b01cae599f9c8041bf6ceb0de832275241adb77fb7a1c585ad9b81c8。新run advgp5volume_45dbd44d9afa27fa1889191c、plan SHA 45dbd44d9afa27fa1889191c31fc99f4fae7446c1df04afc3c76125df61dc1a0，正式root为本设计声明路径。prepared/trained/evaluated stage SHA分别e6db3aba8b27b631649667871f1646236d87ac977565070ac63c662f1c5b4075 / ee6e1c4ed0cd3a86c6bffde2a95ff2542e3d1428e3039ce3e9fdc4873b79ef76 / 69b56d41bd22b2efe80a80f637d1be22ec25ed88018a42b4bad7248ddf809b8c；父GP5 prepared SHA未变。四head journal恰4项，累计107→111研究fit+1历史index。

prepare11.422秒、原386D/7720KEY保留，7660完整OHLC/60partial，新三量及十分钟特征全部7720行已知（测量规则内的partial仍显式保留）；不表示完整逐笔/VWAP或历史vintage。训练同3684成熟行/193D、validation1586仅诊断，train全4380行分钟量已知。2026-10-06 16:15:21UTC fresh QE single/custom_evo/multi-alpha三0后一次四fit，TRAINED9.953秒、至四臂EVALUATED10.531秒；16:16:54UTC三0读回。0QE提交/DB写/未来分钟解码/Selection重建。

80共同完整5TD cohort（四臂使用同一分母；matched单独还有1个已拒买零收益日，不与其它臂的80日均值混比）：

| arm | 平均净收益bps/5TD cohort | 已结算TAKE | TAKE胜率 | known avoid / UNKNOWN / 不可执行 / 未结算 |
|---|---:|---:|---:|---|
| 原Top5 baseline | 129.0145 | 403 | 58.3127% | 0 / 0 / 1 / 1 |
| 固定±300bps rule | 131.5724 | 397 | 58.4383% | 6 / 0 / 1 / 1 |
| 33维matched | 85.3567 | 265 | 56.6038% | 127 / 12 / 1 / 0 |
| 39维candidate | 81.4711 | 256 | 56.2500% | 135 / 12 / 1 / 1 |

candidate−baseline/matched为−47.5435/−3.8856bps每5TD cohort。相对base135项已知干预/54D及3D仅UNKNOWN差异；相对matched26项已知干预/22D、UNKNOWN-only差异0。已知拒买平均避免亏损39.8462bps、错过上涨89.4341bps，净贡献−49.5879bps；UNKNOWN空槽贡献+2.0444bps不归模型，两者严格对账至−47.5435bps。TAKE统计使用各自全81D已结算事件，不能替代同80组配对净收益。81testD/1620候选保留，1配对组未结算null，block5区间null、非投资NAV/MDD，EXPLORATORY_SCREEN/NAVIGATION_ONLY，不支持经济确认或激活。

结果后第四视角复核：输入十量可测量并没有解决误拒盈利股票，不能把去除VWAP缺失称为收益提升；本candidate结束，不换阈值/seed/loss补救。下一主线事前设计联合收益/风险分布模型，在同固定5TD合同内研究结构差异，旧模型与成果不覆盖、UI不阻研发。最终19项小矩阵/Ruff、九文件L0 0blocking（唯一P2是已审的<=7720 unique KEY one-to-one左join）、F2九项0warnings均PASS；结果追加后不再拟合或重复跑旧研究。当前源码待当前HEAD CI交付，backend/DB/profile/dependency/activation/process均NOOP。
