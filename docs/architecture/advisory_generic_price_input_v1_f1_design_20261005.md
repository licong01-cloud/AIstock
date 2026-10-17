# Advisory 策略包无关日频价格输入 v1 F1详细设计

2026-10-05；SOURCE_VERIFIED。完整切片仅为可复用的纯输入计算，不是通用模型、价格建议、API/UI或收益验收。

## Background / Goal

M10源码#5459当前HEAD696d19bb9/CI37243602139 SUCCESS后合入7aed83cc64ca35d7562659d2680e3114efcc928f并自身官方清理；R2实际39研究fit+1index，未新增经济确认/启用。M1工程#5445 current d1ccaae19/CI37240066029 SUCCESS，仍待六UI及BUG-1726公共业务smoke，不重复验证或越界修改。

当前economic_daily_feature_core_v1要求lstm/fund角色、两腿权重、combined_score及有符号leg_norm_score_gap；M1权重还绑定原Program、包与VALUE_REVIEW_5_V1标签。仅把前三个特征删掉，不会使原权重或标签策略包无关。本切片解除未来共享价格模型的必需输入依赖：任意合法原名单都能获得同一股票/市场D特征，模型参数跨包适用性仍需以后独立研究。

截至本切片2026-10-05交付时，固定5个交易日独立价值目标与原策略复评退出适配的产品选择尚未得到答复；2026-10-06用户已明确首版固定5交易日，10/20日以后扩展，后续实施以[GP5 F2](advisory_generic_price_5td_v1_f2_design_20261006.md)为准。本已交付输入切片不生成label、不修改VALUE_REVIEW_5_V1五有效复评，不把现有模型改绑新schema。它是下一通用模型的一项明确输入功能，不建设通用数据/缓存/审批平台。

## Scope / Non-goals

设计树来自最新origin/main 7aed83cc6，仅写本文及advisory_strategy_conditioned_model_blueprint_v1_20260710.md。设计多轮审核/F1校验/当前HEAD必需CI合入后，从最新main独立源码树事前登记四个精确文件：

- backend/services/advisory_model_first/generic_daily_price_input_v1.py
- backend/tests/advisory_model_first/test_generic_daily_price_input_v1.py
- 本文
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

不修改现有daily core、M1～M10计算/reader/config、旧标签/退出/成本、数据库适配器、路由/UI或任何QE/Selection/HMM/StrategyPackage/行业/公共数据/Execution/Paper/CI/工作流/AGENTS代码。允许只读导入既存错误类型与canonical_json_sha256；不传伪造的legs/weights/rank以调用旧core。0fit/研究run/收益或sealed读取、0DB查询写入/DDL、无配置/模型/profile激活、依赖安装或进程控制；X临时/F已存在正式产物保持不变。后端重启归用户，但本纯离线切片没有需要激活的接口或后台任务。

## Design / Architecture / Contracts

新入口build_generic_daily_price_input_v1接收完整原候选DataFrame、21个严格递增原交易session（前20截至D，末尾为紧邻T）、调用方已经按D可见锚调整的OHLC/原始股数volume面板、20个session的固定沪深300收盘序列、D市场宽度声明、来源说明。无I/O、无系统时钟、无model/label参数。predict_batch所需调用方式为逐D使用同一纯函数；本切片不另实现批量来源或调度。

精确接口为build_generic_daily_price_input_v1(*, candidates, calendar, panel, benchmark_daily, market_state, source_context)，返回(features, receipt)。benchmark_daily列为trade_date、instrument、close；market_state键为trade_date、market_up_ratio、market_definition_id、visible_through。source_context键为package_id、run_id、list_version_id、universe_identity、source_evidence、price_basis、volume_basis、source_visible_through、benchmark_visible_through；身份/证据为非空字符串或显式null，universe_identity为不超过64KiB的有限JSON身份对象/字符串或null，不包含完整成员表。calendar为date；DataFrame日期可为规范ISO date或naive午夜Timestamp；可见时钟可为date/规范ISO date或null，不接受含时区/盘中时间。来源独立未知，不进行资格查询。

原候选列固定decision_as_of_trade_date、target_trade_date、instrument、selection_effective_rank、candidate_group_size；0～50只，证券唯一、rank严格1..N、group_size=N、D/T与calendar一致。保留全部原KEY、顺序及rank元数据；它们不进入模型九字段。空名单返回NO_CANDIDATES和明确空schema，不用isfinite检查object空表；仍校验输入结构/时钟，不吞冲突。来源说明包含package_id、run_id、list_version_id、universe_identity、source_evidence、price_basis、volume_basis和source_visible_through；未知身份可以显式null并保留，不查包资格、native receipt、训练时钟、收益或数据库。

universe_identity可描述stock_universe、单指数或index_union等上游已交付身份；本函数不重建成员、调用Selection或验证/改写QE股票池。相同股票、D行情与市场定义在不同包/排序中的九特征数值应相同；元数据及完整输入hash可以不同。这是算法输入可移植，不是声称全部包/全部指数池共享有效权重。

OHLC/volume面板字段固定trade_date、instrument、open、high、low、close、volume：价格D_ADJUSTED_CNY，volume RAW_SHARES。最多50×20=1000唯一股日，真实键属于原候选与D历史20session；基准仅20唯一日期，固定000300.SH。拒绝未来T报价、外来股票、重复/别名日期、timezone/non-midnight、bool/数字字符串/已知inf、非正已知价格、负volume、OHLC矛盾；普通NULL/NaN、缺真实bar或来源时钟未知为正常UNKNOWN，原候选不删除。不补零/前填/压缩session；无行情的停牌不导致名单整体失败；真实零volume合法，但全20日零分母的ratio为UNKNOWN。与旧suspension-normalized特征不是默认parity，本文明确新schema只消费真实报价。

source_visible_through仅表示股票价格/volume来源及D复权锚的消费时钟，已知时须<=D且所有被消费bar日期<=该时钟；晚于D或bar比其来源声明更晚均是计算矛盾。未知时保留六个股票字段UNKNOWN_SOURCE_CLOCK、相对收益因股票ret_5未知而UNKNOWN，不制造D已知声明。基准/市场有自己的时钟，仍可保留各自已知值；不以某来源未知阻断其它独立字段。来源current DB NON_VINTAGE等证据等级原样保留，不伪造原生capture、不要求补native来使用数据。数学结果使用D历史，不证明历史revision曾于当时已知；源声明不将输入坐标升级为已验证历史复权版本。

市场输入为D、market_up_ratio及market_definition_id、visible_through；ratio已知在[0,1]，已知D值的可见日期必须是D（不能使用D-1声明冒充D宽度），分母定义须非空声明。缺值/时钟/定义时只令market_up_ratio UNKNOWN，不把缺失变为0或去掉原股票；不假设指数池宽度与全市场宽度同分母。未来市场声明/坏值/错误D报计算错误。基准来源visible_through独立声明，缺少或未知只令基准/相对收益字段UNKNOWN；未来声明或消费比其声明更晚的bar报错。

## Fixed information / 九字段及无标签语义

| 字段 | 固定公式与完整窗口 | 正常UNKNOWN |
|---|---|---|
| ret_1 | C_D/C_(D-1)-1，两个原session收盘 | 任一真实收盘缺失 |
| ret_5 | C_D/C_(D-5)-1，末6个原session收盘全在 | 中间缺失也不跨过 |
| ret_10 | C_D/C_(D-10)-1，末11个原session收盘全在 | 同上 |
| atr14_close | 最近14个TR的均值/C_D；TR=max(H-L,abs(H-C_prev),abs(L-C_prev))，15个原session收盘及末14个H/L；首个前收盘日H/L不消费，不采用指数平滑 | 所需前收/高低/收盘缺失 |
| csi300_ret_5 | I_D/I_(D-5)-1，末6个原session收盘全在 | 基准任一缺失或可见时钟未知 |
| relative_ret_5_vs_csi300 | ret_5-csi300_ret_5 | 任一未知 |
| close_location_in_day | (C_D-L_D)/(H_D-L_D) | 当日价格缺失或H=L |
| volume_ratio_5_to_20 | mean(V末5)/mean(V末20)，20真实volume完整；先按最大V缩放以避免溢出 | volume缺失或全零分母 |
| market_up_ratio | 使用声明的D宽度，不重新计算/替换分母 | 市场值、定义或时钟未知 |

所有派生已知值有限；有效输入溢出等计算矛盾报错，不clip/winsorize。日期单位为原session，收益为fraction，ATR和位置/volume ratio无量纲。parent_combined_score、parent_rank_pct、leg_norm_score_gap、strategy/package ID、原rank、T价与Y均不进九字段。查询买入价格p/g属于以后价格模型，不是本输入函数输出；无开盘预测、买卖点、最佳时刻或收益承诺。

输出为完整原候选元数据+九字段及逐字段原因；receipt记录schema/公式semantics hash、完整roster/context/source hash、纯特征hash、D/T、候选数、真实known/UNKNOWN计数、source_evidence=调用方声明、computation_only=true、outcomes_read=false、fit_count=0。NaN序列化为null，规范hash不接受非有限JSON。来源hash与特征hash分开，不将不同包metadata当预测信息；不称receipt是新native证据。

schema/semantics hash必须固定公式、价格/volume单位、窗口和UNKNOWN规则；只有来源/context可变，不能修改semantics后仍冒用v1。按(KEY,九字段)生成特征内容hash；行序保留原名单，所以换排序时hash可以变化，数学跨包一致性验收应按instrument/KEY对齐，不断言不同rank的完整receipt相等。输入DataFrame与来源字典均不修改。SOURCE_COMPUTED仅说明计算可用，不是模型READY、激活或收益通过。

## Implementation Plan

设计多轮自审及F1验收→设计合入/自己官方清理→最新main自己的独立树、四文件登记→纯计算/直接高价值测试→信息隔离、PIT/UNKNOWN、数值/边界三轮审核修复→一次稳定小矩阵/Ruff/F1/L0/diff/scope→当前HEAD必需CI后按授权合入/自身清理。M1六UI/公共smoke交付仍工程辅线，未获收据不冒充完成。

后续另立通用价格价值模型F2设计：显式预注册label_contract（独立固定持有期或原策略policy adapter），训练/价格集合/对照/预算独立，新lineage只验证新假设；用户目标口径明确前不启动该研究。新九字段切片不向39次fit总账增加试验，不重读旧负结果或sealed，不因纯schema通用而降低模型scope/效果标准。模型须明确所用字段/缺失策略，不能将纯输入全部九字段必须已知设为未经设计的总门；基准/市场正常UNKNOWN不阻断已知股票计算。

## Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-916 | 无score/leg/包特征输入，包和rank变化不改变相同股票的九字段；不冒称通用权重 |
| F-917 | 完整0～50原候选、真实KEY/rank/order和空schema，单日同核调用约定 |
| F-918 | 原20session/历史价格坐标/来源与基准市场时钟、未来/外来/重复矛盾拒绝 |
| F-919 | 九公式/完整窗口/有限数与成本无关；停牌缺bar、零分母逐字段UNKNOWN |
| F-920 | 独立来源/特征hash、未知身份/证据等级不伪native或新资格门 |
| F-921 | 原M1～M10/label/政策不改；label选择独立、0fit/收益/DB/激活/控制 |
| F-922 | 四文件精确范围、多轮审核、最小直接测试及CI，输入切片与完整产品分报 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-916 | generic_daily_price_input_v1.py / build_generic_daily_price_input_v1 | test: test_package_identity_and_original_rank_are_metadata_not_predictors；artifact: 原20候选改包声明数学不变 | SOURCE_VERIFIED | none |
| F-917 | generic_daily_price_input_v1.py / ROSTER | test: test_original_roster_budget_and_real_empty_float_schema；artifact: 原20个KEY/order保留 | SOURCE_VERIFIED | none |
| F-918 | generic_daily_price_input_v1.py / _frame / _day | test: backend/tests/advisory_model_first/test_generic_daily_price_input_v1.py::test_contradictions_fail_closed_without_mutating_original_inputs | SOURCE_VERIFIED | none |
| F-919 | generic_daily_price_input_v1.py / FEATURES | test: backend/tests/advisory_model_first/test_generic_daily_price_input_v1.py::test_nine_hand_computed_values_hashes_and_input_immutability；test_atr_only_consumes_previous_close_not_unused_first_high_low、normal missing/zero/clock节点 | SOURCE_VERIFIED | none |
| F-920 | generic_daily_price_input_v1.py / receipt | test: 跨包source/feature hash、有限JSON、来源不重验；artifact: 实际NV/native UNPROVEN及宽度UNKNOWN | SOURCE_VERIFIED | none |
| F-921 | §Scope / 实际四文件diff | artifact: 0fit/收益/DB/QE改动/配置或模型激活；旧core/标签/policy无diff | SOURCE_VERIFIED | none |
| F-922 | 本文/直接测试叶/蓝图 | artifact: 26直接项/Ruff/两L0 blocking0、真实20候选D输入及多轮自审 | SOURCE_VERIFIED | none |

直接测试采用一套小OHLCV fixture：手算九字段/ATR14、包与rank变动不改变数学、50/0/51边界、插入中间缺bar不压缩、来源或市场/基准时钟未知的字段级影响、停牌缺bar全候选保留、全零volume和H=L、重复/未来/外来/坏值矛盾、输入不可变及纯hash稳定；无完整实现快照/重复大fixture。现有M1源未改，不重复其43/71/20日业务或旧研究；广回归由当前HEAD必需CI承接。

## Risks / Rollout / Rollback / Production Gates

主要风险是把输入可移植误当权重/标签通用、不同宽度分母混用、当前DB历史revision误当原生PIT、停牌正常缺bar被删除或未来数据作为填补。全部由显式schema/证据声明/逐字段UNKNOWN和后续模型合同处理，不另建审批。新叶默认无人调用；回滚停止新的离线函数调用，不改旧family/数据/运行态。DB、DDL、依赖、runtime、模型和profile激活均NOOP。

## Review / DESIGN-COMPLIANCE-001

设计已明确可交付边界，不把输入切片当完整荐股；正常UNKNOWN和真实矛盾分别返回/报错，不假成功；原模型、标签、退出及公共业务语义不改；不添加QE资格/native/功效审批。后续实施必须逐项证明，而不是借DESIGN_VERIFIED声称SOURCE_VERIFIED。多轮审核记录在实际完成后追加。

本窗口三轮设计自审：信息/产品轮核对不要求score/leg、没有调用旧core造腿、旧权重与复评Y不能直接改名通用；PIT/UNKNOWN轮将原“价格时钟未知使九字段全部未知”修订为六股票字段及依赖相对收益未知、独立基准/市场仍保留已知，避免新增不必要的总门；数值/实施轮固定ATR14的真实session/均值、零volume与H=L未知、调整排序后按KEY比较数学而非误比整receipt，并补semantics版本及输入不可变。均为本窗口不同视角自审，不宣称独立外审；源码/业务尚未实施。

追加接口/时钟复审明确七个命名参数和各输入schema、有限JSON身份预算，并拒绝“声明截至D-1却包含D bar/宽度”的自相矛盾；普通来源未知仍按字段保留，不要求补数据或原生receipt。改动后重新F1/diff验证并绑定新HEAD CI，不借旧HEAD绿灯。

## Source verification / 实际实施验收

设计#5460 HEADbf7e9cb26/CI37244786687 SUCCESS后合入d78b2e053a9375484c18d972948b8a9744540769并自己官方清理。独立源码树base同merge，事前scope四文件。信息/产品、PIT/正常UNKNOWN、数值/交付三轮本窗口自审；首24项通过后修正ATR14不应要求首个前收盘日未消费的H/L、Decimal signaling NaN转换须typed计算错误，并显式source_identity_rechecked=false/qualification_rechecked=false。新问题节点定向15项通过，稳定后一次26项全部通过；changed-file Ruff、两L0入口均0finding/0blocking。非独立外审，无广测试替代当前HEAD CI。

只读既存冻结2024-08-01原20候选KEY+原rank，不读取score/leg/label/收益：D前20原session400真实OHLC/volume行、20基准行，8字段各20已知；宽度分母定义未证明，20个market_up_ratio保持UNKNOWN，不补定义/值或删除股票。重复调用仅改caller包声明，全部数学一致；源文件前后hash不变，总0.125秒。原feature_visible_through作为声明的D计算消费边界，不是新capture/known_from；RECOVERED_LIMITED_NON_VINTAGE/native UNPROVEN保留。0SQL/fit/T行情返回/sealed/DB写入/配置或模型激活。入口尚未被API/UI/既有family调用，不宣称通用价格建议或收益确认完成。

DESIGN-COMPLIANCE-001实际四项：本输入切片完整验证但完整荐股未完成；正常UNKNOWN/矛盾可见，无补零/删股/假成功；原M1～M10/label/退出语义不改；不添加QE/native/收益/确认审批。后端、DB、profile、训练与用户进程控制NOOP；只读frozen数据不是基础数据补齐或旧失败追加研究。
