# Advisory 通用九字段日频DB输入 F1详细设计

2026-10-08，SOURCE_VERIFIED（本源码提交前状态；当前CI与合入另报）、真实历史DB只读读回通过。BUG-1778 源码 #5717 / close-sync #5718 已合入，原公共分类阻断解除。本切片解除固定5TD消费者仅能由调用方准备D字段的功能缺口，不新训模型、不增加资格门。

## Background / Goal

固定5TD纯价集消费者#5641已合入a3a260e5/自身官方cleanup_done18.219秒，10直接/Ruff/L0/F1通过，但不含DB/API。下一必要切片只从数据库生成已交付generic_daily_price_input_v1九个D字段，使用同一纯计算函数，不为旧负模型收集收益或确认。兼容未来EOD和历史批量；不依赖等待实盘更新验证，不改旧M1五review/API。

## Scope / Non-goals

设计仅本文和蓝图；源码最新main独立四文件：backend/services/advisory_model_first/generic_daily_db_input_v1.py、backend/tests/advisory_model_first/test_generic_daily_db_input_v1.py、本文、advisory_strategy_conditioned_model_blueprint_v1_20260710.md。只读复用pure九字段builder、BoundedEntryReadSession/EntryWorkBudget；公共DB pool只调用，不修改。禁止QE/Selection/HMM/StrategyPackage/行业/公共数据/Execution/Paper/CI/workflow/AGENTS变更。0DDL/DML/激活/服务或其他进程控制/安装/训练/收益/Selection重新选股；临时全X，原F产物不改。不存在API/UI/调度/自动落盘；完整切片只是数据读取+特征，不冒称价集API或实盘/利润已完成。

## Architecture / Contracts

1. GenericDailyReadonlyDBInputV1.load_day(**packet)直接调用load_batch(packets=[packet])。packet包含显式decision_date/target_date、完整candidate DataFrame和package_id/run_id/list_version_id/universe_identity元数据。0～50完整原候选，KEY/rank/group沿用pure builder：同D/T、rank1..N/group=N、证券唯一且有序；未来、坏值、外来/重复/别名硬矛盾报AdvisoryModelFirstError。批最多20原D/T、唯一日期且无重复packet；空名单不伪造Selection，输出空schema/NO_CANDIDATES，0报价查询，不造native receipt。数据读取不重新定义股票池或剔除原股票，包/单指数/并集只是元数据。上游提供完整Selection候选人口，若只需published Top20则调用方投影结果，不改变输入人口或重新选股。
2. 一个READ ONLY/REPEATABLE READ、最多30秒共同预算；非空且已收盘的批次只执行三个数据SELECT：calendar、candidate_raw+adjustment、benchmark。按本次真实Asia/Shanghai时钟，未来D拒绝；当日15:00前保留原候选/全UNKNOWN，返回DEFERRED_D_NOT_CLOSED、0查询，不能把日内不完整bar当日频收盘。历史D随时可验证；15:00后仅表示日频时钟成立，eod_ingestion_completeness_attested=false，不伪造行情入库完整收据。会话已有SET LOCAL statement_timeout另报，不是数据写入。calendar每packet一次查询20个D及更早原session，并核验T是D后立即第一个交易日；读取T日历不读取T行情。所有packet校验后才连接；单日与批量复用同核，不每日重建工作区/weights。20packet×50×20最多20000请求股日，真实查询用请求KEY精确连接，不读跨度之间所有天/股票；取max+1检测超预算或重复，不能静默LIMIT截断伪完整。
3. market.kline_daily_raw的open_li/high_li/low_li/close_li除1000至CNY，volume_hand乘100至RAW_SHARES；按每原session的market.adj_factor及同股票D精确anchor factor得到D_ADJUSTED_CNY。不使用未来factor，不改数据或profile。factor缺失正常UNKNOWN_PRICE_COORDINATE，不能前填价格/压缩交易日；真实已知非正factor或价格/负volume是计算矛盾，不能当缺失吞掉。若D anchor缺失，价格未知但已知原volume仍独立保留。真实OHLC/volume缺bar按pure builder逐字段UNKNOWN，停牌不删除/阻断其它股票。
4. benchmark仅请求原calendar中的000300.SH index_daily.close，单位CNY、6原session收益规则复用pure builder。首版不计算市场宽度：market_up_ratio显式UNKNOWN、market_definition_id/visible_through为null，原因MARKET_DEFINITION_NOT_REQUESTED；不能使用当前指数池宽度/零值冒充训练市场定义，不读取全市场/行业/HMM。后续真正新增市场信息另立实验，与本数据消费兼容不同；普通可选缺失不阻九字段。
5. 输出与pure builder一致的features DataFrame和receipt，保留完整原KEY/order/rank/group。source_context新增price_basis/volume_basis、D消费时钟，source_evidence=CURRENT_DATABASE_NON_VINTAGE；时钟只说明本次查询截止，不能证明历史revision当时已知。原数据切片哈希、原roster、源表/单位、source_start/completed、calendar_verified及query_count独立记录，0outcomes/fit/native capture/qualification。三份只读DataFrame同snapshot取得、原输入不变；不从旧trained medians给输入补值，不消费T/Y。
6. raw/benchmark/calendar返回必须与精确请求KEY和有界日期对应，重复/额外股票或未来行fail closed；普通未返回bar/factor/指数是UNKNOWN，不为缺失请求数据窗口/数据回填。空批/全空候选无需DB；它不证明权威T日历，显式calendar_verified=false。非空缺权威20session或T错误是日历错误，不能合成日期。DB连接/查询失败显式报不可用，不能改用旧bin或伪NO_CANDIDATES。

## Implementation Plan

设计三视角/F1/diff先合入→独立四文件 source→三个有界SELECT/单位坐标转换/调用同pure builder→同日批量与UNKNOWN/PIT/只读三视角复审修复→一套小cursor fixture直接测试/Ruff/L0/F1/current CI→合入、自身清理。只读实际历史D做一次源功能校验，不读收益/旧负实验结果；数据库不可用只停真DB验收，不假mock完成。这不是新QE/模型实验，不重跑已结束联合分布或Exit研究，0新增研究fit。该CLI/library切片无需后端重启，后续API挂载单独任务，用户重启边界保留。

## Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-771 | 完整0～50原候选、元数据和0～20批；单日同核/空schema、不重新选池 |
| F-772 | 原20session/立即T、仅D报价和精确KEY、重复/未来/多出行拒绝 |
| F-773 | 精确D factor/单位手算、正常缺bar/factor不填价删股、独立volume |
| F-774 | 九公式/基准复用、市场宽度显式未知、不偷换分母或使用T/Y |
| F-775 | 同snapshot/三SELECT/30秒与row bounds、失败不fallback、输入不变 |
| F-776 | 源证据不伪vintage/native、0fit/DB写/外模块变更、多轮精准验证 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-771 | generic_daily_db_input_v1.py / same kernel | backend/tests/advisory_model_first/test_generic_daily_db_input_v1.py | SOURCE_VERIFIED | none |
| F-772 | generic_daily_db_input_v1.py / request keys | backend/tests/advisory_model_first/test_generic_daily_db_input_v1.py | SOURCE_VERIFIED | none |
| F-773 | generic_daily_db_input_v1.py / conversion | backend/tests/advisory_model_first/test_generic_daily_db_input_v1.py | SOURCE_VERIFIED | none |
| F-774 | generic_daily_price_input_v1.py / existing pure builder | backend/tests/advisory_model_first/test_generic_daily_db_input_v1.py | SOURCE_VERIFIED | none |
| F-775 | generic_daily_db_input_v1.py / readonly bounded read | backend/tests/advisory_model_first/test_generic_daily_db_input_v1.py | SOURCE_VERIFIED | none |
| F-776 | scope/receipt | artifact: docs/architecture/advisory_generic_daily_db_input_v1_f1_design_20261007.md §Review；git diff --check | SOURCE_VERIFIED | none |

此矩阵验收六项源码切片，不冒称API/模型/盈利已完成。稳定最终7项cursor测试通过，覆盖20原日历/立即T、批复用三SELECT、单位/factor手算、停牌缺bar与全缺D anchor下独立volume、未来/重复/外来/已知坏因子或原OHLC拒绝、空/未收盘0读、metadata与batch上限、预算关闭/no fallback。旧pure公式矩阵不在此复制；其整列UNKNOWN缺陷独立BUG-1778（28回归通过）处理。

真实功能读回：2026-02-02→2026-02-03原20候选，prepare只投影ROSTER五列，不解码Y/收益。三SELECT约15.219秒，8可计算字段各20已知、market_up_ratio全UNKNOWN，名单/order保留。随后一次实际批量：2026-01-06..02-02共20D/400原候选、8000精确请求股日，仍只有3数据SELECT，总7.141秒；calendar400行/0.000秒、raw8000行/5.781秒、benchmark39行/0.266秒，8字段各400已知。两次时间含缓存影响，不直接外推全窗。source_evidence=CURRENT_DATABASE_NON_VINTAGE；原投影不带package/run/list/pool身份，显式null，不用当前配置回填、不造原身份收据。临时检查脚本与摘要位于X:/AIstock_temp/advisory/full-delivery-20261006/check_daily_db_readonly.py、db-real-readonly-20261007.json、check_daily_db_batch_readonly.py、db-real-batch-readonly-20261007.json。0fit/outcomes/DB写/激活/服务控制；该读回不是旧负模型再评价。

## Risks / Rollout / Rollback / Production Gates

当前DB不是历史快照，不宣称原复权revision vintage或跨包经济泛化；factor未知保留，不用不存在的数据做预测。新读取并不自动更新模型、确认收益、改变股票池、资金或执行。原真实模型/旧源/API不动，回滚停止新reader调用即可；无数据库迁移、配置/数据/model激活、安装或后端进程操作。DESIGN-COMPLIANCE-001：实现六项才完整交付该数据切片，API/价格预测本体/经济阶段仍分报。

## Review / 多轮自审

业务轮：必要日频DB→九字段适配，不再增加UI/旧证据/ModelOps；model training和parent QE不存在依赖。PIT轮：精确D anchor和原20session、不使用T quote/未来factor；正常缺失独立保留，股票池只来源元数据、不将上市/停牌再设置父包准入。性能轮：三SELECT按请求KEY批量去重、同核单日/批，超限取+1后失败不截断，空人口不访问DB；复用既存只读session/纯公式，无公共改动。以上为本窗口设计审查；实现与真实只读验收结果以下文独立记录为准，不冒称API或模型收益验收。

源码第一轮发现并修复原生date无法JSON哈希的问题，只在新reader规范日期，不改公共hash/helper；第二轮发现旧pure的整列None比较Bug，独立BUG-1778，不用假价格/压缩日期绕过。第三轮确保严格数值校验后float缺失类型，原OHLC矛盾即便复权坐标未知也拒绝；成交量不随factor缺失丢失。第四轮校验真实上海EOD时钟、30秒预算关闭、不伪行情完整/历史vintage；最终7项矩阵/Ruff通过及单日/20日批量真实只读功能验收。第五轮补全缺raw和benchmark的1项定向测试，仍保留原名单而非空人口，单节点通过，未重跑整套矩阵；共8项直接测试已通过。均为本窗口多视角自审，不冒称独立外审。

交付接续：公共分类 #5712 已修复；BUG-1778 源码 #5717（9adcceb4）和 close-sync #5718（f1db67e1）已合入，Issue #5645 已关闭，non-runtime 无需后端重启。2026-10-08 原DB树同步当前main，冲突保留主线的正式BUG闭环与后续研究进展；最终PR仅本设计规定的四文件，不重复提交BUG源码或公共合同。同步后的8项定向、Ruff、L0及F1六项矩阵全通过；同一只读20D/400候选复验仍为3个数据SELECT，总11.031秒（calendar0.015/raw9.344/benchmark0.953秒），8字段各400已知、市场宽度UNKNOWN。速度依赖缓存与并发负载，不把两次耗时差异解释为模型性能。原129研究fit+1旧INDEX_BUILD保持，0新增训练/收益读取；后续API仍须独立源码、用户重启与历史业务验收。
