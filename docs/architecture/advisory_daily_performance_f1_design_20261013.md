# 荐股日频结算统计与策略包共同窗口比较（F1）

日期：2026-10-13。关联 BUG-1851 / Issue #5916。

## 背景

12 日生成 13 日退出计划是正常流程。旧统计将 `EXITED` 记录直接视为已结算，甚至在 13 日开盘前计入胜率；还把 ACTIVE 收盘估值混入已实现胜率，将估值触及止盈/止损当成已完成退出。这些是统计时钟和口径缺陷，不是策略包 alpha 或模型能力证据。

## 范围与边界

本交付落实上轮建议的 P0 与 P1：修正日频统计，提供盈亏分布、固定 5 交易日评价和按共同日期的包间比较，以及既有每日名单变更统计的读回。所有行为仅在 Advisory 内；不改变选股排序、进入/退出政策、实际执行算法、调度或数据库 schema。

P2 的真实参考组合净值、组合回撤/恢复、日期聚类置信区间及行业/市场形态分组仍是独立后续项。本交付不把重叠五日收益复利、不把单笔最差回撤叫组合回撤、不把结果当模型确认或真实成交。

## Design Acceptance Index

- F-001：上海时间收盘发布时钟；退出计划、执行、结算、延期、未知明确分开。
- F-002：已结算毛收益与未实现估值分开；历史计划及未成熟/缺失样本保留且不伪造数值。
- F-003：费用必须显式，未知费用不按零；盈亏数、平均盈亏、盈亏比和 profit factor 的分母可追溯。
- F-004：行情读取有统计截止日且仅 SELECT；收盘估值不以开盘价替代，不吞掉读失败。
- F-005：固定五个市场交易日的成熟性、复权和可执行性；停牌不压缩时间，不删样本。
- F-006：包间总体/月度/滚动 20、60 日对比只使用共同候选日期和共同有效日期，并报告各包缺失与覆盖。
- F-007：旧 API 字段保留但明确 v2 口径；returns 和 metrics 来自同次读取；名单变更复用原字段。
- F-008：模块边界、持久源码方案、重复审核、源码合入与用户重启分离；该项验证发布责任，不冒充实际合入/运行激活已完成。

## 实施方案与接口契约

### 日频统计

`effective_exit_date` 是计划执行日，不是统计发布证明。使用注入的带时区 `now_provider`，15:00 之前仅允许前一自然日及以前的日线；正式日历决定有效交易日期。收盘只是必要条件，目标日缺执行/收盘行情仍为 UNKNOWN/DEFERRED，不能视为已结算。

保留账本 `ACTIVE/EXITED` 及原始记录，不修改历史数据库。只读响应新增 `performance_state`（PLANNED / EXECUTED / SETTLED / DEFERRED / UNKNOWN）和理由。早于统计发布的计划收益不输出为已实现收益。停牌、不可执行或行情缺失单列，不能静默改成零。没有券商成交回报时统一标记 `REFERENCE_HYPOTHETICAL`。

旧 `win_rate/avg_return_bps/median_return_bps/metric_evaluable_count` 改为已结算毛收益口径，schema version 升为 v2；ACTIVE 估值移到 `unrealized`，止盈/止损次数只统计已结算退出。保留 `max_drawdown_bps` 的单笔最差含义并输出说明，不当组合 MDD。

执行价仍按每条记录冻结的价格 basis：`next_open_executable` 用生效日开盘价；`signal_close/next_close` 用生效日收盘价。对数据库中提前写入的计划参考价，以生效日对应行情只读核对/结算，并保留原记录价与差异；不改历史账本、信号日或政策。任何无法证明的执行条件明确缺失。不能把“收盘后发布统计”误改成“收盘价执行”。

正式前向执行已消费目标日行情时，进入/退出生效日直接使用该目标日，不再额外向后偏移一个交易日；只接受该日市场 mark，不以候选旧参考价替代缺失执行价。普通收盘复评仍保留 D 生成 T 计划的语义。已观察目标日参考执行价可以显示 EXECUTED，但收盘之前不得显示已结算收益。

### 收益和费用

持仓退出毛收益延续冻结政策的原始执行价差口径，不是含分红/送转账本的总回报；固定五日评价另报复权价格回报，二者不可混为同一收益序列。净收益按已有 Advisory 成本语义：`(exit × (1-sell_bps/10000) / (entry × (1+buy_bps/10000)) - 1) × 10000`。调用者必须同时显式提供两边费用（可显式为 0），默认无成本合同，净指标为 null/COST_UNSPECIFIED。费用是情景假设，不是券商真实费用；不推导未知成交额/最低佣金。`GET /programs/{program_id}/returns` 与比较接口均支持这对可选费用参数；持仓的 `liquidation_net` 只是同成本假设下的估值，不是已实现收益。

已结算和未实现分别统计样本数、胜/负/平、均值、中位数、平均盈利/亏损、盈亏比、profit factor、有效日期。无亏损时 ratio 为 null 并注明原因，避免 Infinity/NaN。coverage 分母为已到期样本，另报全部样本与尚未到期数，不把不同分母的覆盖率混用。

### 固定五日和共同窗口

新增只读 `GET /api/v1/advisory/performance-comparison`，显式开始/结束日期，可选择 program_ids、费用和 benchmark。仅消费既存荐股记录与数据库日线，既不提交 QE 实验，也不重选候选、不训练或写业务状态。

五日口径为生效买入日开盘至第五个市场交易日收盘（含买入日），与“五次有效复评”及旧持仓退出收益不同。股票停牌不改变市场日历；端点停牌为 SUSPENDED、无法成交为 NON_EXECUTABLE；非停牌缺失为 MISSING_DATA。复权因子/涨跌停价缺失为 MISSING_PRICE_ADJUSTMENT_OR_LIMIT，不把除权当 alpha。收益是复权价格回报代理，不宣称完整现金分红账本。默认沪深300，同期限开盘至收盘基准毛收益，净超额明确为股票情景净收益减基准毛收益。

五日参考评价不假造分钟成交：沿用一字涨停无法买入的判断；第五日收盘跌停按保守口径标记无法确认可执行卖出，单列而不填零。该判断仅描述参考收益样本，不修改策略包准入、当天荐股输出或实际执行政策。

总体/月度/滚动窗口按有效买入日分组：候选日期取各包已进入 episode 的日期交集，不冒充完整 Selection TopK；有效日期进一步要求各包该日所有 episode 的五日指标成熟且有效。每包保留所有请求窗口样本、成熟/停牌/缺失计数、净指标可用数、有效日期与覆盖分母，不以删掉坏日期伪装完整。跨包比较使用日期等权 cohort 均值，不按样本多的日期加权。无费用时只能比较毛收益，净指标为 COST_UNSPECIFIED；费用明确后以净超额比较，基准缺失的日期不进入该比较。共同有效交集为空明确不可比较；不宣称最佳策略、显著性或模型激活。

每日名单的进入、保留、退出及 turnover 直接使用既有 PUBLISHED list-version 字段，分页读取完整请求窗口。`paired_list_rotation_count` 仅为进入/退出数量的配对，不证明逐股票替换或成交。历史绑定仅使用唯一匹配的 ACTIVE/RETIRED 生效区间；未能证明的绑定标记 UNRESOLVED_HISTORICAL_BINDING，不以当前绑定回填、不新增策略包准入门禁。policy hash 明示为 episode 最新已记录政策，并不证明入场政策身份。

## 验证方案

先保存直接失败用例，再实现。仅执行直接时钟/分母/费用/端点/共同窗口/SQL/API 合同的小矩阵；完整历史回放和 watchlist 套件交 CI/nightly，不重跑训练。既有 DEV 只读检查和生产业务读回分别报告，不写库。临时缓存与后续 RTK 日志显式配置 X 盘；canonical 工具指定的任务产物保留在本工作树，首次失败测试曾产生一份默认 C 盘 RTK 日志，此后已纠正目录设置，不宣称此前全程仅 X。fresh Python import 和直接 API 合同测试不启动用户后端。

审核轮次至少覆盖：① 时钟、分母与不成熟处理；② 收益/费用、停牌/复权与跨包偏差；③ API 兼容、SQL 边界、异常可见、模块范围与最后一次最小验证。发现问题修复后先重跑失败 nodeid。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/advisory_program.py；backend/services/advisory_historical_range/program_performance.py | backend/tests/advisory_historical_range/test_program_performance.py::test_target_day_release_requires_close；test_forward_execution_does_not_shift_dates_or_use_missing_target_price | verified | none |
| F-002 | backend/services/advisory_historical_range/program_performance.py | backend/tests/advisory_historical_range/test_program_performance.py::test_future_exit_and_open_marks_are_not_realized_wins | verified | none |
| F-003 | backend/services/advisory_historical_range/program_performance.py | backend/tests/advisory_historical_range/test_program_performance.py::test_settled_distribution_and_unknown_fees_have_honest_denominators | verified | none |
| F-004 | backend/services/advisory_program.py；backend/services/advisory_historical_range/program_performance.py | backend/tests/advisory_historical_range/test_program_performance.py::test_market_read_includes_maturity_tail_but_never_future_prices；只读 SQL 验证见下节 | verified | none |
| F-005 | backend/services/advisory_historical_range/program_performance.py | backend/tests/advisory_historical_range/test_program_performance.py::test_five_day_adjustments_limits_missing_data_and_costs_are_explicit；test_five_sessions_do_not_shorten_for_suspensions_or_count_immature_returns | verified | none |
| F-006 | backend/services/advisory_historical_range/program_performance.py | backend/tests/advisory_historical_range/test_program_performance.py::test_comparison_preserves_exclusions_and_equal_weights_common_dates | verified | none |
| F-007 | backend/routers/advisory.py；backend/services/advisory_program.py | backend/tests/advisory_historical_range/test_program_performance.py::test_returns_api_uses_one_snapshot_and_keeps_next_open_price_after_close；backend/tests/watchlist/test_advisory_program.py | verified | none |
| F-008 | backend/services/advisory_program.py；本设计生产门禁与发布章节 | backend/tests/advisory_historical_range/test_program_performance.py；validation-receipt: BUG-1851 源码边界/多轮审核；未修改 QE/Selection/公共流水线 | verified | none |

矩阵的 verified 只表示本范围源码设计验收，不代表正式运行态、PR CI、合入或经济收益确认。发布依赖阻断独立列在下面，不将其伪装成验收已完成。

## 多轮审核与实际验证结果

1. 时钟/分母审核：原代码直接回归为 RED（未来退出及 ACTIVE 浮盈计入胜率）；修复后通过。补齐收盘前/后的发布时钟，以及正式前向执行日期不得再偏移、不得复用旧候选价格。
2. 收益/费用/共同日期审核：补齐 gross/net、未指定成本、五市场交易日、停牌不压缩、复权/涨跌停与共同日期的公平口径；不生成组合 NAV。
3. API/SQL 审核：直接测试发现 repository.list_programs 不接受 limit/offset，修复后仅重跑失败 node；实际只读 SQL 暴露跨 chunk 查询超时及 list-version 字段名错误，改为明确常量日期边界/MATERIALIZED 和 version_status，再验证通过。
4. 最后口径审核：显式区分持仓原价差与五日复权价格回报、最新记录政策与历史入场绑定、名单配对数与真实替换；补齐两接口的费用参数与有限值校验，定向验证通过。同步流水线修复后，直接 API test 将真实响应交给新运行态语义验证器，已结算、显式费用及缺收盘行情 UNKNOWN 三类响应均通过；这只验证源码/接口合同兼容，不是运行后端已激活。未放宽冻结选股/执行政策或模块边界。

本地最终相关小矩阵 15 case 中首次 14 通过、1 个 API case 失败；失败点修复后定向重跑通过，再追加费用断言的两个直接 case 通过。没有为机械格式/文档修改重复运行完整套件。全套 advisory_historical_range_backend / watchlist_backend 仍交 CI/nightly，本节不声称全套已经通过。

真实数据库验证使用显式只读事务、20 秒 statement_timeout，rollback 后关闭连接。DEV 的四查询及结算查询语法/schema 验证通过，DEV 无业务行情样本，不将其等同业务数据验证。生产只读业务验证通过：旧 multi 已结算 148 笔，旧 single 已结算 156 笔，后者未来退出 5 笔已排除；两个新包各 20 笔未实现估值，尚无已结算笔。四 Program 比较约 0.772 秒：五日成熟数分别 149、155、0、0，未成熟数 15、20、20、20；全四包无共同入场日期，应返回不可比较，不称缺数据或策略优劣。以上是任务源码的只读计算，不是运行后端已激活证明。

公共交付依赖曾因缺少 `/api/v1/advisory/programs/{program_id}/returns` 的 target-owned semantic smoke 而阻断；流水线 BUG-1855 / PR #5934 已合入，登记 `advisory_returns_metrics_v2`。使用 BUG-1851 原记录只读解析确认 `blocking=[]`，该阻断已解除；本任务未修改公共工作流，也未借无关 smoke 绕过。当前按授权同步主线、重新绑定验证收据并进入 PR/CI/合入流程。用户后端重启仍单独待执行；源码合入、重启、运行态复验和 close-sync 必须分别报告。

## 风险和失败模式

旧记录可能使用不同政策或提前生成的价格，必须保留原价及引用，不静默伪造真实成交。数据库不可读时可继续展示计划与旧记录，但统计状态必须说明行情不可用；不能返回旧数字并谎称 READY。未满五日、单日或交集为空不能做包间表现判定。新口径将降低原胜率分母，这是修正，不是策略退化。

## 生产门禁与发布

DDL、DML、依赖、模型、profile 激活：noop。不启动/停止后端。多轮审核和定向验证通过后按用户授权提交合入，canonical main fast-forward；BUG 保持源码已修复/等待用户重启，重启后只读核对 API 版本、统计截止日和计划排除。未获得本任务精确清理授权，不删除工作树。
