# 持仓与自选池择时建议系统首发实施计划

> 日期：2026-09-04
>
> 状态：`FIRST_RELEASE_RUNTIME_VERIFIED_PT_NEXT_002_AUDIT_COMPLETED_PT_NEXT_003_SELL_CONTRACT_LOCAL_VERIFIED`
>
> F2 设计权威：`docs/architecture/position_timing_advice_f2_redesign_20260903.md`
>
> 收口状态：首发运行态已验证；`PT-NEXT-002` 离线 L2 audit 已完成且无 selected/runtime model；`PT-NEXT-003` 首份双方向 L4b-1 audit 已降为历史，risk-exit SELL-only 契约已本地验证并等待绑定干净提交生成新凭证
>
> 目标：用两个连续实现块和一次上线收口，交付完整的 L1 日频行动卡、L1a 盘中到价提醒与 prospective outcome 闭环；两个实现块可以增量源码合入，但都不单独代表首发上线。

本计划不建立第二套设计权威。任何业务语义以 F2 蓝图及其 F-001～F-027 为准；本文件只把已批准设计压缩为可连续执行的实施顺序。

## 0. 2026-09-04 实施检查点

实现块一已经形成内部可联调切片，仍不是完整首发：

- 已实现 timing-owned contracts、冻结 guard/cost policy、保存完整 input/policy identity 与 `cards_sha256` 的 immutable card artifact、`CARD_ISSUED`、持仓与 active watchlist 分页合并、已确认终止上市方向规则、停牌最近可执行 close、T+1 日频卡、5 个块一 API、一个页面以及 composition root 薄接线。
- alert/outcome 的字段契约和 L2 population/sampling/model/inference contract 已冻结；没有创建 alert poll/claim、outcome materializer、L2 pipeline、worker、scheduler、SSE、数据库表或订单路径。
- 代码收口后的最终复核已重跑通过：`position_timing_backend` 50 项、`qe_read_backend` 153 项、`platform_api_backend` 15 项、validation catalog/ownership/UI target 21 项、`frontend_type_lint`、`l0` 与 F2 validator（26/26，0 warning）。前端 lint 仅报告不属于本变更路径的既有 hook warnings；L0 唯一 MEDIUM 为请求体 `JSON.stringify` 的 `RAW_JSON_UI` 启发式命中，不是向用户展示 raw JSON，均无阻塞项。
- DEV 只读 readback 识别 468 个去重后的目标（2 个 holdings）；在已成熟的 2026-09-03 数据上临时根生成 468 张卡、0 张数据缺失卡，468 张均绑定可用的 confirmed-delist identity，card adjustment 均明确为 `NOT_APPLICABLE`。2026-09-04 15:23 当日 raw 仍为零覆盖时，服务返回 `SOURCE_NOT_MATURE_NO_NEW_CARD`，未签卡且未写 `CARD_ISSUED`，避免锁死稍后重试；复权数据不再错误阻塞只使用 raw 的卡片。
- 同次 DEV readback 显示全局 canonical PIT pointer 仍指向 legacy `shsz_st_pit_active_v1`。L1 产品 universe 因而明确不依赖该研究 pointer 迁移；历史 L2 才要求绑定 canonical v2/QE dataset identity。这是去除无关上线门禁，不是把 legacy pointer 冒充 canonical v2。
- 块一已由 PR `#4277` 合入 `main`，merge commit 为 `7c9fdd9cf86aa472fb2e84bac6211eb2378350ed`。这不等于生产进程重启、生产页面 readback、真实盘中提醒或完整首发完成。
- 本次新增 F-027 轻量分析范围设计后，F2 validator 为 27/27、0 warning，docs L0 为 0 finding，`git diff --check` 通过；这只证明设计闭合，源码状态仍为 queued。

## 0.1 2026-09-05 首发源码检查点

`PT-NEXT-001` 已在同一个块二任务内实现，没有拆出新产品或审批阶段：

- `analysis_scope/current.json`、唯一 scope PUT 与既有 intents/page 复选框已经落地；真实持仓始终分析，自选默认不选且只允许 active confirmed watchlist 显式 opt-in，失效来源保留 typed warning 并可移除，历史卡片不改写。
- `alerts.py` 是块二唯一新增服务文件；页面可见且存在今日有效 trigger 时每 60 秒只读轮询，服务按 50 只分批复用 TDX quote，执行 5 分钟/30 秒、持仓/意图与方向性可交易性复核。claim 原子追加唯一 `ALERT_EMISSION_AUTHORIZED`，不接 notification service、SSE、worker、scheduler 或订单路径。
- 同一个 materialize POST 已加入五 horizon `OUTCOME_EVALUATED`：卡片仍只在 T+1 有效，标签终值遇停牌、一字跌停或必要 authority 缺失时最多顺延 5 个交易日；candidate/do-nothing 只比较动作边际数量并逐腿计费，公司行动由 hash-bound adj-factor ratio 进入持股路径，终值可卖性绑定独立的历史 `market.stk_limit` identity。成功水位不会越过 pending/失败 key，evidence 区分 pending-derived、pending-materialization、materialization-missing 与 unavailable。
- 首发仍为 8 个 API、一个页面、一个 artifact root、0 DDL/DML、0 新依赖、0 自动交易。L2 pipeline、L3、分钟新信号和外部通知仍未实现，符合批准范围。
- 当前集中验证：`position_timing_first_release` 完整通过（后端 72 项、TypeScript、frontend lint、production build、目标 Playwright 1 项）；Ruff 与 validation catalog integrity 通过。仓库既有 frontend hook/autoprefixer warnings 未来自本变更路径且不阻断。
- 最新主线合并后的真实 DEV 只读冒烟使用一次性 artifact root：发现 468 个持仓/已确认自选候选，默认 scope 仅 2 个真实持仓生效并生成 2 张 2026-09-07 卡片；未来目标日正确返回 `NO_DUE_OUTCOMES`，evidence 为 `AVAILABLE` 且 10 个 horizon 全部是 `PENDING_DERIVED`。过程未写数据库或生产 artifact。
- 截至本源码检查点，`production_ddl_gate=noop`、`production_dependency_gate=noop`，生产 8001/3000 进程尚未重启、生产 artifact 尚未写入；后续实际激活与 readback 见 §0.2，历史检查点不反向改写。

## 0.2 2026-09-05 首发运行态验收与 BUG-1365

- 用户于 05:00 完成 8001 后端重启；同一时段 3000 前端进程也已加载首发页面。8001 OpenAPI 已包含完整 8 API，页面 `/position-timing` 返回 200。
- 真实运行态发现 468 个候选：2 个真实持仓自动且不可关闭地进入分析范围，466 个自选标的默认不选；没有 scope warning。这验证了“持仓始终分析、自选显式 opt-in”的首发边界。
- 首次幂等 materialize 为决策日 2026-09-04、目标交易日 2026-09-07 生成 2 张 HOLD 卡；立即重试返回 `ALREADY_MATERIALIZED` 且 artifact identity 不变。生产 artifact root 仅新增 card set、2 条 `CARD_ISSUED`、materialization state、冻结 policy snapshot 与锁文件；没有数据库、订单或其他模块写入。
- evidence 返回 10 个 `PENDING_DERIVED` horizon、0 matured/unavailable/materialization-missing，费用口径为 `PER_PARENT_ORDER / BROKER_UNVERIFIED`，阈值为 58,824 / 117,648 / 235,295。非交易日 alert poll 返回 `NO_VALID_CARD_TODAY`；这证明提醒链路可用但没有虚构真实触价或送达。
- 定向 Playwright 与真实 3000/8001 DOM 回读均通过：两张真实持仓卡、2 个锁定勾选项、466 个未选自选项均可见，无 API/page error，且不存在下单或自动交易按钮。
- 运行态验收同时发现 BUG-1365：进程启动后若 canonical worktree 快进，原 `_source_commit()` 会在下次物化时重新读取可变 `HEAD`，使卡片错误绑定未被当前进程加载的提交。修复后源码身份在模块加载时冻结；显式 `AISTOCK_GIT_COMMIT` 仍优先，解析失败仍由 materialize 返回 typed unavailable，不影响其他后端路由导入。
- 本次首张卡在 worktree 快进前已正确绑定进程加载提交 `4506ea73ea5db1b19315577f5226980b425b2463`。截至本节检查点，BUG-1365 合入后仍须由用户再次重启 backend-main 才能激活修复；该外部动作及其最终收口见 §0.3。

## 0.3 2026-09-06 重启后验证与收口

- 用户完成 backend-main 重启后，digest-bound 收据以 BUG-1365 merge commit `19b45dbe38bfea40339bd9d2d9eadc9b1e005fcc` 为 expected identity，以运行进程的 `d942a1bcac82b3af008d3ad807dcf60d8e3be605` 为 observed identity；`origin_main_descendant` 证明、health、identity 与业务 smoke 均通过，收据 SHA-256 为 `8f0555518c619c59867036e0c6ebb8468ff121768342a179e82277cdddc996d0`。
- 业务 smoke 使用 `/api/v1/position-timing/intents` 的 target-owned collection 语义契约，确认 468 个条目。该契约缺口由 BUG-1378 的源码 PR `#4341` 修复并由 close-sync PR `#4342` 固化；它只增加精确端点映射与测试，不改变产品运行时，也不要求再次重启。
- BUG-1365 close-sync PR `#4311` 已合入 `main`（merge commit `a87e239e2a0ffaeae28bc87dec96741d4476144b`）；Issue `#4301` 已关闭，canonical BUG 状态为 `verified`、`post_restart_effective_gate=passed`、`runtime_identity_match=true`。
- 最终只读业务读回为 468 个候选、2 个有效持仓、2 张目标交易日 2026-09-07 的 `HOLD` 卡；10 个 horizon 仍为 pending，2026-09-06 非交易日 alert 为 `NO_VALID_CARD_TODAY`。四个 BUG-1365/BUG-1378 源码与 close-sync 工作树、对应本地/远端分支及 BUG-id reservation 均已按精确清单清理。
- 下一项 `PT-NEXT-003` 已由用户启动；它只实现一分钟 candidate 上的 prospective action-card 执行窗口反事实，不反向阻塞已经可用的 L1/L1a。首份双方向审计已因可达性错误降为历史，当前 SELL-only 契约完成本地验证后须另生成权威凭证。

## 0.4 2026-09-06 `PT-NEXT-002` 离线 L2 可学性审计

- 本任务在首发运行面之外唯一新增文件为 `backend/services/position_timing/learnability_pipeline.py`，复用首发 contracts/policy 与 Advisory registry 的纯实现，但使用 timing-owned request、bundle 和 registry 路径；没有新增 router、页面、worker、scheduler、数据库、final fit、运行时模型或订单路径。既有 evidence API/页面只补一个 hash-bound 总体审计引用，使阴性/不可分辨结果可见而不进入个股卡片。
- 正式 request `ptl2req_163c90896899e65cf3183e5e.json` 绑定代码提交 `e26881abb7366f1726b2365a398d0d04786c6eb7`、canonical-v2 QE/HMM candidate、Python/数值包/线程池、22 项 feature order、Ridge/GBDT 完整参数、CPCV、费用与 exit policy identity。request-era prospective outcome 为 0、全局 N0 历史记录为 36，二者只作报告字段，不是开跑门禁。
- 第一份 request 所含 `selection_rank/selection_score` 只有 2024～2026 的 immutable coverage，无法覆盖 2018～2026 人口；pipeline 在任何完整 hypothesis/receipt 生成前以 typed error 停止。删除这两项不可用特征是一次数据契约修订：没有读取局部收益、没有缩短人口、没有替换特征搜索。随后发现 pandas frame attrs 携带全量 calendar 导致 replay 重复深拷贝；修复只消除复制，不改变数学结果或冻结 request 语义。
- 正式 materialization 得到 96 cohorts、388,035 episodes、7,351,727 review rows、386,737 paired-ready episodes。Ridge point `-45.3852 bps`、family-wise adjusted CI `[-86.6868,-7.7560]`，分类 `NEGATIVE + ADEQUATE`；GBDT point `-35.0801 bps`、adjusted CI `[-82.3353,10.4406]`，分类 `INCONCLUSIVE + ADEQUATE`。study 为 `INCONCLUSIVE`，`selected_model_id=null`，L3 不实现。
- immutable bundle 为 `F:/Dev/AIstock_model_artifacts/position_timing_advice_v1/research/l2_learnability_bundles/eef1f771a5d8ae3c002feaf6ed46007df9a0ee3726894893abdc93cddc8f3f51`；独立 inspect 返回 `BUNDLE_VALID`，exact retry 返回同一 bundle/receipt，两条 timing registry append 均为 duplicate no-op。全局 N0 registry 前后 SHA-256 均为 `53ca1338ee5f725be38eeb217b06ebc33d99a6a75904ea1efc4c8607fbd5e067`。
- 该结果是本次两个冻结函数族/政策的研究终态，不是 L1/L1a 发布门禁，也不证明不存在其他信号。不得在同一 family 追加搜索；未来若研究 OPEN/ADD entry objective，必须另立设计、request 和 trial family。
- 收口验证为 position-timing 88 tests、相邻 Advisory CPCV/registry/control 16 tests、集中 `position_timing_first_release` nox、F2 validator 27/27、validation catalog integrity 与 L0 全部通过；目标 Playwright 同时验证 L2 总体 `INCONCLUSIVE`、Ridge `NEGATIVE/ADEQUATE` 和“无入选模型”只出现在研究证据区。

## 0.5 2026-09-06 `PT-NEXT-003` L4b-1 离线执行窗口审计（SELL-only 可达性修订）

- 复用最新 PASS 的 2026-08-31 minute candidate，只读消费 Qlib Bin 与真实 `CARD_ISSUED`/immutable card；不重建或激活数据集。
- 只比较 `OPENING_30M_VWAP_RAW_V1` 与 `AT_OPEN_RAW_V1`，固定 card side、quantity、一个 parent order 和 20 日 horizon。可达性修订后首个 family 只保留现行 L1 可生成的 risk-exit SELL 单假设；买入/普通卖出均为 ON_PRICE_TRIGGER，不事后选择 branch 或数量。
- 本项复杂度预算是一份 offline service/CLI 文件、一份直接测试和两份既有设计文档更新；无 router、页面、worker、scheduler、数据库、依赖、模型、运行 policy 或订单路径。
- 当前生产只有两张 HOLD 卡，首轮正式结果为 `INSUFFICIENT_PROSPECTIVE_ACTION_CARDS`，没有 selected side。这是诚实的研究结果，不是源码失败、人工审批或 L1/L1a 门禁；首份两方向 receipt 保留为历史，SELL-only 修订须绑定干净修订提交生成新 request/receipt 后才成为当前权威。
- `minute_execution_pipeline.py` 与 11 项直接测试已经落地；`AIstock` 环境下全模块 99 项与集中 `position_timing_backend` 均通过。正式 request 绑定干净代码提交 `e00e624797ae627c7d425a376377938a46e43b68`，没有绑定未提交实现。
- 历史 immutable bundle 为 `research/l4b1_execution_window_bundles/78ee08b9d712c3b62517dc740ce6f2fac3ad39ec19670d5c7ee8c72c6e1f0816`，receipt SHA-256 为 `02250741104c8ced608a26453a9aa01f31e2af07882cf30d9894bd60734ef406`；它继续可 inspect，但不再作为当前 SELL-only hypothesis contract 的结果。新凭证在干净修订提交后生成并回填。
- 全局 N0 registry/current_route、生产 cards/events 的前后 hashes 完全一致；runtime policy/card/event/order 均未写。后续只在新 minute candidate 已覆盖真实 AT_OPEN action-card 时重跑同一入口，不另建平台或开发阶段。

## 1. 执行结论

首发只采用“两个连续实现块 + 增量源码合入 + 一次上线收口”：

1. **实现块一：日频行动卡纵向闭环**。一次完成 contracts、规则、artifact、只读数据适配、API 与页面，使真实持仓和已确认自选能生成并展示 T+1 卡片。
2. **实现块二：范围、提醒与结果证据闭环**。先在同一模块内补一个显式自选分析范围，再完成一分钟轮询、原子 claim、toast、五个 horizon outcome、证据聚合和全量目标测试；不为范围能力另拆阶段或平台。
3. **上线收口：一次完整首发验证、一次激活**。块一与块二可以各自通过普通 PR 增量合入，块二完成后更新 F2 验收矩阵并执行完整首发验证；生产端口激活仍按用户授权边界单独记录。

两个实现块是同一批准范围内的连续开发顺序，不是两次立项、两道批准门禁或两次发布。块一可以作为已验证的内部切片独立 source merge，以缩短后续块二的变更面；它不得单独激活生产、不得称首发完成，也不得把块二缺口伪装成已实现。

首发不等待 L2 样本、MDE、sealed holdout、券商最低佣金核验或 HMM/Selection 可用性。它们都不能阻塞 L1/L1a。L2、L3 和第二阶段分钟执行研究不是本次实施的细分阶段，而是首发之后另行启动的工作。

首发复杂度预算如下。它用于约束实现面，不是新的审批门槛：

| 项目 | 首发数量 |
|---|---:|
| 功能性 backend service 文件 | 5 |
| backend router | 1 |
| frontend 页面 | 1 |
| 既有 runtime composition 文件改动 | 2 |
| API | 8 |
| 数据库 DDL/DML | 0 |
| 新依赖 | 0 |
| worker / scheduler / SSE / queue | 0 |
| 增量 source PR | 2（块一 + 块二） |
| 集中 nox 入口 | 1 |

## 2. 首发的完整结果

用户打开唯一的“持仓择时”页面后，应能完成以下闭环：

```text
只读持仓 + 已确认自选 + 显式分析范围 + 用户仓位意图
  -> 页面首次打开触发幂等 materialize
  -> T 日信息生成 T+1 immutable action cards
  -> 页面显示 OPEN / ADD / HOLD / REDUCE / EXIT / WAIT / UNAVAILABLE
  -> 交易日盘中每 60 秒批量读取 TDX quote
  -> 合格的新触发边先原子 claim，再显示 toast
  -> 页面后续打开补算已到期 OUTCOME_EVALUATED
  -> 证据区持续显示基率、覆盖状态和 typed failure
  -> 用户自行决定是否人工交易
```

首发完成必须同时满足：

- `LEGACY_PORTFOLIO` 持仓与 active confirmed watchlist 合并、canonical symbol 去重，持仓身份优先。
- 用户可以维护 `planned_full_notional_cny` 与 `desired_target_exposure`；缺失 sizing 只影响对应自选股，不影响其他股票。
- 全部真实持仓始终分析；仅被显式选择且仍 active 的 confirmed watchlist 进入下一张 card set。scope 缺失是 `NOT_SELECTED`，不是失败或审批门禁。
- T+1 卡片给出明确动作、目标敞口、合法数量、执行窗口、触发价、逐腿成本与不可执行原因。
- 页面可见 `QUOTE_STALE`、`QUOTE_FUTURE_SKEW`、`QUOTE_UNAVAILABLE`、持仓或 intent 漂移等状态；不以空结果伪装成功。
- 新触发边以 artifact exact-once claim；多标签页视觉去重只尽力而为，已 claim 且仍有效的边继续以非模态条目可见。
- 五个 horizon 的 prospective outcome、do-nothing paired baseline、覆盖水位与基率展示可持续积累。
- 页面不存在下单按钮、自动交易开关、运行时权重或任何发送订单的调用路径。

只实现卡片 API、只实现 mock 页面、只实现提醒或只留下 placeholder outcome，均不构成首发完成。

## 3. Phase 0：已核实的复用入口

Phase 0 是本计划的事实依据，不是额外发布阶段。实施优先调用下列现有入口，不重新发明相同能力。

| 能力 | 允许使用的现有入口 | 首发用法 |
|---|---|---|
| 持仓 authority | `portfolio_manager.get_all_stocks(auto_monitor_only: bool = False) -> List[Dict]` | 只读 `app.portfolio_stocks`；数量与成本是第一阶段唯一持仓账本 |
| 已确认自选 | `WatchlistRepoPG.list_items(category_id=None, page=1, page_size=20, sort_by="updated_at", sort_dir="desc") -> Dict[str, Any]` | 按 `total` 分页读完，再过滤 `advisory_enabled=true` 与 `CANDIDATE/ENTERED/HOLDING` |
| symbol 校验 | `normalize_ts_code()`、`normalize_and_validate_ts_codes()` | timing 内只增加一个 legacy 六位代码到 SH/SZ canonical code 的严格边界适配，之后调用公共 validator；BJ 与未知前缀 typed unavailable |
| 交易日 | `TradingCalendarStatusService.status/ensure_trading_day/list_trading_days/next_trading_day` | 解析 decision/target/valid/maturity 两只时钟，不用自然日替代交易日 |
| ST/停牌/涨跌停 | `DailyTradingContextProvider.load_supporting_facts/load_stk_limit_authority_attempt` 与 `LocalSimDailyLimitAuthorityProvider.load` | 组合为一次冻结的 `DailyTradingContextV2`，逐标的保留 typed authority state |
| 已确认终止上市 | `market.event_signal` 的 `issuer_bound_stock_delisting_v2` confirmed event；已生效 `market.stock_basic` 终态兜底 | timestamp-causal 事实可覆盖持仓为 EXIT、阻断仅自选买入；研究态 event overlay 不接 runtime |
| raw 日线 | `fetch_history_window_ts(..., freq="1d", adj="none")` | card 参考价、trigger、保守 fill 和费用使用 raw CNY；T 日已验证停牌且缺 T 日 bar 时，第二次只为缺失标的读取最近可执行 raw close；非停牌旧 bar 不得冒充成熟 T 日输入；不得调用分钟历史 |
| 复权因子 | `AdjFactorProvider(use_tushare_fallback=False).get_adj_factor_from_db(...)` | outcome 跨公司行动估值；本地缺失即 unavailable，不在线回退 Tushare |
| 买入价格 guard | `trading_core.price_guard.evaluate(PriceGuardContext, PriceGuardPolicy)` | 用 timing-owned V1 snapshot 显式构造 policy，保留共享纯实现 |
| 卖出 guard | `trading_core.exit_guard.evaluate(ExitGuardContext, ExitGuardPolicy)` | 风险退出与用户目标映射；不得修改共享默认值 |
| 交易单位 | `board_lot_rule()`、`round_to_board_lot()` | BUY 与部分 SELL 按板块规则；全量卖出允许合法零股余额 |
| 实时报价 | `fetch_tdx_realtime_quotes(symbols)`、`quote_tradability_evidence(...)` | 自动按 50 只分块并复用 5 分钟/30 秒约束；只取有界标量 |
| artifact 原子模式 | `advisory_model_first/model_binding_resolution.py` 的 `msvcrt/fcntl` 锁模式与 `advisory_modeling/bundle_store.py` 的 fsync/no-replace 模式 | 只参考标准库实现形态，在 timing-owned store 内实现；不 import Advisory 业务 store |
| API 接线 | `APIRouter(prefix="/position-timing")` + `backend/main.py` 的统一 `/api/v1` 注册 | 一个 router 提供蓝图冻结的 7 个端点 |
| UI 接线 | `NEXT_PUBLIC_API_BASE`、App Router 页面模式、`nav-groups.ts` | 一个页面、一个导航入口；复用现有 token。仓库当前没有通用 toast helper，首发在页面内实现轻量 `aria-live` 提示区 |

实施时特别禁止以下近似替代：

- 不用 `watchlist_service.list_items_with_quotes()` 构造 universe，避免把自选读取与额外实时报价耦合。
- 不调用 `fetch_minute_kline_tdx`、`TdxCausalMinuteProvider`、分钟 feature builder 或 `kline-all`。
- 不把 `FeeModel` 的总费率最低收费公式用于本产品；只复用 parent-order 身份思想，费用在 timing `policy.py` 分项计算。
- 不修改 `price_guard` / `exit_guard` 默认工厂，不从实验臂名称反推运行语义。
- 不调用 SmartMonitor task/engine/trade，不依赖 `notification_service.py`，不接 Paper/MiniQMT 持仓或订单能力。
- 不为 toast 新增 npm 依赖、全局 provider 或共享组件；提示状态只属于当前页面。
- 不写 Advisory N0 registry/current route，不写 QE、Selection、Watchlist、Portfolio、Paper 或 MiniQMT 的任何状态。
- 不以 HMM 或 Selection 决定 universe、方向、规模或 trigger；不可用时只返回字段级 typed context status。

## 4. 最小实现面与写入范围

### 4.1 新增运行代码

```text
backend/services/position_timing/
    __init__.py
    contracts.py
    policy.py
    artifact_store.py
    service.py
    alerts.py
backend/routers/position_timing.py
frontend/src/app/position-timing/layout.tsx
frontend/src/app/position-timing/page.tsx
```

职责保持在这五个服务文件内。`layout.tsx` 只导入既有 `paper-v2.css`，不形成第二个页面、样式副本或业务层。首发不增加 `data_access.py`、worker、scheduler、消息队列、SSE、独立 API client、数据库 repository 或通用插件框架。

轻量分析范围管理也只修改现有 `contracts.py`、`artifact_store.py`、`service.py`、router 和页面：新增一个 `analysis_scope/current.json` 当前态和一个 PUT 接口，不增加第六个服务文件、新页面、事件类型或数据库表。

### 4.2 既有文件的最小改动

- `backend/main.py`：只 import/register router。
- `frontend/src/lib/navigation/nav-groups.ts`：只增加页面入口。
- `tests/aistock_validation/catalog/file_ownership.yaml`、`module_registry.yaml`、`test_plans.yaml` 与 `noxfile.py`：只增加一个 `position_timing` 模块归属和一个集中验证入口，不加入产品逻辑。
- F2 蓝图的 Design Acceptance Matrix：实现完成时回填真实代码、测试、receipt 和结论；不改变已批准业务语义。

验证目录元数据属于仓库既有工作流接线，不是既有业务模块对 timing 的反向依赖，也不是新门禁平台。

### 4.3 唯一产品写入根

运行时只允许写：

```text
F:/Dev/AIstock_model_artifacts/position_timing_advice_v1/
```

`PositionTimingArtifactStore(root: Path)` 必须允许测试注入临时目录；生产默认值仍是上述唯一根。测试不得写真实产品 artifact，真实 DEV readback 另行明确记录。

card 与 policy snapshot 使用完整 artifact SHA 命名、临时文件 fsync 后原子 hard-link 的 content-addressed no-replace 发布；event JSONL 在跨进程文件锁内执行幂等键检查、append、flush 与 fsync；intent 和 materialization state 使用临时文件加原子替换。current card set 不另建控制面 registry：按 `(position_source, decision_trade_date)` 目录解析恰好一个 immutable card set，出现多个不同 identity 直接返回 conflict。

## 5. 实现块一：日频行动卡纵向闭环

目标是尽早得到可以用真实只读数据联调的完整日频切片。它可以独立完成源码合入，但不单独发布或激活。

设计依据：蓝图 §4、§5.1～§5.10、§5.13 与 F-001～F-011、F-017、F-024～F-025。

### 5.1 Contracts 与 artifact

- 一次定义 `PositionTimingIntentV1`、`PositionTimingCardV1`、trigger、cost、typed status、三类 event 与 API DTO，枚举和字段直接来自蓝图 §5。
- 同一 `contracts.py` 以不可变 schema/常量冻结 L2 v1 的 population、sampling、Ridge/GBDT 两个 model spec、唯一 monotone policy、两个 hypothesis、`economic_threshold_bps=0.0` 与 inference 分类字段；不创建 pipeline、模型 bundle、trial 或 registry 写入。这样首发 outcome 从第一天起具备未来 L2 所需字段，而不把 L2 实现塞进首发。
- 冻结 `PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1`、`EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1`、`PERSONAL_MANUAL_COMPONENT_COST_V1` 及 snapshot provenance/hash。
- 每张 card/outcome 绑定实际使用的 dataset、calendar、limit/ST/suspend/delist、guard、cost policy 与 source commit identity；card set 额外保存完整 input/policy identity 与 `cards_sha256`。块一 card 不消费 adjustment，固定记 `NOT_APPLICABLE` 且不设出卡门禁；块二 outcome 才强制绑定 adjustment/corporate-action。缺任一实际消费的强制 identity 时返回 typed unavailable，不用“当前默认值”补齐。
- 实现 card/policy immutable publish、intent 原子 current state、event append-only 与三类事件 schema/幂等键；块一即冻结 alert/outcome 的复合键、hash、时区与 maturity 一致性，`materialization_state.json` 的读写和两类未来事件的实际追加留在块二，块二不得另造语义。
- 费用用 `Decimal` 按组件和父订单计算；58,824 / 117,648 / 235,295 只作为派生断言，不写成业务常量。

### 5.2 Universe、规则与 materialize

- 分页读完自选；与 legacy holdings 合并并按 canonical symbol 去重。
- 用一次 position/intent snapshot 构造 card set；单只标的数据缺失只降级该卡，系统级 identity 冲突才使该 card set 失败。
- 先解析已完成 decision trade date 与 target T+1，再加载 raw 日线、calendar、limit/ST/suspend 与 confirmed-delist identity；T 日已验证停牌且缺 bar 时只使用更早的最近可执行 close，非停牌旧 bar 仍视为源未成熟；adjustment 留给块二 outcome。
- 对持仓先调用 exit guard，再按用户目标计算 delta；confirmed delist 对持仓映射 EXIT、对仅自选买入映射 typed unavailable。OPEN/ADD/非风险 REDUCE 只生成由 snapshot 参数派生的冻结条件分支，green/yellow/skip 各有唯一 guard action/reason 映射，页面把 guard 判定与价格条件一起展示，按各自触发价估算分支成本；盘中不新建方向、阈值或数量。
- `POST /materialize` 是首发唯一物化入口。块一只在具备已完成 T 日输入时幂等生成 T+1 card set，并显式返回 `outcome_materialization_status=DEFERRED_TO_IMPLEMENTATION_BLOCK_TWO`；块二在同一端点内加入到期 outcome 扫描，不另建 worker 或第二入口。盘中或源尚未成熟时返回 typed no-new-card 状态，不伪造空成功。

### 5.3 API 与页面

在同一个 router 完成 `GET /intents`、`PUT /intents/{symbol}`、`POST /materialize`、`GET /cards/current` 与 `GET /evidence`。这些路径均位于 `/api/v1/position-timing` 下。页面首次打开顺序固定为：

1. `POST /materialize`；
2. 并行 `GET /intents`、`GET /cards/current`、`GET /evidence`；
3. 显示行动卡、成本、数据状态与意图编辑；
4. intent 更新后只刷新 intent 状态，已签发 card 不被改写，新值进入下一张卡。

内部联调结果必须能回答：“今天为什么建议这只股票买、卖、持有或等待；计划数量和成本是多少；若不能做，缺的是什么。”

实现块一只运行 contracts、universe、PIT、tradability、policy snapshot、cost、artifact 与 card service 的快速测试。它不创建新的业务审批点；在直接测试、F2 validator 与仓库既有 required checks 通过后可以独立合入 main，但仍只称“块一源码已合入”。

反模式保护：不得为数据读取另建 repository 层，不得让单只股票 unavailable 阻断其他卡，不得用当前共享 default factory 代替已冻结 snapshot，不得把 contract-only 的 L2 schema 接到运行路径。

## 6. 实现块二：提醒与 prospective outcome 闭环

设计依据：蓝图 §5.2.1、§5.11～§5.13、§6、§9.3 与 F-012～F-021、F-023～F-027；其中 F-019～F-021 在首发只验证冻结 contract 和“非门禁”语义，当时不实现 L2 pipeline。后续完成状态见 §0.4，不反向改变首发边界。

### 6.0 分析范围管理

- discovery universe 仍为 holdings 与 confirmed watchlist 并集；analysis universe 只保留全部 holdings 与显式选择的 active watchlist。
- 使用 timing-owned `PositionTimingAnalysisScopeV1` 单文件当前态、文件锁和原子替换；不写原 Watchlist/Portfolio，不新增 event、registry、worker 或 repository。
- 复用 `GET /intents` 展示 selected/effective/locked/reason，新增请求体仅含 `analysis_enabled` 的 `PUT /analysis-scope/{symbol}`；现有页面只增加复选框，所有 GET 保持零写入。
- scope 更新仅进入下一 card set，旧 card/alert/outcome 不改写；scope snapshot hash 进入 card-set input identity。

### 6.1 L1a 一分钟提醒

- 页面仅在可见且存在当日有效 trigger 时每 60 秒调用 `GET /api/v1/position-timing/alerts/poll`；一次请求由服务按 50 只分块读取 batch quote。
- GET 严格只读，返回 eligibility、quote identity、staleness、already-alerted 与当前 position/intent 是否仍匹配。
- 新 eligible edge 先调用 `POST /api/v1/position-timing/alerts/{trigger_id}/claim`。服务端重验 card、quote、position、intent 和 `eligibility_identity` 后，只能追加一条 `ALERT_EMISSION_AUTHORIZED`。
- claim 成功后页面在本地 `aria-live` 提示区显示 toast；已经 claim 且仍有效的 edge 只显示非模态条目。首发默认不实现浏览器 Notification；它继续是可选后续增强，不得延迟主链路，也不得扩展为外部通知服务。
- quote 陈旧、未来戳、缺 open/current、源失败、持仓或 intent 漂移时禁止 toast，并在页面显示 typed 状态。

### 6.2 Outcome 与证据

- `POST /materialize` 扫描 `(card_id, 1/3/5/10/20)` 已到期 key，使用 `DAILY_OHLC_CONSERVATIVE_FILL_V1` 生成唯一 `OUTCOME_EVALUATED`。
- candidate 与 do-nothing 只评价动作造成的边际数量，逐腿使用 componentized cost；不可执行动作按 intention-to-treat 留零 lift 和 typed reason。
- terminal value 最多顺延 5 个交易日；卡片本身仍只在 T+1 有效，绝不顺延旧建议。
- 只有本次 due key 全部存在唯一 outcome（含 typed unavailable）才推进成功扫描水位。
- evidence API 同时报 matured、pending、unavailable、materialization-missing 计数；均值只用 paired matured，未到期缺行不得当零。
- 页面证据区显示规则基率和研究级总体状态，卡片本身继续固定 `RULE_BASED_RISK_MANAGEMENT`，不显示个股胜率或 46.9% MDE/oracle 比值。

### 6.3 首发代码完成条件

实现块二结束时，蓝图 §5.13 的 8 个 API、唯一页面、scope/current state、三类 artifact/event、全部 typed failure 和无订单边界必须一起可验证。此时才形成首发候选；L2 在首发时未实现不是缺口，因为蓝图把它定义为后续独立工作。该后续工作现已按 §0.4 完成，但不改变这一定义。

反模式保护：不得把 alert event 当成交或 user-seen 证据，不得用日线触价反推运行时 system eligibility，不得因 outcome 失败推进成功水位，不得为了定时物化引入 scheduler/worker。

## 7. 集中验证，不增加额外门禁

验证按四个能力组集中执行，不按 F-001～F-027 逐项建立审批或流水线：

| 能力组 | 覆盖重点 | 权威测试 |
|---|---|---|
| 卡片正确性 | discovery/analysis universe、scope、PIT、guard snapshot、方向性可交易性、lot、逐腿成本、typed 降级、首发时的 L2 contract 冻结值 | 块一：`backend/tests/position_timing/test_api.py`、`test_artifact_store.py`、`test_card_service.py`、`test_cost_policy.py`、`test_l2_population.py`、`test_pit_clock.py`、`test_policy_snapshot.py`、`test_tradability.py`、`test_universe.py`；块二直接在这些文件补 scope 与 outcome/alert 测试，不新增 scope 专用测试套件；后续 L2 专项矩阵见蓝图 §10.3 |
| 证据完整性 | immutable/append-only、并发幂等、两只时钟、五 horizons、公司行动、coverage state | `test_artifact_store.py`、`test_outcome_materialization.py` |
| 盘中提醒 | batch quote、5 分钟/30 秒、GET 零写、claim、already-alerted、多标签页、禁用分钟 fetch | `test_alerts.py` |
| 隔离与用户结果 | N0/既有表/订单零写、零业务反向依赖、页面卡片/状态/toast/无交易按钮 | `test_isolation.py`、`frontend/tests/position-timing/position-timing.spec.ts` |

实现期间只运行受影响的快速测试；候选完成后统一运行一次：

```powershell
python -m pytest backend/tests/position_timing -q
python -m nox -s position_timing_first_release
python scripts/aistock_feature_workflow.py validate --design docs/architecture/position_timing_advice_f2_redesign_20260903.md --tier F2
git diff --check origin/main...HEAD
```

`position_timing_first_release` 是一个集中入口：后端目标测试、前端 `tsc --noEmit`、目标 Playwright 与一次 frontend build；不再为 contracts、alerts、outcome 或每个 F-ID 新建 nox session。

提交 PR 前尝试一次基于现有 DEV 数据的正向 smoke：只读真实 legacy portfolio/watchlist/daily authorities，以临时 artifact root 生成真实 card set，并把同一 API payload 用于页面断言。若 DEV 当日确无符合 universe 的记录，如实记 `NO_ELIGIBLE_UNIVERSE`，不能用 fabricated success 替代；源码合同验证可以继续，但交付报告必须把“源码已验证”与“真实 DEV 正向样本尚缺”分开，不能把 mock/fixture 证据声明成真实业务成功。该状态披露不增加人工放行。

最终只执行开发规范 `DESIGN-COMPLIANCE-001` 的四项逐条检查：没有未批准的简化交付、没有静默错误、没有改变蓝图业务语义、没有私增门禁审批。PR 合入只等待仓库已存在且绑定当前 HEAD 的 `CI verdict`、`CodeQL verdict`、`AIstock Semgrep guardrails`、`Context, scope, and open-source tooling dry-run`；不增加第五项检查、双人复核或额外人工批准。

## 8. 合入与快速上线

设计依据：蓝图 §10、§12、F-026 与开发规范 `FEATURE-WORKFLOW-001`、`DESIGN-COMPLIANCE-001`。

### 8.1 增量 source PR

- 计划文档与块一实现代码使用当前同一 feature branch；块一通过直接验证后可建立第一个 source PR。
- 块二从块一合入后的最新 `origin/main` 开始，以第二个 source PR 补齐同一首发范围；不重新立项，也不增加一轮业务批准。
- 两个 PR 都直接引用 F2 蓝图和 Design Acceptance Matrix，按各自实际能力组汇总证据，不重复粘贴长日志，也不把块一 source merge 表述为首发完成。
- CI 若暴露真实缺陷，修复后重跑受影响组和最终集中入口；不因诊断性 check 增设永久门禁。

### 8.2 Production gates

首发固定：

```text
production_ddl_gate        = noop
production_dependency_gate = noop
```

本次没有数据迁移；外部通知明确在范围外。二者不另建 gate 名称或审批流程。

合入源码、生产运行激活、backend restart、frontend build/activation 和上线 readback 分别记录。生产 `8001/3000` 的启动、停止或重启只在用户明确授权后执行；这是进程权限边界，不是模型或业务准入门禁。

### 8.3 上线 readback

获得激活授权后只做一次收口：

1. 确认进程加载时冻结的 source commit 是已合入 `origin/main` 的 immutable commit，并由卡片绑定该值；不得在进程启动后用可变 worktree `HEAD` 冒充运行源码身份；
2. 只读调用 intents、current cards、evidence 与 alerts poll；另以 exact-idempotent `POST materialize` 仅写 timing-owned artifact；
3. 浏览器实际打开 `/position-timing`，确认卡片、typed 状态和无交易按钮；交易时段且存在合格 edge 时再验证 claim/toast，否则记录可复现的非交易时段状态；
4. 回读 timing artifact root，确认没有 QE/Selection/Advisory/Watchlist/Portfolio/Paper/MiniQMT 写入；
5. 分别报告 source merged、runtime activated、UI available、live edge observed 四种状态，不把未遇到触发价误报成系统失败。

回滚只回退 source/runtime 路由和页面；既有 immutable cards/events 保留以供审计，不删除、不改写。由于零 DDL、零依赖和零自动交易，回滚不需要数据库补偿或订单撤销。

反模式保护：不得把 merge、runtime activation、UI 可访问和真实触发观察合并成一个“完成”状态，不得因未遇到市场触价而重复发布或增加人工验收门。

## 9. 明确不进入首发的事项

- L2 Ridge/GBDT learnability pipeline 当时不进入首发；它后来仅以 §0.4 的离线任务完成。L3 model-assisted 卡和任何追加模型搜索仍未进入实现。
- 基于分钟 K 线生成新方向、新仓位或新风险信号。
- L4b-1 分钟执行窗口研究与 L4b-2 日内新方向当时均未进入首发；L4b-1 后续只以 §0.5 的离线任务实现，L4b-2 仍未进入。
- SSE、WebSocket、共享 poller、后台 worker、scheduler、队列或新数据库表。
- SmartMonitor engine/task/trade、Paper v2 position、MiniQMT position/order、外部通知服务。
- actual user execution event、券商成交回报接入与自动化下单。
- 第二套真实或虚拟持仓池、组合编辑器、scope 标签/分组/审批/批量工作流；普通待分析股票继续复用 confirmed watchlist。
- 以 MDE、sealed holdout、最低金额、成本敏感性、HMM/Selection availability 或样本数作为首发批准门槛。

首发上线后已用独立任务完成 L2；其 `INCONCLUSIVE` study 和无 selected model 不得反向削弱已经可用的 L1/L1a 人工建议产品。

## 10. 最终完成定义

只有在以下事实同时成立后，才可以报告“首发源码完成”：

- F2 蓝图 F-001～F-027 的首发适用项均回填真实实现和验证证据；在首发收口时，F-019/F-020 曾如实标明 contract-only 与 pipeline 后移，F-021 验证非门禁语义，F-022 曾标明 `SECOND_STAGE_DEFERRED_BY_APPROVED_SCOPE`。§0.4 完成后，F-018～F-021 已更新为真实 L2 evidence；§0.5 又只把 F-022 更新为 L4b-1 离线 pipeline/evidence，仍不得把 L3、L4b-2 或运行时分钟 policy 伪造成 implementation evidence。
- 8 个 API、一个页面、scope 当前态、三类 artifact/event、逐腿成本、typed errors、隔离性和无订单边界均已验证。
- 集中本地验证和当前 PR HEAD 的四项仓库稳定检查通过。
- `production_ddl_gate=noop`、`production_dependency_gate=noop` 有明确记录。

“首发源码完成”不自动等于“生产运行已激活”。生产进程和前端激活完成并读回后，再单独报告“已上线”；没有发生市场触价时，只能报告提醒链路可用，不能虚构真实 alert delivery。

## 11. 后续任务列表（执行顺序，不是新增门禁）

| 顺序 | task_id | 状态 | 一次性交付边界 |
|---:|---|---|---|
| 1 | `PT-NEXT-001` | `RUNTIME_VERIFIED` | 已在同一个块二任务中完成 F-027、L1a 与 prospective outcome，并通过 §0.3 的重启后收据；只增加批准的 `alerts.py`，未拆新阶段、审批或发布平台 |
| 2 | `PT-NEXT-002` | `AUDIT_COMPLETED_INCONCLUSIVE` | 已按冻结契约完成一次 L2 Ridge+GBDT learnability audit；Ridge `NEGATIVE`、GBDT/study `INCONCLUSIVE`、selected 0，无 runtime model，结果不阻塞 L1/L1a |
| 3 | `PT-NEXT-003` | `SELL_CONTRACT_LOCAL_VERIFIED` | L4b-1 pipeline 已完成可达性修订；首份双方向 request/bundle/receipt 仅保留为历史，修订后的 risk-exit SELL-only 正式凭证须绑定干净提交后生成；L4b-2 仍在范围外 |

`PT-NEXT-001` 已完成并通过运行态验收，`PT-NEXT-002` 已完成首轮离线审计；`PT-NEXT-003` 的历史双方向审计不再是研究权威，当前 SELL-only 契约须在干净提交后生成新凭证。现有证据仍不支持 L3 或静态分钟窗口进入运行卡片。prospective 样本量、MDE、HMM/Selection 可用性、最低金额或人工审批均不反向阻塞已实现的 L1/L1a；L4b-1 后续重跑是数据到位后的同入口操作，不新增阶段。
