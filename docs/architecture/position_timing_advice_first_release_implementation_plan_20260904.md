# 持仓与自选池择时建议系统首发实施计划

> 日期：2026-09-04
>
> 状态：`IMPLEMENTATION_READY_NOT_STARTED`
>
> F2 设计权威：`docs/architecture/position_timing_advice_f2_redesign_20260903.md`
>
> 适用分支：`feature/position-timing-first-release-20260904`
>
> 目标：用一个实现分支、一个 PR 和一次上线收口，交付完整的 L1 日频行动卡、L1a 盘中到价提醒与 prospective outcome 闭环。

本计划不建立第二套设计权威。任何业务语义以 F2 蓝图及其 F-001～F-026 为准；本文件只把已批准设计压缩为可连续执行的实施顺序。

## 1. 执行结论

首发只采用“两个连续实现块 + 一次上线收口”：

1. **实现块一：日频行动卡纵向闭环**。一次完成 contracts、规则、artifact、只读数据适配、API 与页面，使真实持仓和已确认自选能生成并展示 T+1 卡片。
2. **实现块二：提醒与结果证据闭环**。补齐一分钟轮询、原子 claim、toast、五个 horizon outcome、证据聚合和全量目标测试。
3. **上线收口：一次验证、一个 PR、一次激活**。更新 F2 验收矩阵，执行集中验证，等待仓库既有四项稳定 CI 判定后合入；生产端口激活仍按用户授权边界单独记录。

两个实现块是同一分支、同一 PR 内的开发顺序，不是两次立项、两道批准门禁或两次发布。实现块一完成后只能称为“内部可联调切片”，不得称首发完成。

首发不等待 L2 样本、MDE、sealed holdout、券商最低佣金核验或 HMM/Selection 可用性。它们都不能阻塞 L1/L1a。L2、L3 和第二阶段分钟执行研究不是本次实施的细分阶段，而是首发之后另行启动的工作。

首发复杂度预算如下。它用于约束实现面，不是新的审批门槛：

| 项目 | 首发数量 |
|---|---:|
| 功能性 backend service 文件 | 5 |
| backend router | 1 |
| frontend 页面 | 1 |
| 既有 runtime composition 文件改动 | 2 |
| API | 7 |
| 数据库 DDL/DML | 0 |
| 新依赖 | 0 |
| worker / scheduler / SSE / queue | 0 |
| source PR | 1 |
| 集中 nox 入口 | 1 |

## 2. 首发的完整结果

用户打开唯一的“持仓择时”页面后，应能完成以下闭环：

```text
只读持仓 + 已确认自选 + 用户仓位意图
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
| raw 日线 | `fetch_history_window_ts(..., freq="1d", adj="none")` | card 参考价、trigger、保守 fill 和费用使用 raw CNY；不得调用分钟历史 |
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
frontend/src/app/position-timing/page.tsx
```

职责保持在这五个服务文件内。首发不增加 `data_access.py`、worker、scheduler、消息队列、SSE、独立 API client、数据库 repository 或通用插件框架。

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

card 与 policy snapshot 使用 content-addressed no-replace 发布；event JSONL 在跨进程文件锁内执行幂等键检查、append、flush 与 fsync；intent 和 materialization state 使用临时文件加原子替换。current card set 不另建控制面 registry：按 `(position_source, decision_trade_date)` 目录解析恰好一个 immutable card set，出现多个不同 identity 直接返回 conflict。

## 5. 实现块一：日频行动卡纵向闭环

目标是尽早得到可以用真实只读数据联调的完整日频切片，但不单独发布。

设计依据：蓝图 §4、§5.1～§5.10、§5.13 与 F-001～F-011、F-017、F-024～F-025。

### 5.1 Contracts 与 artifact

- 一次定义 `PositionTimingIntentV1`、`PositionTimingCardV1`、trigger、cost、typed status、三类 event 与 API DTO，枚举和字段直接来自蓝图 §5。
- 同一 `contracts.py` 以不可变 schema/常量冻结 L2 v1 的 population、sampling、Ridge/GBDT 两个 model spec、唯一 monotone policy、两个 hypothesis、`economic_threshold_bps=0.0` 与 inference 分类字段；不创建 pipeline、模型 bundle、trial 或 registry 写入。这样首发 outcome 从第一天起具备未来 L2 所需字段，而不把 L2 实现塞进首发。
- 冻结 `PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1`、`EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1`、`PERSONAL_MANUAL_COMPONENT_COST_V1` 及 snapshot provenance/hash。
- 每张 card/outcome 绑定实际使用的 dataset、calendar、limit/ST/suspend、adjustment/corporate-action、guard、cost policy 与 source commit identity；缺任一强制 identity 时返回 typed unavailable，不用“当前默认值”补齐。
- 实现 card/policy immutable publish、intent 原子 current state、event append-only、materialization state 与三类幂等键。
- 费用用 `Decimal` 按组件和父订单计算；58,824 / 117,648 / 235,295 只作为派生断言，不写成业务常量。

### 5.2 Universe、规则与 materialize

- 分页读完自选；与 legacy holdings 合并并按 canonical symbol 去重。
- 用一次 position/intent snapshot 构造 card set；单只标的数据缺失只降级该卡，系统级 identity 冲突才使该 card set 失败。
- 先解析已完成 decision trade date 与 target T+1，再加载 raw 日线、calendar、limit/ST/suspend 和 adjustment identity。
- 对持仓先调用 exit guard，再按用户目标计算 delta；OPEN/ADD/非风险 REDUCE 只生成冻结条件分支，盘中不新建方向、阈值或数量。
- `POST /materialize` 每次同时做两件幂等工作：扫描到期 outcome；若具备已完成 T 日输入则生成 T+1 card set。盘中或源尚未成熟时返回 typed no-new-card 状态，不伪造空成功。

### 5.3 API 与页面

在同一个 router 完成 `GET /intents`、`PUT /intents/{symbol}`、`POST /materialize`、`GET /cards/current` 与 `GET /evidence`。这些路径均位于 `/api/v1/position-timing` 下。页面首次打开顺序固定为：

1. `POST /materialize`；
2. 并行 `GET /intents`、`GET /cards/current`、`GET /evidence`；
3. 显示行动卡、成本、数据状态与意图编辑；
4. intent 更新后只刷新 intent 状态，已签发 card 不被改写，新值进入下一张卡。

内部联调结果必须能回答：“今天为什么建议这只股票买、卖、持有或等待；计划数量和成本是多少；若不能做，缺的是什么。”

实现块一只运行 contracts、universe、PIT、tradability、policy snapshot、cost、artifact 与 card service 的快速测试。它不创建独立评审点，也不合入 main。

反模式保护：不得为数据读取另建 repository 层，不得让单只股票 unavailable 阻断其他卡，不得用当前共享 default factory 代替已冻结 snapshot，不得把 contract-only 的 L2 schema 接到运行路径。

## 6. 实现块二：提醒与 prospective outcome 闭环

设计依据：蓝图 §5.11～§5.13、§6、§9.3 与 F-012～F-021、F-023～F-026；其中 F-019～F-021 在首发只验证冻结 contract 和“非门禁”语义，不实现 L2 pipeline。

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

实现块二结束时，蓝图 §5.13 的 7 个 API、唯一页面、三类 artifact/event、全部 typed failure 和无订单边界必须一起可验证。此时才形成首发候选；L2 未实现不是缺口，因为蓝图已经把它定义为后续独立工作。

反模式保护：不得把 alert event 当成交或 user-seen 证据，不得用日线触价反推运行时 system eligibility，不得因 outcome 失败推进成功水位，不得为了定时物化引入 scheduler/worker。

## 7. 集中验证，不增加额外门禁

验证按四个能力组集中执行，不按 F-001～F-026 逐项建立审批或流水线：

| 能力组 | 覆盖重点 | 权威测试 |
|---|---|---|
| 卡片正确性 | universe、PIT、guard snapshot、方向性可交易性、lot、逐腿成本、typed 降级、L2 contract-only 冻结值 | `backend/tests/position_timing/test_contracts.py` 至 `test_card_service.py`；`test_l2_population.py`、`test_l2_model_specs.py`、`test_l2_inference.py` |
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

### 8.1 一个 PR

- 计划文档与实现代码使用当前同一 feature branch，不先发独立计划 PR。
- 实现期间可以有若干本地 commit，但只建立一个 source PR。
- PR 描述直接引用 F2 蓝图和回填后的 Design Acceptance Matrix，按能力组汇总证据，不重复粘贴长日志。
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

1. 确认运行源码 commit 等于已合入 immutable merge commit；
2. 只读调用 intents、materialize、current cards、evidence 与 alerts poll；
3. 浏览器实际打开 `/position-timing`，确认卡片、typed 状态和无交易按钮；交易时段且存在合格 edge 时再验证 claim/toast，否则记录可复现的非交易时段状态；
4. 回读 timing artifact root，确认没有 QE/Selection/Advisory/Watchlist/Portfolio/Paper/MiniQMT 写入；
5. 分别报告 source merged、runtime activated、UI available、live edge observed 四种状态，不把未遇到触发价误报成系统失败。

回滚只回退 source/runtime 路由和页面；既有 immutable cards/events 保留以供审计，不删除、不改写。由于零 DDL、零依赖和零自动交易，回滚不需要数据库补偿或订单撤销。

反模式保护：不得把 merge、runtime activation、UI 可访问和真实触发观察合并成一个“完成”状态，不得因未遇到市场触价而重复发布或增加人工验收门。

## 9. 明确不进入首发的事项

- L2 Ridge/GBDT learnability pipeline、L3 model-assisted 卡和任何模型搜索。
- 基于分钟 K 线生成新方向、新仓位或新风险信号。
- L4b-1 分钟执行窗口研究与 L4b-2 日内新方向。
- SSE、WebSocket、共享 poller、后台 worker、scheduler、队列或新数据库表。
- SmartMonitor engine/task/trade、Paper v2 position、MiniQMT position/order、外部通知服务。
- actual user execution event、券商成交回报接入与自动化下单。
- 以 MDE、sealed holdout、最低金额、成本敏感性、HMM/Selection availability 或样本数作为首发批准门槛。

首发上线并开始积累 prospective outcome 后，再用独立任务决定 L2；L2 的阴性、功效不足或延后不得反向削弱已经可用的 L1/L1a 人工建议产品。

## 10. 最终完成定义

只有在以下事实同时成立后，才可以报告“首发源码完成”：

- F2 蓝图 F-001～F-026 的首发适用项均回填真实实现和验证证据；F-019/F-020 分别标明首发已验证的 contract 与 `PIPELINE_DEFERRED_BY_APPROVED_SCOPE`，F-021 验证非门禁语义，F-022 标明 `SECOND_STAGE_DEFERRED_BY_APPROVED_SCOPE`，不得把后移部分伪造成 implementation evidence。
- 7 个 API、一个页面、三类 artifact/event、逐腿成本、typed errors、隔离性和无订单边界均已验证。
- 集中本地验证和当前 PR HEAD 的四项仓库稳定检查通过。
- `production_ddl_gate=noop`、`production_dependency_gate=noop` 有明确记录。

“首发源码完成”不自动等于“生产运行已激活”。生产进程和前端激活完成并读回后，再单独报告“已上线”；没有发生市场触价时，只能报告提醒链路可用，不能虚构真实 alert delivery。
