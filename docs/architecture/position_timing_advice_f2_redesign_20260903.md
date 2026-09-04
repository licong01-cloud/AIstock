# 持仓与自选池择时建议系统 F2 蓝图

> 版本：v2.0
> 日期：2026-09-03
> Feature tier：F2
> 状态：`IMPLEMENTATION_BLOCK_ONE_SOURCE_MERGED_BLOCK_TWO_AND_SCOPE_QUEUED`
> objective contract：`POSITION_TIMING_ADVICE_V1`
> decision use：`HUMAN_TRADING_ADVICE`
> 对照蓝图：`F:/Dev/AIstock_worktrees/stock-timing-strategy-blueprint-20260831/docs/architecture/stock_timing_strategy_system_blueprint_f2_20260831.md`
> 权威规范：`docs/standards/aistock_development_standard_v1.5_20260523.md`

`DESIGN_VERIFIED` 只表示对应设计条款已经闭合。实现块一已由 PR `#4277` 合入 `main`（merge commit `7c9fdd9cf86aa472fb2e84bac6211eb2378350ed`）；这只证明块一源码合入。L1a 提醒、prospective outcome、轻量分析范围管理、完整首发验证与生产激活仍未完成。源码合入不授权自动交易、数据库变更、进程控制或生产激活，也不得被表述为完整首发已经交付。

## 1. Background / 背景与结论

### 1.1 终极目标

本系统只为“全部当前持仓股 ∪ 用户从已确认自选池中显式选择的股票”解决一个问题：

> 在不替用户选股、不自动下单的前提下，于 T 日收盘后给出每只目标股票在 T+1 的明确行动、原始价格触发条件、建议数量或仓位、不可执行原因和成本；盘中只提醒日频卡片已经冻结的买卖点，由用户决定是否交易。

第一阶段必须形成“收盘出计划、盘中到价提醒、事后可评价”的完整人工决策闭环。荐股模块尚不成熟不构成依赖：股票身份与目标仓位来自用户持仓和显式意图，Selection 与 HMM 仅提供可缺失的上下文。

### 1.2 现有证据给出的方向

1. N2 exit hindsight oracle 显示较大的事后动作空间，但冻结 Ridge 政策没有捕获正值下界，且审计功效不足。这支持先交付规则型风险与执行纪律，并积累 prospective outcome，而不是先建复杂模型平台。
2. N2 entry 中 `FIXED_5_CASH` 与零不可分辨，`DYNAMIC_Q90_CASH` 显著为负。它们是研究臂，不是可直接搬入运行时的 guard。
3. N3 分钟信息集的 `selected_trial_count=0` 只否定该次 `ALPHA_RANKING` 横截面增量，不回答特定股票、同方向同规模下的执行时点问题。
4. 因此产品边界冻结为：第一批交付 L1 规则行动卡与 L1a 实时报价提醒；L2 数据契约同批冻结但训练管线后移；第二阶段分钟执行研究另行启动。

### 1.3 证据语义目录

本设计引用任何 N 线实验臂、政策名、`result_class` 或契约常量时，首次定义语义必须引用代码，首次引用数值必须引用 receipt；不得仅凭名称推断含义。后文可引用本目录锚点。

| evidence_id | 语义与已核事实 | 权威路径 |
|---|---|---|
| `EVID-N2-EXIT-ACTION` | 1930 个 baseline episode，1928 个可评价，其中 1349 个存在正的 hindsight intervention；oracle mean `386.6023 bps` | `F:/Dev/AIstock_model_artifacts/advisory_n2_entry_exit_formal_v1_20260902/action_audit_bundles/5c5946a7adfb1e41c5287d5781f240fa290d690c63be0679edb5b00960556f2c/exit_summary.json` |
| `EVID-N2-EXIT-LEARN` | Ridge 政策点估计 `-56.8073 bps`，95% CI `[-200.3336, 52.6247]`，`mde_bps=181.2860`，`oracle_capture_ratio=-0.14694`，`evidence_state=INCONCLUSIVE`，`result_class=EXPLORATORY`，`selected_trial_count=0` | `F:/Dev/AIstock_model_artifacts/advisory_n2_exit_learnability_formal_v1_20260902/exit_learnability_bundles/03d17a18f01af0c6d9055c1efb8f977bee1e26b19fa9cb823e79495071633937/learnability_receipt.json` |
| `EVID-ENTRY-SEMANTICS` | `FIXED_5_CASH` 是 `FIXED_GAP_5`（500 bps 上限）加 `CASH` 填充；可产生 `REDUCE`，`SKIP/WAITING` 时槽位留现金。`FROZEN_DYNAMIC` 是通用模式，本身不等于 Q90 | `backend/services/advisory_model_first/entry_guard_decision.py:17`、`:54`、`:239`、`:300`；`backend/services/advisory_model_first/entry_exit_formal_contracts.py:178`、`:184`；`backend/services/advisory_model_first/entry_exit_formal_pipeline.py:223`、`:598`、`:627` |
| `EVID-ENTRY-Q90` | `DYNAMIC_Q90_CASH` 才把通用 `FROZEN_DYNAMIC` 实例化为 `max(0, entry_gap_q90) × 10000 bps` 并采用 `CASH`；其 lift `-23.5491 bps`，CI `[-32.6066, -5.7124]`。该结果不得外推到所有 frozen-dynamic 定义 | `backend/services/advisory_model_first/entry_exit_formal_pipeline.py:401`；`F:/Dev/AIstock_model_artifacts/advisory_n2_entry_exit_formal_v1_20260902/action_audit_bundles/5c5946a7adfb1e41c5287d5781f240fa290d690c63be0679edb5b00960556f2c/entry_summary.json` |
| `EVID-ENTRY-FIXED5` | `FIXED_5_CASH` lift `2.4587 bps`，CI `[-0.7481, 7.3808]`；只能得出与零不可分辨，不能假定它机械增加或推迟一轮交易 | 同上 `entry_summary.json` |
| `EVID-N3-MINUTE` | candidate RankIC `0.090195`，parent/comparator `0.122839`；Top5 成本后净超额 `128.3145` 对 `443.6526 bps`；family-wise 四项均未通过，`selected_trial_count=0`，objective 为 `ALPHA_RANKING` | `F:/Dev/AIstock_model_artifacts/advisory_n3_minute_information_set_formal_v1_20260903/minute_information_set_bundles/0076a3a6c1e0fa40f6a29a73ab35c4015ae27431fb68989ce13fbb79e56a89f9/model_summary.json`、`learnability_receipt.json`、`request.json` |
| `EVID-N0-CONTROL` | 当前全局 N0 registry 为 25 条，其中 10 条 `CANDIDATE_MODEL`；route 绑定 `trial_registry_sha256`。既有 N1/N2 delivery 在 append 后立即重建 route，故择时不得写该控制面 | `backend/services/advisory_model_first/research_control.py:97`、`:876`；`backend/services/advisory_model_first/entry_exit_formal_pipeline.py:1032`；`backend/services/advisory_model_first/exit_learnability_pipeline.py:1442`；`F:/Dev/AIstock_model_artifacts/advisory_n0_research_control_20260830/trial_registry.jsonl`、`current_route.md` |
| `EVID-TDX-CONTRACT` | 批量报价上限 50，最大陈旧度 5 分钟，最大未来偏斜 30 秒 | `backend/services/simulation_data/contracts.py:47` |
| `EVID-TDX-MINUTE` | 当前 `fetch_minute_kline_tdx` 调用无日期/count 参数的 `/api/kline-all/tdx`，取得端点完整响应后在客户端筛 `trade_date`；第一阶段不得调用 | `backend/data_service/tdx_adapter.py:191` |
| `EVID-GUARD-DEFAULTS` | 当前 `PriceGuardPolicy` 与 `ExitGuardPolicy` 的 `rule_v1/rule_default` 默认值 | `backend/services/trading_core/price_guard.py:77`、`:81`、`:102`、`:112`；`backend/services/trading_core/exit_guard.py:28`、`:38`、`:42`、`:46`、`:50` |
| `EVID-PRICE-BASIS` | 仓库已有 `market.kline_daily_raw + market.adj_factor` 的 qfq 链路；数据服务在复权因子缺失时抛错而非默认为 1 | `backend/data_service/api.py:355`、`:379`、`:389`；`backend/data_service/qe_data_service.py:354`、`:471`、`:508`；`backend/qlib_exporter/db_reader.py:1026`、`:1075`、`:1099` |
| `EVID-FEE-MODEL` | ledger 以 `(order_id, symbol, side)` 累计多次 fill，但 `FeeModel` 把最低 5 元作用于总费率，不等于本设计“佣金下限、规费另加”的分项公式 | `backend/services/trading_core/ledger.py:92`、`:419`、`:458` |
| `EVID-BOARD-LOT` | 沪深主板和创业板 `(min=100, increment=100)`；科创板 `(min=200, increment=1)`；只有卖出全部剩余持仓时才允许零股尾单 | `backend/execution_algos/board_lot.py:36`、`:71`；`backend/services/trading_core/ledger.py:373` |
| `EVID-UNIVERSE` | 第一阶段 legacy 持仓来自 `portfolio_manager.get_all_stocks`；`notification_service.py` 位于仓库根目录；watchlist 生命周期为 `CANDIDATE/ENTERED/HOLDING/EXITED` | `backend/routers/portfolio.py:6`、`:8`、`:45`；`portfolio_manager.py:166`；`backend/services/advisory_lifecycle.py:29` |
| `EVID-MINUTE-SNAPSHOT` | 当前本地分钟快照为 `qlib_minute_authoritative_full_candidate_20240102_20260630`，SH/SZ、排除 BJ，日历 `2024-01-02 09:30:00` 至 `2026-06-30 15:00:00` | `/home/lc999/data/qlib_minute_bin/meta_export.json`、`calendars/1min.txt`；`F:/Dev/AIstock_model_artifacts/advisory_n3_minute_source_spike_v1_20260903/source_spike_receipt.json` |

### 1.4 数据可行性结论

- **L1/L1a 可实现**：持仓、自选、PIT 日线、复权因子、交易日、ST/停牌/涨跌停、board-lot 与 TDX quote 均已有本地 authority 或纯实现，不需要新表或荐股模块先成熟。
- **L2 工程上可实现、效果仍未知**：历史日频/QE 导出足以构造冻结 synthetic episode；第一批事件补足 deployment sizing 与 outcome。现有 exit receipt 只说明复杂模型不应先于数据证据，不等于 L2 必然有效。
- **分钟执行研究在现有覆盖内可实现**：当前快照支持 2024-01-02 至 2026-06-30 的离线 L4b-1；研究近期卡片前需补齐其后的分钟数据。第一阶段盘中提醒只用 quote，不受该缺口影响。
- **盘中新方向尚无依据**：现有 N3 不能回答个股执行择时，也不能支持 L4b-2；后者保持范围外。

## 2. Scope / 范围

### 2.1 第一批可运行交付

1. L1：每日规则行动卡，覆盖唯一持仓账本与显式选择的已确认自选。
2. 轻量 analysis scope：只负责从 confirmed watchlist 选择实际出卡标的；全部真实持仓始终覆盖。
3. L1a：前端打开期间的分钟级批量报价轮询与到价提示；只消费 L1 冻结方向、规模和触发价。
4. timing-owned 不可变卡片 artifact 与 append-only 事件：`CARD_ISSUED`、`ALERT_EMISSION_AUTHORIZED`、`OUTCOME_EVALUATED`。
5. outcome 物化与证据页面，持续积累 candidate 对 do-nothing 的 prospective 配对结果。
6. L2 的人口、抽样、字段、模型、政策与统计分类契约；不实现 L2 训练管线。

### 2.2 后续范围

- L2：在第一批 outcome 数据积累后单独实现一次预注册 Ridge + GBDT 可学性审计。
- L3：只有 L2 `effect_evidence=SUPPORTED` 的模型可让相应卡片标为 `MODEL_ASSISTED`。
- L4b-1：第二阶段单独研究同方向、同规模、同 horizon 下的分钟执行窗口。
- L4b-2：第二阶段盘中新方向；只有独立证据证明逐腿成本后正下界时才进入设计与实现。

## 3. Non-goals / 非目标

1. 不选股、不替代荐股模块，不要求荐股模块先成熟。
2. 不自动下单，不生成订单、StrategyPackage、可部署模型、运行时仓位权重或 MiniQMT/OMS 输入。
3. 不接 SmartMonitor 的任务库、engine、`auto_trade` 或 trade 记录。
4. 第一阶段不读取分钟 K 线、不从分钟数据生成方向或新交易。
5. 不写 QE、Selection、Advisory、Watchlist、Legacy Portfolio、Paper v2、MiniQMT 的既有表、artifact、registry、route 或调度状态。
6. 不新建数据库表，不注册既有调度器，不新建 worker，不控制进程。
7. 不引入 SSE、WebSocket、外部消息通道、TCN、Transformer、Offline RL、DeepLOB 或 HMM 四路消融。
8. 不把 L1 规则卡包装成已验证 alpha，不把研究级统计量包装成个股置信度。
9. 不把 sealed holdout、MDE、最低成交额或人工复核变成第一批、L2 或 L3 的批准门禁。

## 4. Architecture / 整体架构

### 4.1 轻量独立产品边界

`position_timing` 是独立 namespace 与 artifact owner，但不是第二套研究平台：

```text
Legacy Portfolio ────────────────┐
Active Watchlist ─> scope filter ┼─> position_timing service ─> immutable cards/events ─> position-timing page ─> human
Daily/PIT data ──────────────────┤             │                         │
guard pure APIs ─────────────────┤             └─ realtime quote poll ───┘
optional context ────────────────┘

QE / Selection / Advisory / Paper / MiniQMT  <── no reverse dependency, no timing write
global N0 registry / current_route           <── read-only background, zero write
```

边界选择兼顾两件事：独立 namespace、registry 与 artifact 防止污染既有模块；成本、交易日、涨跌停、PIT、guard 和板块交易单位仍调用现有纯实现或权威服务，避免复制算法。

### 4.2 最小实现面

第一批只使用一个服务包、一个路由和一个页面；文件是职责映射而非强制拆分数量。后续分析范围管理继续修改这些既有文件，不增加新服务文件或页面：

```text
backend/services/position_timing/
    contracts.py          # card, intent, event, API DTO
    policy.py             # guard snapshots, componentized cost policy, deterministic mapping
    artifact_store.py     # immutable cards, append-only events, atomic idempotency/coverage state
    service.py            # universe, L1 card, outcome materialization
    alerts.py             # quote eligibility and atomic alert claim
backend/routers/position_timing.py
frontend/src/app/position-timing/layout.tsx  # only imports the existing paper-v2 visual tokens
frontend/src/app/position-timing/page.tsx
backend/tests/position_timing/
frontend/tests/position-timing/
```

仅允许两处现有 composition root 做薄接线：`backend/main.py` 注册 router（统一 `/api/v1` 前缀），`frontend/src/lib/navigation/nav-groups.ts` 增加页面入口。新路由的 `layout.tsx` 只导入既有 `paper-v2.css`，不复制样式或承载业务逻辑。两处既有接线文件不得包含择时业务逻辑；除 composition root 外，任何既有业务模块不得 import `position_timing`。

L2 后续最多新增 `backend/services/position_timing/learnability_pipeline.py`，不放进 `advisory_model_first`，也不复用其全局 registry 路径。

### 4.3 数据和基础设施复用

| 能力 | 复用方式 |
|---|---|
| 持仓 | 第一阶段唯一 authority 为 `portfolio_manager.get_all_stocks()` / `app.portfolio_stocks`，只读 |
| 已确认自选 | 只读 watchlist；`advisory_enabled=true` 且 `lifecycle_status` 属于 `CANDIDATE/ENTERED/HOLDING` |
| 交易日 | `TradingCalendarStatusService`、`TradeCalendarProvider` |
| PIT 股票身份与日频行情 | L1 标的身份由 holdings/watchlist 与公共 symbol validator 决定，不把研究 universe 当产品准入门；raw 日线读取本地权威服务。历史 L2 才绑定 `aistock_equity_pit_canonical_v2` / `shsz_a_252td_st_delist_asof_v2` 与 QE 导出 manifest/H5/Parquet |
| 已确认终止上市 | 只读 `market.event_signal` 中符合 `issuer_bound_stock_delisting_v2` 的 timestamp-causal confirmed event；`market.stock_basic` 只在 `list_status=D AND delist_date <= decision_trade_date` 时作为已生效终态兜底；研究态 event overlay 不接入 runtime |
| 双价格/公司行动 | raw 使用 `market.kline_daily_raw`；跨日经济值复用 `market.adj_factor` / `AdjFactorProvider` 或已绑定同源 factor 的 Qlib 导出，按 `EVID-PRICE-BASIS` fail closed |
| 涨跌停、停牌、ST | `a_share_live_limit_rule.py` 与现有 daily-limit/suspend authority |
| 风险与价格规则 | 只调用 `trading_core.exit_guard.evaluate`、`trading_core.price_guard.evaluate` |
| 手数 | `execution_algos.board_lot.board_lot_rule/round_to_board_lot` |
| 实时报价 | TDX batch quote，按 `EVID-TDX-CONTRACT` 校验 |
| HMM/市场态势 | 只读、可缺失、仅 context |
| 研究证据 | 只读 N 线 receipt、QE 导出与 N0 历史计数；择时另有 registry |

数据优先级按用途冻结，禁止静默换源：

1. 当前用户状态只取 legacy portfolio、active watchlist、timing analysis scope 与 timing intent。
2. T 日产品卡的价格、ST、停牌和 limit 只取本地 PIT/daily authority；缺失即 typed unavailable，不用实时 T+1 报价倒填 T 日特征。
3. 历史 L2 优先消费带 manifest/hash 的 QE/Qlib/H5/Parquet 导出；如需用本地数据库补数据，先导出新的 immutable dataset identity，再进入同一次研究，不把 DB “最新值”直接混入旧 bundle。
4. T+1 运行观察只取绑定的 TDX batch quote；它不反向改写日频数据、card 或历史 dataset。

### 4.4 隔离矩阵

| 模块 | timing 可读 | timing 可写 | 反向依赖 |
|---|---|---|---|
| QE / Qlib exports | 已冻结 dataset、manifest、日频数据 | 否 | 禁止 |
| Selection | 可选排名上下文 | 否 | 禁止 |
| Advisory N 线 | 纯实现、receipt、N0 历史计数 | **全局 registry/current_route 零写入** | 禁止 |
| Watchlist | 已确认 active rows | 否 | 禁止 |
| Legacy Portfolio | 第一阶段唯一持仓 authority | 否 | 禁止 |
| Paper v2 | 仅未来显式 `PAPER_V2_PREVIEW` 独立运行 | 否 | 禁止 |
| MiniQMT | 可选只读 daily-limit authority | 否；不读其持仓、不下单 | 禁止 |
| SmartMonitor | UI 风格可参考 | 否；不调用 engine/task/trade | 禁止 |
| TDX | L1a 批量报价只读 | 否 | 禁止 |

同一运行只能选择一个 `position_source`。第一阶段正式模式固定为 `LEGACY_PORTFOLIO`；`PAPER_V2_PREVIEW` 若后续实现，必须使用独立 card set 和 artifact identity，绝不与 legacy 或 MiniQMT 持仓拼账。

### 4.5 六类 HARD 约束

1. **因果与新鲜度**：所有市场/特征输入满足 `feature_available_at <= decision_as_of`；报价陈旧、未来戳或源失败时禁止弹窗并返回 typed 状态。
2. **控制面隔离**：零既有表写入、零 DDL、零既有调度注册、零进程控制、N0 registry/current_route 零写入；除 backend/frontend composition root 的路由与导航接线外，既有业务模块不得反向依赖 timing。
3. **账本与身份唯一**：一次 card set 只有一个持仓 authority；canonical symbol 去重；card artifact 不可变并绑定全部输入与 policy hash。
4. **共享实现不漂移**：不修改 guard 默认值；timing 使用显式、版本化、hash-bound snapshot 调用同一 `evaluate` 实现。
5. **无交易副作用**：不产出订单、部署物、运行时权重或自动交易输入。
6. **证据完整性**：事件 append-only；同一幂等键最多一条；物化缺失、报价失败和字段缺失不得伪装成零结果或成功。

六类之外的 MDE、sealed holdout、成本敏感性、小额成交和 UI 弹窗去重均为报告、范围或 advisory，不得升级为批准门禁。

## 5. Contracts / 核心契约

### 5.1 决策时钟与 PIT

- T 日收盘后生成服务 T+1 的卡片；`decision_as_of = T 15:00:00 Asia/Shanghai`。
- `created_at` 可以晚于 15:00；市场、公告、Selection、HMM 等特征仍不得使用 `available_at > decision_as_of` 的内容。
- 用户持仓与意图另记 `position_snapshot_as_of` / `intent_snapshot_as_of`，只描述用户状态，不得作为晚到市场特征绕过 PIT。
- T+1 盘中只评价已冻结 card，不刷新方向、目标仓位或触发价。
- intent 在 card 首次签发后不创建或改写同一 decision date 的 card；新 intent 正式进入下一交易日。它可以立即使旧提醒失效：盘中若 legacy 持仓 hash 或 intent hash 已不同于 card snapshot，返回 `POSITION_SNAPSHOT_CHANGED/INTENT_SNAPSHOT_CHANGED` 并禁止旧卡弹窗，等待下一张卡重新决策。
- card 在 T+1 收盘失效。T+1 停牌、T+1 不可卖或方向性一字板记 `POLICY_FILL_UNAVAILABLE_EXPIRED`，不得把旧卡顺延到 T+2；T+1 收盘后由新卡重新决策。
- 若 suspend authority 证明标的在 T 日停牌且不存在 T 日 bar，card 只能使用 T 日之前最近一根可执行 raw close，并显式记 `DECISION_DAY_SUSPENDED_USING_LAST_EXECUTABLE_CLOSE`；非停牌标的的旧 bar 不得冒充 T 日成熟数据，也不得因此提前冻结 unavailable card set。
- 第一阶段不调用 `fetch_minute_kline_tdx`、`TdxCausalMinuteProvider` 或任何分钟 feature builder。

### 5.2 Universe、用户意图与去重

`PositionTimingUniverseV1 = holdings ∪ confirmed_watchlist`：

1. holdings 从 `LEGACY_PORTFOLIO` 读取，数量与成本以该账本为准。
2. confirmed watchlist 定义为 `advisory_enabled=true` 且 lifecycle 为 `CANDIDATE/ENTERED/HOLDING`；`EXITED` 排除。
3. 统一 canonical symbol 后去重。若同时是持仓与自选，`HOLDING` 身份优先，自选只追加 `source_provenance`，不得生成两张卡。
4. 未识别代码、BJ 或非 SH/SZ 标的第一阶段返回 `UNSUPPORTED_SYMBOL/UNAVAILABLE`，不得默认套 100 股或 10% 涨跌幅。
5. `PositionTimingIntentV1` 为 timing-owned 用户输入，至少含 `canonical_symbol`、`planned_full_notional_cny`、`desired_target_exposure` 与更新时间。允许 exposure 为 `{0, 0.25, 0.50, 1.00}`。
6. 持仓缺少 intent 时默认目标等于当前持仓，仅给风险型 `HOLD/EXIT`，并以 `pre_action_qty × reference_price_raw` 记录当卡的 `planned_full_notional_cny`；非持仓自选缺少 sizing intent 时仍生成 `WAIT` 卡并返回 `SIZING_INPUT_UNAVAILABLE`，不得伪造默认仓位，也不影响其他股票出卡。

#### 5.2.1 轻量分析范围管理（已设计，排入下一实施任务）

当前块一把全部 confirmed watchlist 纳入出卡集合，适合验证链路，但不适合长期人工关注大量标的。下一实施任务在不改变持仓 authority 的前提下，把“候选发现”和“实际择时分析”分开：

```text
PositionTimingDiscoveryUniverseV1 = holdings ∪ confirmed_watchlist
PositionTimingAnalysisUniverseV1  = holdings ∪ (confirmed_watchlist ∩ explicitly_selected_watchlist)
```

1. `PositionTimingUniverseV1` 保留为块一历史契约名，语义等同 discovery universe；它继续用于展示可选择股票、校验代码和读取 intent，不再等同于实际出卡集合。
2. 全部 `LEGACY_PORTFOLIO` 持仓始终进入 analysis universe。这是风险建议覆盖，不是第二个持仓池，也不允许 scope 记录覆盖数量、成本或持仓身份。
3. 仅自选标的只有在当前仍满足 confirmed watchlist 条件且被用户显式选择时才进入 analysis universe。没有 scope 状态时默认为 `NOT_SELECTED`，不生成该标的行动卡；这是显式 opt-in，不是数据失败或审批门禁。
4. 新增一个 timing-owned `PositionTimingAnalysisScopeV1` 当前态，字段只包含 `schema_version`、排序去重后的 `selected_watchlist_symbols`、timezone-aware `updated_at` 与 `scope_sha256`；hash 由除自身外的三个字段计算。它保存于既有 artifact root 的 `analysis_scope/current.json`；PUT 在同一文件锁内完成 read-modify-write 与原子替换，相同请求不更新时间也不重写。不建表、不写 Watchlist/Portfolio、不追加新事件、不另建 registry。
5. 未初始化 scope 使用确定性的 `EMPTY_EXPLICIT_SCOPE_V1` identity，等价于空的自选选择集；不得把全部 watchlist 当作静默兼容默认值。scope 文件损坏或 hash 不一致返回 `ANALYSIS_SCOPE_INVALID`，不得将其误读为空集合。
6. scope 中已经选择、但当前不再满足 watchlist lifecycle/advisory 条件的代码保留在用户当前态中，effective analysis 为 false；`GET /intents` 顶层 `scope_warnings[]` 返回 `SELECTED_SOURCE_INELIGIBLE` 与 canonical symbol。不得自动删除，也不得绕过 watchlist authority 出卡；重新满足条件后可恢复生效。
7. canonical symbol 同时成为持仓时仍按 `HOLDING` 身份唯一出卡；scope 只保留来源意图，不能生成第二张卡。对持仓请求关闭分析返回 typed `HOLDING_ALWAYS_INCLUDED`，不写一份虚假的 disabled 状态。
8. scope 更新只影响下一次尚未签发的 card set。已经签发的 card、当日 alert eligibility、后续 outcome 与历史事件保持不可变；API 返回 `effective_card_policy=NEXT_CARD_SET_ONLY`。
9. card set 的 `input_identity` 在未来实现后保留 `universe_identity_sha256` 并令其明确等于 effective analysis universe identity，同时嵌入完整 canonical `analysis_scope_snapshot`，并新增 `discovery_universe_identity_sha256`、用于校验嵌入内容的 `analysis_scope_snapshot_sha256` 与同值别名 `analysis_universe_identity_sha256`。旧 scope 当前态被改写后，历史 card set 仍能回读当时选择；现有 `PositionTimingCardSetV1` 的 content hash 已覆盖 `input_identity`，因此只增加向后兼容的 identity 材料，不复制 card schema、额外 immutable scope artifact 或 v2 控制面。
10. 不设置最大选择数、最低样本数、审批或人工放行。范围越大只影响页面信息量与只读计算量，不构成业务准入门禁。

API/UI 只做最小扩展：在现有 `GET /intents` 每行增加 `analysis_selected`、`analysis_effective`、`analysis_locked`、`analysis_reason_code`，顶层 `scope_warnings[]` 只列 scope 当前态中来源已失效的有限 symbol/reason 集合；GET 与缺省 scope 解析均保持零写入。新增唯一写接口 `PUT /api/v1/position-timing/analysis-scope/{symbol}`，请求体仅为 `analysis_enabled: bool`，响应返回 `UPDATED/UNCHANGED`、effective 状态、scope hash 与 `effective_card_policy=NEXT_CARD_SET_ONLY`。启用只接受当前 confirmed watchlist；取消允许作用于 scope 中已经存在但来源已失效的代码；持仓行显示“持仓始终分析”且不可关闭。不开新页面、不提供标签、分组、排序规则、批量工作流、虚拟持仓、组合编辑器或第二套通知设置。

集合与写入真值表冻结如下：

| 当前持仓 | active confirmed watchlist | scope selected | effective analysis | PUT 关闭/启用语义 |
|---|---|---|---|---|
| 是 | 任意 | 任意 | 是，`HOLDING_ALWAYS_INCLUDED` | 关闭返回同名 typed 状态且零写；启用为 `UNCHANGED` |
| 否 | 是 | 是 | 是，`SELECTED` | 可幂等关闭 |
| 否 | 是 | 否或未初始化 | 否，`NOT_SELECTED` | 可幂等启用 |
| 否 | 否 | 是（历史残留） | 否，`SELECTED_SOURCE_INELIGIBLE` | 允许关闭；禁止重新启用 |
| 否 | 否 | 否 | 不在 discovery/analysis universe | 启用返回现有 `SYMBOL_OUTSIDE_TIMING_UNIVERSE`，零写 |

所谓“专用持仓股票池”本轮明确不实现。若未来需要录入假设成本和数量，必须作为独立 `MANUAL_TIMING_PREVIEW` position source、独立 card set/artifact identity 另行设计，绝不能与 `LEGACY_PORTFOLIO` 拼账；普通“我想分析这只股票”应先进入现有 confirmed watchlist，再由 analysis scope 选择。

### 5.3 行动卡

`PositionTimingCardV1` 至少冻结：

| 字段组 | 必备字段 |
|---|---|
| 身份 | `card_id`、`card_set_id`、`canonical_symbol`、`primary_source_role`、`source_roles`、`position_source` |
| 时钟 | `decision_trade_date`、`decision_as_of`、`target_trade_date`、`valid_until` |
| 当前与目标 | `pre_action_qty`、`pre_action_exposure`、`planned_full_notional_cny`、`desired_target_exposure`、`requested_delta_qty`、`requested_leg_notional_cny` |
| 建议 | `action=OPEN/ADD/HOLD/REDUCE/EXIT/WAIT/UNAVAILABLE`、`execution_window=AT_OPEN/ON_PRICE_TRIGGER/WAIT_UNAVAILABLE` |
| 触发 | `triggers[]`；每项含 `trigger_id`、side/operator/raw price、共享 guard action/reason 条件、分支对应的 `planned_delta_qty`、`planned_leg_notional_cny` 与合法 target exposure；另含 `reference_price_raw` |
| 可执行性 | `tradability_status`、`st_flag`、`t1_sellable_qty`、`limit_up_raw`、`limit_down_raw`、typed reason codes |
| 成本 | 买卖逐腿估算、parent-order 情景、`SMALL_TRADE_COST_HEAVY`、cost policy identity |
| 上下文 | `holding_trading_days`、`holding_age_bucket`、`market_regime=DOWN/UP_OR_FLAT/UNKNOWN`，已确认退市 flag/status，以及 Selection/HMM status 与 evidence ref；缺失必须 typed |
| 证据 | `evidence_tier`、`historical_base_rate_status`；L1 固定 `RULE_BASED_RISK_MANAGEMENT` |
| 可复现性 | dataset、calendar、limit、delist、intent、guard snapshot、cost policy、code commit 的 hash/provenance；card set 同时保存完整 input/policy identity 与 `cards_sha256` |

卡片本身不显示 MDE/oracle 比值、L2 总体置信区间或“个股胜率”。这些研究级结论只在页面证据区展示。

`holding_age_bucket` 只用于展示与 deployment weighting，边界冻结为 `AGE_0/AGE_1_3/AGE_4_5/AGE_6_10/AGE_11_20/AGE_21_PLUS/UNKNOWN`；`market_regime` 使用与 L2 相同的 benchmark 日频规则，不使用 HMM state。二者不得因样本结果事后改桶。

### 5.4 L1 决策映射

L1 只做确定性风险与执行映射，不从历史 PnL 回选规则：

1. 用户 intent 决定希望向哪个 exposure 移动；Selection 不决定 universe，也不替用户发起新股票方向。
2. 持仓先调用冻结的 `ExitGuardPolicy`。硬止损、可用的 alpha decay 或 timestamp-causal 已确认终止上市事实可把用户目标覆盖为 `EXIT`；仅自选标的命中已确认终止上市时为 `CONFIRMED_DELISTING_BUY_UNAVAILABLE`。Selection 缺失时 alpha-decay 不运行，不能用默认排名代替；T+1 不可卖时为 `WAIT_UNAVAILABLE`。
3. T 日只使用已知的 signal close、limit 与 snapshot 生成有限个条件分支；已验证的 T 日停牌按 §5.1 使用更早的最近可执行 close。T+1 报价到来后才调用同一个冻结 `PriceGuardPolicy.evaluate`，并以 evaluator 的 action/reason 在卡片内选择唯一分支；不能把相互重叠的价格上界单独解释为多个同时成立的建议。buy-side guard 的 `REDUCE` 分支表示缩小本次 `OPEN/ADD` 数量，不表示卖出现有持仓。
4. 冲突顺序固定为：方向性可执行性 > exit 风险 > 用户目标移动 > price guard 规模调整。
5. `OPEN/ADD` 卡预先冻结 green/yellow/skip 对应的 trigger 与合法数量，使用 `ON_PRICE_TRIGGER`；风险型 `EXIT` 使用 `AT_OPEN`，非风险型 `REDUCE/EXIT` 可使用 `ON_PRICE_TRIGGER`；`HOLD/WAIT/UNAVAILABLE` 使用 `WAIT_UNAVAILABLE` 并给出 no-trade/原因码。
6. T+1 runtime 只选择已冻结 trigger branch 或返回不可执行，不能创建新方向、新价格阈值或新规模；因此 L1a 是提醒器而不是第二个决策器。
7. Selection 缺失时禁用本次 alpha-decay 分支并标 `selection_context_status=UNAVAILABLE`，硬止损和用户意图仍可生成；HMM 缺失不改变方向。

`HMMContextV1` 可同时携带市场态势与板块轮动摘要、`as_of`、source artifact/hash 和 `hmm_context_status=AVAILABLE/UNAVAILABLE/NOT_APPLICABLE`。第一阶段只展示该字段；它不改变 action、target、trigger 或整卡 status。L2 v1 feature vector 也不含 HMM；若未来要验证其增量，只能在更新本设计后作为一个冻结 feature block 假设，而不是四路消融。

### 5.5 冻结 guard snapshot

运行 authority 唯一为共享 `evaluate`，但每张卡绑定 timing-owned 显式快照，禁止依赖未来会变化的 default factory。

`PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1`：

- 通用：`contract=execution_price_guard_v1`、`enabled=true`、`mode=rule_v1`、`price_basis=raw`、`guidance_status=rule_default`。
- signal reference：buy/sell=`signal_close`，intraday=`arrival_price`。
- buy：`max_open_gap_bps=300`、`yellow_open_gap_bps=150`、`yellow_size_multiplier=0.5`、`max_chase_bps=100`、`yellow_chase_bps=50`、`near_limit_up_skip_bps=80`、`allow_partial=true`。
- breakout addon：`enabled=false`、`require_momentum_regime=true`、`min_score_bucket=top5`、`dist_to_limit_up_lt_bps=200`、`min_volume_ratio_open=1.5`、`add_size_multiplier=0.5`、`min_fill_probability=0.6`。
- sell：`rebalance_max_slippage_bps=150`、`risk_exit_max_slippage_bps=500`、`near_limit_down_rebalance_skip_bps=80`、`allow_partial=true`。

`EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1`：

- 通用：`contract=exit_guard_v1`、`enabled=true`、`mode=rule_v1`、`price_basis=raw`、`t1_handling=defer_to_next_tradable_day`、`guidance_status=rule_default`。
- stop loss：`enabled=true`、`max_loss_bps=600`、`soft_loss_bps=400`、`volatility_multiple=2.5`、`reference=actual_entry_cost`。
- take profit：`enabled=false`、`take_profit_bps=1200`、`trailing_stop_bps=500`。
- alpha decay：`enabled=true`、`rank_drop_below=top40%`、`confirm_days=2`。
- time stop：`enabled=false`、`max_holding_days=10`。

当前 `evaluate` 未必消费 snapshot 中每个保留字段；字段完整序列化用于防漂移，不得据字段名宣称不存在的运行效果。

snapshot 中共享 `t1_handling=defer_to_next_tradable_day` 只描述 guard 在当日不可卖时返回 defer 语义；position-timing 将其映射为本卡 `WAIT_UNAVAILABLE/POLICY_FILL_UNAVAILABLE_EXPIRED`，由下一交易日的新卡重新评价，绝不携带旧 card。这保留共享 guard 的返回含义，同时服从本产品 T+1 单日有效期。

快照级 provenance 固定含：`source_module`、`source_symbol`、`source_repository_commit=f870debe3b963d9d3d41ce9663db9722af921e80`、`source_captured_at`、`source_defaults_sha256`、`timing_policy_sha256`。未来共享默认值变化只能创建 snapshot v2，历史卡继续绑定 v1。

`entry_guard_decision` 的 `FIXED_*` / `FROZEN_DYNAMIC` 仅作 `EVID-ENTRY-*` 研究证据，不叠加为第二套运行时 guard。

### 5.6 双价格与公司行动

价格用途必须分离：决策触发、涨跌停判断、模拟成交与费用一律使用 raw CNY；跨日收益、趋势和经济结果使用带明确 identity 的 total-return/复权口径。不得用 raw 价格比直接跨越除权除息日，也不得在 adjustment factor 缺失时默认为 `1.0`。

| 用途 | 冻结口径 |
|---|---|
| card 参考价、trigger、limit、quote | raw CNY |
| 成交名义金额与逐腿费用 | raw CNY × 当时合法数量 |
| T 日跨日特征 | `available_at <= decision_as_of` 的 qfq/total-return source identity |
| prospective/L2 经济结果 | raw fill 加可复现的公司行动数量/现金流路径，或与其等价且 hash-bound 的 total-return valuation |

`OUTCOME_EVALUATED` 绑定用于该 horizon 的 corporate-action/adjustment source、版本、覆盖区间与 hash。该信息属于事后标签，可在 outcome materialization 时使用当时已成熟的权威数据，但不得反向进入旧 card。缺失、版本冲突或无法把 raw fill 与 terminal valuation 对齐时记 `UNAVAILABLE_AT_HORIZON`。

块一日频卡不计算跨日收益，因而 card 的 `adjustment_identity` 固定为 `NOT_APPLICABLE / BLOCK_ONE_CARD_USES_RAW_PRICE_ONLY`；复权因子缺失不得阻塞 L1 出卡。只有实现块二的 outcome 评价才读取并强制绑定 adjustment/corporate-action identity。

### 5.7 可交易性、ST 与交易单位

- ST 是涨跌幅比例与风险属性，不等于不可交易。
- 一字涨停只阻断买入；一字跌停只阻断卖出。相反方向不得被笼统判 `UNAVAILABLE`。
- T 日已验证停牌且有更早可执行 close 时保留风险方向并要求 T+1 重验；连最近可执行 close 也缺失、涨跌停权威缺失或 T+1 可卖数量不足时返回独立 typed reason。
- 买入用 `round_to_board_lot(..., side="BUY")` 向下取合法数量。
- 任一板块卖出全部剩余持仓时可按实际剩余数量一次退出；否则沪深主板/创业板买入与部分卖出按 100 股倍数。
- 科创板买入与部分卖出至少 200 股，超过 200 后可按 1 股递增；不足 200 的余额只通过上述全量退出处理。
- `SMALL_TRADE_COST_HEAVY` 使用手数处理后的预计实际逐腿成交额判定；满仓金额档位只作预筛。

交易单位依据为[深交所交易规则（2026 年修订）](https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf)、[上交所交易规则（2026 年修订）](https://www.sse.com.cn/lawandrules/sselawsrules2025/stocks/exchange/c/c_20260424_10816482.shtml)及[上交所科创板交易规则说明](https://edu.sse.com.cn/tib/ysptj/c/4869120.shtml)。

### 5.8 分项成本政策

冻结 `PERSONAL_MANUAL_COMPONENT_COST_V1`：

```text
net_commission_rate        = 0.000085   # 0.85 bps，双边
minimum_commission_cny     = 5
transfer_fee_rate          = 0.000010   # 0.10 bps，双边
regulatory_fee_rate        = 0.000020   # 0.20 bps，双边
handling_fee_rate          = 0.0000341  # 0.341 bps，双边
stamp_duty_sell_rate       = 0.000500   # 5.00 bps，仅卖出
commission_quote_basis     = NET_EX_REGULATORY_FEES
min_commission_scope       = PER_PARENT_ORDER
min_commission_scope_verification = BROKER_UNVERIFIED
assumed_parent_order_count = 1
```

对每个用户实际提交的父订单 `j`：

```text
commission_j = max(5, 0.000085 × notional_j)
common_regulatory_j = 0.0000641 × notional_j
buy_cost_j  = commission_j + common_regulatory_j
sell_cost_j = commission_j + common_regulatory_j + 0.0005 × notional_j
```

不触发最低佣金时，买入单边 `1.491 bps`，卖出单边 `6.491 bps`，等名义金额完整往返 `7.982 bps`。已持仓股票的历史买入腿是沉没成本，EXIT 边际比较只计未来实际腿；只有政策确实形成“卖出 + 再买回”循环时才使用完整往返尺度。

`FeeModel` 只复用 parent-order 多 fill 聚合身份，不复用其数值公式。把 `1.491/6.491 bps` 直接填入现有 `FeeModel` 会让 5 元下限覆盖规费，产生错误拐点。timing 必须在自身 `policy.py` 分项计算；只有 Paper/LocalSim 也绑定同一 componentized policy identity 时，结果才可直接数值比较。

冻结字段还包括 `fee_schedule_as_of=2026-09-03`、`fee_source_refs`、`cost_policy_version`、`cost_policy_sha256`。净佣与 5 元下限来自用户账户报价；真实券商对父订单/部分成交的结算归集仍披露 `BROKER_UNVERIFIED`，不阻塞设计或建议，也不得宣称估算就是最终交割费用。卡片/UI 固定标注“按单一委托估算”，并提供下述 2/3 个父订单敏感性，不把基准口径写成已核券商结算事实。

官方规费来源：[上交所收费一览表（2026 年 1 月）](https://www.sse.com.cn/services/tradingservice/charge/ssecharge/)、[深交所收费及代收税费标准（2026 年 1 月）](https://www.szse.cn/marketServices/deal/payFees/)、[中国结算上海市场收费表](https://www.chinaclear.cn/zdjs/fbzyls/202506/9d22b74d9f2e40edb67b44d1f6596f18/files/%E4%B8%8A%E6%B5%B7%E5%B8%82%E5%9C%BA%E8%AF%81%E5%88%B8%E7%99%BB%E8%AE%B0%E7%BB%93%E7%AE%97%E4%B8%9A%E5%8A%A1%E6%94%B6%E8%B4%B9%E5%8F%8A%E4%BB%A3%E6%94%B6%E7%A8%8E%E8%B4%B9%E4%B8%80%E8%A7%88%E8%A1%A8.pdf)、[中国结算深圳市场收费表](https://www.chinaclear.cn/zdjs/fbzyls/202506/ab6384ba25514554a7eceaee3e521032/files/%E6%B7%B1%E5%9C%B3%E5%B8%82%E5%9C%BA%E8%AF%81%E5%88%B8%E7%99%BB%E8%AE%B0%E7%BB%93%E7%AE%97%E4%B8%9A%E5%8A%A1%E6%94%B6%E8%B4%B9%E5%8F%8A%E4%BB%A3%E6%94%B6%E7%A8%8E%E8%B4%B9%E4%B8%80%E8%A7%88%E8%A1%A8.pdf)、[证券交易印花税减半公告](https://shanghai.chinatax.gov.cn/zcfw/zcfgk/yhs/202308/t468451.html)。

### 5.9 最低佣金拐点与敏感性

阈值不得写成业务常量，必须用 `Decimal` 从组件派生，并只在最终人民币元处向上取整：

```text
planned_full_notional_threshold(f)
  = ceil(minimum_commission_cny / (net_commission_rate × f))
```

| exposure step `f` | 满仓金额基准阈值 |
|---|---:|
| `1.00` | 58,824 元 |
| `0.50` | 117,648 元 |
| `0.25` | 235,295 元 |

`235,295 × 0.25 × 0.000085 = 5.00001875`，而 `235,294` 对应 `4.99999750`；旧值 235,296 是“先 ceil 再乘 4”的双重取整，已废止。

UI 的成本友好档位建议为：满仓金额至少 235,295 元可显示四档；117,648 至 235,294 元优先 `{0, 0.50, 1.00}`；58,824 至 117,647 元优先 `{0, 1.00}`；更低金额仍可给 `{0, 1.00}`，但必报实际成本。该建议不删除用户已经冻结的 exposure intent，也不阻塞卡片。

等名义金额、每腿一个父订单的说明性成本：

| 每腿名义金额 | 买入成本 | 卖出成本 | 往返成本率 |
|---:|---:|---:|---:|
| 5,000 | 5.3205 元 | 7.8205 元 | 26.282 bps |
| 10,000 | 5.6410 元 | 10.6410 元 | 16.282 bps |
| 20,000 | 6.2820 元 | 16.2820 元 | 11.282 bps |
| 30,000 | 6.9230 元 | 21.9230 元 | 9.615 bps |
| 50,000 | 8.2050 元 | 33.2050 元 | 8.282 bps |
| 58,824 及以上 | 按 1.491 bps | 按 6.491 bps | 7.982 bps |

卡片仍输出低于阈值的建议，同时标 `SMALL_TRADE_COST_HEAVY`、绝对费用与“可考虑合并委托”。不得用该标签拦截建议。

每个 receipt 固定报告三个非新增 trial 的费用情景：

- `ONE_PARENT_ORDER_BASE`
- `TWO_PARENT_ORDERS_NEAR_EQUAL_LEGAL_QTY`
- `THREE_PARENT_ORDERS_NEAR_EQUAL_LEGAL_QTY`

三者保持相同总合法数量和方向，按板块 increment 尽量等分；卖出零股尾数只进入最后一个全量退出父订单。若 base 情景为 `SUPPORTED`，但任一拆单情景的 adjusted lower bound 不再为正，只加 `COST_ASSUMPTION_SENSITIVE`，不改变 effect 分类、不构成门禁。

### 5.10 Artifact、事件与幂等

timing 唯一写入根为：

```text
F:/Dev/AIstock_model_artifacts/position_timing_advice_v1/
    intents/
    policy_snapshots/
    cards/<decision_trade_date>/<card_set_id>/card_set-<artifact_sha256>.json
    events/<yyyy-mm>.jsonl
    research_registry/timing_trial_registry_v1.jsonl
    materialization_state.json
```

- cards 与 policy snapshots content-addressed、不可变、hash-bound；card set 保存完整 input/policy identity，并用 `cards_sha256` 检出卡片内容篡改。
- event log append-only，使用文件锁和 fsync；不改写旧事件。
- intents 与 `materialization_state.json` 是 timing-owned 当前态，使用临时文件 + 原子替换；它们不冒充 append-only 证据。
- `CARD_ISSUED` 幂等键为 `card_id`。
- `ALERT_EMISSION_AUTHORIZED` 幂等键为 `(card_id, trigger_id)`。
- `OUTCOME_EVALUATED` 幂等键为 `(card_id, horizon_trading_days)`。
- `(position_source, decision_trade_date)` 只有一个 current card set。相同 semantic identity 重试返回原 artifact；首次发布后若同一逻辑键出现不同 input/policy hash，返回 `CARD_SET_IDENTITY_CONFLICT`，不得改写旧卡或静默切换 current 指针。
- POST 副作用只允许落上述 timing-owned 路径；相同 input/policy identity exact-idempotent，不得写 N0 或既有模块。

### 5.11 Outcome 评价

第一批冻结并实现：

```text
evaluation_horizons_trading_days = (1, 3, 5, 10, 20)
primary_horizon_trading_days     = 20
terminal_exit_max_defer_trading_days = 5
```

两只时钟不得混用：

- 行动卡只在 T+1 有效，绝不顺延。
- outcome 的终值在 nominal horizon 不可取得时，最多向后找 5 个交易日；这只延长标签终值观察，不延长建议。

由于 `ON_PRICE_TRIGGER` 可能发生在 T+1 开盘后，outcome 将 `target_trade_date` 计为 holding session 1，并以第 h 个 session 的官方 raw close 作为 terminal valuation 时点，再按 §5.6 纳入公司行动路径；这与现有 N 线 T+20 open 标签价格端点不同，分叉理由是避免 h=1 终值先于盘中触发。若 OPEN/ADD 新增数量在 h=1 仍受 T+1 卖出锁定，terminal liquidation proxy 顺延到首个可卖交易日并记 `DEFERRED_THEN_MATURED/TERMINAL_T1_LOCKED`。日期、价格端点、数量/现金流路径和分叉理由必须写入 receipt。

每个 `(card_id, horizon)` 追加一个 `OUTCOME_EVALUATED`，至少含：

- `policy_fill_status=FILLED/SKIPPED_BY_GUARD/POLICY_FILL_UNAVAILABLE_EXPIRED/NO_ACTION`
- `maturity_status=MATURED/DEFERRED_THEN_MATURED/UNAVAILABLE_AT_HORIZON`
- selected `trigger_id`、`planned_delta_qty`、effective target exposure 与 fill raw price/time policy；无动作时显式为空并给 reason
- nominal/effective terminal trade date、deferred trading days、typed reason
- candidate 与 do-nothing 的数量/现金流路径、逐腿成本、gross/net CNY 与 lift bps
- card、dataset、calendar、limit、board-lot、corporate-action/adjustment、cost-policy hash

T+1 因方向性不可交易而未执行时采用 intention-to-treat：candidate 自该点继续 do-nothing 路径，paired lift 为零并保留 `POLICY_FILL_UNAVAILABLE_EXPIRED`，不得删除失败动作。输入/终值本身无法评价时才是 `UNAVAILABLE_AT_HORIZON`。

paired path 只评价卡片动作造成的边际数量，不重算未受动作影响的共同持仓：

- `planned_delta_qty > 0`：candidate 在冻结 fill 买入该数量并持有到 terminal proxy，do-nothing 不买；candidate 计实际买入腿和 terminal 估算卖出腿。
- `planned_delta_qty < 0`：candidate 在冻结 fill 卖出绝对数量并持有现金，do-nothing 持有同一数量到 terminal proxy 后卖出；两边只计各自未来卖出腿，历史买入腿均为沉没成本。
- `planned_delta_qty = 0`：两条路径相同，lift 为零但仍保留 typed action/fill 状态。
- v1 现金收益固定为 0；分红、送转和拆并股由 §5.6 的公司行动路径进入持股侧，不得遗漏或通过 raw 价格比重复计算。

第一批用 `DAILY_OHLC_CONSERVATIVE_FILL_V1` 物化 prospective policy，而不是用 alert 是否送达决定成交：

- `AT_OPEN` 在方向可交易时取 T+1 raw open；风险退出不因 rebalance 最低卖价而回退。
- buy `price <= trigger` 若 open 已满足则取 open；否则仅当 raw low 证明触价时按冻结 trigger price 成交。
- sell `price >= trigger` 若 open 已满足则取 open；否则仅当 raw high 证明触价时按冻结 trigger price 成交；其他 operator 必须在 outcome policy 中逐项显式映射，禁止笼统交换 high/low。
- 一根日线内若多个规模分支都可能成立但顺序不可辨，选择最小合法成交数量和对 candidate 最不利的允许价格；reason 记 `INTRADAY_SEQUENCE_UNOBSERVED_CONSERVATIVE_FILL`。
- alert event、页面是否打开和轮询是否观察到只衡量 delivery/system observation，不进入该 policy-fill 判定。

该规则只读成熟后的日线 OHLC，不引入分钟信号；若以后以分钟路径替换，必须成为新的 outcome policy version，不能改写旧事件。

读取态分四类：

1. 尚未到 expected maturity：`PENDING_DERIVED`。
2. 已到期但晚于成功扫描水位：`PENDING_MATERIALIZATION`。
3. 已被成功扫描覆盖、应有事件但缺失：`MATERIALIZATION_MISSING`。
4. 已有事件但没有可用终值：事件内 `UNAVAILABLE_AT_HORIZON`。

页面首次打开时调用同一个 exact-idempotent materialize POST，顺带扫描全部到期 key；不建 scheduler。独立 operational state 至少含 `last_successful_materialization_scan_through_trade_date`、`last_run_at`、`expected_due_count`、`accounted_outcome_count`。只有本次范围内全部 due key 都已有唯一 `OUTCOME_EVALUATED`（包括 typed unavailable）才推进水位；数据尚未成熟、计算失败、写入失败或幂等冲突均不推进。

聚合必须同时报告 matured、pending、unavailable、materialization-missing 计数；均值只使用 paired matured。全体 card 的 intention-to-treat 与“至少一个冻结 trigger branch 的 `planned_delta_qty != 0`”的 intervention-intent 子集分开报告，避免大量 `HOLD` 的零 lift 稀释动作效果。未到期表现为“没有事件行”，读取器不得把缺失当作零 lift。

卡片基率只取相同 `primary_source_role + action_side + holding_age_bucket` 的 paired matured intervention-intent 样本，展示 `N`、正 lift 数、正 lift 比例与中位 lift；`N < 30` 时固定 `INSUFFICIENT_HISTORY`。该阈值只控制展示措辞，不阻塞出卡或提醒。

第一批不实现 `actual_user_execution_event`。prospective outcome 衡量冻结政策的可执行反事实，不声称用户实际按建议成交。

### 5.12 L1a 报价提醒

1. 页面每 60 秒调用只读 GET；服务按最多 50 只分批取 TDX quote。
2. GET 返回 `system_edge_eligibility`、quote 时间、source、staleness、`already_alerted` 与 `eligibility_identity`；后者 hash-bound 到 card/trigger、quote payload、evaluation time 及当前 position/intent hash。GET 不得写事件。
3. 超过 5 分钟、未来偏斜超过 30 秒、源失败、字段不全或当前 position/intent hash 已偏离 card 时，不弹窗；API/UI 显式返回 `QUOTE_STALE/QUOTE_FUTURE_SKEW/QUOTE_UNAVAILABLE/POSITION_SNAPSHOT_CHANGED/INTENT_SNAPSHOT_CHANGED`。
4. 新 eligible trigger edge 先调用 atomic claim POST。POST 必须重验 eligibility identity、card/current position/current intent 与报价年龄/future-skew，不重新解释方向、规模或阈值；成功追加唯一 `ALERT_EMISSION_AUTHORIZED` 后返回 `granted=true`，页面再弹出 toast。
5. 事件只保存重算 eligibility 所需的有界标量：`card_id`、`card_artifact_sha256`、`trigger_id`、`quote_price_raw`、`quote_open_raw`、`quote_observed_at`、`alert_evaluated_at`、`quote_source`、`staleness_state`、可选 `quote_age_seconds`、`user_seen_evidence=false`；不保存无界上游 payload。某 trigger 所需的 open/current 字段缺失时不得 claim。
6. 事件语义是“服务端授予一次提醒发送权”，不是 user-seen delivery receipt；第一批不加 ACK。
7. 若 claim 后页面崩溃，后续 GET 仍返回 `already_alerted=true` 的可执行边，页面以非模态条目展示但不重复弹窗。artifact at-most-once 是 HARD；UI 弹窗 at-most-once 仅 ADVISORY。
8. 浏览器 Notification 是可选增强；页面 toast 是主通道。`notification_service.py` 虽存在，外部投递配置与健康未验证，第一批完全不依赖。

三种 estimand 必须分开：

- `market_touch_opportunity`：事后日线/分钟线判断市场是否触及冻结价。
- `system_edge_eligibility`：页面实际打开、轮询实际观察且报价合格时是否出现新边；第一阶段不承诺离线完整重建。
- `alert_emission`：是否成功取得 `ALERT_EMISSION_AUTHORIZED`。

缺失 alert event 不能解释为“市场未触价”或“系统未观察到”；三者不得混用。

### 5.13 API 与 UI

同一 `backend/routers/position_timing.py` 提供：

| API | 副作用 |
|---|---|
| `GET /api/v1/position-timing/intents` | 只读 |
| `PUT /api/v1/position-timing/intents/{symbol}` | 仅 timing-owned intent，幂等 |
| `PUT /api/v1/position-timing/analysis-scope/{symbol}` | 仅 timing-owned scope 当前态，原子且幂等；不改已签发 card |
| `POST /api/v1/position-timing/materialize` | 仅 timing-owned cards/events/coverage state，exact-idempotent |
| `GET /api/v1/position-timing/cards/current` | 只读 |
| `GET /api/v1/position-timing/evidence` | 只读 |
| `GET /api/v1/position-timing/alerts/poll` | 只读报价与 edge |
| `POST /api/v1/position-timing/alerts/{trigger_id}/claim` | 原子追加唯一 alert authorization |

页面只设一个产品入口，使用现有 shadcn-compatible token。主要区域为：当前/最近行动卡（明确显示 `UPCOMING/VALID_TODAY/EXPIRED`）、非模态已提醒边、typed 数据状态、成本明细、研究证据。不得出现“一键下单”、自动交易开关或把 `46.9%` 显示成个股置信度。

## 6. L2/L3 研究契约

### 6.1 为什么 L2 后移但契约现在冻结

首批先交付 L1/L1a 并积累真实 deployment outcome；若事件字段以后才补，会永久失去早期样本。因此第一批实现日志和评价，冻结 L2 population/sampling/model/inference spec，但不实现训练管线、不让 L2 阻塞产品。

### 6.2 L2 population 与 baseline

`POSITION_TIMING_L2_POPULATION_V1`：

首个 L2 audit 的 objective 固定为 held-position `EXIT/REDUCE versus HOLD`，不宣称验证 watchlist 的入场 alpha。第一批仍保存 OPEN/ADD outcome，以便未来另立 entry objective，而不得把两种 estimand 混成一个 trial。

- 研究范围：`2018-08-01..2026-06-30` 内具备冻结日频 source identity 的 SH/SZ A 股。
- episode 的起点 `entry_decision_date=E`：从起始交易日起，每第 20 个全局交易日取一个 cohort；每个 cohort 纳入 E 时点 PIT active、E+1 有可判定入场状态的全部 canonical symbols。
- 不因当前是否属于 Selection Top20 而筛选；ST 作为属性保留，停牌/缺失按 typed status 保留。
- episode baseline：E 决策、E+1 raw open 建立合成满仓，持有至第 20 个 holding session 的 raw close；terminal 不可用最多顺延 5 个交易日。
- review row：从第 1 至第 19 个 holding session 收盘逐日形成 `review_decision_date=R`，只用 R 收盘时已知特征；候选 `target_action_date` 固定为 R+1。监督 row label 是在 R+1 raw open 卖出该 episode 的完整合法初始数量、相对继续持有同一数量至 baseline terminal 的逐腿成本后增量 bps。
- policy path：`MONOTONE_EXPOSURE_V1` 把 OOF 预测映射为目标 exposure，实际目标固定为 `min(previous_effective_exposure, mapped_exposure)`，因此只能 HOLD/REDUCE/EXIT、不能重新加仓。每次 reduction 是独立父订单并逐腿收费；剩余数量在 terminal 统一卖出。R+1 不可卖时该次动作过期并留在原 exposure，下一 review row 重新决策，不顺延旧动作。
- 两条路径共享的 E+1 合成买入腿在 lift 中相消；study 只比较未来卖出路径。所有 raw fill、公司行动与 terminal valuation 继续服从 §5.6，不因使用合成 episode 改回 raw-price return。
- `episode_id = hash(population_identity, canonical_symbol, entry_decision_date, entry_trade_date)`，且 `(canonical_symbol, entry_decision_date)` 唯一；sampling identity、calendar hash、universe hash、预计与实际 episode count 写 receipt。
- 预计量级约 25 万至 45 万 episode；精确数量是运行结果，不是开跑门禁。

最低佣金依赖名义金额，故 L2 不得用“1 元归一化仓位”计算成本。`L2_DEPLOYMENT_NOTIONAL_ASSIGNMENT_V1` 在 request 冻结：从 study cutoff 前 held-position `CARD_ISSUED` 的正 `planned_full_notional_cny` 构造按 `(card_id, value)` 排序的 deployment notional 序列及 hash；以 `{episode_id, distribution_sha256}` 的 canonical JSON hash 对序列长度取模，为每个 synthetic episode 确定性分配一个 full notional，再按 E+1 raw open 与板块规则得到合法初始数量。无可用 notional 序列、合法数量为零或 source/hash 不闭合时保留 typed unavailable，不以任意固定金额代替；这不影响 L1/L1a。该赋值不看 outcome，不新增 hypothesis。

这不是“消除分布偏移”，而是以合成入场人口替换 Selection Top20 入场偏差。正式报告同时给 synthetic population 未加权结果，以及只按 prospective `CARD_ISSUED` 中 `pre_action_qty > 0` 的 held-position 卡片所形成的 source role、action side、holding-age bucket、regime、ST 状态 deployment-population 加权结果；不支撑的 cell 显式 unavailable。

`CARD_ISSUED` 因而必须冻结 `pre_action_qty`、`planned_full_notional_cny`、每个 trigger branch 的 planned delta、reference raw price、持有期、来源角色、sizing/board-lot/cost-policy hash，保证后续 weighting、名义金额赋值和同方向同规模执行反事实可计算。

### 6.3 两个模型、一个政策、两个假设

两模型一次性冻结，不设置“MDE 恶化则回落单模型”的条件分支：

1. `SKLEARN_RIDGE_V1`：`scikit-learn=1.8.0`、`alpha=100`、`fit_intercept=true`、`solver=svd`。
2. `LIGHTGBM_GBDT_V1`：`lightgbm=4.6.0`、`boosting_type=gbdt`、`objective=regression_l2`、`n_estimators=300`、`learning_rate=0.03`、`num_leaves=15`、`max_depth=4`、`min_child_samples=100`、`subsample=1.0`、`subsample_freq=0`、`colsample_bytree=1.0`、`reg_alpha=0`、`reg_lambda=1`、`random_state=20260903`、`n_jobs=1`、`deterministic=true`、`force_col_wise=true`、early stopping disabled。

两模型都必须在 request 中序列化并 hash 完整 estimator `get_params(deep=false)`、预处理规格、feature order、target、package version 与随机性设置；上列显式参数是稳定语义摘要，不允许未记录的库默认值改变 trial identity。Ridge 数字列使用训练折 median 后 `StandardScaler`，GBDT 使用训练折 median 但不缩放；预编码类别固定为 `DOWN/UP_OR_FLAT/UNKNOWN`，未知值只进入 `UNKNOWN`，不由验证折扩展 vocabulary。

监督目标固定为每个 review row 的 `full_exit_incremental_net_value_bps`；study estimand 才是完整 monotone policy path 相对 do-nothing baseline 的 primary-horizon `net_lift_bps`。数字特征均在训练折内 median impute；`market_regime` 固定 one-hot 为 `DOWN/UP_OR_FLAT/UNKNOWN`；两模型使用以下同一有序 feature vector，不加入 HMM：

```text
selection_rank
selection_score
holding_trading_days_elapsed
holding_fraction_of_time_stop
unrealized_close_return_bps
relative_return_since_entry_bps
return_1d_bps
return_3d_bps
return_5d_bps
return_10d_bps
realized_vol_5d_bps
realized_vol_10d_bps
realized_vol_20d_bps
drawdown_from_peak_since_entry_bps
runup_from_entry_peak_bps
distance_to_stop_bps
distance_to_take_profit_bps
distance_to_trailing_stop_bps
intraday_range_bps
close_location_in_day
volume_ratio_5d_to_20d
market_regime_down
market_regime_up_or_flat
market_regime_unknown
```

每折同时输出 feature availability report；某数字列在训练折全缺失、固定类别无法编码或 feature order/hash 漂移时，该 L2 bundle typed failed，不删除列、不以零填充、不切换模型。该失败不影响 L1/L1a。

交叉验证冻结为 8 blocks、2 validation blocks、20-trading-day embargo、28 paths、每行 7 个 OOF；block 以 `entry_decision_date` 分配，同一 episode 的全部 review rows 必须留在同一折，同一 entry date 的所有股票不得拆分。七个 OOF 预测取算术均值；无 final refit、无参数搜索。

唯一 selected-eligible 政策 `MONOTONE_EXPOSURE_V1`：

- `predicted_full_exit_incremental_net_value_bps <= 0`：target exposure `1.00`。
- 正预测在训练折正值分布的 `(0, q50]`：`0.50`。
- `(q50, q75]`：`0.25`。
- `> q75`：`0.00`。

quantile 只由对应训练折生成并应用到验证折，政策形态、分位点和档位事前固定。Ridge 与 GBDT 各运行同一政策，共两个 hypotheses。`A0_DO_NOTHING` 是基线不计 hypothesis；既有 `A1_FIRST_CROSSING_5BPS` 若保留只作历史兼容 diagnostic，不参与 selected；holding-age 条件臂删除。

若某训练折没有正预测，`q50/q75` 不可定义，该折固定全部输出 exposure `1.00` 并记 `NO_POSITIVE_TRAIN_PREDICTIONS`；不改阈值、不借验证折估计分位点、不切换政策。

GBDT 的作用只是缩小“只有线性模型错设”的歧义。双阴性只能表述为“在两个预注册函数族及冻结规格中未发现成本后优势”，不得表述为不存在可学习信号。holding-age 与 regime 分层只作 diagnostic，不得反向选择模型、政策或阈值。

### 6.4 统计分类

L2 冻结 `economic_threshold_bps=0.0`，因为 estimand 已扣完逐腿成本。现有 N2 exit 的阈值是 `5.0`；这里只复用 inference 结构，不宣称取值语义一致。

每个 `entry_decision_date` 先对该日全部 paired evaluable episode 等权平均，形成两个模型各自的 cohort policy-lift series；unavailable 只进入 coverage，不按零值混入。由于 cohort 每隔 20 个交易日而 terminal 最多延至第 25 日，相邻 cohort 仍可重叠，区间冻结为按时间排序 cohort 的 circular moving-block percentile bootstrap：`block_length_cohorts=2`、`bootstrap_repetitions=2000`、`bootstrap_seed=20260903 + model_offset(0/1)`、`confidence_level=0.95`、`target_power=0.8`。nominal interval 使用 `alpha=0.05`；family-wise interval 对两个冻结 hypothesis 使用 Bonferroni `alpha=0.05/2`，不得运行后改变 family 或 seed。

- 每个 hypothesis 独立分类：adjusted lower `> 0` 为 `SUPPORTED`；adjusted upper `<= 0` 为 `NEGATIVE`；其余为 `INCONCLUSIVE`。
- study-level `effect_evidence=SUPPORTED`：至少一个 hypothesis 为 `SUPPORTED`。
- study-level `effect_evidence=NEGATIVE`：两个 hypothesis 都为 `NEGATIVE`。
- study-level `effect_evidence=INCONCLUSIVE`：其余组合；不得用一个模型的负结果覆盖另一个模型的不可分辨结果。
- nominal interval 为正而 adjusted interval 跨零时为 `INCONCLUSIVE`，reason `MULTIPLICITY_ADJUSTMENT_ERASED_NOMINAL_SIGNAL`。
- `power_status=UNDERPOWERED` 当 `mde_bps / oracle_mean_lift_bps > 0.25`，否则 `ADEQUATE`。MDE 是跑后报告义务，不是运行准入。

receipt 同时报 nominal per-model interval、两个假设的 family-wise adjusted interval、MDE/oracle、逐腿成本情景与 `COST_ASSUMPTION_SENSITIVE`。新增第二个假设会扩大 simultaneous interval，这个功效代价如实报告，不触发回落分支。

本次 simultaneous family 固定 `familywise_hypothesis_count=2`。全局 N0 的 25 条历史记录另以 `historical_registry_context_count` 只读披露，提醒解释者已有搜索历史；由于 objective、population 与 trial family 不同，不把它们伪装成这次两个预注册假设，也不从历史记录回选候选。

trial 只写 `position_timing_advice_v1/research_registry/timing_trial_registry_v1.jsonl`。全局 N0 registry/current_route 只读历史计数，绝不写入或重算。

实现复用 `AdvisoryResearchTrialRegistryV1` 的 append/identity 机制，但路径独立、不得调用 `generate_current_route`。为兼容现有 record schema，两条模型 trial record 均冻结 `objective_contract=RISK_MANAGED_ADVISORY`、`study_type=LEARNABILITY_AUDIT`；产品 objective 仍为 `POSITION_TIMING_ADVICE_V1` 并单独写 study receipt。每个模型的 `SUPPORTED/NEGATIVE/INCONCLUSIVE` 分别映射 `result_class=CONTROL_READY/NEGATIVE/EXPLORATORY`；前两者 `decision_use=DIRECTION_GATE`，后者 `NAVIGATION_ONLY`。这里的 `DIRECTION_GATE` 只控制该模型能否进入 L3 标签，不阻塞 L1/L1a、L2 运行、PR 或发布，也不调用全局 route。

### 6.5 L3

只有 `effect_evidence=SUPPORTED` 的模型/政策可让对应 evidence block 与适用卡片进入 `MODEL_ASSISTED`。首个 L2 objective 只覆盖 held-position EXIT/REDUCE-versus-HOLD，因此即使 SUPPORTED 也不得把 watchlist `OPEN/ADD` 卡改称 model-assisted。`NEGATIVE/INCONCLUSIVE` 或任意 `power_status` 均不影响 L1/L1a 继续运行；结论与 reason 在页面证据区可见，卡片保持 `RULE_BASED_RISK_MANAGEMENT`。

若两个模型都 `SUPPORTED`，固定优先 Ridge；只有 Ridge 非 `SUPPORTED` 且 GBDT `SUPPORTED` 时选择 GBDT，不按历史点估计、Sharpe 或区间宽度事后回选。每个模型 record 固定 `planned/generated/evaluated_trial_count=1`；只有上述唯一被选模型的 `selected_trial_count=1`，其余为 0，study-level selected 数等于两条 record 之和且最多为 1。

sealed holdout 是用户可选择的一次性确认动作，读后记录 consumed；不读取不妨碍 L1/L1a、L2 运行或上述 effect 分类。

## 7. 第二阶段分钟机制

### 7.1 L4b-1 执行窗口

L4b-1 只能通过独立预注册反事实回答：在 card 已固定 symbol、方向、合法数量和 horizon 后，不同 T+1 分钟执行窗口相对 `AT_OPEN` 的成交价差是否在成本与缺失处理后为正。

多 horizon outcome 只提供 signal-decay/holding sensitivity context，不能充当 L4b-1 的 `SUPPORTED/NEGATIVE` 证据。第一阶段的义务只是冻结足够字段，使未来同方向同规模反事实可计算。

若以后得到支持，T 日收盘时用已收盘历史分钟特征预先输出静态 execution window；T+1 运行时仍只读实时报价，不调用 `fetch_minute_kline_tdx`。Almgren–Chriss 只提供 arrival-price、成本与执行风险的研究背景，不定义本系统的窗口枚举。

### 7.2 数据现状

`EVID-MINUTE-SNAPSHOT` 证明本地分钟链路可用于离线研究，但它是 2026-09-03 的 evidence snapshot，不是永久能力声明。2026-06-30 之后发出的卡无法用该快照补算分钟反事实；第二阶段要评价近期卡片时须先扩展分钟数据并绑定新的 snapshot identity。这是第二阶段数据条件，不是第一阶段交付阻塞。

当前 `fetch_minute_kline_tdx` 的 `kline-all` 请求会随 symbol × poll 次数放大。第一阶段完全绕开；第二阶段若确需自适应当日分钟链路，才设计当日增量/缓存，不提前建设共享 poller 或新 worker。

### 7.3 L4b-2 盘中新方向

L4b-2 会直接增加交易次数与真实亏损敞口，因此只有独立审计证明逐腿成本后 adjusted lower bound 为正时才重新设计。该证据条件不扩散为 L1/L1a/L2/L3 的审批或人工放行。

N3 只回答 `ALPHA_RANKING`；不得用 N3 否决 L4b-1，也不得用未来执行窗口正结果推翻 N3。

## 8. 方法论与文献映射

可行原理按“是否直接服务人工建议”分层，不因技术新颖而进入实现：

| 原理 | 能回答的问题 | 本蓝图处置 |
|---|---|---|
| 风险/可交易性规则 | 已持仓是否应止损、能否交易、用户目标如何合法落地 | L1 正式采用；明确标为 rule-based risk management，不冒充 alpha |
| 趋势、相对强弱与波动 | 当前持仓继续持有的状态是否恶化 | 只取 T 时点可见的固定特征进入 L2；不单独搜索阈值 |
| 缺口/短期均值回归 | 既定方向是否应等待更好的当日价格 | L1 只执行现有 price guard；L4b-1 才独立检验分钟执行价差 |
| continuation/exit value 监督学习 | 下一可执行日减仓相对继续持有是否有成本后价值 | L2 主线；Ridge 与 GBDT 预测同一 row target，单调 policy 决定 exposure |
| meta-label / act-or-hold | 已有动作意图应执行还是保持基线 | 已由 L2 的 candidate-versus-do-nothing estimand 覆盖，不另建第三套模型 |
| barrier、hazard、最优停止 | 退出风险何时集中、删失如何处理 | 可作未来 challenger；第一批与首个 L2 不需要 |
| HMM / regime conditioning | 市场或板块状态是否改变规则有效性 | 第一批 context-only；首个 L2 不入模也不按 HMM 分层，未来增量必须另立一个冻结 feature-block 假设 |
| 成本约束动态仓位/执行 | 应一次完成还是分档、何时成交 | L1 用确定性 exposure 与逐腿成本；L4b-1 研究执行窗口 |
| TCN/Transformer/Offline RL | 是否能从长分钟序列学到更复杂控制 | 当前不采用；现有本地证据和人工建议目标不足以抵偿复杂度与模拟器风险 |

1. [Moskowitz, Ooi & Pedersen (2012), Time Series Momentum](https://doi.org/10.1016/j.jfineco.2011.11.003)研究的是期货的一至十二月收益持续性，只支持把中期趋势当作背景，不能据此声称“日内到数日不稳健”或否决分钟机制。
2. [Gârleanu & Pedersen (2013), Dynamic Trading with Predictable Returns and Transaction Costs](https://doi.org/10.1111/jofi.12080)支持有成本时向目标部分移动；在本设计中对应 exposure 档位、逐腿成本和 monotone policy，不等于为个股规则提供现成参数。
3. 既有 purged/embargoed CPCV 与 family-wise 结构继续复用；目的在于时间因果与诚实计数，不增加人工审批。
4. [Almgren & Chriss, Optimal Execution of Portfolio Transactions](https://www.smallake.kr/wp-content/uploads/2016/03/optliq.pdf)只作为第二阶段执行成本/风险背景，不声称它定义 `AT_OPEN/ON_PRICE_TRIGGER/WAIT_UNAVAILABLE`。
5. HMM 可作为市场态势 context 与后续 diagnostic slice，但在没有 L2 支持前不参与方向、校准或四路消融。

方法选择以终极产品目标和本地证据为准，不以“技术更新”本身为目标。

## 9. Implementation Plan / 分阶段实施

### 9.1 已完成：蓝图冻结

- 建立证据语义目录，修正臂名、fee、board-lot、minute fetch、到期时钟与统计分类。
- 冻结 F2 contracts、Design Acceptance Index、验证路径与隔离矩阵。
- 蓝图文档已通过 F2 validator 并作为本分支实现权威；文档合入本身不曾代表功能实现。

### 9.2 已合入：实现块一 L1 contracts、artifact 与规则卡

1. 实现 `contracts.py`、`policy.py`、`artifact_store.py`。
2. 实现唯一持仓 authority、自选去重、intent 与方向性可交易性。
3. 生成 immutable card set 与 `CARD_ISSUED`。
4. 实现 componentized cost、board-lot、guard snapshots 与 typed errors。
5. 实现 materialize/current/evidence/intents API 与页面行动卡。

块一还完成了四项收口：已确认终止上市的 PIT 只读输入及买卖方向映射；已验证 T 日停牌时用更早的最近可执行 close 保留风险方向，而非停牌旧 bar 不会锁死卡片；green/yellow/skip 分支由冻结 guard 参数派生、以 guard action/reason 消歧并按各自触发价估算成本；card set 保存完整 input/policy identity 和 `cards_sha256`。复权因子不参与日频卡，明确为 `NOT_APPLICABLE`，不构成出卡门禁。PR `#4277` 的 required checks 已通过并合入；该状态仍只是完整首发内的块一源码，source merge 不改变范围判断。

### 9.3 第一批 B：L1a 与 prospective outcome

1. 先在同一个 `position_timing` 包、router 和页面内实现 §5.2.1 的轻量 scope 当前态与过滤；它只是本任务的一个小条目，不拆新阶段或服务。
2. 实现批量 quote GET、edge 状态机与 atomic claim POST。
3. 页面 toast、already-alerted 非模态条目、typed stale/unavailable。
4. 实现五 horizon `OUTCOME_EVALUATED`、coverage watermark 与基率聚合。
5. 完成隔离、并发、PIT、费用、scope 与 UI 结果验证。

第一批结束的用户可见结果是：日频明确卡、盘中到点提示、失败原因、成本和持续累积的结果证据。L2 未实现不降低这一定义。

### 9.4 后续 C：L2/L3

- outcome 样本积累后，按冻结 spec 实现两个模型、一个政策、两个假设。
- 无搜索运行一次并写 timing-owned immutable bundle/registry。
- 只把 `SUPPORTED` 接入 L3；其余结果保留在证据区。
- held-position audit 完成后，才可基于已保存的 OPEN/ADD outcome 另立 entry objective；它使用独立 request、trial family 与结论，不得与 exit 两个假设合并或事后追加到同一 family。

### 9.5 后续 D：第二阶段

- 先扩展/冻结分钟 snapshot，再做 L4b-1 同方向同规模反事实。
- L4b-2 继续维持范围外，除非其独立证据条件成立且用户决定扩展范围。

## 10. Verification Plan / 验证方案

### 10.1 文档 gate

```powershell
python scripts/aistock_feature_workflow.py validate --design docs/architecture/position_timing_advice_f2_redesign_20260903.md --tier F2
git diff --check
```

### 10.2 第一批实现 gate

1. `backend/tests/position_timing/test_artifact_store.py` 与 `test_api.py`：card/intent/event schema、枚举、完整 input/policy/card hash identity，以及三个 GET 在空 artifact 根上的零写入。
2. `backend/tests/position_timing/test_universe.py`：唯一 authority、watchlist active 过滤、holding 优先去重、Paper/MiniQMT 隔离。
3. `backend/tests/position_timing/test_pit_clock.py`：`feature_available_at <= decision_as_of`，同日 15:00 后信息也 fail closed。
4. `backend/tests/position_timing/test_tradability.py`：停牌、ST、买入一字涨停、卖出一字跌停、相反方向、T+1 可卖量。
5. `backend/tests/position_timing/test_policy_snapshot.py`：v1 与 `EVID-GUARD-DEFAULTS` 逐项一致；共享默认值未修改；旧 card 不受未来默认变化影响。
6. `backend/tests/position_timing/test_cost_policy.py`：逐腿分项、父订单累计、三种拆单、阈值 58,824/117,648/235,295、买卖 lot 不对称。
7. `backend/tests/position_timing/test_card_service.py`：L1 mapping、已确认终止上市的买卖方向、真实停牌缺 T 日 bar 的最近可执行 close、非停牌旧 bar 不锁卡、green/yellow/skip 唯一映射、逐分支触发价成本、缺 Selection/HMM 的字段级降级，以及 adjustment 非门禁。
8. `backend/tests/position_timing/test_artifact_store.py`：immutable conflict、input/policy/cards/intent 篡改 fail closed、append-only、三类事件的复合幂等键/hash/时区契约与并发 lock。
9. `backend/tests/position_timing/test_outcome_materialization.py`：两只时钟、五 horizons、5 日 terminal defer、h=1 新买数量 T+1 锁定、保守日线 fill、边际 paired path、公司行动/双价格、intention-to-treat、四种读取态、水位不越过部分失败。
10. `backend/tests/position_timing/test_alerts.py`：GET 零写、50-symbol chunk、5 分钟/30 秒、有界 open/current quote identity、position/intent 漂移、atomic claim、多标签页、already-alerted 可见条目、无 minute fetch。
11. `backend/tests/position_timing/test_isolation.py`：N0/现有表/调度/SmartMonitor/Paper/MiniQMT 零写；除 `backend/main.py` 与前端导航外零反向依赖；无 order/runtime-weight 输出。
12. `frontend/tests/position-timing/position-timing.spec.ts`：卡片、typed errors、toast、研究证据区、无交易按钮。

分析范围管理不新增测试文件或独立验证 lane，直接补入既有矩阵：`test_universe.py` 验证 discovery/analysis 集合与持仓强制纳入，`test_artifact_store.py` 验证 scope 原子更新/hash，`test_api.py` 验证唯一新增接口和三个 GET 零写，`test_card_service.py` 验证 scope snapshot 进入 card-set identity，前端既有 spec 验证复选框与“下一卡片生效”。

### 10.3 L2 后续 gate

- `backend/tests/position_timing/test_l2_population.py`（episode/review identity、确定性 notional assignment、board-lot 与 deployment weighting）
- `backend/tests/position_timing/test_l2_model_specs.py`
- `backend/tests/position_timing/test_l2_cpcv_identity.py`
- `backend/tests/position_timing/test_l2_inference.py`
- immutable receipt readback，验证两个 hypothesis、threshold 0、nominal/family-wise、effect/power 正交分类、cost sensitivity、own registry 零 N0 写入。

## 11. Risks / 风险与失败模式

| 风险 | 设计处置 |
|---|---|
| 用户未打开页面，卡片/outcome 未及时物化 | 页面显示成功扫描水位与 `PENDING_MATERIALIZATION`；下次打开补扫，不伪装成尚未到期 |
| 发卡后用户已手工改仓或改 intent | poll 比对 snapshot hash，旧卡不弹窗并返回 typed changed 状态；新意图下一张卡生效 |
| TDX 免费源无 SLA | stale/future/source failure typed；禁止弹窗，不静默隐藏状态 |
| claim 后浏览器崩溃漏弹窗 | event 明确是 authorization；仍有效的已 claim edge 以非模态条目可见；不引入 ACK 基础设施 |
| 多标签页重复视觉提示 | 服务端 event exact-once；UI 弹窗 at-most-once 只尽力而为，数据完整性优先 |
| 荐股/Selection 进展慢 | universe 与 intent 不依赖荐股；Selection 缺失只禁用 alpha-decay context |
| HMM 缺失或漂移 | 字段级 `hmm_context_status=UNAVAILABLE`；方向不受影响 |
| 退市研究 overlay 尚非运行 authority | L1 只消费 issuer-bound、timestamp-causal confirmed terminal event，并仅用已生效 `stock_basic` 终态兜底；研究 profile 不接入；仅自选买入与持仓卖出按方向处理 |
| 用户未提供 planned full notional | 该股票 `SIZING_INPUT_UNAVAILABLE`，不虚构仓位；其他股票正常 |
| 未显式选择的自选股被误当成数据缺失或仍批量出卡 | discovery 与 analysis universe 分离；缺 scope 为 `NOT_SELECTED`，只对显式选择的 active watchlist 出卡；持仓始终覆盖 |
| scope 更新改写当日卡或变成第二套 watchlist | `NEXT_CARD_SET_ONLY`、card immutable；scope 只保存 selected symbol set，成员资格仍由只读 watchlist authority 决定 |
| 最低佣金实际聚合口径与假设不同 | `BROKER_UNVERIFIED` 披露 + 1/2/3 parent-order sensitivity；不阻塞 |
| 费率变化使手写阈值漂移 | Decimal 从 versioned components 派生并写 receipt，禁止业务常量 |
| 除权除息使 raw 收益失真 | raw 只用于触发/成交/费用；经济结果绑定公司行动或等价 total-return identity，缺失即 unavailable |
| 复权数据暂未成熟 | 不阻塞只使用 raw 的块一 card；实现块二 outcome 到期评价时才 fail closed 为 `UNAVAILABLE_AT_HORIZON` |
| 文件 artifact 并发冲突 | content address、file lock、fsync、atomic replace；冲突 fail closed |
| 规则卡被误读成 alpha | 卡片固定 `RULE_BASED_RISK_MANAGEMENT`；研究统计只在证据区 |
| L2 双模型仍无正下界或功效不足 | 如实 `NEGATIVE/INCONCLUSIVE` 与 `UNDERPOWERED`；L1/L1a 不受影响，不追加搜索 |
| 合成 L2 population 与用户人口仍偏移 | 同时报 synthetic 与 prospective deployment-weighted；不用“消除偏移”措辞 |
| 分钟快照截至 2026-06-30 | 第二阶段先扩展数据；不影响第一阶段 |
| `kline-all` 放大请求 | 第一阶段零调用；第二阶段需要时才设计增量/缓存 |

## 12. Rollout / Rollback / Production gates

### 12.1 蓝图文档历史合入边界

- 蓝图初次合入只包含 Markdown；当前 feature 分支已经在其约束下实现块一源码，不能再用本段宣称“当前只改文档”。
- `production_ddl_gate=noop`。
- `production_dependency_gate=noop`。
- `runtime_activation=noop`。
- `backend_restart=noop`。
- `frontend_activation=noop`。
- DB、artifact、registry 和运行进程均不修改。

文档回滚通过后续 PR revert；不删除任何 artifact 或历史证据。

### 12.2 当前块一与未来首发激活

块一已完成无进程控制的本地测试及真实 DEV 只读数据联调，并由 PR `#4277` 合入 `main`；源码合入不自动加载到既有生产进程。没有启动开发端口，也没有激活生产运行态。分析范围管理继续使用同一个文件 artifact root，完整首发仍为零 DDL；若后续偏离为数据库表，必须先更新本 F2 设计并按 DEV/生产授权边界重新处理。生产端口 8001/3000 的激活与 backend restart 始终由用户明确授权和执行。

## 13. Design Acceptance Index / 设计验收索引

`legacy_review_ref` 保留旧稿 PT-ID 的评审谱系；F-ID 是唯一规范身份。

| design_item | legacy_review_ref | level | requirement |
|---|---|---|---|
| F-001 | PT-015 | HARD | 只产出人工建议，不生成订单、部署物或运行时权重 |
| F-002 | PT-001, PT-014 | HARD | 轻量独立 namespace/artifact；仅 composition root 薄接线，既有业务模块零反向依赖、零既有写入 |
| F-003 | PT-002 | HARD | 唯一 `LEGACY_PORTFOLIO` authority，active watchlist 合并并按 holding 优先去重 |
| F-004 | PT-003 | HARD | timestamp 级 PIT、T+1 单日 card 时钟与旧卡不顺延 |
| F-005 | PT-001, PT-002 | SCOPE | 复用本地 DB、QE exports、交易日、限价/停牌/ST 与纯 guard 实现 |
| F-006 | PT-005, PT-006, PT-013 | SCOPE | L1 由用户 intent + 冻结 guard 形成明确行动卡，不把研究臂当 runtime guard |
| F-007 | PT-003, PT-005 | HARD | ST 与一字板按方向判定，board-lot 与 T+1 可卖量正确 |
| F-008 | PT-007 | HARD | timing-owned 明细 snapshot/hash/provenance，shared defaults 零修改 |
| F-009 | PT-004 | HARD | componentized 逐腿成本、parent-order 假设与 FeeModel 边界 |
| F-010 | PT-017 | ADVISORY | Decimal 派生阈值、实际合法逐腿金额标注，不拦截小额建议 |
| F-011 | PT-008, PT-014 | HARD | own artifact/registry、immutable cards、append-only events、N0 零写入 |
| F-012 | PT-014 | HARD | 页面触发 exact-idempotent materialization，不建 scheduler/worker |
| F-013 | PT-008 | HARD | 五 horizons 的 `OUTCOME_EVALUATED`、candidate/do-nothing 逐腿配对与两只时钟 |
| F-014 | PT-008 | HARD | 四种 outcome 读取态与成功扫描水位，物化失败不可伪装为 pending |
| F-015 | PT-018, PT-019 | HARD | L1a 只用 batch quote，分钟级 poll；stale/future/unavailable 显式且禁止弹窗 |
| F-016 | PT-019 | HARD | alert claim artifact exact-once；UI at-most-once 仅 advisory，already-alerted 仍可见 |
| F-017 | PT-002, PT-016 | SCOPE | HMM/Selection 只作可缺失 context，不阻塞整卡或决定 universe |
| F-018 | PT-008, PT-010 | SCOPE | 第一批冻结 L2 population/sampling/outcome 字段，训练管线后移 |
| F-019 | PT-009, PT-010, PT-011 | SCOPE | Ridge+GBDT、一个 monotone policy、两个 hypotheses，无搜索、own registry |
| F-020 | PT-009, PT-010 | SCOPE | threshold 0、effect/power 正交、nominal/family-wise 与 cost-sensitive 标签 |
| F-021 | PT-009, PT-012, PT-017 | ADVISORY | MDE 报告、sealed 可选、小额标注均非批准门禁 |
| F-022 | PT-018, PT-020 | SCOPE | L4b-1 独立分钟执行反事实；N3 objective 不混用；L4b-2 保持范围外 |
| F-023 | PT-016, PT-018 | SCOPE | 第一批仅 L1/L1a/outcome 与轻量 analysis scope；无 SSE、SmartMonitor engine 或复杂模型 |
| F-024 | PT-003, PT-019 | HARD | 所有缺失/陈旧/不支持/物化失败均 typed，不静默、不空结果冒充成功 |
| F-025 | PT-001, PT-004 | HARD | card/receipt 绑定实际消费的 dataset、calendar、limit、delist、policy、fee 与 code provenance；card 未消费的 adjustment 明确 NOT_APPLICABLE，outcome 必须绑定 adjustment/corporate-action |
| F-026 | PT-014 | HARD | 文档与未来实现具备逐条测试、隔离、回滚和 production gate 证据 |
| F-027 | NEW-20260904 | SCOPE | discovery 与 analysis universe 分离；全部真实持仓始终分析，仅显式选择的 confirmed watchlist 出卡；scope 为 timing-owned 原子当前态并绑定 card-set identity，不建第二持仓池或新平台 |

## 14. Design Acceptance Matrix / 设计验收矩阵

`DESIGN_VERIFIED` 只证明设计条款已经闭合；`BLOCK_ONE_*_VERIFIED` 表示对应的实现块一范围已有直接测试。块一 source merge 已由 PR `#4277` 单独证明，但任何块一状态都不表示完整首发或生产激活已经完成。带 `APPROVED_BY_USER` 的 gap 可以记录用户明确排入队列的实现块二或后续研究范围，但不是新增门禁。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/position_timing/contracts.py`；router output boundary | `backend/tests/position_timing/test_isolation.py` | BLOCK_ONE_VERIFIED | none |
| F-002 | `backend/services/position_timing/`、own artifact root、composition wiring | `backend/tests/position_timing/test_isolation.py` | BLOCK_ONE_VERIFIED | none |
| F-003 | `backend/services/position_timing/service.py` universe adapter | `backend/tests/position_timing/test_universe.py` | BLOCK_ONE_VERIFIED | none |
| F-004 | `PositionTimingCardV1` decision clock 与 PIT checks | `backend/tests/position_timing/test_pit_clock.py`；`backend/tests/position_timing/test_card_service.py` | BLOCK_ONE_VERIFIED | none |
| F-005 | service read adapters；§4.3 reuse table | `backend/tests/position_timing/test_api.py`；2026-09-04 DEV 468-target readback | BLOCK_ONE_RUNTIME_REUSE_VERIFIED_APPROVED_BY_USER | canonical v2 QE/L2 dataset binding remains in approved later L2 scope |
| F-006 | `backend/services/position_timing/policy.py` 与 `service.py` L1 mapping | `backend/tests/position_timing/test_card_service.py`（含 confirmed delist 买卖方向） | BLOCK_ONE_VERIFIED | none |
| F-007 | shared limit/suspend/board-lot adapters 与冻结方向分支 | `backend/tests/position_timing/test_tradability.py`；`backend/tests/position_timing/test_card_service.py` | BLOCK_ONE_VERIFIED_APPROVED_BY_USER | target-day quote direction recheck remains in approved block two |
| F-008 | timing-owned guard snapshot artifact/hash/provenance | `backend/tests/position_timing/test_policy_snapshot.py` | BLOCK_ONE_VERIFIED | none |
| F-009 | componentized `PERSONAL_MANUAL_COMPONENT_COST_V1` | `backend/tests/position_timing/test_cost_policy.py` | BLOCK_ONE_VERIFIED | none |
| F-010 | Decimal threshold、合法数量与 cost-heavy label | `backend/tests/position_timing/test_cost_policy.py`；`backend/tests/position_timing/test_card_service.py` | BLOCK_ONE_VERIFIED | none |
| F-011 | `backend/services/position_timing/artifact_store.py` | `backend/tests/position_timing/test_artifact_store.py`（input/policy/cards/intent tamper 与跨月幂等）；`backend/tests/position_timing/test_isolation.py` | BLOCK_ONE_VERIFIED | none |
| F-012 | router `POST /materialize` 的 card publication | `backend/tests/position_timing/test_api.py`；`backend/tests/position_timing/test_card_service.py` | BLOCK_ONE_API_VERIFIED_APPROVED_BY_USER | outcome materialization remains in approved block two |
| F-013 | `OutcomeEvaluatedEventV1` schema 已冻结；runtime materializer 尚不存在 | `backend/tests/position_timing/test_artifact_store.py`（复合键/hash/时钟 schema） | BLOCK_ONE_CONTRACT_VERIFIED_APPROVED_BY_USER | OUTCOME_RUNTIME_DEFERRED_BY_APPROVED_SCOPE |
| F-014 | §5.11 coverage-state 设计已闭合；`materialization_state.json` 尚不存在 | artifact: `docs/architecture/position_timing_advice_f2_redesign_20260903.md#511-outcome-评价`；runtime test 尚不存在 | DESIGN_VERIFIED_APPROVED_BY_USER | OUTCOME_RUNTIME_DEFERRED_BY_APPROVED_SCOPE |
| F-015 | §5.12 quote-poll 设计已闭合；`position_timing/alerts.py` 尚不存在 | artifact: `docs/architecture/position_timing_advice_f2_redesign_20260903.md#512-l1a-报价提醒`；runtime test 尚不存在 | DESIGN_VERIFIED_APPROVED_BY_USER | ALERT_RUNTIME_DEFERRED_BY_APPROVED_SCOPE |
| F-016 | `AlertEmissionAuthorizedEventV1` schema 已冻结；atomic claim 尚不存在 | `backend/tests/position_timing/test_artifact_store.py`（复合键/hash/时区 schema） | BLOCK_ONE_CONTRACT_VERIFIED_APPROVED_BY_USER | ALERT_RUNTIME_DEFERRED_BY_APPROVED_SCOPE |
| F-017 | card context fields and typed field-level unavailability | `backend/tests/position_timing/test_card_service.py` | BLOCK_ONE_VERIFIED | none |
| F-018 | L2 population/sampling/outcome schema in `contracts.py` | `backend/tests/position_timing/test_l2_population.py` | CONTRACT_VERIFIED | none |
| F-019 | frozen Ridge/GBDT/monotone policy spec；runtime pipeline absent | `backend/tests/position_timing/test_l2_population.py` | CONTRACT_VERIFIED_APPROVED_BY_USER | PIPELINE_DEFERRED_BY_APPROVED_SCOPE |
| F-020 | frozen threshold/effect/power/inference spec；runtime receipt absent | `backend/tests/position_timing/test_l2_population.py` | CONTRACT_VERIFIED_APPROVED_BY_USER | PIPELINE_DEFERRED_BY_APPROVED_SCOPE |
| F-021 | frozen non-gating semantics | `backend/tests/position_timing/test_l2_population.py`；`backend/tests/position_timing/test_cost_policy.py` | CONTRACT_VERIFIED | none |
| F-022 | §7 objective separation and minute snapshot binding | `backend/tests/position_timing/test_l2_population.py` | DESIGN_VERIFIED_APPROVED_BY_USER | SECOND_STAGE_DEFERRED_BY_APPROVED_SCOPE |
| F-023 | 5 block-one API、one page、no SSE/worker/model imports | `backend/tests/position_timing/test_api.py`；`python -m nox -s frontend_type_lint` | BLOCK_ONE_SCOPE_VERIFIED_APPROVED_BY_USER | alerts and outcome remain in approved block two |
| F-024 | typed block-one reason/error DTOs and no-new-card maturity state | `backend/tests/position_timing/test_api.py`；`backend/tests/position_timing/test_card_service.py` | BLOCK_ONE_TYPED_FAILURES_VERIFIED_APPROVED_BY_USER | alert and outcome failure states remain in approved block two |
| F-025 | immutable card source identities、confirmed delist identity、guard snapshot hashes；card adjustment=`NOT_APPLICABLE` | `backend/tests/position_timing/test_artifact_store.py`；`backend/tests/position_timing/test_policy_snapshot.py`；2026-09-04 DEV readback | BLOCK_ONE_CARD_IDENTITY_VERIFIED_APPROVED_BY_USER | outcome adjustment/corporate-action receipt identities remain in approved block two |
| F-026 | module-owned nox/L0/catalog routing and block-one isolation | `python -m nox -s position_timing_backend`；`python -m nox -s l0`；PR `#4277` required checks 与 merge readback | BLOCK_ONE_SOURCE_MERGE_VERIFIED_APPROVED_BY_USER | FULL_FIRST_RELEASE_VALIDATION_AND_RUNTIME_ACTIVATION_DEFERRED_APPROVED_BY_USER |
| F-027 | §5.2.1 `PositionTimingAnalysisScopeV1`、单一增量 API/既有页面复选框、card-set scope identities | artifact: `docs/architecture/position_timing_advice_f2_redesign_20260903.md#521-轻量分析范围管理已设计排入下一实施任务`；未来复用 `backend/tests/position_timing/test_universe.py`、`test_artifact_store.py`、`test_api.py`、`test_card_service.py` 及既有前端 spec | DESIGN_VERIFIED_APPROVED_BY_USER | IMPLEMENTATION_QUEUED_IN_NEXT_POSITION_TIMING_TASK_APPROVED_BY_USER |

## 15. DESIGN-COMPLIANCE-001 最终复核

1. **禁止简化交付**：当前只报告“实现块一源码已合入”，明确列出尚未实现的 L1a、outcome、分析范围管理、完整首发验证与生产激活；没有把块一或 F-027 设计伪装成完整首发实现。
2. **禁止静默错误**：块一对 PIT、source identity、可交易性、sizing、已确认终止上市、HMM/Selection 和不支持标的使用 typed 状态；系统级零覆盖不签空卡，单标的缺失不拖垮其他卡；块二的报价/outcome 失败语义仍由设计契约约束。
3. **禁止改变业务逻辑**：实现保持唯一持仓 authority、用户 intent、共享 guard 纯实现、逐腿成本、T+1 card 与 N3 objective 边界。F-027 只在 confirmed watchlist 之内增加显式分析选择，真实持仓仍全部覆盖，且不改变既有 card、Watchlist、Portfolio 或研究语义。
4. **禁止私增门禁审批**：删除了与 L1 无关的 adjustment 出卡门禁；MDE 仅报告、sealed holdout 可选、最低成交额只标注，且没有审批、双人确认或人工放行。仅 L4b-2 保留既有直接证据条件，因为它属于未来范围并会新增交易次数与真实亏损敞口。

结论：蓝图仍是唯一 position-timing 设计基线；实现块一已通过本地/CI 验证并由 PR `#4277` 合入。F-027 已完成轻量设计并排入下一 position-timing 实施任务；实现块二、F-027 源码、完整首发验证、生产进程加载与页面运行态 readback 均未完成，不得据此报告完整功能已交付。
