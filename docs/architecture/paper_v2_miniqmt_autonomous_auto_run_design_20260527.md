# Paper v2 MiniQMT 无人值守自动运行详细设计（2026-05-27）

> 状态：详细设计，待按阶段实现  
> 分支：`docs/miniqmt-auto-run-design-20260527`  
> 范围：Paper Trading v2、MiniQMT SIM、StrategyPackage 接入模拟盘、自动调度、后端重启恢复、组合级运行配置、UI 可观测性、验证矩阵  
> 非范围：MiniQMT live 实盘下单、QE 训练流程重构、StrategyPackage alpha core 修改、多个 MiniQMT 真实账户的券商侧开户/登录自动化

## 1. 目标结论

本设计的目标是把 MiniQMT 模拟盘从“人工创建 session / 人工观察 / 人工补救”的模式升级为 Paper v2 的正式无人值守能力：

1. **后端重启后自动恢复**：只要配置启用，FastAPI 后端启动时自动启动 Paper v2 session scheduler，并从数据库恢复所有 `auto_run_enabled=true` 的组合。
2. **策略包一键进入 MiniQMT 模拟盘**：用户选择合格 StrategyPackage 后，可以直接创建 MiniQMT SIM 组合并启用自动运行，不需要每天手工创建 session、手工生成 HMM snapshot、手工点运行。
3. **每日自动运行**：每个交易日自动判断交易日、等待可下单窗口、生成当日权威 selection artifact、自动计算/复用 HMM 当日缓存、生成调仓指令、提交 MiniQMT、查询券商权威持仓/成交并落库。
4. **平台能力不变成策略包门禁**：交易日、停牌、涨跌停、MiniQMT 连接、HMM 当日系数、TopK、行业黑名单、TDX/DB/MiniQMT 数据状态都属于组合运行时平台能力，只能形成当日 run/session 的等待、告警或失败，不能阻止策略包进入选股/模拟盘。
5. **不伪造成成功**：MiniQMT 未连接、数据未就绪、订单被拒、部分成交、当日已完成等状态都必须以 broker-authoritative 或 structured error 方式展示，不得用空订单、默认价格、默认持仓或默认成功掩盖。

验收上，完成后应满足：后端重启、MiniQMT 客户端保持登录、交易日数据自动同步正常时，用户不需要在开盘前人工处理，MiniQMT 模拟盘可以按配置自动执行当日调仓；如果运行条件不满足，系统自动等待/重试/明确失败，并给出可复制诊断信息。

## 2. 现状依据

本设计基于当前代码和已落地设计文档，不重新发明链路：

| 现有能力 | 位置 | 当前结论 |
|---|---|---|
| 后端启动可选启动 Paper v2 scheduler | `backend/main.py` | 仅当 `ENABLE_PAPER_TRADING_V2_SCHEDULER` 为 true-like 时自动启动；默认不启动。 |
| scheduler 轮询 tickable session | `backend/services/paper_trading_v2/scheduler.py` | `run_once()` 读取 `TICKABLE_SESSION_STATUSES`，逐个调用 `PaperTradingSessionRunner.tick()`；线程内失败不会伪造成成功。 |
| session 可持久化和重启恢复 | `backend/services/paper_trading_v2/session.py`、`repository.py` | session、session_day、session_event 已入库，后端重启后可被 scheduler 重新读取。 |
| MiniQMT live session 路由 | `backend/services/paper_trading_v2/live_session.py` | `broker_backend=minqmt_sim` 进入 `_tick_minqmt_live_intraday()`，要求 `live_data_source=MINIQMT_REALTIME`。 |
| MiniQMT day runner | `backend/services/paper_trading_v2/day_runner.py` | 能加载 MiniQMT account snapshot、生成 signal/targets/intents、提交 MiniQMT、保存 broker-authoritative snapshot。 |
| MiniQMT broker adapter | `backend/services/paper_trading_v2/broker/minqmtsim.py` | 通过 `qmt_client.place_order()` 下单，MiniQMT 是订单/成交/持仓权威。 |
| 官方交易日服务 | `backend/services/trading_calendar_status.py`、`backend/routers/trading_calendar.py` | 从 `market.trading_calendar` 生成文件缓存，禁止 weekday fallback，已支持最近/下一交易日和缓存覆盖告警。 |
| runtime gate 清理原则 | `docs/architecture/paper_v2_selection_runtime_gate_cleanup_addendum_20260526.md` | 仅 StrategyPackage 资产完整性属于准入；平台运行依赖属于 run/session，不应成为策略包门禁。 |
| 连续运行和 cutoff 设计 | `docs/architecture/paper_v2_live_continuous_daily_cutoff_and_symbol_name_design_20260514.md` | LIVE 每个交易日必须自动计算 `effective_cutoff_date=previous_trading_day(trade_date)`。 |
| MiniQMT 多策略方向 | `docs/architecture/simulation_remediation_project_design_20260521.md`、`docs/architecture/miniqmt_multi_strategy_virtual_account_poc_design_20260518.md` | 长期需要多策略分仓/归因；短期必须先保证单账户单 active portfolio 的安全无人值守。 |

### 2.1 当前缺口

1. **重启自动运行依赖环境变量**：session 已持久化，但 scheduler 是否随后端自动启动取决于启动环境，不是组合级配置。
2. **缺少组合级 auto-run 配置**：当前 runtime_config 存在于 session/run 请求内，缺少“这个 portfolio 今后每天都自动运行”的持久业务开关和配置 hash。
3. **缺少 MiniQMT 可下单窗口等待**：MiniQMT live tick 当前按交易日直接尝试运行，缺少“未到下单窗口只等待，不创建失败 run”的状态机。
4. **缺少自动重试分层**：MiniQMT 断线、数据未刷新、HMM 当日缓存计算中等可恢复问题，应在截止时间前等待/重试；订单已提交后的失败则必须转入对账而不是重复提交。
5. **缺少一键创建 MiniQMT 自动组合 API/UI**：用户仍需要理解 portfolio、session、live source、runtime_config 等底层参数。
6. **多策略共享 MiniQMT 账户尚未安全隔离**：当前 MiniQMT 真实模拟账户是券商侧合并账户；没有完整分仓归因前，不应允许多个 active MiniQMT auto-run portfolio 共享同一账户自动下单。

### 2.2 允许复用的现有 API / 服务入口

本项目实现时只能复用或扩展下列已存在入口，不得绕过主链路新建“临时下单脚本”：

| 类别 | 允许入口 | 说明 |
|---|---|---|
| scheduler 状态 | `GET /api/v1/paper-v2/session-scheduler/status` | 检查当前进程 scheduler 是否运行。 |
| scheduler 启动 | `POST /api/v1/paper-v2/session-scheduler/start` | 当前进程内启动；重启持久化仍必须靠 env/启动配置。 |
| scheduler 单次 tick | `POST /api/v1/paper-v2/session-scheduler/run-once` | 运维诊断入口；不能作为每日人工运行依赖。 |
| session 创建 | `PaperTradingSessionService.create_session(...)` | 创建持久 `LIVE_ONLY`/`CATCHUP_THEN_LIVE` session。 |
| session tick | `PaperTradingSessionRunner.tick(...)` | scheduler 和 UI 必须共用同一 tick API。 |
| MiniQMT live tick | `PaperTradingLiveMinuteExecutor._tick_minqmt_live_intraday(...)` | MiniQMT SIM 主执行路径；只允许 `MINIQMT_REALTIME`。 |
| 单日执行 | `PaperTradingDayRunner.run_day(...)` | 生成 signal、target、order intent，并调用 MiniQMT broker。 |
| MiniQMT broker | `MiniQMTSimBackend.submit_order_intent(...)` | 唯一允许的 MiniQMT Paper v2 下单适配层。 |
| 交易日状态 | `TradingCalendarStatusService` / `/api/v1/trading-calendar/status` | 唯一官方交易日服务，禁止 weekday fallback。 |

禁止模式：

- 不得使用 `/api/v1/qmt/order` 裸诊断口作为 Paper v2/MiniQMT 自动运行主路径。
- 不得用 TDX/LocalSim 成交结果替代 MiniQMT broker-authoritative 成交/持仓。
- 不得在 StrategyPackage manifest 中写入 auto-run、MiniQMT 账号、HMM 当日 cache、TopK、黑名单等组合运行配置。
- 不得为了绕过 MiniQMT 连接失败或数据未就绪而返回空订单成功、默认价格成功或默认持仓成功。

## 3. 设计原则

1. **StrategyPackage 只负责 alpha core 资产**：入场只检查包存在、manifest/hash 一致、核心模型/因子/必要资产可读、来源合规、未删除/隔离。随机种子模型如果回测效果优秀且资产完整，可以进入模拟盘；不支持滚动训练不是模拟盘门禁。
2. **平台运行能力是组合级 runtime**：交易日、HMM、TopK、行业黑名单、停牌剔除、ST、涨跌停、MiniQMT 连接、行情源、执行算法、费用、风控、资金账户都属于组合运行配置或每日运行依赖，不写入 StrategyPackage frozen manifest。
3. **MiniQMT broker-authoritative**：MiniQMT 路径不得用 TDX 或 LocalSim 结果伪造成交；订单、成交、现金、持仓以 MiniQMT 查询为权威。
4. **同一交易日幂等**：同一 portfolio + trade_date 已经完成 broker reconciliation 时，不重复下单，只同步状态和展示。
5. **等待不是成功，失败不是门禁**：非交易日、盘前、等待数据、等待 broker 是等待状态；当日运行失败是 run/session 失败；二者都不能改变策略包资格。
6. **先安全单策略，后多策略分仓**：Phase 1 保证一个 MiniQMT 账户同一时间只有一个 active auto-run portfolio；Phase 2 再引入虚拟资金/lot 归因后支持多策略。
7. **所有自动行为必须可见、可停、可诊断**：UI 必须展示自动运行开关、下一次计划、当前等待原因、最近订单/成交/持仓、错误摘要和可复制诊断文本。

## 4. 参数配置总表

### 4.1 后端全局环境变量

| 参数 | 默认值 | 推荐生产值 | 说明 |
|---|---:|---:|---|
| `ENABLE_PAPER_TRADING_V2_SCHEDULER` | 空/false | `true` | 后端启动时自动启动 Paper v2 session scheduler；这是今天后端重启后自动运行的关键配置。 |
| `PAPER_TRADING_V2_SCHEDULER_INTERVAL_SEC` | `30` | `30` | scheduler 轮询间隔；生产建议 30 秒，开发可 5-60 秒。 |
| `PAPER_V2_AUTO_RUN_ENABLED` | `true` | `true` | 新增：是否允许组合级 auto-run coordinator 工作；关闭时 scheduler 只处理普通 tickable session。 |
| `PAPER_V2_AUTO_RUN_MAX_SESSIONS_PER_TICK` | `50` | `50` | 每次调度最多处理的 auto-run/session 数量。 |
| `PAPER_V2_AUTO_RUN_TIMEZONE` | `Asia/Shanghai` | `Asia/Shanghai` | 交易日和交易窗口的唯一时区；不得使用系统本地时区猜测。 |
| `PAPER_V2_AUTO_RUN_BOOTSTRAP_MISSING_SESSION` | `true` | `true` | 新增：后端启动/scheduler tick 时，若 auto-run portfolio 没有 active session，自动创建 LIVE_ONLY session。 |
| `PAPER_V2_AUTO_RUN_MISFIRE_POLICY` | `catch_up_same_day` | `catch_up_same_day` | 后端/调度器在交易日盘中恢复时的处理；在提交窗口内补跑，过 cutoff 则记录跳过/失败。 |
| `PAPER_V2_AUTO_RUN_REQUIRE_OFFICIAL_CALENDAR` | `true` | `true` | 必须使用 `TradingCalendarStatusService` / `market.trading_calendar`，禁止 weekday fallback。 |
| `PAPER_V2_AUTO_RUN_LOG_DIAGNOSTIC_JSON` | `true` | `true` | 错误事件保存可复制 JSON 诊断信息，但 UI 默认展示中文摘要。 |

### 4.2 MiniQMT 连接环境变量

以下变量继续由 `backend/infra/qmt_client.py` 读取；自动运行只消费其状态，不在 StrategyPackage 内保存：

| 参数 | 默认值 | 推荐值 | 说明 |
|---|---:|---:|---|
| `MINIQMT_ENABLED` | `false` | `true` | 是否启用 xtquant-backed MiniQMT client。 |
| `MINIQMT_ACCOUNT_ID` | 空 | 例如 `62266303` | 当前 MiniQMT SIM 账号；auto-run binding 必须与此一致。 |
| `MINIQMT_MODE` | `SIM` | `SIM` | 自动模拟盘仅允许 `SIM`；未来 live 另走 LiveApproval。 |
| `MINIQMT_USERDATA_PATH` | 空 | `F:\QMT_SIM\userdata_mini` | MiniQMT userdata_mini 路径。 |
| `MINIQMT_SESSION_ID` | 进程 PID | 固定值，例如 `123456` | xtquant session id；生产建议固定，避免重启后重复连接语义漂移。 |
| `MINIQMT_XTQUANT_DIR` | repo bundled | `F:\Dev\AIstock\xtquant` | xtquant 模块目录。 |
| `MINIQMT_CONNECT_TIMEOUT_SECONDS` | `15` | `15` | 连接超时。 |
| `MINIQMT_QUERY_TIMEOUT_SECONDS` | `2` | `2-5` | 查询账户/订单/成交/持仓超时。 |
| `MINIQMT_STATUS_AUTOCONNECT_INTERVAL_SECONDS` | `30` | `30` | 状态查询自动连接间隔。 |
| `MINIQMT_RETRY_NEW_SESSION_ON_RC_MINUS1` | `true` | `true` | 连接返回 -1 时尝试新 session。 |
| `MINIQMT_STOP_TIMEOUT_SECONDS` | `2` | `2` | 停止 xtquant 背景线程超时。 |

### 4.3 MiniQMT 自动运行交易窗口参数

这些参数新增到 auto-run coordinator，默认值可由环境变量覆盖，也可在组合 `auto_run_config.trade_window_policy` 覆盖：

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `prepare_start` | `08:50` | 允许开始检查交易日、数据、broker、HMM cache、selection artifact 的时间。 |
| `morning_submit_start` | `09:25` | 最早允许提交 MiniQMT 买卖单；避免凌晨/盘前误提交。 |
| `morning_submit_end` | `11:30` | 上午提交窗口结束。 |
| `afternoon_submit_start` | `13:00` | 下午恢复提交窗口开始；MiniQMT 上午不可用时可下午继续。 |
| `final_submit_cutoff` | `14:55` | 最晚提交截止；之后不再新提交订单，只允许查询/对账/记录失败。 |
| `after_cutoff_policy` | `fail_day_without_submit` | 超过截止仍未提交时，记录当日最终失败/跳过，不能伪造成成功。 |
| `allow_intraday_start` | `true` | 允许盘中创建/恢复自动运行；用于 MiniQMT 上午不可用、下午恢复的场景。 |
| `wait_before_window_status` | `LIVE_WAITING_MARKET_WINDOW` | 新增 session 状态或事件类型，表示今天是交易日但尚未到提交窗口。 |

### 4.4 数据和选股参数

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `signal_data_source` | `DB_HISTORICAL` | MiniQMT SIM 的选股信号来源；不得列出 `TDX_REALTIME`/`MINIQMT_REALTIME` 作为因子计算源。 |
| `live_data_source` | `MINIQMT_REALTIME` | MiniQMT broker 权威通道；只用于 MiniQMT 下单/查询/对账。 |
| `selection_artifact_config.auto_generate` | `true` | 当日没有权威 selection artifact 时自动生成。 |
| `selection_artifact_config.inference_backend` | `wsl` | 现有 live inference 后端。 |
| `selection_artifact_config.pit_mode` | `PREVIOUS_TRADING_DAY_CLOSE` | 每个 trade_date 使用上一交易日及以前数据。 |
| `selection_artifact_config.cutoff_date` | 自动计算 | LIVE auto-run 禁止长期固定；每个交易日计算 `previous_trading_day(trade_date)`。 |
| `selection_artifact_config.include_reference_price` | `true` | 保存入场价参考；历史使用 PIT 截止日收盘，当前日根据规则使用 TDX/pre_close。 |
| `selection_artifact_config.artifact_reuse` | `same_trade_date_config_hash` | 同一交易日同一 package/runtime hash 可复用；跨日必须新 artifact。 |
| `selection.top_k` | 组合配置 | 组合级每日运行配置，非策略包门禁。 |
| `selection.display_top_n` | UI 配置 | 只影响展示，不影响交易行为。 |
| `tradability.exclude_suspended` | `true` | 停牌剔除属于运行时平台能力；数据缺失形成 run/session 错误。 |
| `industry_blacklist` | `[]` | 组合级黑名单；UI 复用 QE 自定义任务行业选择器。 |

### 4.5 HMM 参数

HMM 彻底取消“每天手工生成/选择 snapshot”的业务模式。组合只保存模型选择和运行策略：

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `hmm.enabled` | `false` | 是否启用 HMM。 |
| `hmm.model_config_id` | `null` | 用户选择的 HMM 模型配置；启用时必填。 |
| `hmm.signal_preset` | `default` | 信号预设。 |
| `hmm.auto_compute` | `true` | 当日无缓存时自动计算。 |
| `hmm.cache_scope` | `model_config_id + signal_preset + trade_date + input_hash` | 同一天同配置只计算一次。 |
| `hmm.manual_snapshot_required` | `false` | 禁止手工 snapshot 作为日常必需步骤。 |
| `hmm.on_compute_failure` | `fail_run` | HMM 启用且自动计算失败时，本次 run 失败；不得用中性系数伪造成功。 |
| `hmm.cache_storage` | `paper_v2/hmm_daily_coeff_cache` 或既有 HMM artifact store | 缓存必须可审计，记录输入 hash、模型版本、计算时间。 |

### 4.6 组合级 `auto_run_config` 结构

新增 portfolio 级持久配置。推荐 JSON schema：

```json
{
  "schema_version": "paper_v2_auto_run_v1",
  "enabled": true,
  "broker": {
    "broker_backend": "minqmt_sim",
    "broker_mode": "SIM",
    "account_id": "62266303",
    "live_data_source": "MINIQMT_REALTIME",
    "authority_source": "MINIQMT_QUERY",
    "account_binding_mode": "exclusive_account_phase1",
    "strategy_name_template": "paper_{portfolio_id_short}",
    "order_remark_schema": "aistock_paper_v2_json_v1"
  },
  "session_policy": {
    "mode": "LIVE_ONLY",
    "create_on_enable": true,
    "recover_on_backend_start": true,
    "manual_tick_only": false,
    "duplicate_trade_date_policy": "reconcile_no_duplicate_submit",
    "missing_session_policy": "auto_create_live_only"
  },
  "calendar_policy": {
    "timezone": "Asia/Shanghai",
    "calendar_service": "TradingCalendarStatusService",
    "non_trading_day_policy": "wait_next_trading_day",
    "missing_calendar_row_policy": "fail_fast"
  },
  "trade_window_policy": {
    "prepare_start": "08:50",
    "submit_windows": [
      {"start": "09:25", "end": "11:30"},
      {"start": "13:00", "end": "14:55"}
    ],
    "final_submit_cutoff": "14:55",
    "allow_intraday_start": true,
    "after_cutoff_policy": "fail_day_without_submit"
  },
  "selection_artifact_config": {
    "signal_data_source": "DB_HISTORICAL",
    "auto_generate": true,
    "inference_backend": "wsl",
    "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
    "include_reference_price": true,
    "artifact_reuse": "same_trade_date_config_hash"
  },
  "runtime_profile": {
    "selection": {
      "top_k": 10,
      "daily_strategy_id": "score_weighted_topk_v2",
      "daily_strategy_params": {}
    },
    "hmm": {
      "enabled": false,
      "model_config_id": null,
      "signal_preset": null,
      "auto_compute": true,
      "manual_snapshot_required": false
    },
    "tradability": {
      "exclude_suspended": true
    },
    "industry_blacklist": []
  },
  "retry_policy": {
    "broker_connect_retry_interval_seconds": 30,
    "data_ready_retry_interval_seconds": 60,
    "hmm_compute_retry_interval_seconds": 60,
    "max_retry_until": "14:55",
    "retryable_error_codes": [
      "MINIQMT_NOT_CONNECTED",
      "MINIQMT_QUERY_TIMEOUT",
      "DATA_REFRESH_NOT_READY",
      "HMM_DAILY_CACHE_BUILDING"
    ]
  },
  "reconciliation_policy": {
    "post_submit_query_delay_seconds": 3,
    "query_orders": true,
    "query_trades": true,
    "query_positions": true,
    "already_reconciled_policy": "no_duplicate_orders",
    "partial_submit_policy": "broker_authoritative_reconcile"
  },
  "ui_policy": {
    "show_next_plan": true,
    "show_compact_error": true,
    "show_copyable_diagnostic": true
  }
}
```

## 5. 数据模型设计

### 5.1 `paper_v2.portfolio` 新增列

建议第一阶段直接在 portfolio 上增加 auto-run 开关和配置，避免为 MVP 引入过多表：

| 字段 | 类型 | 说明 |
|---|---|---|
| `auto_run_enabled` | `BOOLEAN NOT NULL DEFAULT FALSE` | 是否启用组合级自动运行。 |
| `auto_run_config` | `JSONB NOT NULL DEFAULT '{}'::jsonb` | 组合自动运行配置，schema 为 `paper_v2_auto_run_v1`。 |
| `auto_run_config_sha256` | `TEXT` | 规范化 JSON 的 sha256，用于审计、幂等和 artifact 复用。 |
| `auto_run_updated_at` | `TIMESTAMPTZ` | 最近一次 auto-run 配置更新时间。 |
| `auto_run_updated_by` | `TEXT` | 操作者或系统来源。 |

DDL 必须包含 `COMMENT ON TABLE` / `COMMENT ON COLUMN`。示例语义：`auto_run_config` 注释必须说明它是 portfolio runtime 配置，不属于 StrategyPackage frozen manifest。

### 5.2 `paper_v2.broker_account_binding`

Phase 1 为避免多个策略包共享同一 MiniQMT 模拟账户互相抢现金/持仓，新增账户绑定表：

| 字段 | 类型 | 说明 |
|---|---|---|
| `binding_id` | `TEXT PRIMARY KEY` | 绑定 ID。 |
| `broker_backend` | `TEXT NOT NULL` | `minqmt_sim` / future `minqmt_live`。 |
| `broker_mode` | `TEXT NOT NULL` | `SIM` / future `LIVE`；本设计只允许 `SIM`。 |
| `broker_account_id` | `TEXT NOT NULL` | MiniQMT 账号。 |
| `portfolio_id` | `TEXT NOT NULL REFERENCES paper_v2.portfolio(portfolio_id)` | 绑定组合。 |
| `binding_status` | `TEXT NOT NULL` | `ACTIVE` / `PAUSED` / `RETIRED`。 |
| `allocation_mode` | `TEXT NOT NULL DEFAULT 'exclusive_account'` | Phase 1 只允许 `exclusive_account`。 |
| `initial_cash` | `NUMERIC` | 组合创建时资金；Phase 2 用于虚拟分仓。 |
| `created_at` / `updated_at` | `TIMESTAMPTZ` | 审计时间。 |
| `created_by` | `TEXT` | 操作者。 |

约束：Phase 1 增加唯一约束，禁止同一 `(broker_backend, broker_mode, broker_account_id)` 存在多个 `ACTIVE` 绑定。这个约束是账户资源隔离，不是策略包门禁；UI 必须解释为“该 MiniQMT 模拟账户已有自动运行组合，需要停用/退役后再绑定新组合”。

### 5.3 状态和事件复用

优先复用现有：

- `paper_v2.trade_session`
- `paper_v2.session_day`
- `paper_v2.session_event`
- `paper_v2.run`
- `paper_v2.order`
- `paper_v2.daily_snapshot` / `intraday_snapshot`

新增 session 状态建议：

| 状态 | 含义 |
|---|---|
| `LIVE_WAITING_MARKET_WINDOW` | 今日是交易日，但尚未到 MiniQMT 自动提交窗口。 |
| `LIVE_WAITING_PLATFORM_DATA` | 交易窗口前/中等待平台数据，例如 `suspend_d`、选股输入、HMM cache。 |
| `LIVE_WAITING_BROKER` | 等待 MiniQMT 连接/账号匹配/可查询。 |
| `LIVE_RETRYING` | 当日可恢复错误，截止时间前继续重试。 |

如果暂不扩展 enum，也必须以 `session_event.event_type` 记录同名事件，并在 UI 聚合为上述中文状态。

## 6. 后端运行流程

### 6.1 后端启动流程

```text
FastAPI startup
  -> 读取 ENABLE_PAPER_TRADING_V2_SCHEDULER
  -> 启动 paper_trading_v2_scheduler
  -> scheduler 首次 run_once
      -> AutoRunCoordinator.recover_enabled_portfolios()
          -> 查询 auto_run_enabled=true portfolio
          -> 校验 broker_account_binding
          -> 若无 active session 且 missing_session_policy=auto_create_live_only，创建 LIVE_ONLY session
          -> 不立即越过交易窗口；只进入等待/可 tick 状态
```

今天的最小配置路径：

```powershell
[Environment]::SetEnvironmentVariable("ENABLE_PAPER_TRADING_V2_SCHEDULER", "true", "User")
[Environment]::SetEnvironmentVariable("PAPER_TRADING_V2_SCHEDULER_INTERVAL_SEC", "30", "User")
```

后端重启后验收：

```powershell
Invoke-RestMethod http://127.0.0.1:8001/api/v1/paper-v2/session-scheduler/status
```

必须返回 `running=true`、`thread_alive=true`、`interval_seconds=30`。

### 6.2 每次 tick 的状态机

```text
AutoRunCoordinator.tick(session, as_of_time)
  1. 读取 portfolio、auto_run_config、broker_account_binding
  2. 使用 TradingCalendarStatusService 解析 Asia/Shanghai 当前日期
  3. 非交易日 -> WAIT_NEXT_TRADING_DAY，不失败
  4. 已有当日 SUCCEEDED/reconciled run -> ALREADY_RECONCILED，不重复下单
  5. 未到 prepare_start -> WAIT_MARKET_WINDOW
  6. prepare_start 后执行 preflight：
       - MiniQMT env/account/mode 匹配
       - broker 可连接/可查询
       - trading_calendar 覆盖
       - 必要平台数据是否就绪
       - HMM 当日 cache 是否存在，不存在则自动计算
       - selection artifact 是否存在，不存在则自动生成
  7. preflight 可恢复失败且未到 cutoff -> WAIT_PLATFORM_DATA / WAIT_BROKER / RETRYING
  8. 到 submit window 且 preflight 通过 -> 调用 MiniQMT day runner
  9. 下单后立即查询订单/成交/持仓 -> 保存 broker-authoritative snapshot
  10. 状态进入 RECONCILED / WAIT_NEXT_TRADING_DAY
  11. cutoff 后仍未提交 -> FINAL_FAILED_NO_SUBMIT，保存错误，不伪成功
```

### 6.3 幂等和重复提交防护

1. `portfolio_id + trade_date` 已有 `RunStatus.SUCCEEDED`：不得再次调用 `place_order()`。
2. 当日已保存 `MINIQMT_LIVE_TICK_RECONCILED` 事件：后续 tick 只查询/展示，不提交。
3. 如果 run 已创建但订单部分提交后进程崩溃，恢复后必须先按 `order_remark` / `strategy_name` / MiniQMT order id 查询 broker，再决定 reconcile；不得重新生成一批新订单。
4. `order_remark` 必须包含 `portfolio_id`、`package_id`、`intent_id`，长度超限时保留可映射短格式。

### 6.4 MiniQMT 可恢复错误分类

| 错误 | 截止前行为 | 截止后行为 |
|---|---|---|
| MiniQMT 未登录 / xtquant 连接失败 | `LIVE_WAITING_BROKER`，按 retry interval 重试 | `AUTO_FAILED_FINAL`，无订单 |
| 账号不匹配 | `LIVE_WAITING_BROKER`，告警账号期望/实际 | final failed，需要人工处理 |
| 查询超时 | retry | final failed 或保留部分 broker 状态 |
| `suspend_d`/`stk_limit`/输入数据未就绪 | `LIVE_WAITING_PLATFORM_DATA` | final failed，无订单 |
| HMM cache 自动计算中 | `LIVE_WAITING_PLATFORM_DATA` | final failed，无中性 fallback |
| selection artifact 自动生成失败 | retry，记录失败阶段 | final failed |
| MiniQMT 拒单 | 订单级失败，run 进入失败或部分失败 | 不重复提交；以 broker 状态为准 |
| 已有当日 run succeeded | no-op reconcile | no-op reconcile |

## 7. API 设计

### 7.1 创建 MiniQMT 自动组合

`POST /api/v1/paper-v2/auto-run/miniqmt-portfolios`

请求：

```json
{
  "package_id": "pkg_...",
  "portfolio_name": "miniqmt_loop16_auto",
  "initial_cash": 30000000,
  "start_date": "2026-05-28",
  "broker_account_id": "62266303",
  "top_k": 10,
  "hmm": {"enabled": false, "model_config_id": null, "signal_preset": null},
  "industry_blacklist": [],
  "fee_policy": {"commission_rate": 0.00025, "min_cost": 5},
  "trade_window_policy": null,
  "created_by": "operator"
}
```

响应：

```json
{
  "ok": true,
  "portfolio": {"portfolio_id": "paper_...", "broker_backend": "minqmt_sim"},
  "session": {"session_id": "psess_...", "status": "LIVE_WAITING_NEXT_TRADING_DAY"},
  "auto_run": {"enabled": true, "config_sha256": "...", "next_plan": "2026-05-28 09:25 Asia/Shanghai"}
}
```

### 7.2 启用/停用已有组合自动运行

- `POST /api/v1/paper-v2/portfolios/{portfolio_id}/auto-run/enable`
- `POST /api/v1/paper-v2/portfolios/{portfolio_id}/auto-run/disable`
- `GET /api/v1/paper-v2/portfolios/{portfolio_id}/auto-run/status`
- `PATCH /api/v1/paper-v2/portfolios/{portfolio_id}/auto-run/config`

启用时仅做组合运行配置校验和 broker account binding 冲突校验；不得重新校验一堆策略包级平台门禁。

### 7.3 scheduler 运维 API

保留现有：

- `GET /api/v1/paper-v2/session-scheduler/status`
- `POST /api/v1/paper-v2/session-scheduler/start`
- `POST /api/v1/paper-v2/session-scheduler/stop`
- `POST /api/v1/paper-v2/session-scheduler/run-once`

新增建议：

- `GET /api/v1/paper-v2/session-scheduler/bootstrap-status`：展示 env 是否持久启用、当前线程、最近 tick、auto-run portfolio 数量。
- `POST /api/v1/paper-v2/session-scheduler/recover-auto-run`：手动触发恢复，不下单，只补齐缺失 session/状态。

## 8. UI 设计

### 8.1 策略包入口

策略包页面/选股中心增加“加入 MiniQMT 模拟盘自动运行”：

- 展示策略包中文名、package_id 小字、manifest hash、资产检查结果。
- 仅资产检查失败时阻止创建。
- MiniQMT 连接/交易日/数据/HMM 不作为创建阻断，只在预览区显示“运行时依赖，将由自动运行处理”。

### 8.2 创建弹窗

字段：

1. MiniQMT 账号：默认读取当前 `MINIQMT_ACCOUNT_ID`，只允许 SIM。
2. 组合名称。
3. 初始资金。
4. 开始日期。
5. TopK。
6. HMM：选择模型配置 + signal preset + 是否启用；不显示可用 snapshot 列表。
7. 行业黑名单：复用 QE 自定义演进任务行业选择器。
8. 自动运行开关：默认开启。
9. 高级：交易窗口、retry policy、费用。

### 8.3 组合卡片/控制台

必须显示：

- 自动运行：已启用/已停用。
- 下次计划：日期 + 时间窗口。
- MiniQMT：连接状态、账号、SIM/LIVE。
- 当前业务状态：等待交易日、等待开盘窗口、等待数据、等待 MiniQMT、正在选股、正在下单、已完成、失败。
- 今日 evidence：selection artifact id、cutoff date、HMM cache date、订单数、成交数、持仓数、NAV。
- 错误：中文摘要 + “复制诊断信息”按钮；不得用多层 raw JSON 表格抽屉作为主要展示。

## 9. 多策略包 MiniQMT 策略

### 9.1 Phase 1：单账户单 active portfolio

立即落地的安全规则：

- 同一个 MiniQMT SIM account 同一时间只允许一个 active auto-run portfolio。
- 已有 active 绑定时，创建第二个组合返回账户资源冲突：提示用户停用/退役/删除旧组合或使用其他 MiniQMT 账号。
- 这是 broker account 资源隔离，不是策略包门禁。

### 9.2 Phase 2：虚拟分仓多策略

支持多个策略包共享同一 MiniQMT SIM 账号前，必须完成：

1. 虚拟现金分配和冻结现金归因。
2. 按策略记录 lot，禁止策略 A 卖出策略 B 的持仓。
3. 同一股票多个策略持仓时，broker 侧合并、AIstock 侧分解。
4. order_remark 和 strategy_name 可反查 portfolio/package/intent。
5. 每日 broker reconciliation 能解释券商持仓与虚拟分仓差异。
6. UI 展示每个策略独立收益、现金、持仓、订单、成交。

在 Phase 2 完成前，不允许“为了方便”放开同账户多 active 自动下单。

## 10. 开发方案

### Phase 0：文档和现状基线

- 读取并确认 `docs/codex_project_memory.md`、Paper v2 session/scheduler/MiniQMT 相关设计。
- 记录当前端口和生产影响边界：不得由测试脚本重启生产 8001。
- 输出本设计文档并合入 main。

验收：设计文档合入 `origin/main`，`production_ddl_gate=noop`。

### Phase 1：启动配置和运行手册

实现内容：

1. 增加生产启动配置检查：后端启动日志明确打印 Paper v2 scheduler 是否启用。
2. 增加 `/session-scheduler/bootstrap-status`。
3. 补充运维文档：如何设置 `ENABLE_PAPER_TRADING_V2_SCHEDULER=true`、如何检查重启后状态。
4. 增加测试覆盖 env true/false、interval、scheduler status。

验收：后端重启后 scheduler 自动 running；不依赖手工 POST `/start`。

### Phase 2：组合级 auto-run 配置

实现内容：

1. DB migration：`paper_v2.portfolio` 新增 auto-run 列，添加 comments。
2. DB migration：新增 `paper_v2.broker_account_binding`，添加 comments 和唯一约束。
3. 新增 `AutoRunConfig` Pydantic model 和 normalize/hash 函数。
4. `PaperTradingV2PortfolioService` 增加 enable/disable/update auto-run 方法。

验收：配置可持久化、hash 稳定、重启后可读取；DB comment 检查通过。

### Phase 3：AutoRunCoordinator

实现内容：

1. 新增 `backend/services/paper_trading_v2/auto_run.py`。
2. scheduler 在 run_once 前/中调用 coordinator 恢复 enabled portfolios。
3. 缺失 active session 自动创建 `LIVE_ONLY + MINIQMT_REALTIME + manual_tick_only=false`。
4. 非交易日、缺交易日行、缺配置返回结构化状态。

验收：删除/停掉 session 后，enabled portfolio 能自动补 session；非交易日不失败。

### Phase 4：MiniQMT 交易窗口和重试状态机

实现内容：

1. `_tick_minqmt_live_intraday()` 前增加交易窗口判断。
2. 未到窗口保存 `LIVE_WAITING_MARKET_WINDOW` 事件/状态，不创建 run。
3. broker/data/HMM/selection preflight 在 cutoff 前返回等待状态；cutoff 后才 final failed。
4. 已有当日 run/order 时先 broker reconciliation，防重复提交。

验收：盘前 tick 不下单不失败；午后恢复可执行；同日重复 tick 不重复下单。

### Phase 5：HMM 自动日缓存

实现内容：

1. 组合选择 HMM model_config 后，运行时按 trade_date 自动查缓存。
2. 无缓存时自动计算并保存 cache artifact。
3. 同日同配置复用缓存。
4. 移除 UI 日常 snapshot 手工选择依赖；保留模型训练/诊断页面显示最近 cache date。

验收：HMM enabled 时无需手工 snapshot；缺数据失败清晰；无中性 fallback。

### Phase 6：API 和 UI

实现内容：

1. 增加 MiniQMT auto-run portfolio 创建 API。
2. 增加 auto-run enable/disable/status/update API。
3. UI 创建弹窗和组合卡片接入。
4. 错误展示改为中文摘要 + 可复制诊断。

验收：用户从策略包页面一次点击可创建 MiniQMT 自动组合；无需手工 session 参数。

### Phase 7：验证和生产激活

实现内容：

1. 单元/API/UI/业务流验证矩阵全部通过。
2. 生产 DDL migration 应用并验证 comments。
3. 后端重启验证 scheduler 自动启动。
4. MiniQMT SIM 交易时段小资金或测试账户验证一次完整 broker-authoritative run。

验收：所有验收矩阵 PASS 后才合入代码实现；实盘 live 仍不可用。

## 11. 验证矩阵

| 编号 | 场景 | 验证方法 | 通过标准 |
|---|---|---|---|
| V-01 | 后端启动 env=false | 单元/启动测试 | scheduler 不自动启动，接口显示 disabled。 |
| V-02 | 后端启动 env=true | 单元/启动测试 + API | `/session-scheduler/status` 为 running。 |
| V-03 | enabled portfolio 缺 active session | API/integration | 自动创建 LIVE_ONLY session，不下单。 |
| V-04 | 非交易日 tick | calendar mock | 状态等待下一交易日，无 run、无订单、无失败。 |
| V-05 | 交易日盘前 tick | time mock 08:00/09:00 | 等待 market window，不调用 MiniQMT `place_order()`。 |
| V-06 | 上午 MiniQMT 未连接 | fake qmt client | 等待 broker 并重试，未到 cutoff 不 final failed。 |
| V-07 | 下午 MiniQMT 恢复 | fake qmt client 状态切换 | 在 13:00-14:55 成功提交并 reconcile。 |
| V-08 | cutoff 后仍未连接 | time mock 14:56 | 当日 final failed，无订单，错误可复制。 |
| V-09 | 数据未就绪 | refresh_audit mock | 等待平台数据，cutoff 前不创建失败 run。 |
| V-10 | HMM 无缓存 | HMM service mock | 自动计算一次，第二次同日复用。 |
| V-11 | HMM 计算失败 | HMM service mock error | run/session 失败，禁止中性 fallback。 |
| V-12 | 当日已 reconciled | 已有 SUCCEEDED run | 不重复提交订单，只记录 already reconciled。 |
| V-13 | 进程崩溃后恢复 | 创建部分订单后重启模拟 | 先 broker 查询/对账，不生成重复 intent 下单。 |
| V-14 | 同账号第二个 auto-run portfolio | API | 返回账户 binding 冲突，中文说明，不标记策略包不可用。 |
| V-15 | 策略包资产不完整 | API | 只有 asset eligibility 阻止创建，错误指出缺失文件/hash。 |
| V-16 | runtime 参数修改 | API/UI | TopK/HMM/黑名单可改组合配置，不触发 StrategyPackage gate。 |
| V-17 | UI 一键创建 | Playwright | 从策略包到 MiniQMT 自动组合创建成功，显示下一计划。 |
| V-18 | UI 错误诊断 | Playwright | 中文摘要 + 可复制 JSON；无多层 raw JSON 主视图。 |
| V-19 | DB comments | migration test | 新表/新列 comment 全覆盖。 |
| V-20 | 生产不误用 live | static/API | `minqmt_live` 仍不可由 Paper v2 auto-run 创建；live 另走 LiveApproval。 |

## 12. 今日临时运行建议

在代码实现 Phase 1 前，今天要保证“后端重启后继续自动 tick”可以先执行用户级环境变量：

```powershell
[Environment]::SetEnvironmentVariable("ENABLE_PAPER_TRADING_V2_SCHEDULER", "true", "User")
[Environment]::SetEnvironmentVariable("PAPER_TRADING_V2_SCHEDULER_INTERVAL_SEC", "30", "User")
```

然后由用户重启后端，确认：

```powershell
Invoke-RestMethod http://127.0.0.1:8001/api/v1/paper-v2/session-scheduler/status
```

当前已存在的 MiniQMT session 如果仍为 `LIVE_WAITING_NEXT_TRADING_DAY` 且 MiniQMT 客户端保持 `SIM` 登录，重启后 scheduler 会继续 tick。注意：在交易窗口等待逻辑正式实现前，仍建议明早人工观察一次，防止过早 tick 被 MiniQMT 拒绝后进入失败状态。

## 13. 合入标准

代码实现合入 main 前必须满足：

1. 设计条款逐项对照：每个 Phase 有实现位置和验证证据。
2. `production_ddl_gate` 明确：涉及新增 DB 列/表时，生产 migration 必须应用并验证 comments；本文档合入为 `noop`。
3. `production_backend_dependency_gate` 明确：无新增依赖或依赖已同步。
4. `production_frontend_dependency_gate` 明确：UI 如新增依赖必须 build 验证。
5. MiniQMT SIM 验证只允许 SIM 账号；不得触发 live 实盘。
6. 不允许把未实现的 Phase 报告为完成。
