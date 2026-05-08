# Trading Core 测试矩阵

日期：2026-05-09
归属：Claude Code 工作面（per `codex_project_memory.md` line 944：paper_trading_v2 + Paper v2 tests/docs，no QE shared implementation files）
设计依据：`docs/architecture/trading_core_v2.md` + `paper_trading_v2_top_level_design.md` §8（vn.py 接入要求）

## 模块定位

trading_core 是 paper_trading_v2 的撮合 / OMS / Ledger / 风控核心层，包括：
- `oms.py`：Order 状态机、生命周期管理
- `ledger.py`：账本 + 现金 + 持仓 + Fill 应用
- `minute_execution.py`：MinuteExecutionEngine 撮合引擎
- `risk.py`：风险规则前置校验
- `execution_algo_adapter.py` / `execution_algo_capabilities.py`：分钟级执行算法集成
- `limit_price_provider.py`：涨跌停参考价
- `errors.py`：typed error 层级（含 R-Q9 broker 错误类）

vn.py 接入由 `backend/services/paper_trading_v2/poc/` 提供 PoC（已 task #3 验证），未来完整 vnpy_xt 集成走 PR-005。

| 维度 | 取值 |
| --- | --- |
| 模块 ID | `trading_core` |
| 风险等级 | high（撮合 / OMS / Ledger 任一漂移即破坏 paper v2 业务连续性 + Mode G 等价性物理基础） |
| 工作面 | Claude Code 维护 |
| 是否触动 main | 文档先行；trading_core 代码改动走 PR + 此矩阵驱动测试 |

## L0 静态守卫

L0 trigger：trading_core 任一 .py 文件变更 / errors.py 新加 typed error / models.py schema 变更。

- L0-G1：禁止 trading_core 代码引用生产 8001（per `codex_project_memory.md` line 314）
- L0-G2：禁止 silent fallback / `except: pass` / fallback 默认值（参考 `feedback_no_silent_errors.md`）
- L0-G3：所有 broker / 撮合 / OMS / Ledger 异常必须 typed（继承 `TradingCoreError` 或 `StrategyEngineError`）
- L0-G4：禁止 hardcoded 路径 / 数据库密码默认值（参考 `feedback_no_empty_db_password.md`）
- L0-G5：禁止 daily 数据 fallback（per Paper v2 oracle "minute data only"）

pass criteria：
- L0 nox session 通过
- semgrep `.semgrep/aistock/` 规则全绿
- typed error 类层级一致性脚本通过

## L1 单能力

L1 trigger：单个 trading_core 模块 / 单个 typed error / 单个 OMS 状态转移变更。

### L1-C1：errors.py 类型层级（含 R-Q9 broker 错误类）
- 验 `TradingCoreError` 根 + 各子类完整：
  - `BrokerSubmitError` / `BrokerRejectedError` / `BrokerConnectivityError`（broker 通信层，adapter 端抛）
  - `BrokerMarketSourceMismatchError`（市场源 vs broker 不匹配，§3.6.4）
  - `RiskRuleError` / `InvalidStateTransitionError` / `DataUnavailableError` / `ExecutionAlgoError`
- 任一 typed error 抛出 → error.context 必含 module_name + operation + relevant_ids
- pass：所有 typed error 在 errors.py 中定义并被 grep 引用 ≥1 次

### L1-C2：OMS 状态机（oms.py）
- Order 状态机：`pending → working → partially_filled → filled / cancelled / rejected`
- 非法转移（如 `filled → working`）→ `InvalidStateTransitionError`
- pass：所有合法转移有 unit test；非法转移 100% 抛 typed error

### L1-C3：Ledger 一致性（ledger.py）
- Fill 应用后 cash + positions + nav 公式：`new_nav = sum(positions[s].quantity * positions[s].mark_price) + new_cash`
- 反向 Fill（卖出）后 cash 增加 = price * quantity - commission
- pass：5 个典型 Fill 序列（买入 / 卖出 / 部分成交 / 拒单 / 撤单后再成交）账本一致

### L1-C4：MinuteExecutionEngine 时序（minute_execution.py）
- bar_ts 顺序严格升序；同一 bar 内 OrderIntent 按 intent_id 字典序确定
- 跨 bar OrderIntent 应用顺序与 bar_ts 一致
- 同一 OrderIntent 不可在多个 bar 重复成交
- pass：时序 invariant 在 fixture 数据上 100% 严格

### L1-C5：限价 / 涨跌停过滤（risk.py + limit_price_provider.py）
- 涨停状态买入 OrderIntent → `RiskRuleError`，含 limit_state + symbol
- 跌停状态卖出 OrderIntent → 同
- 缺涨跌停参考价（pre_close 缺失）→ `DataUnavailableError`，不静默放行
- pass：limit_state 4 类（normal / limit_up / limit_down / suspended）覆盖；缺数据 100% fail-fast

### L1-C6：分钟数据校验（minute data only invariant）
- 注入 daily 数据替代 minute → `DataUnavailableError`
- 缺 minute bar 但有 daily → 不允许 fallback；抛错
- pass：从未出现"daily fallback"路径

## L2 组件 / API / DB 流

L2 trigger：trading_core 内部多组件协作；DB 持久化；API 暴露。

### L2-F1：Order 完整生命周期 + Ledger 同步
- 提交 OrderIntent → OMS 创建 Order → MinuteExecutionEngine 撮合 → 产生 Fill → Ledger 应用
- 事件日志：`order_submitted → order_working → fill_event → order_filled → ledger_updated`
- pass：每事件持久化（DB / event log）；事件顺序严格；ledger 终态与 Order 终态一致

### L2-F2：BrokerBackend ↔ OMS ↔ Ledger 三方协作（LocalSim）
- `LocalSimBackend.submit_order_intent(intent)` → 内部调 OMS + MinuteExecutionEngine + Ledger
- 同步语义：返回时 OrderHandle.status 已为终态（filled / partial_filled / rejected）
- pass：现有 `test_submit_order_intent_returns_terminal_status_synchronously` 测试通过 + 整链 ledger 一致

### L2-F3：subscribe_fill_callback 生命周期（R-Q9.5 D3 + R-Q9.6）
- subscribe → fill_callback 在 submit 返回前触发（同步语义）→ unsubscribe → 回调释放
- 重复 unsubscribe 同一 handle / unknown handle / shutdown 期 unsubscribe → silent noop
- 真实底层连接故障期 unsubscribe → `BrokerConnectivityError`
- pass：现有 `test_subscribe_returns_handle_and_unsubscribe_releases` + `test_unsubscribe_unknown_handle_is_silent_noop` 通过

### L2-F4：errors API 暴露 / 持久化
- 抛出的 typed error 写入 `errors` 表；API `/api/v1/paper-trading-v2/errors` 可读
- error.context 完整持久化（不丢字段）
- pass：error 写入 + 读取一致；context 字段不被截断

### L2-F5：MarketDataSource 三态切换（per `paper_trading_v2/market_data.py`）
- `TDX_REALTIME` → `MINIQMT_REALTIME` 必须重启 broker（per §3.6.4）
- 同 backend 内 `TDX_REALTIME` ↔ `DB_HISTORICAL` 切换路径（CATCHUP_THEN_LIVE）
- 跨配（LocalSim 配 MINIQMT_REALTIME）→ `BrokerMarketSourceMismatchError`，三处时机（portfolio 启动 / live_session bootstrap / Engine init）全覆盖
- pass：`test_market_data_broker_match.py` 全绿；切换路径不引入 ledger 漂移

## L3 模块 UI/API 回归

> trading_core 通过 paper v2 portfolio 页面间接对外；本节 L3 与 paper_v2_selection_center.md 部分重叠，但聚焦 trading_core 视角。

### L3-I1：Paper v2 portfolio 一日完整流（end-to-end via trading_core）
- 创建 portfolio (LocalSim + TDX_REALTIME) → 一日 OrderIntent → fill → ledger 更新 → 持仓 / NAV / Fill / 错误持久化
- pass：UI 显示 ledger 终态；不出现原始 typed error 类名；console 无 pageerror

### L3-I2：DB_HISTORICAL 重放 + 切实时（CATCHUP_THEN_LIVE）
- portfolio 重放历史日 → 切 TDX_REALTIME 当日
- live_session 启动后 OMS 顺利接管；ledger 跨 session 状态一致（无 cash / positions 漂移）
- pass：`paper_v2_live` nox session 通过；live bar cursor 持续推进

### L3-I3：BrokerConnectivityError UI 显示
- 注入 broker 连接故障（如 LocalSim shutdown）→ adapter 抛 `BrokerConnectivityError`
- UI 必须显示 banner（顶部红色横条）+ 不允许"自动重连"按钮
- pass：UI 渲染符合 §6.3；console 无 Python traceback；error.context 完整展示

### L3-I4：vn.py 接入 PoC 路径（task #3 已验证）
- 走 `paper_trading_v2/poc/` 已验证的 vnpy_xt 集成路径
- assert PoC 执行不破坏现有 LocalSim 路径
- pass：PoC 子进程正常退出；现有 paper v2 backend tests 全绿

## Pass Criteria 汇总

| 等级 | 必须项 |
| --- | --- |
| L0 | semgrep 全绿；typed error 层级一致；无 daily fallback |
| L1 | 6 类单能力 (errors / OMS / Ledger / MinuteExecutionEngine / 限价过滤 / minute data only) 全绿 |
| L2 | OrderIntent 完整生命周期 + ledger 一致；同步语义 R-Q9.5 D4；subscribe / unsubscribe R-Q9.5 D3 + R-Q9.6 |
| L3 | paper v2 portfolio 一日 + CATCHUP_THEN_LIVE + UI 错误显示 + vn.py PoC 路径 |

## 失败处理预期

- L0 失败 → 阻断 trading_core PR；先修 lint / typed error 层级
- L1 失败 → 阻断对应模块（OMS / Ledger / MinuteExecutionEngine 等）合 main
- L2 失败 → 阻断 paper v2 backend tests；ledger 漂移类失败必须立即修复（业务连续性）
- L3 失败 → 阻断 paper v2 release；live_session bootstrap 失败时**不允许**降级到"手动模式"

## 与 Codex 模块的边界

| 不属于本模块（Codex / 其他范围） | 落地位置 |
| --- | --- |
| QE Qlib backtest 撮合 | `qe.md`（Codex 维护） |
| Strategy Engine 决策内核 | `strategy_engine.md`（同目录） |
| Selection Center / StrategyPackage 选股 | `paper_v2_selection_center.md`（已有） |
| QE / Paper 一致性 cross-test | `qe_paper_consistency.md`（同目录） |
| 阻断点修复（live inference preflight 等） | `paper_v2_blockers.md`（同目录） |
| UI 简化 / 用户向 UI 测试 | `ui_simplification.md`（同目录） |
| Validation Center 自身 | `validation_center.md`（已有） |

本模块覆盖 trading_core 实施层；不重定义 Engine 决策语义、不改 selection 选股逻辑、不写 UI 测试（除典型 typed error 显示路径）。

## 取材源

- `docs/architecture/trading_core_v2.md`
- `docs/architecture/paper_trading_v2_top_level_design.md` §8 vn.py 参考要求
- `backend/services/trading_core/{oms,ledger,minute_execution,risk,execution_algo_adapter,limit_price_provider,errors,models}.py`
- `backend/tests/paper_trading_v2/test_*.py`（13 个 test 文件，含 day_runner / live_session / market_data / portfolio_broker_backend / risk_targets / runner / runtime_profile / session / v25_day_features 等）
- `backend/services/paper_trading_v2/poc/`（vn.py PoC，task #3 验证）

## Deferred Scope

- MiniQMTSim BrokerBackend 实施（PR-005）：等 task #10 vn.py 盘中复测后启动；本矩阵 v1.1 增量
- minqmt_live 实盘准入：等用户授权 + 主体 §11 准入流程
- 撤单 / 改单完整状态机：当前 OMS 仅覆盖核心生命周期；改单语义后续设计
- 多账户聚合（同一进程多 portfolio MiniQMT）：明确**不在本期**，等用户单独决策
