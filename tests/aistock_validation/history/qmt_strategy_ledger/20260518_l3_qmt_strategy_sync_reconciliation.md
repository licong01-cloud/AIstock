# L3 MiniQMT 多策略只读同步与对账验证记录（2026-05-18）

## 范围

- 分支：`codex/miniqmt-multi-strategy-plan-20260518`
- 阶段：Phase 3，只读同步 API 与对账
- 模块：
  - `backend/services/qmt_strategy_ledger/sync_service.py`
  - `backend/services/qmt_strategy_ledger/reconciliation.py`
  - `backend/routers/qmt_strategy_ledger.py`
  - `backend/main.py`
  - `backend/tests/qmt_strategy_ledger`
- 生产影响：未触碰生产后端 `8001`；未连接真实 MiniQMT；未下单；未执行 migration；未写生产数据库。

## 验证目标

Phase 3 的目标是在 Phase 2 schema/repository 基础上，提供只读同步与对账闭环：

- 通过注入的 read-only client 拉取 `orders` / `trades` / `positions`。
- 将可归因订单同步为 `order_ledger` 和 `order_status_event`。
- 将可归因成交幂等同步为 `trade_ledger`，并为买入成交生成策略级 `position_lot`。
- 空 `strategy_name`、重复 `order_remark`、未知策略、未知 order intent、trade without order 均进入 unattributed 队列。
- 对账比较 MiniQMT 合并持仓与 AIstock 策略 lot 合计，识别 position mismatch 和 unattributed trade/order。
- Router 注册只读 `sync-snapshot` / `reconciliation` API，不自动启动后台同步任务。

## 验证命令

```powershell
python -m py_compile backend/services/qmt_strategy_ledger/models.py backend/services/qmt_strategy_ledger/reconstruct.py backend/services/qmt_strategy_ledger/repository.py backend/services/qmt_strategy_ledger/sync_service.py backend/services/qmt_strategy_ledger/reconciliation.py backend/routers/qmt_strategy_ledger.py scripts/qmt_strategy_ledger_reconstruct_poc.py
pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider
pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q -p no:cacheprovider
rg -n 'place_order|cancel_order|trade_password|QMT_TRADE_PASSWORD' backend/services/qmt_strategy_ledger backend/routers/qmt_strategy_ledger.py backend/tests/qmt_strategy_ledger
git diff --check
```

## 结果

- `py_compile`：通过
- `pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider`：16 passed
- `pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q -p no:cacheprovider`：24 passed
- 反模式扫描：未发现 `place_order`、`cancel_order`、`trade_password`、`QMT_TRADE_PASSWORD`
- 允许项：`backend/routers/qmt_strategy_ledger.py` 引用 `get_qmt_client_singleton` 仅用于 read-only `get_orders` / `get_trades` / `get_positions` 查询，不提交/撤销订单。
- `git diff --check`：通过

## 关键断言

- fake client 可归因订单和成交同步后，生成 1 个 order ledger、1 个 trade ledger、1 个 position lot。
- 同一成交重复同步不会重复生成 trade ledger 或 position lot。
- 空策略名、重复 `order_remark`、缺失订单的成交进入 unattributed 队列，不进入策略收益。
- 两个策略共同持有 `001358.SZ` 时，只要 MiniQMT 合并持仓等于策略 lot 合计，对账通过且识别 overlap symbol。
- MiniQMT 合并持仓与策略 lot 合计不一致时生成 `POSITION_MISMATCH`。
- unattributed trade 生成 `UNATTRIBUTED_TRADE` reconciliation issue。

## 残余风险

- 本阶段 router 已注册，但未启动后端、未调用生产 `8001`，因此未做 live API smoke。
- 本阶段不实现托管下单、T+1 下单预检、撤单冻结释放或 StrategyPackage 到订单意图转换；这些属于 Phase 4/5。
- 真实 MiniQMT 同步仍需在显式人工确认后，通过开发端口和只读账号快照验证。
