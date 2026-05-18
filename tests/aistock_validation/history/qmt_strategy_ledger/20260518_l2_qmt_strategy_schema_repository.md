# L2 MiniQMT 多策略 schema/repository 验证记录（2026-05-18）

## 范围

- 分支：`codex/miniqmt-multi-strategy-plan-20260518`
- 阶段：Phase 2，`qmt_strategy` schema 与 repository 基础持久化边界
- 模块：
  - `backend/migrations/qmt_strategy_ledger_20260518.sql`
  - `backend/services/qmt_strategy_ledger/models.py`
  - `backend/services/qmt_strategy_ledger/repository.py`
  - `backend/tests/qmt_strategy_ledger`
- 生产影响：未触碰生产后端 `8001`；未连接 MiniQMT；未下单；未执行 migration；未写生产数据库。

## 验证目标

Phase 2 的目标是把 2026-05-18 POC 暴露的多策略分仓需求固化为可审查的数据边界：

- 新增独立 `qmt_strategy` schema，不混入 `paper_v2`。
- 为每个策略建立 `virtual_account`，为 StrategyPackage 建立 active binding 唯一约束。
- 订单意图、broker 订单镜像、成交、lot、现金流水、每日快照、对账和异常归因具备表结构。
- 每张表和每个字段都有 PostgreSQL `COMMENT ON TABLE` / `COMMENT ON COLUMN`。
- repository 不连接 MiniQMT、不提交/撤销订单；数据库写入只限显式 repository 方法，测试使用 in-memory repository。

## 验证命令

```powershell
python -m py_compile backend/services/qmt_strategy_ledger/models.py backend/services/qmt_strategy_ledger/reconstruct.py backend/services/qmt_strategy_ledger/repository.py scripts/qmt_strategy_ledger_reconstruct_poc.py
pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider
pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q -p no:cacheprovider
rg -n 'place_order|cancel_order|get_qmt_client|trade_password|QMT_TRADE_PASSWORD' backend/services/qmt_strategy_ledger backend/tests/qmt_strategy_ledger
git diff --check
```

## 结果

- `py_compile`：通过
- `pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider`：12 passed
- `pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q -p no:cacheprovider`：24 passed
- 反模式扫描：未发现 `place_order`、`cancel_order`、`get_qmt_client`、`trade_password`、`QMT_TRADE_PASSWORD`
- `git diff --check`：通过

## 关键断言

- migration 覆盖 14 张表：`virtual_account`、`strategy_package_binding`、`order_batch`、`order_intent`、`order_ledger`、`order_status_event`、`trade_ledger`、`position_lot`、`cash_ledger`、`daily_snapshot`、`reconciliation_run`、`reconciliation_issue`、`unattributed_order`、`unattributed_trade`。
- `backend/tests/qmt_strategy_ledger/test_migration_comments.py` 校验每张新表、每个新字段都有 comment。
- 关键唯一约束已入库文件：active package binding、`(account_id, order_remark)`、`(account_id, qmt_order_id)`、`(account_id, trade_date, trade_id)`、`(strategy_id, trade_date)`。
- `InMemoryQmtStrategyLedgerRepository` 覆盖虚拟账户创建/查询、active binding 唯一语义、重复 `order_remark` 拒绝、成交幂等、现金流水 append-only、策略/股票 lot 过滤。

## 残余风险

- 本阶段未执行 SQL migration，需后续在非生产数据库显式执行并做真实 DB repository 集成测试。
- 本阶段未实现 MiniQMT sync service、托管下单 service、T+1 预检、PnL 服务或 UI。
- `QmtStrategyLedgerRepository` 是持久化边界实现，尚未接入 router/service；因此不会改变当前运行时交易行为。
