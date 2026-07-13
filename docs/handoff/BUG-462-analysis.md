# BUG-462 LocalSim cash_ledger 幂等与事务原子性返工分析

## Tier2 返工结论

- Tier2 驳回点成立：PR #1439 第一版把 `backend/db/pg_pool.py::get_conn()` 默认改成 `autocommit=False/manage_transaction=True`，会波及 MiniQMT、QE、Research Assistant、migrations 等所有裸 `get_conn()` 调用方，超出 LocalSim 边界。
- 本次返工已恢复全局默认：`get_conn(*, autocommit=True, manage_transaction=False)`，默认 checkout 继续走历史行为：设置 `conn.autocommit=True` 并应用 statement timeout，不进行事务状态检查、commit 或 rollback。
- `pg_pool` 仅保留显式 opt-in 能力：只有调用方传入 `autocommit=False, manage_transaction=True` 时，才进入 `_prepare_connection()`、context 成功 commit、异常 rollback。
- LocalSim 原子性收敛到 Paper v2 repository 内部：普通仓储方法继续通过默认 `_conn_factory()` 走 autocommit；仅 LocalSim `save_positions()` 的 DELETE+INSERT 与 LocalSim live tick 事务显式使用 `_conn(autocommit=False, manage_transaction=True)`。
- MiniQMT/QE/qmt_strategy_ledger 等跨模块调用方未改文件、未新增事务参数，继续使用 `get_conn()` 默认 autocommit 行为。

## 原始根因

- `PaperTradingV2Repository.save_cash_entry()` 对 `paper_v2.cash_ledger` 裸 `INSERT`，DDL 未约束 `run_id + fill_id`，LocalSim fill 重放会重复写 cash ledger 并污染 `load_latest_cash()`。
- `save_positions()` 的 `DELETE FROM paper_v2.positions WHERE run_id = %s` 与后续逐行 `INSERT` 在 autocommit 下存在中途失败后 positions 被清空或半写的窗口。
- LocalSim live session tick 内的 fill/cash/positions/intraday snapshot/session day cursor 需要同一逻辑单元提交，否则 bar cursor 可能先推进而业务账本未完整落库。

## 返工后的实现边界

- DDL 保留：`paper_v2.cash_ledger` 增加 `cash_ledger_run_fill_unique UNIQUE(run_id, fill_id)`；forward migration 发现既有重复 `run_id/fill_id` 时 loud fail，rollback SQL 为 `ALTER TABLE paper_v2.cash_ledger DROP CONSTRAINT IF EXISTS cash_ledger_run_fill_unique;`。
- 幂等保留：`save_cash_entry()` 使用 `ON CONFLICT(run_id, fill_id) DO NOTHING`；InMemory repository 按 `fill_id` 去重。
- `pg_pool` 默认行为恢复：`get_conn()` 默认参数是 `autocommit=True/manage_transaction=False`，默认路径不 commit/rollback，不检查 transaction status，保持既有调用方零语义变化。
- 显式事务 opt-in：LocalSim `save_positions()` 判断 `broker_backend=local_sim` 后使用 `_write_conn()`；非 LocalSim 路径仍走默认 `_conn_factory()`。
- LocalSim live tick：`session_tick_lock()` 先用默认 autocommit advisory lock 判定是否 LocalSim；仅 LocalSim live/replay-live session 进入 `local_sim_session_transaction()`，业务写入复用同一事务连接，异常时 loud log 并 rollback。
- 失败持久化：LocalSim tick 内异常进入 session failure persistence 时，先回滚业务事务，再用默认 autocommit 连接写失败状态，避免失败记录被同一业务事务回滚；相关失败均带 reason_code 并抛出。

## MiniQMT / 跨模块复核

- 未修改 `backend/services/miniqmt_execution_runtime/**`、`backend/services/qmt_strategy_ledger/**`、`backend/services/paper_trading_v2/broker/minqmtsim.py`、`backend/services/paper_trading_v2/execution/**`。
- `rg` 复核 MiniQMT/QE/qmt_strategy_ledger/strategy_package/Research Assistant 相关 `get_conn()` 调用仍未传事务参数，因此继续命中 `autocommit=True/manage_transaction=False` 默认。
- `session_tick_lock()` 非 LocalSim 分支继续只持 advisory lock，不把 MiniQMT 分支纳入 LocalSim 事务语义。

## 回归测试

新增/调整覆盖：

- `test_pg_pool_get_conn_defaults_keep_legacy_autocommit_true`：断言签名默认值为 `autocommit=True/manage_transaction=False`，并用 fake pool 断言默认 checkout 不进入 explicit transaction preparation、无 commit/rollback。
- `test_pg_cash_ledger_insert_uses_unique_conflict_guard`：断言 PG cash ledger SQL 使用 `ON CONFLICT(run_id, fill_id) DO NOTHING` 且默认 autocommit。
- `test_pg_non_localsim_positions_keep_autocommit_default`：断言非 LocalSim `save_positions()` 不进入事务 opt-in。
- `test_pg_localsim_positions_delete_insert_rollback_is_single_transaction`：断言 LocalSim positions DELETE+INSERT 使用显式事务，INSERT 失败回滚。
- `test_session_tick_lock_reuses_one_connection_for_cursor_and_writes`：断言 LocalSim tick 内 cash 与 cursor 写入在同一事务连接内提交。
- `test_session_tick_lock_rolls_back_cash_and_cursor_on_failure`：断言 LocalSim tick 异常时业务事务回滚并释放 advisory lock。

## 验证记录（2026-06-22 返工）

- `python -m pytest backend/tests/paper_trading_v2/test_localsim_backend.py backend/tests/paper_trading_v2/test_day_runner.py -q`：61 passed。
- `python -m ruff check backend/db/init_trading_core_v2_schema.py backend/db/pg_pool.py backend/services/paper_trading_v2/repository.py backend/tests/paper_trading_v2/test_day_runner.py`：passed。
- `git diff --check`：passed。
- `python -m nox -s l0`：passed，blocking=0。
- `python -m nox -s validation_module_registry_l0`：passed。
- `cmd /c "set AISTOCK_HOSTED_CI=1&& set PAPER_V2_L3_SKIP_UI=1&& python -m nox -s paper_v2_l3"`：passed；`paper_v2_backend` 640 passed, 1 skipped, 1 deselected；data-quality 中 legacy ledger consistency WARN 为历史数据告警，非阻断。

## 生产门禁

- `production_ddl_gate=pending`：本 PR 含 cash ledger 唯一约束 DDL，生产 DDL 未由本 agent 执行，合并后需运营/Tier2 执行并验证。
- `production_backend_dependency_gate=noop`。
- `production_frontend_dependency_gate=noop`。
- 本 lane 未启动、停止或重启任何服务，未写生产 DB。
