# BUG-462 LocalSim cash_ledger 幂等与事务原子性分析

## 结论

- GitHub issue #1427 的主因成立：`PaperTradingV2Repository.save_cash_entry()` 对 `paper_v2.cash_ledger` 裸 `INSERT`，且 DDL 未约束 `run_id + fill_id`，LocalSim fill 重放会重复写 cash ledger 并污染 `load_latest_cash()`。
- 事务主因成立：`backend/db/pg_pool.py` 旧实现把 direct/pool connection 统一设为 `autocommit=True`，Paper v2 repository 的多写逻辑缺少显式事务边界。
- `save_positions()` 的 `DELETE FROM paper_v2.positions WHERE run_id = %s` 与逐行 `INSERT` 在旧 autocommit 下存在中途失败后 positions 被清空/半写的窗口。
- Live session tick 内的 fill/cash/positions/intraday snapshot/session day cursor 依赖同一 `session_tick_lock()`；旧实现 advisory lock 和后续写入不是同一事务上下文，bar cursor 推进与业务写入不原子。

## 当前代码证据

- `backend/services/paper_trading_v2/repository.py`：`save_cash_entry()` 旧 SQL 无 `ON CONFLICT`；`save_positions()` 旧逻辑为 DELETE 后逐行 INSERT。
- `backend/migrations/trading_core_v2_schema.sql` 与 `backend/db/init_trading_core_v2_schema.py`：`paper_v2.cash_ledger` 旧表定义仅有 `cash_id` 主键，无 `run_id, fill_id` 唯一约束。
- `backend/db/pg_pool.py`：旧 `get_conn()` 在 direct 与 pool 路径均将 `conn.autocommit = True`，调用方即使多个 SQL 连续执行也无法自动回滚。

## 修复方案

- DDL：为 `paper_v2.cash_ledger` 增加 `cash_ledger_run_fill_unique UNIQUE(run_id, fill_id)`；forward migration 在发现既有重复 `run_id/fill_id` 时 loud fail，错误包含 `reason_code=PAPER_V2_CASH_LEDGER_DUPLICATE_FILL_ID`；SQL 注释中给出 rollback：`ALTER TABLE paper_v2.cash_ledger DROP CONSTRAINT IF EXISTS cash_ledger_run_fill_unique;`。
- 幂等写入：`save_cash_entry()` 改为 `ON CONFLICT(run_id, fill_id) DO NOTHING`；InMemory repository 同步按 `fill_id` 去重，避免单测与 PG 行为分叉。
- 事务连接：`pg_pool.get_conn()` 支持 `autocommit` 与 `manage_transaction` 参数，默认关闭 autocommit 并在 context 成功/异常时 commit/rollback；commit/rollback 失败均 loud 上抛并记录 reason_code。
- Repository 写边界：`PaperTradingV2Repository` 增加 `transaction()`、`transactional_write` 与线程本地事务连接复用，常规写方法自动进入显式事务；嵌套写在同一个事务连接中执行。
- Session tick 原子性：`session_tick_lock()` 复用同一 DB connection，先取 session advisory lock，再切入显式事务；tick 内 LocalSim 写入与 `save_session_day()` cursor 推进同一事务提交；异常时回滚业务事务，再允许失败记录落入新的事务并最终释放 advisory lock。

## 验证与边界

- 新增回归覆盖：cash ledger idempotent、PG `ON CONFLICT(run_id, fill_id)` SQL、`save_positions()` DELETE+INSERT 回滚原子性、session tick 内 cursor 与 cash 写入共用同一连接/事务边界。
- 未修改 MiniQMT 专属文件与分支；未触碰 `backend/services/miniqmt_execution_runtime/**`、`backend/services/qmt_strategy_ledger/**`、`broker/minqmtsim.py`、`execution/**` 或 `day_runner.py/live_session.py/scheduler.py` 的 MiniQMT 分支。
- 生产 DDL 未执行；本 PR 仅提交 migration/init DDL，合入后需用户执行生产 schema gate。

## 生产门禁

- `production_ddl_gate=pending`
- `production_backend_dependency_gate=noop`
- `production_frontend_dependency_gate=noop`
- 需要用户在合入后重启相关后端进程以加载代码变更；本 lane 未启动、停止或重启任何服务。

## 验证记录（2026-06-22）

- `python -m pytest backend/tests/paper_trading_v2/test_localsim_backend.py backend/tests/paper_trading_v2/test_day_runner.py -q`：58 passed。
- `python -m ruff check backend/db/init_trading_core_v2_schema.py backend/db/pg_pool.py backend/services/paper_trading_v2/repository.py backend/tests/paper_trading_v2/test_day_runner.py`：passed。
- `git diff --check`：passed。
- `python -m nox -s l0`：passed，guardrail blocking=0。
- `python -m nox -s validation_module_registry_l0`：passed。
- `cmd /c "set AISTOCK_HOSTED_CI=1&& set PAPER_V2_L3_SKIP_UI=1&& python -m nox -s paper_v2_l3"`：passed；UI 按要求跳过，data-quality 中 legacy ledger consistency WARN 为非阻断历史数据告警。
- 本 lane 未启动、停止、重启任何服务，未触碰生产 DB，未执行生产 DDL；合并后需要用户/Tier2 执行并验证生产 DDL，并重启相关后端进程加载代码。
