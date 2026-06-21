# BUG-448 独立分析：Paper v2 run 状态跃迁与成功终态校验

## 结论

BUG-448 在当前 origin/main（a5a4eb28，已包含 BUG-447）中仍然存在。问题集中在 Paper v2 的 PaperRun 状态写入完整性，而不是 simulation_runtime 的 submit_result_gate；本次修复不触碰 simulation_runtime。

## 当前代码核验

- backend/services/paper_trading_v2/repository.py::update_run_status 的 PostgreSQL 实现直接执行 UPDATE paper_v2.run SET status = %s ... WHERE run_id = %s，没有当前状态谓词；rowcount != 1 只能识别 run_id 不存在，不能识别 FAILED -> SUCCEEDED 这种非法覆盖。
- InMemoryPaperTradingV2Repository.update_run_status 也直接覆盖 self.runs[run.run_id]，因此单测路径与 PG 路径同样允许终态覆盖。
- CodeGraph 调用方核对结果显示调用点包括：day_runner.py 的成功/失败路径、live_session.py 的失败/成功路径、session.py 的失败标记、以及测试 fake helper；其中标记 SUCCEEDED 的主要位置是 day_runner.py:614/754/1694、live_session.py:1435/1865、测试 helper test_live_session.py:273。
- day_runner.py:754 在本地分钟回放订单执行后直接标 SUCCEEDED，但没有确认 orders 是否均为 FILLED/CANCELLED/REJECTED；若执行引擎返回 PARTIALLY_FILLED 且 allow_partial_fill=True，当前代码可保存非终态订单并把 run 标成功。
- day_runner.py:1694 的 MiniQMT broker-authoritative 快照路径也直接标 SUCCEEDED，虽然订单来自 broker reconcile，但仍缺少统一的终态断言。
- live_session.py:1435 的 no-rebalance 路径无订单，标成功是合理场景；但应通过同一终态断言显式允许空订单集。
- live_session.py:1865 在收盘 finalize 时仅在 allow_partial_fill=False 时阻止剩余未成交订单；allow_partial_fill=True 时可带着 remaining_states 标 SUCCEEDED，与 BUG-448 的验收要求冲突。

## 根因

1. 仓储层缺少原子状态谓词，导致业务层 stale PaperRun 对象可以覆盖数据库真实终态。
2. 成功路径缺少统一的订单终态断言，导致账户/仓位快照已保存被误当成交易日 run 已成功完成。
3. InMemory 与 PG 仓储缺少一致的非法跃迁行为，使单测无法覆盖生产数据库竞态。

## 修复方案

- 在 repository.py 增加统一的合法状态校验：终态 SUCCEEDED/FAILED 不允许再跃迁；PG 更新使用 SQL 谓词 WHERE run_id = %s AND status NOT IN ('SUCCEEDED','FAILED')，rowcount=0 时重新读取当前状态并抛 InvalidStateTransitionError，context 包含 reason_code=PAPER_V2_RUN_TERMINAL_STATE_TRANSITION_BLOCKED、run_id、from_status、to_status。
- InMemory 仓储复用同一校验逻辑，确保测试路径与 PG 行为一致。
- 在 day_runner.py 与 live_session.py 增加成功前订单终态断言，只允许 FILLED/CANCELLED/REJECTED；发现 PENDING/SUBMITTED/PARTIALLY_FILLED 等非终态时抛 InvalidStateTransitionError，context 包含 reason_code=PAPER_V2_RUN_SUCCEEDED_REQUIRES_TERMINAL_ORDERS、run_id、open_orders。
- 收盘后如仍有未终态订单，保持 run 非成功，并通过既有 PaperTradingSessionRunner 失败处理链路记录失败；不得静默标成功。
- 不修改 simulation_runtime 的 submit_result_gate，不改变其以 ledger 事实判定成败的设计。

## 与 issue 描述的差异

issue 描述与当前代码基本一致。额外发现：live_session.py:1435 no-rebalance 是无订单成功路径，应由终态断言自然通过；不应把无订单误判为失败。
