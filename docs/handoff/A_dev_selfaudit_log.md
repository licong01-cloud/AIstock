# MiniQMT runtime A 自审日志

## 2026-06-23 Phase 0 / 接缝契约冻结

### 设计复读

- 已重读 `docs/architecture/miniqmt_durable_execution_runtime_design_20260623.md` §3、§5、§9、§9.x、§10。
- 已确认 Phase 0 只冻结 A/B 共用接缝并建 epic，不引入 event_loop 产品切换。
- 已确认 `MINIQMT_EXECUTION_RUNTIME` 必须默认 `compiler`，A 默认 inert。

### §10 grep guard 输出

```text
event_loop_range_timer_count=0
all_range_timer_hits=2
backend\services\miniqmt_execution_runtime\client.py:345:for index in range(_timer_iterations(algo_code, dict(policy_json.get("algo_config") or {}))):
backend\services\miniqmt_execution_runtime\client.py:487:for index in range(_timer_iterations(algo_code, dict(policy_json.get("algo_config") or {}))):

a_gateway_sync_return_empty_count=0
all_sync_return_empty_hits=12
backend\services\miniqmt_execution_runtime\client.py:741:return []
backend\services\miniqmt_execution_runtime\client.py:744:return []
backend\services\miniqmt_execution_runtime\client.py:747:return []
backend\services\miniqmt_execution_runtime\client.py:839:return []
backend\services\miniqmt_execution_runtime\client.py:842:return []
backend\services\miniqmt_execution_runtime\client.py:845:return []
backend\services\miniqmt_execution_runtime\client.py:877:return []
backend\services\miniqmt_execution_runtime\client.py:880:return []
backend\services\miniqmt_execution_runtime\client.py:883:return []
backend\services\miniqmt_execution_runtime\gateway.py:153:return []
backend\services\miniqmt_execution_runtime\gateway.py:159:return []
backend\services\miniqmt_execution_runtime\gateway.py:165:return []

vnpy_style changed files: none
new order-status literal forks in Phase 0 changed files: none
MINIQMT_EXECUTION_RUNTIME default compiler test: passed
```

### 三问

1. 我这段是否真事件驱动，还是悄悄做成了查一次/合成 timer？
   - Phase 0 未实现事件循环，只冻结接缝；未新增查一次或合成 timer 生命周期。
2. 我是否新造了第二套非 durable OMS？
   - 否；新增 `MiniQMTStrategyLedgerOmsContract` 明确 event_loop OMS 只能用 `qmt_strategy_ledger` 权威接口。
3. 我是否动了 B 或分叉了算法核？
   - 否；未改 `backend/execution_algos/vnpy_style/`，新增 flag 默认 `compiler`，B 路径默认行为测试锁定。

### 子交付物与测试

```text
rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_phase0_seam_contracts.py backend/tests/miniqmt_execution_runtime/test_miniqmt_vnpy_algo_import_boundary.py -q
.....                                                                    [100%]
5 passed in 1.02s
```

### 偏差拦截

- 无偏差拦截。
