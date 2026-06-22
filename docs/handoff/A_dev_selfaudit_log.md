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

## 2026-06-23 Phase 1 / Gateway 真事件源

### 设计复读

- 已重读 docs/architecture/miniqmt_durable_execution_runtime_design_20260623.md §3、§4.2、§9 Phase 1、§9.x、§10。
- 已确认 Phase 1 只新增 explicit event_loop gateway 事件源；B 兼容 gateway 与 compiler 默认路径不切换。
- 已确认 sync fallback 不允许在 A path 
eturn []，断连必须 loud + reason_code。

### §10 grep guard 输出

`	ext
range(_timer_iterations) in changed event_loop gateway/runtime files: 0 matches

event_loop_gateway_return_empty_count=0
event_loop_gateway_sync_methods=sync_orders,sync_trades,sync_positions

JsonFile event_loop authority hits:
docs/architecture/miniqmt_event_loop_runtime_phase0_seam_contract_20260623.md:88:- JsonFileMiniQMTExecutionRuntimeRepository 只能保留给 compiler 兼容测试或只读调试快照；不得作为 event_loop 权威 OMS。

vnpy_style changed files: none
new order-status literal forks in Phase 1 diff: none
flag inert evidence: Phase 0 test still passes get_miniqmt_execution_runtime_kind({}) == compiler
`

### 三问

1. 我这段是否真事件驱动，还是悄悄做成了查一次/合成 timer？
   - 真事件源：新增 QmtClientMiniQMTEventLoopGateway.on_order/on_trade/on_tick/on_account/on_disconnect 直接写 runtime event；未新增 timer loop 或提交后查一次生命周期。
2. 我是否新造了第二套非 durable OMS？
   - 否；事件写入仍经 MiniQMTExecutionRuntime append-only events + child order projection；Phase 2 才推进 qmt_strategy OMS 落库。
3. 我是否动了 B 或分叉了算法核？
   - 否；B 兼容 QmtClientMiniQMTGateway 未切换，默认 flag 仍 compiler，ackend/execution_algos/vnpy_style/ 无改动。

### 子交付物与测试

`	ext
rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_phase1_gateway_event_source.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase0_seam_contracts.py backend/tests/miniqmt_execution_runtime/test_miniqmt_execution_runtime_event_loop.py -q
.........                                                                [100%]
9 passed in 0.98s

rtk python -m ruff check backend/services/miniqmt_execution_runtime backend/tests/miniqmt_execution_runtime/test_miniqmt_phase1_gateway_event_source.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase0_seam_contracts.py
All checks passed!

rtk git diff --check
<no output, exit 0>
`

### 偏差拦截

- 无偏差拦截。

## 2026-06-23 Phase 2 / durable EventLoop + qmt_strategy OMS checkpoint

### 设计复读

- 已重读 docs/architecture/miniqmt_durable_execution_runtime_design_20260623.md §3、§4.1、§4.3、§6、§9 Phase 2、§9.x、§10。
- 已重读 docs/architecture/miniqmt_unified_vnpy_execution_runtime_design_20260608.md §4.1、§5.2、§6、§10.8。
- 本 checkpoint 只推进 Phase 2：event_loop client 注入 qmt_strategy_ledger OMS authority；order/trade 回调落 qmt_strategy facts；compiler 默认路径继续 inert。

### §10 grep guard 输出

`	ext
range_timer_iterations_event_loop_path_count=0
event_loop_gateway_return_empty_list_count=0
jsonfile_event_loop_authority_count=0
vnpy_style changed files: none (rtk git diff --name-only -- backend/execution_algos/vnpy_style 输出为空)
is_open_like_order_status=5
is_terminal_order_status=5
is_partial_order_status=4
status_predicate_bad_literal_probe_count=0
flag inert evidence: test_miniqmt_phase0_seam_contracts.py locks get_miniqmt_execution_runtime_kind({}) == compiler; Phase 2 test locks compiler runtime does not write qmt_strategy OMS.
no silent probe: no except: pass / except Exception: pass in changed A runtime/oms/gateway/client/router files.
`

### 三问

1. 我这段是否真事件驱动，还是悄悄做成了查一次/合成 timer？
   - 事件来源仍是 Phase 1 的真实 on_order/on_trade/on_tick/on_account/on_disconnect；Phase 2 还显式拒绝 event_loop 走 compiler-style managed vn.py request building，避免 
ange(_timer_iterations) 合成生命周期。
2. 我是否新造了第二套非 durable OMS？
   - 否。event_loop client 绑定 QmtStrategyLedgerRepository/注入的 InMemoryQmtStrategyLedgerRepository；child order/trade facts 写 qmt_strategy_ledger，JSON runtime repo 不作为 OMS authority。
3. 我是否动了 B 或分叉了算法核？
   - 否。默认 runtime_kind 仍来自 MINIQMT_EXECUTION_RUNTIME 且默认 compiler；compiler test 锁定不写 qmt_strategy OMS；ackend/execution_algos/vnpy_style/ 无改动。

### 子交付物与测试

`	ext
rtk python -m pytest backend/tests/miniqmt_execution_runtime -q
..........................................                               [100%]
42 passed in 1.31s

rtk python -m ruff check backend/services/miniqmt_execution_runtime backend/routers/simulation_runtime.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase2_qmt_strategy_oms.py
All checks passed!

rtk git diff --check
<no output, exit 0>
`

### 偏差拦截

- 拦截一次潜在偏差：最初考虑让 event_loop 继续兼容 managed vn.py request build；自审后判定这会保留 compiler-style 合成 timer/一次性 build 生命周期，已改为 event_loop loud reject MINIQMT_EVENT_LOOP_REQUIRES_REAL_CALLBACKS，compiler 路径不变。
