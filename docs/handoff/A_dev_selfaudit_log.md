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
   - 否；B 兼容 QmtClientMiniQMTGateway 未切换，默认 flag 仍 compiler，ackend/execution_algos/vnpy_style/ 无改动。

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
   - 否。默认 runtime_kind 仍来自 MINIQMT_EXECUTION_RUNTIME 且默认 compiler；compiler test 锁定不写 qmt_strategy OMS；ackend/execution_algos/vnpy_style/ 无改动。

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

## 2026-06-23 Phase 2 / event_loop 拒绝 compiler-style 生命周期补强

### 设计复读

- 已重读 `docs/architecture/miniqmt_durable_execution_runtime_design_20260623.md` §3、§9 Phase 2、§9.x、§10。
- 已重读 `docs/adr/0002-miniqmt-execution-runtime-event-loop-target-architecture.md` 关于禁止“合成 timer / 提交后查一次 / JSON 文件 OMS”近似 A 的要求。
- 本次补强只扩大 event_loop 显式拒绝范围：preview/submit managed requests、managed vn.py build、Paper v2 sync lifecycle 都在 event_loop 下 loud reject；默认 compiler 路径不变。

### §10 grep guard 输出

```text
range_timer_iterations_event_loop_files_count=0
compiler_lifecycle_reject_guard_count=5
all_range_timer_iterations_count=2
event_loop_gateway_sync_return_empty_count=0
jsonfile_event_loop_authority_count=0
vnpy_style_changed_files=none
is_open_like_order_status_usage_count=5
is_terminal_order_status_usage_count=7
is_partial_order_status_usage_count=6
status_predicate_bad_literal_probe_count=0
no_silent_except_pass_count=0
miniqmt_runtime_default_compiler=True
```

### 三问

1. 我这段是否真事件驱动，还是悄悄做成了查一次/合成 timer？
   - 本次没有新增任何事件生命周期；反而补强 event_loop 对 compiler-style sync lifecycle 的 loud reject，避免 A 误走一次性 build/timer 路径。
2. 我是否新造了第二套非 durable OMS？
   - 否；无新增 OMS，只保持 event_loop 绑定 qmt_strategy_ledger facts，JSON runtime repo 不作为 event_loop 权威 OMS。
3. 我是否动了 B 或分叉了算法核？
   - 否；默认 `MINIQMT_EXECUTION_RUNTIME` 仍为 `compiler`，`backend/execution_algos/vnpy_style/` 无改动；新增测试只断言 event_loop 拒绝 B-style 生命周期。

### 子交付物与测试

```text
rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_phase2_qmt_strategy_oms.py -q
.....                                                                    [100%]
5 passed in 1.09s

rtk python -m pytest backend/tests/miniqmt_execution_runtime -q
...........................................                              [100%]
43 passed in 1.49s

rtk python -m ruff check backend/services/miniqmt_execution_runtime backend/routers/simulation_runtime.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase0_seam_contracts.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase1_gateway_event_source.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase2_qmt_strategy_oms.py
All checks passed!

rtk python -m nox -s l0
Session l0 was successful.

rtk python -m nox -s validation_module_registry_l0
Session validation_module_registry_l0 was successful.

rtk cmd /c "set AISTOCK_HOSTED_CI=1&& set PAPER_V2_L3_SKIP_UI=1&& python -m nox -s paper_v2_l3"
Ran 5 sessions: paper_v2_l3 success; l0 success; paper_v2_backend 661 passed, 1 skipped, 1 deselected; paper_v2_data_quality success with legacy ledger consistency WARN; data_quality_deep 10 passed, 21 skipped.
```

### 偏差拦截

- 拦截一次潜在偏差：接手后发现 event_loop 仅拒绝 `build_managed_vnpy_order_requests`，但 preview/submit managed requests 和 Paper v2 sync lifecycle 仍可能被显式 event_loop client 调用，存在“先走一次性 compiler 生命周期”的偏航风险；已改为全部 loud reject `MINIQMT_EVENT_LOOP_REQUIRES_REAL_CALLBACKS` 并加测试锁定。


## Phase 3 start self-audit - 2026-06-23T03:26:20.836920+00:00

### Scope
- Worktree: `F:/Dev/AIstock_worktrees/miniqmt-event-loop-runtime-phase3-20260623`
- Base: `origin/main` at `7a38aaa2` (contains Phase 0-2 `5197edb0` and rule 9 `439a916e`)
- Read: ADR 0002; durable design §3/§4.4/§9/§10 including rule 9; Phase0 seam contract; 0608 §4.1/Phase3 table.

### §10 grep guard output (baseline before Phase 3 edits)

```text
event_loop range(_timer_iterations) in A modules:
<no matches>

all range(_timer_iterations) references under backend/services/miniqmt_execution_runtime:
<no matches>

event_loop gateway sync return [] (broad gateway.py scan, needs class-qualified interpretation):
33:    def sync_orders(self, *, runtime_id: str) -> list[dict[str, Any]]:
36:    def sync_trades(self, *, runtime_id: str) -> list[dict[str, Any]]:
39:    def sync_positions(self, *, runtime_id: str) -> list[dict[str, Any]]:
136:    def sync_orders(self, *, runtime_id: str) -> list[dict[str, Any]]:
139:    def sync_trades(self, *, runtime_id: str) -> list[dict[str, Any]]:
142:    def sync_positions(self, *, runtime_id: str) -> list[dict[str, Any]]:
214:    def sync_orders(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
217:            return []
220:    def sync_trades(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
223:            return []
226:    def sync_positions(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
229:            return []
369:    def sync_orders(self, *, runtime_id: str) -> list[dict[str, Any]]:
378:    def sync_trades(self, *, runtime_id: str) -> list[dict[str, Any]]:
386:    def sync_positions(self, *, runtime_id: str) -> list[dict[str, Any]]:

Interpretation: line 214/220/226 are legacy QmtClientMiniQMTGateway compatibility, not QmtClientMiniQMTEventLoopGateway; Phase 3 will add an explicit event_loop loud test.

JsonFile OMS event_loop authority references:
docs/architecture/miniqmt_event_loop_runtime_phase0_seam_contract_20260623.md:88:- JsonFileMiniQMTExecutionRuntimeRepository only for compiler/test/debug, not event_loop authority.
backend/services/miniqmt_execution_runtime/__init__.py:54,76 export compatibility repository.
backend/services/miniqmt_execution_runtime/repository.py:152 class JsonFileMiniQMTExecutionRuntimeRepository; repository.py:247 default store factory.

vnpy_style attribution/source map diff:
<empty git diff>

BUG-470 predicates in A modules:
runtime.py imports/uses is_open_like_order_status and is_terminal_order_status; oms.py imports/uses the same predicates. STATUS_* constants are used only as qmt_strategy_ledger canonical statuses, not new literal forks.

flag default compiler evidence references:
config.py defaults MINIQMT_EXECUTION_RUNTIME to compiler; test_miniqmt_phase0_seam_contracts.py asserts default compiler and unsupported value reason_code MINIQMT_EXECUTION_RUNTIME_UNSUPPORTED.

MiniQMT/event_loop TDX guard:
Only ADR/design docs contain fetch_tdx_realtime_quotes / TDX_REALTIME; backend/services/miniqmt_execution_runtime and backend/tests/miniqmt_execution_runtime have no matches.
```

### Self questions
- 真事件驱动还是查一次/合成 timer? 当前工作尚未改代码；基线 runtime 已无 `range(_timer_iterations)`，后续只接受真实 `MarketTick/TradeFill/AlgoTimer` 事件。
- 是否新造第二套非 durable OMS? 否；仅使用 `MiniQMTOmsLedger` / `qmt_strategy_ledger` seam，不引入 JSON/内存权威 OMS。
- 是否动 B 或分叉算法核? 否；`backend/execution_algos/vnpy_style/` diff 为空，flag 默认 compiler 不变。
- 是否引入 TDX 行情? 否；A runtime/backend tests 无 `fetch_tdx_realtime_quotes` / `TDX_REALTIME`。

### Deviation intercept
- 未拦截偏差。注意：broad grep 命中 legacy `QmtClientMiniQMTGateway` sync return []; 这不是 event_loop class，但本轮需用测试锁定 `QmtClientMiniQMTEventLoopGateway` 缺 qmt_client 方法 loud。


## Phase 3 algo lifecycle fix self-audit - 2026-06-23T03:35:21.631962+00:00

### Deliverable
- Added Phase 3 characterization for BestLimit cancel/requote lifecycle and TWAP window lifecycle.
- Fixed runtime adapter so vn.py-style instances are not terminalized merely because their current child order is terminal; they remain active until the core emits FINISH or operator command explicitly terminalizes/cancels.
- Hardened event_loop gateway missing `get_trades` / `get_positions` tests to loud failures.

### Design reread
- Re-read durable design §3 rules 1/2/3/5/7/9, §4.4, §9 Phase 3, §9.x, §10.
- Re-read Phase0 seam contract gateway sync and OMS rules.
- Re-read 0608 §4.1/§4.2 algo semantics and Phase3 acceptance table.

### §10 grep guard output

```text
event_loop runtime/gateway range(_timer_iterations count:
<no matches>

all range(_timer_iterations references:
backend/services/miniqmt_execution_runtime/client.py:369:            for index in range(_timer_iterations(algo_code, dict(policy_json.get("algo_config") or {}))):
backend/services/miniqmt_execution_runtime/client.py:512:        for index in range(_timer_iterations(algo_code, dict(policy_json.get("algo_config") or {}))):
Interpretation: compiler/compat client paths only; event_loop runtime/gateway/oms/models count is 0.

event_loop class sync methods and return [] context:
QmtClientMiniQMTEventLoopGateway.sync_orders/sync_trades/sync_positions call _required_qmt_list with reason_code MINIQMT_EVENT_LOOP_SYNC_*_UNAVAILABLE; no return [] in this class.

broad gateway return []:
legacy QmtClientMiniQMTGateway lines 214/220/226 still contain return [] compatibility for non-event_loop path; event_loop subclass overrides with loud _required_qmt_list. Test locks orders/trades/positions loud failures.

JsonFile OMS event_loop authority references:
docs seam contract only says JsonFile not event_loop authority; repository export/factory remains for compatibility/debug. No event_loop authority use added.

vnpy_style attribution/source map diff:
<empty git diff -- backend/execution_algos/vnpy_style>

BUG-470 predicates / literal status scan in A runtime:
runtime.py and oms.py use is_open_like_order_status/is_terminal_order_status for broker numeric statuses. Existing raw text fallback remains only for text status snapshots; no new status literal fork added.

flag default compiler tests:
rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_phase0_seam_contracts.py::test_miniqmt_execution_runtime_flag_defaults_to_compiler_and_rejects_unknown_values backend/tests/miniqmt_execution_runtime/test_miniqmt_phase2_qmt_strategy_oms.py::test_event_loop_client_uses_qmt_strategy_oms_authority_and_compiler_default_is_inert -q
.. [100%]
2 passed in 0.91s

MiniQMT/event_loop TDX guard:
rg -n 'fetch_tdx_realtime_quotes|TDX_REALTIME' backend/services/miniqmt_execution_runtime backend/tests/miniqmt_execution_runtime
<no matches>

Targeted test:
rtk python -m pytest backend/tests/miniqmt_execution_runtime/ -q
45 passed in 2.02s
```

### Self questions
- 真事件驱动还是查一次/合成 timer? 真事件驱动；新增测试通过真实 `on_tick`/`on_timer`/`record_trade_event`/`record_order_event` 推动 core，没有同步 for-loop 或查一次生命周期。
- 是否新造第二套非 durable OMS? 否；仍通过 runtime OMS facade 和 repository seam，未新增 JSON/内存权威 OMS。
- 是否动 B 或分叉算法核? 否；未改 `backend/execution_algos/vnpy_style/`，compiler 默认测试通过。
- 是否引入 TDX 行情? 否；A runtime/backend tests grep 为 0。

### Deviation intercept
- 拦截并修正一个 Phase 3 偏差：基线 adapter 会在 child terminal 后提前终结 vn.py-style instance，导致 TWAP/BestLimit 不能存活至执行窗口/后续 tick。已改为 vn.py-style instance 仅由 core FINISH 或 operator command 终结。


## Phase 3 operator cancel characterization self-audit - 2026-06-23T03:43:16.007248+00:00

### Deliverable
- Added explicit Phase 3 operator cancel characterization for a runtime-owned vn.py Sniper instance: active child order is cancelled through gateway and the owning algo instance is terminalized as CANCELLED.

### Design reread
- Re-read durable design §3 rules 1/2/3/5/7/9, §4.4 operator cancel semantics, §9 Phase 3, §9.x, §10.
- Re-read Phase0 seam contract OMS/gateway boundaries.

### §10 grep guard output

```text
event_loop runtime/gateway range(_timer_iterations count:
<no matches>

event_loop class sync methods no return []:
QmtClientMiniQMTEventLoopGateway.sync_orders/sync_trades/sync_positions all call _required_qmt_list with MINIQMT_EVENT_LOOP_SYNC_*_UNAVAILABLE reason_code; no return [] in class.

JsonFile OMS authority:
Only seam doc and repository/export compatibility references; no event_loop authority use added.

vnpy_style diff:
<empty git diff -- backend/execution_algos/vnpy_style>

status predicates scan:
runtime.py/oms.py use is_open_like_order_status and is_terminal_order_status for broker numeric statuses. No new status literal fork added.

flag inert tests:
rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_phase0_seam_contracts.py::test_miniqmt_execution_runtime_flag_defaults_to_compiler_and_rejects_unknown_values backend/tests/miniqmt_execution_runtime/test_miniqmt_phase2_qmt_strategy_oms.py::test_event_loop_client_uses_qmt_strategy_oms_authority_and_compiler_default_is_inert -q
.. [100%]
2 passed in 1.02s

MiniQMT/event_loop TDX guard:
rg -n 'fetch_tdx_realtime_quotes|TDX_REALTIME' backend/services/miniqmt_execution_runtime backend/tests/miniqmt_execution_runtime
<no matches>

Targeted test:
rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_operator_commands.py backend/tests/miniqmt_execution_runtime/ -q
46 passed in 1.30s
```

### Self questions
- 真事件驱动还是查一次/合成 timer? 真事件驱动；operator command event cancels active child and terminalizes instance, no query-once lifecycle.
- 是否新造第二套非 durable OMS? 否。
- 是否动 B 或分叉算法核? 否；只补 runtime test，`vnpy_style` diff 为空。
- 是否引入 TDX 行情? 否；A runtime/backend tests grep 为 0。

### Deviation intercept
- 未新增偏差；该用例锁定前一条已拦截的实例生命周期偏差不会影响 operator cancel 的强制终结语义。


## Pre-Phase 4 self-audit - 2026-06-23T03:44:31.606603+00:00

### Scope decision
- Phase 3 checkpoints are committed and worktree is clean.
- Time permits a narrow Phase 4 skeleton only: real RiskEngine hook and kill-switch behavior with tests; no full rule set, no service/runtime activation.

### Design reread
- Re-read durable design §3 hard rules including rule 9, §4.5 RiskEngine, §9 Phase 4 gate, §9.x, §10.

### §10 grep guard output

```text
event_loop runtime/gateway range(_timer_iterations count:
<no matches>

vnpy_style diff:
<empty git diff -- backend/execution_algos/vnpy_style>

MiniQMT/event_loop TDX guard:
rg -n 'fetch_tdx_realtime_quotes|TDX_REALTIME' backend/services/miniqmt_execution_runtime backend/tests/miniqmt_execution_runtime
<no matches>

working tree status before Phase 4:
<clean>
```

### Self questions
- 真事件驱动还是查一次/合成 timer? Planned Phase 4 hook will run after real event callbacks (`tick/order/trade/disconnect`), not polling/query-once.
- 是否新造第二套非 durable OMS? 否；kill-switch will use existing runtime OMS child order records and gateway cancel path.
- 是否动 B 或分叉算法核? 否。
- 是否引入 TDX 行情? 否。

### Deviation intercept
- 未拦截偏差。Phase 4 scope capped to real hook + kill-switch skeleton; no placeholder-only code.


## Phase 4 risk hook self-audit - 2026-06-23T03:50:38.779934+00:00

### Deliverable
- Added `MiniQMTRiskEngine` realtime hook and `MiniQMTRiskDecision` skeleton.
- Runtime evaluates risk after real event-loop events (`TICK`, `ORDER_EVENT`, `TRADE_EVENT`, `ACCOUNT_EVENT`, `GATEWAY_DISCONNECTED`, `TIMER`).
- Kill-switch cancels active child orders through gateway, terminalizes owning instances, persists `RISK_KILL_SWITCH_TRIGGERED`, pauses runtime, and blocks new child orders with loud reason_code.
- Tests cover disconnect kill-switch and tick price-limit kill-switch running before algo submission.

### Design reread
- Re-read durable design §3 hard rules including rule 9, §4.5 RiskEngine, §9 Phase 4 gate, §9.x, §10.
- Confirmed this is a real hook + kill-switch skeleton, not a placeholder; no full pre-trade/production rules attempted.

### §10 grep guard output

```text
event_loop runtime/gateway range(_timer_iterations count:
<no matches>

all range(_timer_iterations references:
backend/services/miniqmt_execution_runtime/client.py:369: compiler/compat path
backend/services/miniqmt_execution_runtime/client.py:512: compiler/compat path

event_loop class sync methods no return []:
QmtClientMiniQMTEventLoopGateway.sync_orders/sync_trades/sync_positions all call _required_qmt_list with MINIQMT_EVENT_LOOP_SYNC_*_UNAVAILABLE reason_code.

JsonFile OMS event_loop authority references:
Only seam doc and repository/export compatibility references; no event_loop authority use added.

vnpy_style attribution/source map diff:
<empty git diff -- backend/execution_algos/vnpy_style>

BUG-470 predicates / status scan:
runtime.py/oms.py use is_open_like_order_status/is_terminal_order_status for broker numeric statuses. No new broker status literal fork added in risk hook.

flag inert tests:
rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_phase0_seam_contracts.py::test_miniqmt_execution_runtime_flag_defaults_to_compiler_and_rejects_unknown_values backend/tests/miniqmt_execution_runtime/test_miniqmt_phase2_qmt_strategy_oms.py::test_event_loop_client_uses_qmt_strategy_oms_authority_and_compiler_default_is_inert -q
.. [100%]
2 passed in 0.98s

MiniQMT/event_loop TDX guard:
rg -n 'fetch_tdx_realtime_quotes|TDX_REALTIME' backend/services/miniqmt_execution_runtime backend/tests/miniqmt_execution_runtime
<no matches>

Targeted tests:
rtk python -m pytest backend/tests/miniqmt_execution_runtime/ -q
48 passed in 1.48s
rtk python -m ruff check changed runtime/risk/test files -> All checks passed
rtk git diff --check -> passed
```

### Self questions
- 真事件驱动还是查一次/合成 timer? 真事件驱动；risk hook 在真实 event-loop events 之后运行，tick kill-switch 测试证明在 algo submit 前阻断。
- 是否新造第二套非 durable OMS? 否；kill-switch 使用既有 child order/OMS/gateway 路径。
- 是否动 B 或分叉算法核? 否；`risk_engine` 默认 `NoopMiniQMTRiskEngine`，flag inert 测试通过，`vnpy_style` diff 为空。
- 是否引入 TDX 行情? 否；A runtime/backend tests grep 为 0。

### Deviation intercept
- 拦截并修正一个 Phase 4 fail-fast 顺序偏差：kill-switch 后对已终结实例提交新单原先会先报 `active algo instance not found`，已改为 kill-switch 优先 loud，返回风险 reason_code。


## Final PR gate self-audit - 2026-06-23T03:54:25.953169+00:00

### Deliverable
- Final gate for Phase 3 complete + Phase 4 risk hook skeleton.
- Added paper_v2_l3 validation history record with exact commands/evidence.

### Design reread
- Re-read durable design §3 hard rules including rule 9, §9 Phase 3/4, §9.x, §10; Phase0 seam contract; ADR 0002; 0608 Phase3 acceptance table.

### §10 grep guard output

```text
event_loop runtime/gateway/risk range(_timer_iterations count:
<no matches>

all range(_timer_iterations references:
backend/services/miniqmt_execution_runtime/client.py:369: compiler/compat path
backend/services/miniqmt_execution_runtime/client.py:512: compiler/compat path

event_loop gateway class sync methods:
QmtClientMiniQMTEventLoopGateway.sync_orders/sync_trades/sync_positions all call _required_qmt_list with reason_code MINIQMT_EVENT_LOOP_SYNC_*_UNAVAILABLE; no return [] in event_loop subclass.

broad gateway return []:
legacy QmtClientMiniQMTGateway lines 214/220/226 still contain return [] compatibility for non-event_loop path; event_loop subclass overrides with loud _required_qmt_list and tests lock all three sync methods.

JsonFile OMS event_loop authority references:
Only seam doc and repository/export compatibility references; no event_loop authority use added.

vnpy_style attribution/source map diff:
<empty git diff -- backend/execution_algos/vnpy_style>

BUG-470 predicates / status scan:
runtime.py/oms.py use is_open_like_order_status/is_terminal_order_status for broker numeric statuses. Existing text fallback remains for broker text snapshots; no new broker status literal fork added by this work.

flag inert tests:
rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_phase0_seam_contracts.py::test_miniqmt_execution_runtime_flag_defaults_to_compiler_and_rejects_unknown_values backend/tests/miniqmt_execution_runtime/test_miniqmt_phase2_qmt_strategy_oms.py::test_event_loop_client_uses_qmt_strategy_oms_authority_and_compiler_default_is_inert -q
.. [100%]
2 passed in 0.92s

MiniQMT/event_loop TDX guard:
rg -n 'fetch_tdx_realtime_quotes|TDX_REALTIME' backend/services/miniqmt_execution_runtime backend/tests/miniqmt_execution_runtime
<no matches>
```

### Validation evidence

```text
rtk python -m pytest backend/tests/miniqmt_execution_runtime/ -q
48 passed in 1.30s

rtk python -m ruff check <changed runtime/risk/test files>
All checks passed

rtk git diff --check
passed

rtk python -m nox -s l0
success

rtk python -m nox -s validation_module_registry_l0
success; 8 passed; ownership scan files=12 mapped=12 unmapped=0 ambiguous=0

rtk cmd /c "set AISTOCK_HOSTED_CI=1&& set PAPER_V2_L3_SKIP_UI=1&& python -m nox -s paper_v2_l3"
Ran 5 sessions successfully: paper_v2_l3, l0, paper_v2_backend (661 passed, 1 skipped, 1 deselected), paper_v2_data_quality, data_quality_deep (10 passed, 21 skipped)
```

### Self questions
- 真事件驱动还是查一次/合成 timer? 真事件驱动；Sniper/BestLimit/TWAP tests use real `on_tick`/`record_trade_event`/`record_order_event`/`on_timer` event calls, not compiler for-loop; Phase4 risk hook runs on real event-loop events.
- 是否新造第二套非 durable OMS? 否；uses existing runtime OMS/qmt_strategy seam; no JSON authority added.
- 是否动 B 或分叉算法核? 否；`vnpy_style` diff empty; compiler default tests pass.
- 是否引入 TDX 行情? 否；A runtime/backend tests grep zero.

### Deviation intercept
- Recorded deviations intercepted earlier remain fixed: Phase 3 premature vn.py instance terminalization; Phase 4 kill-switch fail-fast reason-code ordering.

## 2026-06-23 Phase 4 self-audit checkpoint - configurable RiskEngine ruleset

- 子交付物: Phase 4 收尾, 增加 `ConfigurableMiniQMTRiskEngine` / `MiniQMTRiskRuleSet` / `MiniQMTRiskPriceBand`, 覆盖 pre-submit 越限、账户亏损阈值、敞口、tick 价格带、断连 kill-switch；`submit_child_order` 前置实时风控挂载点与 kill-switch 对齐, 全部 loud + reason_code。
- 重读设计: ADR 0002；`docs/architecture/miniqmt_durable_execution_runtime_design_20260623.md` §3(含规则9)、§4.5、§9 Phase4、§10；Phase0 seam contract。
- 验证:
  - `rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_phase4_risk_engine.py -q` -> 5 passed
  - `rtk python -m pytest backend/tests/miniqmt_execution_runtime/ -q` -> 51 passed
  - `rtk python -m ruff check backend/services/miniqmt_execution_runtime/risk.py backend/services/miniqmt_execution_runtime/runtime.py backend/services/miniqmt_execution_runtime/__init__.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase4_risk_engine.py` -> All checks passed
  - `rtk git diff --check` -> pass

### §10 grep guard output

```text
rtk python -c "... event_loop gateway return [] count ..." -> event_loop_gateway_return_empty_list_count=0
rtk python -c "... event_loop timer range count ..." -> event_loop_timer_range_count=0
rtk python -c "... TDX guard count ..." -> tdx_in_miniqmt_runtime_count=0
rtk git diff --name-only -- backend/execution_algos/vnpy_style -> <empty>
rtk rg -n "JsonFileMiniQMTExecutionRuntimeRepository|uses_qmt_strategy_authority|strategy_ledger_repository" backend/services/miniqmt_execution_runtime backend/tests/miniqmt_execution_runtime -> JsonFile remains only repository/debug/restart-test compatible; event_loop client injects QmtStrategyLedgerRepository and tests assert uses_qmt_strategy_authority=True.
rtk rg -n "is_open_like_order_status|is_terminal_order_status|is_partial_order_status|STATUS_OPEN_LIKE|STATUS_PART_SUCC|STATUS_CANCELLED|STATUS_FILLED|STATUS_REJECTED" backend/services/miniqmt_execution_runtime backend/tests/miniqmt_execution_runtime -> runtime/oms/client continue through BUG-470 predicates; no new status literal fork in Phase4 changes.
rtk rg -n "MINIQMT_EXECUTION_RUNTIME|get_miniqmt_execution_runtime_kind\(\{\}\)|MiniQMTExecutionRuntimeKind.COMPILER" backend/services/miniqmt_execution_runtime backend/tests/miniqmt_execution_runtime -> config default remains compiler; Phase0/Phase2 tests lock compiler inert.
```

### 自问

1. 是否真事件驱动, 还是悄悄查一次/合成 timer? 答: 真事件驱动。RiskEngine 在 Tick/Order/Trade/Account/Disconnect 事件与 pre-submit 挂载点同步评估；未引入 timer loop 或提交后查一次。
2. 是否新造第二套非 durable OMS? 答: 否。未新增 OMS, 订单/成交仍经 `MiniQMTOmsLedger` 与 qmt_strategy_ledger 权威路径。
3. 是否动 B 或分叉算法核? 答: 否。默认 compiler 未改；`backend/execution_algos/vnpy_style/` diff 为空。
4. 是否引入 TDX 行情? 答: 否。MiniQMT runtime/tests TDX guard count=0。
5. 影子是否真对账? 答: Phase 5 尚未开始, 下一子交付物实现 durable shadow reconciliation。

- 偏差拦截: 无。



## 2026-06-23 Phase 5 self-audit checkpoint - shadow reconciliation dry-run replay (rewritten ASCII 2026-06-23T05:37:55.873741+00:00)

- Deliverable: Phase 5 shadow/parallel run and automatic reconciliation. Added `MiniQMTShadowReconciler`, `MiniQMTShadowParallelRunner`, `MiniQMTShadowEventLoopAdapter`, `MiniQMTShadowCompilerAdapter`, and a no-broker-mutation dry-run gateway. A/B receive the same input stream and reconcile child order count/price/quantity/status, trades, cash, and positions. Reports persist as `SHADOW_RECONCILIATION_REPORTED` runtime events plus runtime metadata. Fatal drift is loud with `MINIQMT_SHADOW_RECONCILIATION_FATAL`.
- Design reread: ADR 0002; `docs/architecture/miniqmt_durable_execution_runtime_design_20260623.md` section 3 including rule 9 TDX isolation, section 7 shadow mode, section 8 scenario matrix, section 9 Phase 5 gate, section 9.x drift blockers, section 10 grep guards, and Phase0 seam contract. This checkpoint does only Phase5 shadow/dry-run work; it does not switch traffic, start services, or place real orders.

### Section 10 grep guard output

```text
rtk python -c "... event_loop gateway return [] count ..."
event_loop_gateway_return_empty_list_count= 0

rtk python -c "... event_loop core range(_timer_iterations) count ..."
event_loop_core_range_timer_iterations_count= 0
all_timer_iteration_refs=
<none>

rtk python -c "... TDX guard count ..."
tdx_in_miniqmt_runtime_tests_count= 0

rtk git diff --name-only -- backend/execution_algos/vnpy_style
<empty>

rtk rg -n "JsonFileMiniQMTExecutionRuntimeRepository|uses_qmt_strategy_authority|strategy_ledger_repository" backend/services/miniqmt_execution_runtime backend/tests/miniqmt_execution_runtime
JsonFile remains only repository/export/restart-test compatibility. event_loop client injects qmt_strategy ledger and tests assert event_loop uses_qmt_strategy_authority=True while compiler stays false.

rtk rg -n "is_open_like_order_status|is_terminal_order_status|is_partial_order_status|STATUS_OPEN_LIKE|STATUS_PART_SUCC|STATUS_CANCELLED|STATUS_FILLED|STATUS_REJECTED" backend/services/miniqmt_execution_runtime backend/tests/miniqmt_execution_runtime
runtime.py/oms.py/client.py/shadow.py continue through BUG-470 predicates for broker numeric status classification. shadow uses qmt_strategy constants only to build dry-run broker snapshots and does not add hard-coded numeric forks.

rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_phase0_seam_contracts.py::test_miniqmt_execution_runtime_flag_defaults_to_compiler_and_rejects_unknown_values backend/tests/miniqmt_execution_runtime/test_miniqmt_phase2_qmt_strategy_oms.py::test_event_loop_client_uses_qmt_strategy_oms_authority_and_compiler_default_is_inert -q
.. [100%]
2 passed in 1.28s
```

### Validation evidence

```text
rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_phase5_shadow_reconciliation.py -q
................... [100%]
19 passed in 1.18s

rtk python -m pytest backend/tests/miniqmt_execution_runtime/ -q
...................................................................... [100%]
70 passed in 1.65s

rtk python -m ruff check backend/services/miniqmt_execution_runtime/risk.py backend/services/miniqmt_execution_runtime/runtime.py backend/services/miniqmt_execution_runtime/models.py backend/services/miniqmt_execution_runtime/shadow.py backend/services/miniqmt_execution_runtime/__init__.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase4_risk_engine.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase5_shadow_reconciliation.py
All checks passed!

rtk git diff --check
passed
```

### Self questions

1. Is this truly event-driven rather than a one-shot query or synthetic timer loop? Yes. The A adapter uses runtime `create_vnpy_algo_instance`, `on_tick`, `record_trade_event`, `record_order_event`, `record_disconnect_event`, and `recover`. Timer events are consumed only when the input stream explicitly contains `timer`/`algo_timer`; no `range(_timer_iterations)` loop is used in event_loop replay.
2. Did I create a second non-durable OMS? No. Shadow reports are persisted into runtime events and runtime metadata. A replay still uses the runtime OMS path; no JSON file OMS is authoritative for event_loop.
3. Did I change B or fork the algo core? No. Compiler default flag tests pass and `backend/execution_algos/vnpy_style/` diff is empty.
4. Did I introduce TDX market data? No. MiniQMT runtime/tests grep for `fetch_tdx_realtime_quotes|TDX_REALTIME` is zero. Shadow tick input is caller-provided and does not import TDX.
5. Is shadow doing real reconciliation? Yes. Phase5 tests cover full_fill, partial_55_stream, delay, reject, cancel, disconnect, and restart_recovery. A/B same-input dry-run replay is diffed across child orders, trades, cash, and positions; fatal drift is loud and durable.

### Deviation intercept

- Intercepted one deviation before commit: the first Phase5 draft had static snapshot/echo adapters, which was insufficient evidence for same-input A/B replay. It was replaced with `MiniQMTShadowEventLoopAdapter` plus `MiniQMTShadowCompilerAdapter` real replay, and the section 8 scenario matrix tests were added.

## 2026-06-23 Final PR gate self-audit - Phase 4/5 after rebase (rewritten ASCII 2026-06-23T05:37:55.873761+00:00)

- Deliverable: final Phase4/5 PR gate after rebasing on latest `origin/main`. The branch includes the merged P0 MiniQMT no-TDX chain from main. This PR keeps scope to Phase4 RiskEngine/kill-switch and Phase5 shadow reconciliation.
- Design reread: ADR 0002; durable design section 3 including rule 9, section 4.5 RiskEngine, section 7 shadow mode, section 8 scenarios, section 9 Phase4/5 gates, section 9.x drift blockers, and section 10 grep guards. No services were started/restarted, no production DB/DDL was touched, and no gray switch was enabled.

### Section 10 grep guard output

```text
rtk python -c "... event_loop gateway return [] count ..."
event_loop_gateway_return_empty_list_count= 0

rtk python -c "... event_loop core range(_timer_iterations) count ..."
event_loop_core_range_timer_iterations_count= 0
all_timer_iteration_refs=
<none>

rtk python -c "... TDX guard count ..."
tdx_in_miniqmt_runtime_tests_count= 0

rtk git diff --name-only -- backend/execution_algos/vnpy_style
<empty>

JsonFile OMS guard:
JsonFileMiniQMTExecutionRuntimeRepository remains compatibility/restart-test repository only. event_loop client injects qmt_strategy ledger and tests assert event_loop uses_qmt_strategy_authority=True while compiler stays false.

Status predicate guard:
runtime.py/oms.py/client.py/shadow.py continue through BUG-470 predicates (`is_open_like_order_status`/`is_terminal_order_status`/`is_partial_order_status`) for broker numeric status classification. No new broker status literal fork.

flag inert evidence:
rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_phase0_seam_contracts.py::test_miniqmt_execution_runtime_flag_defaults_to_compiler_and_rejects_unknown_values backend/tests/miniqmt_execution_runtime/test_miniqmt_phase2_qmt_strategy_oms.py::test_event_loop_client_uses_qmt_strategy_oms_authority_and_compiler_default_is_inert -q
.. [100%]
2 passed in 1.28s
```

### Validation evidence after rebase

```text
rtk python -m pytest backend/tests/miniqmt_execution_runtime/ -q
...................................................................... [100%]
70 passed in 1.60s

rtk python -m ruff check backend/services/miniqmt_execution_runtime/risk.py backend/services/miniqmt_execution_runtime/runtime.py backend/services/miniqmt_execution_runtime/models.py backend/services/miniqmt_execution_runtime/shadow.py backend/services/miniqmt_execution_runtime/__init__.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase4_risk_engine.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase5_shadow_reconciliation.py
All checks passed!

rtk git diff --check
passed

rtk python -m nox -s l0
Session l0 was successful.

rtk python -m nox -s validation_module_registry_l0
8 passed; Module ownership scan completed: files=12, mapped=12, unmapped=0, ambiguous=0; session successful.

rtk cmd /c "set AISTOCK_HOSTED_CI=1&& set PAPER_V2_L3_SKIP_UI=1&& python -m nox -s paper_v2_l3"
Ran 5 sessions successfully: paper_v2_l3; l0; paper_v2_backend (665 passed, 1 skipped, 1 deselected); paper_v2_data_quality; data_quality_deep (10 passed, 21 skipped).
```

### Self questions

1. Is this truly event-driven rather than a one-shot query or synthetic timer loop? Yes. Phase4 hooks run on runtime events/pre-submit. Phase5 A replay uses runtime event APIs and no `range(_timer_iterations)`.
2. Did I create a second non-durable OMS? No. A replay uses runtime OMS/qmt_strategy seam; shadow reports are runtime events/metadata and not OMS authority.
3. Did I change B or fork the algo core? No. Default compiler flag inert tests pass and `backend/execution_algos/vnpy_style/` diff is empty.
4. Did I introduce TDX market data? No. MiniQMT runtime/tests TDX grep is 0; main now includes the rule9/P0 no-TDX fix chain.
5. Is shadow doing real reconciliation? Yes. Section 8 full fill, partial 55, delay, reject, cancel, disconnect, and restart recovery pass A/B same-input dry-run replay; fatal drift is loud and durable.

### Deviation intercept

- Intercepted one deviation in this turn: the Phase5 static snapshot/echo draft was replaced by event_loop adapter + compiler adapter replay and section 8 matrix tests before final commit.

## 2026-06-23 Phase 6 self-audit checkpoint - scoped gray switch evidence gate

- Deliverable: Phase 6 gray/canary control plane. Added `MiniQMTGraySwitchController` with per `portfolio_id`/`strategy_slot_id` overrides, durable audit events for switch/rollback apply/reject, SIM-only live gate, exact-scope no-fatal shadow evidence gate, and explicit in-flight ambiguity rejection that requires operator reset/cancel before switching. Global default remains `compiler`; unswitched scopes resolve to compiler.
- Design reread: ADR 0002; durable design section 3 hard rules including rule 9 MiniQMT no-TDX; section 7 shadow/gray/rollback; section 9 Phase6/9.x drift blockers; section 10 grep guards; Phase0 seam contract. This checkpoint starts no service, places no real order, and does not touch production DB/DDL.

Grep/static guards:

```text
rtk python -c "... scan event_loop core for range(_timer_iterations) ..."
event_loop_core_range_timer_iterations_count= 0

rtk python -c "... scan QmtClientMiniQMTEventLoopGateway segment for return [] ..."
event_loop_gateway_return_empty_list_count= 0

rtk python -c "... scan MiniQMT runtime/tests for fetch_tdx_realtime_quotes|TDX_REALTIME ..."
miniqmt_event_loop_tdx_guard_count= 0

rtk git diff --name-only -- backend/execution_algos/vnpy_style
<empty>

rtk python -c "... scan gray/runtime/oms/shadow for JsonFile authority references ..."
<empty in gray/runtime/oms/shadow; existing repository/export/test references remain compatibility/restart-test only>

Status predicates:
Phase6 gray code uses `MiniQMTChildOrderStatus` enum terminal set for in-flight checks and does not add new broker numeric/text status classifiers. Existing runtime/oms/shadow broker status handling continues through `is_open_like_order_status` / `is_terminal_order_status` / `is_partial_order_status`.

Flag inert evidence:
rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_phase6_gray_switch.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase0_seam_contracts.py::test_miniqmt_execution_runtime_flag_defaults_to_compiler_and_rejects_unknown_values backend/tests/miniqmt_execution_runtime/test_miniqmt_phase2_qmt_strategy_oms.py::test_event_loop_client_uses_qmt_strategy_oms_authority_and_compiler_default_is_inert -q
.......                                                                  [100%]
7 passed in 1.05s
```

Validation:

```text
rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_phase6_gray_switch.py -q
.....                                                                    [100%]
5 passed in 1.06s

rtk python -m ruff check backend/services/miniqmt_execution_runtime/gray.py backend/services/miniqmt_execution_runtime/models.py backend/services/miniqmt_execution_runtime/__init__.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase6_gray_switch.py
All checks passed!

rtk git diff --check
<no output; exit 0>
```

Self questions:

1. Is this truly event-driven rather than one-shot query or synthetic timer? Yes. The shadow prerequisite uses Phase5 same-input durable replay through event_loop/compiler adapters, and the gray switch controller only consumes persisted shadow report metadata; it adds no timer loop or submit-then-query behavior.
2. Did I create a second non-durable OMS? No. Gray state is runtime metadata plus append-only runtime events; it is not an OMS authority and does not replace `qmt_strategy`/runtime OMS.
3. Did I change B or fork the algo core? No. Default `MINIQMT_EXECUTION_RUNTIME` remains compiler; unswitched scopes resolve compiler; `backend/execution_algos/vnpy_style/` diff is empty.
4. Did I introduce TDX in MiniQMT/event_loop? No. TDX guard count is 0 for MiniQMT runtime and MiniQMT runtime tests.
5. Is gray truly portfolio/strategy-slot scoped and rollback capable? Yes. Tests cover exact-scope switch, unswitched scope staying compiler, rollback to compiler, wrong-scope evidence rejection, missing/fatal evidence rejection, and in-flight ambiguity rejection until operator reset.
6. Did I switch without same-scope shadow evidence or touch LIVE? No. Missing/fatal/wrong-scope evidence and LIVE/LIVE_PENDING_APPROVAL are loud rejects with reason_code; no live path is enabled.

Deviation intercepts:

- None in this checkpoint.

## 2026-06-23 Phase 7 self-audit checkpoint - B fallback evaluation

- Deliverable: Phase 7 evaluation doc `docs/architecture/miniqmt_phase7_b_fallback_retirement_evaluation_20260623.md`. Decision: keep B/compiler as explicit fallback and default; do not delete B in this phase. The doc records explicit rollback semantics, no-silent rejection cases, retirement criteria, and remaining live/canary gates.
- Design reread: ADR 0002; durable design section 7 shadow/gray/rollback; section 9 Phase7 gate; section 9.x no simplified implementation; section 10 grep guards. This checkpoint is documentation/evaluation only and does not switch traffic, start services, or touch production DB/DDL.

Grep/static guards:

```text
rtk python -c "... scan event_loop core for range(_timer_iterations) ..."
event_loop_core_range_timer_iterations_count= 0

rtk python -c "... scan QmtClientMiniQMTEventLoopGateway segment for return [] ..."
event_loop_gateway_return_empty_list_count= 0

rtk python -c "... scan MiniQMT runtime/tests for fetch_tdx_realtime_quotes|TDX_REALTIME ..."
miniqmt_event_loop_tdx_guard_count= 0

rtk git diff --name-only -- backend/execution_algos/vnpy_style
<empty>

JsonFile OMS authority:
No new JsonFile usage in Phase7 doc or gray controller. Existing references remain repository/export/restart-test compatibility only; no event_loop OMS authority use was added.

Status predicates:
No product code change in this checkpoint. Phase6 gray controller uses enum terminal states for in-flight checks and does not add broker status literal classifiers; existing broker numeric status handling remains in runtime/oms/shadow predicate paths.

Flag inert evidence:
rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_phase6_gray_switch.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase0_seam_contracts.py::test_miniqmt_execution_runtime_flag_defaults_to_compiler_and_rejects_unknown_values backend/tests/miniqmt_execution_runtime/test_miniqmt_phase2_qmt_strategy_oms.py::test_event_loop_client_uses_qmt_strategy_oms_authority_and_compiler_default_is_inert -q
.......                                                                  [100%]
7 passed in 1.61s
```

Validation:

```text
rtk python -m ruff check backend/services/miniqmt_execution_runtime/gray.py backend/services/miniqmt_execution_runtime/models.py backend/services/miniqmt_execution_runtime/__init__.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase6_gray_switch.py
All checks passed!

rtk git diff --check
<no output; exit 0>
```

Self questions:

1. Is this truly event-driven rather than one-shot query or synthetic timer? Yes. Phase7 adds no runtime execution path and preserves Phase6/Phase5 gates.
2. Did I create a second non-durable OMS? No. The doc explicitly states gray metadata/events are not OMS authority and B remains fallback.
3. Did I change B or fork the algo core? No. This checkpoint only adds a doc; `backend/execution_algos/vnpy_style/` diff is empty.
4. Did I introduce TDX in MiniQMT/event_loop? No. TDX guard count is 0.
5. Is gray truly portfolio/strategy-slot scoped and rollback capable? Yes. The doc binds fallback semantics to per-scope overrides and durable rollback events, not global default mutation.
6. Did I switch without same-scope shadow evidence or touch LIVE? No. The doc keeps LIVE blocked until separate admission gates pass.

Deviation intercepts:

- None in this checkpoint.
