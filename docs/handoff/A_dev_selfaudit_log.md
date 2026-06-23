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
