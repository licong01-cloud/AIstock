# BUG-291 Phase 3 Runtime-owned vn.py-derived algo instance 验收记录

- BUG: BUG-291 / GitHub #849
- 分支: `bug/BUG-291-p0-miniqmt-phase3-runtime-owned-vn-py-derived-al-20260609`
- 日期: 2026-06-09
- 阶段: Phase 3 - runtime-owned vn.py-derived algo instance parity
- 设计文档: `docs/architecture/miniqmt_unified_vnpy_execution_runtime_design_20260608.md`
- 堆叠基线: BUG-290 / PR #848 / `bug/BUG-290-p0-miniqmt-phase2-miniqmtexecutionruntime-durabl-20260609`
- 生产影响: 未连接生产 MiniQMT, 未改 DB schema, 未重启 backend/frontend/TDX/MiniQMT

## 1. 设计追踪矩阵

| 设计项 | 设计章节 | 实现文件 | 测试文件 | 状态 |
|---|---|---|---|---|
| Sniper/BestLimit/TWAP 继续使用固定 vnpy_algotrading commit 语义 | 4.2, 10.8.2 Phase 3 | `backend/execution_algos/vnpy_style/*` | `backend/tests/miniqmt_execution_runtime/test_miniqmt_vnpy_algo_parity_*.py`, `backend/tests/trading_core/test_vnpy_style_execution_assets.py` | PASS |
| `ExecutionAlgoInstance` 由 `MiniQMTExecutionRuntime` 持有 | 4.1, 5.2, 10.8.2 Phase 3 | `backend/services/miniqmt_execution_runtime/runtime.py`, `backend/services/miniqmt_execution_runtime/models.py` | `test_miniqmt_vnpy_algo_parity_sniper.py` | PASS |
| tick/timer/order/trade 先进入 runtime event loop，再驱动 algo core | 4.1, 4.3, 10.8.2 Phase 3 | `backend/services/miniqmt_execution_runtime/runtime.py` | `test_miniqmt_vnpy_algo_parity_sniper.py`, `test_miniqmt_vnpy_algo_parity_twap.py` | PASS |
| timer 不再用一次性同步 for-loop 伪造 | 10.8.2 Phase 3 | `backend/services/miniqmt_execution_runtime/runtime.py` | `test_miniqmt_vnpy_algo_parity_twap.py` | PASS |
| algo core 不导入 DB/FastAPI/MiniQMT/vn.py runtime/xtquant | 4.3, 10.8.2 Phase 3 | `backend/execution_algos/vnpy_style/*` | `test_miniqmt_vnpy_algo_import_boundary.py` | PASS |
| restart recovery 后恢复 active order / algo state，不重复下单 | 6.1, 10.8.2 Phase 2/3 | `backend/services/miniqmt_execution_runtime/runtime.py`, `repository.py` | `test_miniqmt_vnpy_algo_restart_recovery.py` | PASS |
| MiniQMT 执行算法必须使用 broker quote，不允许用 last price 合成盘口 | 3.1, 4.2, 10.8.3 | `backend/services/miniqmt_execution_runtime/runtime.py` | `test_miniqmt_vnpy_algo_parity_best_limit.py` | PASS |

## 2. 路径证据

- Phase 3 只把现有 `backend/execution_algos/vnpy_style` broker-neutral core 挂入 `MiniQMTExecutionRuntime`；没有新增第二条 MiniQMT 产品执行路径。
- `MiniQMTExecutionRuntime.create_vnpy_algo_instance()` 持久化 `runtime_algo_family=vnpy_style`、limit price、algo config、source attribution、core audit state。
- `on_tick()` / `on_timer()` 先持久化 runtime event，再分发给 runtime-owned algo core；core 只返回 `VnpyAction`，下单/撤单仍由 runtime/gateway 负责。
- `record_order_event()` / `record_trade_event()` 把 broker fact 写入 event stream 和 OMS 后，再驱动 owning algo core 的 `update_order()` / `update_trade()`。
- `on_tick()` 对 runtime-owned vn.py algo 强制要求 `bid_price_1/bid_volume_1/ask_price_1/ask_volume_1`，缺失时 fail-fast，不用 last price 合成盘口。
- 仍未把 Paper v2 或 `simulation_runtime` 默认入口切到 runtime；这属于 Phase 4。

## 3. 正向测试证据

- `python -m pytest backend/tests/miniqmt_execution_runtime -q` -> 12 passed
- `python -m pytest backend/tests/trading_core/test_vnpy_style_execution_assets.py -q` -> 11 passed
- `python -m pytest backend/tests/miniqmt_execution_runtime backend/tests/trading_core/test_vnpy_style_execution_assets.py backend/tests/simulation_runtime/test_miniqmt_signal_contract.py backend/tests/simulation_runtime/test_miniqmt_path_uniqueness.py backend/tests/simulation_runtime/test_miniqmt_rejects_v25_broker_execution.py -q` -> 41 passed
- `python -m nox -s paper_v2_backend` -> 605 passed, 1 skipped, 2 xfailed
- `python -m nox -s validation_module_registry_l0` -> 8 passed, module ownership scan mapped=12/unmapped=0/ambiguous=0
- `python -m nox -s validation_center_backend` -> 389 passed, coverage line=80.08 branch=62.3

覆盖能力:

- Sniper: ask/bid crossed limit 后由 runtime 提交 child order；active order 再次 tick 时先发 cancel request，不重复 submit。
- BestLimit: 买入挂一档买价，盘口价变化时 runtime 发 cancel request。
- TWAP Lite: runtime timer 第 1 秒不提交，第 2 秒按 interval 提交 slice，证明 timer 由 runtime event loop 驱动。
- restart recovery: 使用 JSON repository 重新加载后，active vn.py algo/order state 可恢复；新进程 tick 不重复 submit，只发 cancel request。
- broker quote fail-fast: 缺 `bid_volume_1/ask_volume_1` 等盘口字段时直接报错，不产生合成盘口成功。

## 4. 负向测试证据

- `test_miniqmt_vnpy_algo_import_boundary.py` 扫描 `backend/execution_algos/vnpy_style/*.py`，禁止 core 导入 `backend.db`、`backend.infra`、`backend.routers`、`backend.services`、`fastapi`、`vnpy`、`xtquant`。
- `test_miniqmt_vnpy_algo_parity_best_limit.py::test_runtime_owned_vnpy_algo_fails_fast_without_broker_best_quote_fields` 阻断缺盘口字段的 MiniQMT tick。
- Phase 1/2 回归 negative tests 同跑：
  - `test_miniqmt_signal_contract.py` 阻断 `AlphaSignalBook` 携带 broker/order/execution/native 字段。
  - `test_miniqmt_path_uniqueness.py` 阻断非 canonical runtime owner 和固定策略数量 gate。
  - `test_miniqmt_rejects_v25_broker_execution.py` 阻断 `V25_*` 进入 MiniQMT broker execution。

## 5. 静态扫描和编码证据

- `python -m ruff check backend/services/miniqmt_execution_runtime backend/tests/miniqmt_execution_runtime` -> passed
- `git diff --check` -> passed
- `python -m compileall -q backend/services/miniqmt_execution_runtime backend/tests/miniqmt_execution_runtime` -> passed
- `rg XtQuantQMTClient|QmtManagedOrderService\.submit_batch|\.place_order\(|raw_qmt|V25_TWO_STAGE|V25_1_SMALL_CAP|max_concurrent_packages|broker_account_id|order_remark|execution_algo_code backend/services/miniqmt_execution_runtime backend/tests/miniqmt_execution_runtime` -> 0 matches（`rg` exit 1 为无匹配预期）
- 新增/变更 Python 文件编码检查: no-bom UTF-8。

## 6. L3/跨模块验证

- `PAPER_V2_L3_SKIP_UI=1 PAPER_V2_SKIP_REALTIME=1 python -m nox -s paper_v2_l3` -> 5 sessions success：`paper_v2_l3`、`l0`、`paper_v2_backend`、`paper_v2_data_quality`、`data_quality_deep`。
- `paper_v2_data_quality` 中 `paper_v2_ledger_consistency` 返回历史 legacy WARN（order_fill_quantity_mismatches=4），该 WARN 是既有历史数据一致性问题，本次 Phase 3 不修改 Paper v2 账本路径，未阻断 nox session。
- `QE_READ_L3_SKIP_UI=1 python -m nox -s qe_read_l3` -> 2 sessions success：`qe_read_l3`、`qe_read_backend`，backend 14 passed。
- UI 子项因本次变更不涉及 frontend 且按用户约束不重启前端/后端，显式 skip；未声明 UI 生产就绪。

## 7. 生产门禁

- `production_ddl_gate`: noop。本阶段没有 SQL migration，没有生产库写入。
- `production_frontend_dependency_gate`: noop。未改前端依赖。
- `production_backend_dependency_gate`: noop。未新增后端依赖。
- 服务重启: 不需要；也未执行 backend/frontend/TDX/MiniQMT 重启。

## 8. DESIGN-COMPLIANCE-001

| 检查项 | 状态 | 说明 |
|---|---|---|
| 不交付简化版/占位版并声称完整架构整改完成 | PASS | 本 PR 只声明 Phase 3 algo instance 化，不声明 Phase 4-7 完成。 |
| vn.py 参考明确 | PASS | 复用已有 `vnpy_style` core 的 attribution/source mapping；测试覆盖 Sniper/BestLimit/TWAP 核心行为。 |
| P0 核心项不延期 | PASS | Phase 3 的 runtime-owned core、timer/tick/order/trade routing、import boundary、restart recovery、broker quote fail-fast 已覆盖。 |
| 未覆盖项显式延期 | PASS | Paper v2/simulation_runtime 切换到 runtime 属于 Phase 4；SELL-first 属于 Phase 5；operator 命令完整业务化属于 Phase 6。 |
| 生产 gate 明确 | PASS | 三个 gate 均为 noop，未触碰生产 runtime/DB。 |

## 9. 残余风险和后续阶段

- 本阶段是 stacked branch，必须在 BUG-290 / PR #848 合入后再独立合入，或在 PR 中明确依赖 #848。
- Phase 4 之前不得把 Paper v2 / `simulation_runtime` 默认入口切到 runtime。
- Phase 5 仍需实现 funds-only capacity、SELL-first proceeds、unfilled sell blocking dependent buy。
- Phase 6 仍需实现 operator `FLATTEN_ALL_POSITIONS`、`CANCEL_ALL_OPEN_ORDERS`、`RESET_STRATEGY_SLOT`、`REPLACE_ALPHA_SIGNAL_BOOK` 的完整命令状态机。
