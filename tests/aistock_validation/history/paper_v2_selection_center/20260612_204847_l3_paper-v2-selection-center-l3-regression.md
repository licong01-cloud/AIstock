# Paper v2 / MiniQMT 设计符合性 L3 验收记录（BUG-345 / BUG-347）

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-06-12T20:48:47+08:00
- Worktree: `F:\Dev\AIstock_worktrees\BUG-345-p0-miniqmt-designcompliance-miniqmt-paths-still-20260612`
- Branch: `bug/BUG-345-p0-miniqmt-designcompliance-miniqmt-paths-still-20260612`
- Related issues: `BUG-345` / GitHub #1027, `BUG-347` / GitHub #1029

## Scope

本次验收覆盖 Paper v2 / MiniQMT 与 `docs/architecture/miniqmt_unified_vnpy_execution_runtime_design_20260608.md` 不符合的两类问题：

1. `BUG-345`: MiniQMT Paper v2 和 simulation_runtime 路径仍允许非 vn.py-style 的直接 broker 执行。
2. `BUG-347`: LocalSim unattended 成功运行可以缺少 `RUN_SUCCEEDED` / no-rebalance / finalized 事件追踪，导致 `paper_v2_run_traceability` 失败。

未触发真实 MiniQMT 下单、撤单、清仓；未重启 backend/frontend/TDX/MiniQMT；未执行 DDL。

## Design Compliance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| MiniQMT 产品执行必须走唯一 canonical vn.py-style runtime，不允许 Paper v2 / simulation_runtime 自己降级为直接 broker submit | `backend/services/paper_trading_v2/day_runner.py`, `backend/services/miniqmt_execution_runtime/client.py`, `backend/services/simulation_runtime/bridges.py` | targeted pytest 64 passed; static rg direct-path guard no hits | PASS | 无 |
| MiniQMT broker execution 遇到 `V25_*` 必须 fail-fast 为 unsupported，不得降级为 TWAP/最新价整笔提交 | `day_runner.py`, `bridges.py`, `test_minqmt_vnpy_execution_adapter.py`, `test_miniqmt_rejects_v25_broker_execution.py` | V25 negative regression included in targeted pytest | PASS | 无 |
| MiniQMT broker execution 只接受 `SNIPER_MINIQMT` / `BEST_LIMIT_MINIQMT` / `TWAP_LITE_MINIQMT` 等 approved vn.py-style policy | `day_runner.py`, `bridges.py`, `test_minqmtsim_backend.py`, `test_minqmt_vnpy_shared_adapter.py` | missing policy / `CLOSE_PRICE` rejection tests included in targeted pytest | PASS | 无 |
| 成功运行不能是 false success，Paper v2 durable event trace 必须能证明 success/no-rebalance/finalized | `backend/services/simulation_runtime/scheduler.py`, `backend/tests/simulation_runtime/test_lifecycle_scheduler.py` | `paper_v2_run_traceability` PASS after code fix and idempotent event repair | PASS | 历史遗留 LocalSim run 仅补事件，不改状态/订单/成交/资金 |
| 设计驱动交付不能只以 L0/L3 通过代替逐项验收 | 本记录与 batch PR body | 本矩阵逐项列出设计条款、实现引用和验证证据 | PASS | 无 |

## Commands And Results

```bash
python -m pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/paper_trading_v2/test_minqmt_vnpy_execution_adapter.py backend/tests/simulation_runtime/test_miniqmt_rejects_v25_broker_execution.py backend/tests/trading_core/test_minqmt_vnpy_shared_adapter.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py::test_scheduler_reuses_existing_plans_on_restart_without_reselection_or_resubmit -q -p no:cacheprovider
# 64 passed

python -m py_compile backend/services/paper_trading_v2/day_runner.py backend/services/miniqmt_execution_runtime/client.py backend/services/simulation_runtime/bridges.py backend/services/simulation_runtime/scheduler.py backend/tests/paper_trading_v2/test_minqmt_vnpy_execution_adapter.py backend/tests/simulation_runtime/test_miniqmt_rejects_v25_broker_execution.py backend/tests/trading_core/test_minqmt_vnpy_shared_adapter.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py
# passed

python -m ruff check backend/services/paper_trading_v2/day_runner.py backend/services/miniqmt_execution_runtime/client.py backend/services/simulation_runtime/bridges.py backend/services/simulation_runtime/scheduler.py backend/tests/paper_trading_v2/test_minqmt_vnpy_execution_adapter.py backend/tests/simulation_runtime/test_miniqmt_rejects_v25_broker_execution.py backend/tests/trading_core/test_minqmt_vnpy_shared_adapter.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py
# All checks passed

rg -n "PAPER_V2_DIRECT_MINIQMT|submit_paper_order_intents|MINIQMT_MANAGED_ORDER|_miniqmt_uses_vnpy_style_execution|submit_managed_order_requests|preview_managed_order_requests|_managed_request_signature" backend/services/paper_trading_v2/day_runner.py backend/services/miniqmt_execution_runtime/client.py backend/services/simulation_runtime/bridges.py backend/tests/paper_trading_v2/test_minqmt_vnpy_execution_adapter.py backend/tests/simulation_runtime/test_miniqmt_rejects_v25_broker_execution.py backend/tests/trading_core/test_minqmt_vnpy_shared_adapter.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py
# no hits; exit code 1 expected for no matches

python -m nox -s validation_module_registry_l0
# successful; 8 passed; module ownership scan files=12 mapped=12 unmapped=0 ambiguous=0

$env:PAPER_V2_L3_SKIP_UI='1'; python -m nox -s paper_v2_l3
# successful; l0 passed; paper_v2_backend 615 passed, 1 skipped, 2 xfailed; paper_v2_data_quality PASS; data_quality_deep 10 passed, 21 skipped
```

## DB / Data Repair Evidence

`BUG-347` 初次 L3 数据质量失败：`paper_v2_run_traceability` 中 `missing_success_event=2`。只读定位到两个已经是 `SUCCEEDED` 的 LocalSim unattended run：

- `simrun_0e2c3ab2300f03b0` / portfolio `paper_b26d2312d986441f8497f7484c05f0ec`
- `simrun_6d62e7e36a61bd51` / portfolio `paper_e225bf8a68244c54b4cc25506dadad81`

已执行幂等最小数据修复：仅在 `paper_v2.run_events` 为上述两个 run 补 `RUN_SUCCEEDED` 事件，event_seq `15951`、`15952`；没有修改 run status、订单、成交、现金流水、持仓或 daily snapshot。

## Result

- Final status: PASS
- Remaining risks: `paper_v2_ledger_consistency` 仍报告 legacy WARN（order/fill quantity mismatch=4），本 PR 不把历史账本问题伪装为已修复；如当前代码仍可复现，应另行登记 Paper v2 bug 处理。
- Need production backend restart: no（代码合入后由用户决定何时重启生产 backend）
- Need frontend restart: no
- production_ddl_gate: noop
- production_frontend_dependency_gate: noop
- production_backend_dependency_gate: noop
