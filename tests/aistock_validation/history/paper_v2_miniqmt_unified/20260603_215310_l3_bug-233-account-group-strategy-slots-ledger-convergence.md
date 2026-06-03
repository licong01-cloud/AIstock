# BUG-233 account group strategy slots ledger convergence

- Module: paper_v2_miniqmt_unified
- Level: L3
- Date: 2026-06-03T21:53:10+08:00
- Git commit: 86c82745 (working tree before task commit)
- Operator: lc999
- BUG: BUG-233 / GH #652

## Scope

- Changed files: qmt_strategy_ledger models/repository/order_service, MiniQMTSim broker adapter, Paper v2 auto-run defaults/service binding mode, simulation_runtime models, focused regression tests.
- Impacted flows: MiniQMT account group/strategy slots, qmt_strategy virtual account metadata, managed order batch preflight, Paper v2 MiniQMTSim submit attribution, simulation runtime binding metadata.
- Business goal: ?? MiniQMT SIM broker_account_id ???? account group ?? N=1/N=2 strategy slots????????strategy_name?order_remark_prefix??? slot??? preflight ??? slot ??????? legacy exclusive ??????????
- Out of scope: ?? DDL??? DB ????? backend/frontend/TDX ???Paper v2 repository active binding ???????Phase 4 scheduler/API/UI ???Phase 7 legacy ???
- Protected assets reviewed: ??? StrategyPackage manifest??????QE workspace??? DB?????????

## Environment

- Backend port: ???/????? 8001?validation_center_backend ?? pytest/nox ???????
- Frontend port: ???/??? 3000?paper_v2_l3 ?? `PAPER_V2_L3_SKIP_UI=1` ??/?? L3?
- TDX port: ??? 19080?
- Conda/env: Windows PowerShell, Python via `python` / `C:\Users\lc999\miniconda3\python.exe` in nox sessions.
- Database: `paper_v2_l3` data quality smoke ???? `F:\Dev\AIstock\.env` ????? aistock DB???? DDL?
- Browser/headless: UI E2E skipped by explicit `PAPER_V2_L3_SKIP_UI=1` for this backend scoped BUG.

## DESIGN-COMPLIANCE-001 Matrix

| Design item | Implementation refs | Test/evidence | Status | Gap/exception |
|---|---|---|---|---|
| F2M-01 account group: same broker_account_id hosts N=1/N=2 slots | `backend/services/qmt_strategy_ledger/models.py`, `backend/services/qmt_strategy_ledger/repository.py` | `test_account_group_slots.py::test_account_group_slots_create_n1_and_n2_under_same_minqmt_account`; qmt ledger suite 105 passed | implemented | No DDL by BUG scope; slots stored in existing `virtual_account.metadata`. |
| Funds/slot uniqueness: allocated cash total, strategy_name, order_remark_prefix, duplicate slot constraints | `MiniQmtAccountGroup`, `_validate_account_group_slots` | parametrized duplicate/over-allocation tests in `test_account_group_slots.py` | implemented | Existing Paper broker binding unique index not changed; ledger is authoritative Phase 3 slot layer. |
| Legacy exclusive remains readable/disableable; not deleted | `set_account_group_slot_status`, slot `legacy_portfolio_id` metadata | `test_legacy_exclusive_mapping_remains_readable_and_disableable` | implemented | Legacy Paper v2 exclusive path retained as compatibility shim. |
| Broker raw positions reconcile against strategy virtual lots | `QmtStrategyLedgerReconciliationService` existing path plus account group virtual accounts | `test_broker_raw_positions_reconcile_against_strategy_virtual_lots_in_account_group` | implemented | No production MiniQMT query invoked in validation. |
| BUG-204 fill idempotency/no-overfill inherited in slot ledger | Existing `qmt_strategy_ledger` sync/idempotency suite plus new slot tests | `python -m pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider` -> 105 passed | implemented | Evidence is unit/integration ledger path; no live broker replay. |
| F2M-05 batch preflight across slots | `QmtManagedOrderService._batch_preflight`, account group cash metadata | `test_submit_batch_rejects_account_group_cash_overcommit_across_strategy_slots` and existing can_sell/board-lot/duplicate tests | implemented | Broker cash source remains metadata/cap based in this no-DDL phase. |
| MiniQMTSim no longer exclusive-only in unified mode | `backend/services/paper_trading_v2/broker/minqmtsim.py` | account_group_slots mode test checks capacity > 1 and cross portfolio/package with explicit slot attribution | implemented | Legacy mode still enforces portfolio/package match. |
| MiniQMTSim explicit attribution, no silent fallback | `_account_group_attribution`, required metadata keys and order_remark_prefix check | missing metadata test verifies no QMT call | implemented | Does not synthesize strategy_name/order_remark in unified mode. |
| Paper v2 auto-run default points to account_group_slots | `AUTO_RUN_BROKER_DEFAULTS`, `normalize_account_binding_mode`, service allocation_mode | `test_auto_run.py` included in focused regression 107 passed and paper_v2_backend 596 passed | implemented | `paper_trading_v2/repository.py` unique active binding unchanged by allowed scope. |
| simulation_runtime binding carries group/slot identity; alpha release stays broker-neutral | `SimulationReleaseBinding`, `ExecutionPlanIntent`, `ExecutionPlan` optional fields and forbidden release keys | `test_minqmt_multi_strategy_unified.py` | implemented | Compiler wiring remains later Phase 4 scope. |

## Commands

```powershell
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'
python -m py_compile backend/services/qmt_strategy_ledger/models.py backend/services/qmt_strategy_ledger/repository.py backend/services/qmt_strategy_ledger/order_service.py backend/services/paper_trading_v2/broker/minqmtsim.py backend/services/paper_trading_v2/auto_run.py backend/services/paper_trading_v2/service.py backend/services/simulation_runtime/models.py
python -m pytest backend/tests/qmt_strategy_ledger/test_account_group_slots.py backend/tests/qmt_strategy_ledger/test_order_service_preflight.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/simulation_runtime/test_minqmt_multi_strategy_unified.py -q -p no:cacheprovider
python -m pytest backend/tests/paper_trading_v2/test_auto_run.py backend/tests/qmt_strategy_ledger/test_order_service_submit_fake_qmt.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py -q -p no:cacheprovider
python -m pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider
python -m nox -s validation_module_registry_l0
python -m nox -s l0
python -m nox -s paper_v2_backend
$env:PAPER_V2_L3_SKIP_UI='1'; python -m nox -s paper_v2_l3
python -m nox -s validation_center_backend
git diff --check
```

## Evidence

- Targeted tests: 67 passed in 13.51s for new/changed MiniQMT account group, preflight, broker adapter, simulation runtime tests.
- Surrounding focused tests: 107 passed in 11.38s for new tests + Paper auto-run + batch submit + simulation lifecycle scheduler.
- QMT strategy ledger suite: 105 passed in 12.38s, covering sync/idempotency/no-overfill inheritance.
- `validation_module_registry_l0`: 8 passed; ownership scan files=12, unmapped=0, ambiguous=0.
- `l0`: successful; guardrail scans completed with no blocking findings. Existing/baseline P2/P1 findings are unrelated to changed scope.
- `paper_v2_backend`: 596 passed, 1 skipped, 2 xfailed in 27.82s standalone; also passed inside `paper_v2_l3`.
- `paper_v2_l3` with `PAPER_V2_L3_SKIP_UI=1`: sessions `paper_v2_l3`, `l0`, `paper_v2_backend`, `paper_v2_data_quality`, `data_quality_deep` all successful; data quality smoke reports PASS with one nonblocking legacy ledger consistency WARN.
- `validation_center_backend`: 355 passed, coverage line=80.07 branch=62.34, session successful in ~3 min.
- `git diff --check`: passed.
- API calls: no live API call; no production backend restart.
- DB checks: `paper_v2_l3` read-only data quality checks against local DB; no DDL/no writes.
- Log files: no production logs touched.
- Playwright report/trace/screenshots: not applicable; UI skipped because BUG scope is backend/ledger and `PAPER_V2_L3_SKIP_UI=1` was used.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Initial nox `paper_v2_l3` generated a generic `paper_v2_selection_center` validation md outside BUG allowed history scope | nox record step writes its default module history | Removed the untracked generated md and created this allowed `paper_v2_miniqmt_unified` evidence record | `git status --short` shows no out-of-scope history artifact after cleanup |

## Result

- Final status: passed for implemented BUG-233 backend/ledger scope.
- Remaining risks: Paper v2 repository active binding uniqueness and scheduler/API/UI unified status fields are Phase 4+ or require scope expansion/migration; not changed here.
- Need production backend restart: no.
- Need dev service restart: no.
- Production gates: `production_ddl_gate=noop`, `production_backend_dependency_gate=noop`, `production_frontend_dependency_gate=noop`.
- Production runtime/DB touched: no service restart, no DDL, no production DB write.
