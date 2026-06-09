# MiniQMT Unified Runtime Phase 1 Validation ? BUG-287

- issue: `BUG-287`
- github_issue: https://github.com/licong01-cloud/AIstock/issues/837
- pr: https://github.com/licong01-cloud/AIstock/pull/841
- branch: `bug/BUG-287-p0-miniqmt-phase1-enforce-unified-runtime-contra-20260609`
- commit: `fddbe129`
- phase: `Phase 1 - contracts and disabled-path blockers`
- design_doc: `docs/architecture/miniqmt_unified_vnpy_execution_runtime_design_20260608.md`
- design_sections: `3`, `4`, `9`, `10.8`, `10.9`, `10.10`, `11`, `13.1`, `14`

## Phase Scope

????? Phase 1 ????????Alpha ????? broker-neutral?MiniQMT ? V25 broker execution fail-fast?????????????????????????? canonical runtime owner?Phase 1 ??? durable runtime skeleton?vn.py ?? parity?SELL-first ? L3/L5 ???????

## Design Trace Matrix

| design_item | design_ref | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|---|
| AlphaSignalBook broker-neutral contract | `?3.2`, `?10.8.2`, `?11.2` | `backend/services/simulation_runtime/models.py`, `backend/services/simulation_runtime/__init__.py` | `backend/tests/simulation_runtime/test_miniqmt_signal_contract.py`; simulation_runtime suite `102 passed` | PASS | ? |
| Runtime request / slot / operator DTO boundary | `?5`, `?10.8.2`, `?13.1` | `backend/services/simulation_runtime/models.py` | `test_miniqmt_signal_contract.py`; import/export checks | PASS | Durable runtime behavior deferred to Phase 2 |
| MiniQMT V25_* fail-fast | `?10.8.2`, `?11.1`, `?11.2` | `backend/services/simulation_runtime/bridges.py` | `backend/tests/simulation_runtime/test_miniqmt_rejects_v25_broker_execution.py`; no silent fallback | PASS | ? |
| Canonical runtime owner gate | `?3.1`, `?10.8.2`, `?13.1` | `backend/services/simulation_runtime/models.py` validators; bridge submit validation | `backend/tests/simulation_runtime/test_miniqmt_path_uniqueness.py` | PASS | Full path convergence remains Phase 4 |
| No fixed strategy-count product gate | `?3.3`, `?10.8.2`, `?13.1` | `assert_no_fixed_strategy_count_gate` in `models.py` | `test_miniqmt_path_uniqueness.py` negative coverage | PASS | Funds-only capacity model remains Phase 5 |
| Scheduler price payload for vn.py-style adapter | `?4`, `?10.8.2` | `backend/services/simulation_runtime/scheduler.py` | `backend/tests/trading_core/test_minqmt_vnpy_shared_adapter.py -> 6 passed` | PASS | Algorithm parity remains Phase 3 |

## Required Evidence

- positive_tests:
  - `python -m pytest backend/tests/simulation_runtime/test_miniqmt_signal_contract.py backend/tests/simulation_runtime/test_miniqmt_path_uniqueness.py backend/tests/simulation_runtime/test_miniqmt_rejects_v25_broker_execution.py backend/tests/simulation_runtime -q -> 102 passed`
  - `python -m pytest backend/tests/trading_core/test_minqmt_vnpy_shared_adapter.py -q -> 6 passed`
- negative_tests:
  - `AlphaSignalBook` rejects broker/order/execution forbidden keys.
  - MiniQMT bridge rejects `V25_TWO_STAGE` / `V25_1_SMALL_CAP` as broker execution algos.
  - fixed strategy-count gate keys are rejected by contract validator.
  - non-canonical MiniQMT runtime owner is rejected.
- static_guard_scan:
  - `python -m ruff check changed simulation_runtime files and tests -> passed`
  - `git diff --check -> passed`
  - `python -m nox -s l0 -> passed`
  - `python -m nox -s validation_module_registry_l0 -> passed`
  - Design `?11.1` grep items are represented by the new contract/negative tests in this Phase 1 scope; legacy hits outside allowed scope are not modified in this phase.
- runtime_evidence:
  - Phase 1 is contract/guard only; no L2/L3/L4/L5 runtime claim is made.
- validation_history_path:
  - `tests/aistock_validation/history/paper_v2_miniqmt_unified/20260609_bug-287_phase1_contracts_and_blockers.md`

## Explicit Non-Regression Claims

- No new MiniQMT broker submit path: PASS; changes add DTO/validators/guard tests, not a new submit owner.
- No direct product call to raw QMT order: PASS for changed files; no raw QMT product path added.
- No V25_* MiniQMT broker execution: PASS; V25 codes fail-fast via `MiniQMTUnsupportedExecutionAlgoError`.
- No fixed strategy-count gate: PASS for Phase 1 contract; validator rejects fixed count gate keys.
- No mock-only completion claim: PASS; this record only claims Phase 1 contract/guard completion, not full runtime readiness.

## DESIGN-COMPLIANCE-001

| item | result | evidence |
|---|---|---|
| ???? | PASS | Phase 1 scoped items implemented: contracts, forbidden-field tests, V25 fail-fast, canonical owner/fixed-count blockers. Phase 2+ explicitly not claimed. |
| ???? | PASS | `assert_canonical_miniqmt_runtime_gate` and path uniqueness tests prevent non-canonical owner drift in this scope. |
| ???? | PASS | Implementations map to design `?10.8.2` Phase 1 and `?13.1` issue 1.1/1.2. |
| vn.py ?? | PASS | This phase only blocks non-vnpy/V25 broker execution; upstream algorithm parity remains Phase 3 and is not claimed. |
| ? silent fallback | PASS | `MINIQMT_UNSUPPORTED_EXECUTION_ALGO` raises fail-fast instead of fallback when V25 is used for MiniQMT broker execution. |
| ??? | PASS | No runtime persistence/recovery behavior changed; Phase 2 owns durable recovery. |
| ???? | PASS | Fixed strategy-count gate is blocked at contract level; full funds-only/SELL-first model remains Phase 5. |
| ???? | PASS | `production_ddl_gate=noop`; `production_frontend_dependency_gate=noop`; `production_backend_dependency_gate=noop`; `restart_required=no` for code merge, runtime activation requires user-managed backend restart later. |

## Production Gates

- production_ddl_gate: `noop`
- production_frontend_dependency_gate: `noop`
- production_backend_dependency_gate: `noop`
- restart_required: `no` for PR validation; user-managed backend restart is required only when merged code is to be activated in the live service.
- production_runtime_touched: `false`
- production_db_touched: `false`

## Known Gaps

- Phase 2 durable event loop/restart recovery is not implemented here.
- Phase 3 Sniper/BestLimit/TWAP upstream vn.py parity is not implemented here.
- Phase 4 Paper v2/simulation_runtime full path convergence is not implemented here.
- Phase 5 funds-only capacity and SELL-first cash model are not implemented here.
- Phase 7 L0-L5 full validation and legacy deprecation are not implemented here.
