# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-06-23T11:51:42
- Git commit: 19f0abd6
- Operator: lc999

## Scope

- Changed files: MiniQMT durable execution runtime Phase 3/4 files and tests only.
- Impacted flows: MiniQMT event_loop runtime characterization, Paper v2 L3 regression gates.
- Business goal: prove Phase 3/4 event_loop additions remain inert for B/compiler and do not regress Paper v2 validation gates.
- Out of scope: production service restart, production DB DDL/write, PR merge.
- Protected assets reviewed: MiniQMT event_loop TDX guard, flag default compiler, vnpy_style attribution/source map.

## Environment

- Backend port: not started
- Frontend port: not started
- TDX port: not started
- Conda/env: repo default Python via rtk
- Database: read-only validation smoke used existing configured test/validation DB; no production DDL
- Browser/headless: `PAPER_V2_L3_SKIP_UI=1`

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No blocking high-risk guardrail finding | `rtk python -m nox -s l0` | PASS |
| Module registry | Ownership registry remains valid | `rtk python -m nox -s validation_module_registry_l0` | PASS |
| Backend tests | Paper v2 + Selection Center backend tests pass | `paper_v2_backend`: 661 passed, 1 skipped, 1 deselected | PASS |
| Data quality | Paper v2 data quality smoke and deep checks pass | `paper_v2_data_quality`: success; `data_quality_deep`: 10 passed, 21 skipped | PASS |
| UI E2E | UI skipped by requested hosted-CI env | `PAPER_V2_L3_SKIP_UI=1` | SKIPPED |
| Asset safety | No protected MiniQMT event_loop TDX or vnpy_style fork | final §10 guard in `docs/handoff/A_dev_selfaudit_log.md` | PASS |

## Commands

```bash
rtk python -m pytest backend/tests/miniqmt_execution_runtime/ -q
rtk python -m ruff check backend/services/miniqmt_execution_runtime/__init__.py backend/services/miniqmt_execution_runtime/models.py backend/services/miniqmt_execution_runtime/runtime.py backend/services/miniqmt_execution_runtime/risk.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase1_gateway_event_source.py backend/tests/miniqmt_execution_runtime/test_miniqmt_vnpy_algo_parity_best_limit.py backend/tests/miniqmt_execution_runtime/test_miniqmt_vnpy_algo_parity_twap.py backend/tests/miniqmt_execution_runtime/test_miniqmt_operator_commands.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase4_risk_engine.py
rtk git diff --check
rtk python -m nox -s l0
rtk python -m nox -s validation_module_registry_l0
rtk cmd /c "set AISTOCK_HOSTED_CI=1&& set PAPER_V2_L3_SKIP_UI=1&& python -m nox -s paper_v2_l3"
```

## Evidence

- MiniQMT targeted tests: 48 passed in 1.30s.
- Ruff changed files: All checks passed.
- `git diff --check`: passed.
- `nox -s l0`: successful.
- `nox -s validation_module_registry_l0`: successful; 8 passed, ownership scan unmapped=0 ambiguous=0.
- `paper_v2_l3`: ran 5 sessions successfully: `paper_v2_l3`, `l0`, `paper_v2_backend`, `paper_v2_data_quality`, `data_quality_deep`.
- Data-quality warning: legacy Paper v2 ledger consistency warning remains non-blocking under this gate (`order_fill_quantity_mismatches=4`), not introduced by this change.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Phase 3 lifecycle tests initially failed | vn.py-style instances were terminalized when current child became terminal, before core/window finish | Keep vn.py-style instances alive unless core FINISH or operator/risk terminalizes | MiniQMT targeted tests 48 passed |
| Phase 4 kill-switch block assertion initially failed | kill-switch after terminalizing old instance reported active algo missing before kill-switch reason_code | Check kill-switch before requiring active instance | Phase 4 risk tests passed; MiniQMT targeted tests 48 passed |

## Result

- Final status: PASS
- Remaining risks: Phase 5 shadow/parallel reconciliation not started; full production risk rule set is future work beyond Phase 4 skeleton.
- Need production backend restart: no
- Need dev service restart: no
