# BUG-389 QE read-only gate run

- Module: qe
- Level: L3
- Date: 2026-06-16T19:30:25
- Git commit: feb96fd0
- Operator: lc999

## Scope

- Changed files: BUG-389 factor cache cleanup files under QuantEvolver backend/frontend/tests/scripts.
- Impacted flows: official factor cache metadata, correlation cache read path, QE prepare-factors cache contract, factor library UI, correlation UI.
- Business goal: verify old realtime factor cache removal does not regress QE read-only routes or validation guardrails.
- Out of scope: production backend restart, production DB writes, DDL.
- Protected assets reviewed: `rdagent_assets/factor_values` is read-only for this validation; `factor_values_realtime` is removed from business source paths.

## Environment

- Backend port: no test backend started by this record; live 8001 was checked separately as read-only.
- Frontend port: skipped by `QE_READ_L3_SKIP_UI=1`.
- TDX port: not used.
- Conda/env: current AIstock Python environment.
- Database: no writes.
- Browser/headless: not used.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No blocking finding for required gate | `nox -s l0` exit 0; non-blocking existing guardrail findings printed | pass |
| Module registry | Validation module ownership stays mapped | `validation_module_registry_l0`: 8 passed; ownership scan mapped=12 unmapped=0 ambiguous=0 | pass |
| QE read backend | QE read-only backend tests pass | `qe_read_backend`: 14 passed | pass |
| UI E2E | UI leg skipped intentionally | `QE_READ_L3_SKIP_UI=1` | skipped |
| Asset safety | No protected asset modified silently | no writes to `rdagent_assets`; only source/test/history files changed | pass |

## Commands

```bash
QE_READ_L3_SKIP_UI=1 python -m nox -s l0 validation_module_registry_l0 qe_read_l3
```

## Evidence

- API calls: see `tests/aistock_validation/history/qe/20260616_bug389_realtime_cache_removal.md`.
- DB checks: no DB writes; live correlation status was read-only.
- Log files: no separate log artifact captured.
- Playwright report/trace: skipped by `QE_READ_L3_SKIP_UI=1`.
- Screenshots: none.
- Business output summary: `l0`, `validation_module_registry_l0`, `qe_read_l3`, and `qe_read_backend` all successful; `qe_read_backend` reported 14 passed.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| none | n/a | n/a | n/a |

## Result

- Final status: pass
- Remaining risks: live 8001 OpenAPI still shows old `/factor-values*` routes because the running backend is not loaded from the BUG-389 cleaned code.
- Need production backend restart: no
- Need dev service restart: no
