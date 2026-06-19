# BUG-432 QE read-only workspace access regression

- Module: qe
- Level: L3
- Date: 2026-06-20T02:27:31+08:00
- Git commit at run start: 79aa6c5e
- Operator: Codex

## Scope

- Changed files: `backend/services/quantevolver/offline_code_text_factor_executor.py`, `backend/tests/quantevolver/test_official_factor_batch_compute.py`, `tests/aistock_validation/bugs/20260620_BUG-432-static-factors-12.json`, `tests/aistock_validation/history/qe/20260620_bug432_static_factors.md`.
- Impacted flows: official offline factor code_text execution, base-data memory cache existence compatibility, QE read-only L3 backend checks.
- Business goal: verify the BUG-432 executor fix does not regress QE read-only workspace access.
- Out of scope: production backend/frontend/TDX restart, full factor recompute, and live UI E2E.
- Protected assets reviewed: no DB DDL, no service restart, no protected asset mutation.

## Environment

- Backend port: not started by this run
- Frontend port: skipped via `QE_READ_L3_SKIP_UI=1`
- TDX port: not touched
- Conda/env: `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`
- Database: not directly modified by this nox gate
- Browser/headless: not used

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Skill metadata | In-tree validation skill remains valid | quick_validate `.codex/skills/verify-aistock-feature` | pass |
| Guardrails | No blocking high-risk finding in QE read L3 scoped paths | `scan_quality_guardrails.py ... --fail-on HIGH` | pass |
| Backend QE read tests | QE read path backend tests pass | `14 passed in 10.46s` | pass |
| UI E2E | Not required for this backend executor BUG; skipped to avoid dev service start | `QE_READ_L3_SKIP_UI=1` | documented skip |
| Asset safety | No production runtime restart or DDL | no service commands run | pass |

## Commands

```powershell
$env:QE_READ_L3_SKIP_UI='1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_read_l3
```

## Evidence

- nox output: `qe_read_l3: success`; notified `qe_read_backend: success`.
- Backend tests: `backend/tests/unified_engine/test_qe_evolution_read_paths.py`, `backend/tests/unified_engine/test_qe_experiment_read_paths.py`, `backend/tests/unified_engine/test_qe_experiment_log_terminal.py` -> `14 passed`.
- UI: skipped intentionally by env var because BUG-432 does not change UI and this gate should not start dev services.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| none | n/a | n/a | n/a |

## Result

- Final status: passed
- Remaining risks: UI live route was not re-run in this gate; BUG-432 has separate WSL factor smoke for the changed executor path.
- Need production backend restart: user-owned after merge only if activating code.
- Need dev service restart: no
