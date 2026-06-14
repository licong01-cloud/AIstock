# QE read-only workspace access regression

- Module: qe
- Level: L3
- Date: 2026-06-14T12:49:20+08:00
- Git commit: 781ee506 (BUG-362 worktree base commit)
- Operator: lc999

## Scope

- Changed files: backend/services/quantevolver/correlation_compute_service.py; backend/services/quantevolver/factor_value_loader.py; backend/routers/quantevolver_evolution.py; backend/tests/test_correlation_compute_independence.py; backend/tests/quantevolver/test_bug_013_014_factor_eligibility_correlation.py; tests/aistock_validation/bugs/20260614_BUG-362-factor-values-realtime-105-575.json.
- Impacted flows: factor correlation compute/cache-status/overview and QE read-only regression gate.
- Business goal: factor correlation uses offline research/backtest cache `rdagent_assets/factor_values` only, never `factor_values_realtime` or official-evaluation snapshot cache.
- Out of scope: production service restart, production DB writes outside local tests, DDL, and live/realtime cache backfill.
- Protected assets reviewed: no StrategyPackage, model weight, HMM snapshot, QE artifact, or production DB schema file modified.

## Commands

```bash
python -m nox -s qe_read_l3
```

Executed with `QE_READ_L3_SKIP_UI=1` to keep validation read-only and avoid starting or touching production/dev UI services.

## Evidence

- `qe_read_l3`: success.
- `qe_read_backend`: success, `14 passed in 10.31s`.
- Guardrail scan emitted existing MEDIUM RAW_JSON_UI findings in QE UI files; no blocking HIGH finding.
- No production backend/frontend/TDX restart was performed.

## Result

- Final status: passed for BUG-362 required QE read-only L3 gate.
- Remaining risks: UI portion intentionally skipped by `QE_READ_L3_SKIP_UI=1`; this bug changes backend cache source and tests, not UI layout.
- Need production backend restart: no.
- Need dev service restart: no.
