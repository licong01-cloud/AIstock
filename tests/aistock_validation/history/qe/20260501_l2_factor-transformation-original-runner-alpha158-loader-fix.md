# QE factor transformation original-runner fix validation

- Date: 2026-05-01
- Module: QuantEvolver factor transformation
- Level: L2 backend workflow regression
- Change under test: `backend/services/quantevolver/factor_transformation_service.py`

## Business Risk

The factor transformation UI displayed `Original factor execution failed` for Alpha158 factors even though transformed factor code could execute. This made the original-vs-transformed comparison unavailable and could hide real transformation regressions.

## Root Cause

Alpha158 catalog asset files are already DB-loader compatible and call injected globals such as `_REALTIME_LOADER`. The original-factor subprocess runner executed those files in an isolated namespace without injecting `_REALTIME_LOADER` or `_STATIC_FACTORS_LOADER`, so every Alpha158 original execution failed at runtime.

## Validation Commands

- `python -m py_compile backend/services/quantevolver/factor_transformation_service.py`
- `git diff --check -- backend/services/quantevolver/factor_transformation_service.py`
- Direct service validation for Alpha158 samples `KLOW`, `CORD10`, `CORR5` over `2023-01-01` to `2023-03-31`.
- Direct service validation for all 20 files in `rdagent_assets/alpha158_factors` over `2023-01-01` to `2023-06-30`.
- File-based compatibility smoke using a synthetic `calculate_TESTX()` that reads `daily_pv.h5` from the temporary original-code layout.

## Results

- `py_compile`: passed.
- `git diff --check`: passed; Git reported only the repository line-ending warning for this file.
- Alpha158 samples: `KLOW`, `CORD10`, and `CORR5` returned non-empty DataFrames with `err=None`.
- Full Alpha158 set: 20/20 passed, `failed=[]`.
- File-based original-code smoke: passed with a non-empty result, preserving the legacy `daily_pv.h5` path.

## Residual Risk

- The validation used direct service calls instead of browser UI because the fix is isolated to the backend runner and does not change frontend behavior.
- The running FastAPI service must be restarted before the UI sees this backend code change.
