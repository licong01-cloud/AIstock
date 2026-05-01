# L2 Validation - qe_eval_v2 Authority Kernel Hardening - 2026-05-01

## Scope

- AIstock-owned `qe_eval_v2` independent metrics kernel and Qlib reader.
- Official factor metric paths no longer depend on `rdagent.app.factor_metrics`.
- PIT coverage semantics: listed/tradable/non-warm-up denominator with suspend_d exclusion.
- Factor classification write hardening for missing catalog rows and duplicate `factor_catalog_id` conflicts.

## Non-Scope

- No full-library independent metric recalculation.
- No full-library factor classification rerun.
- No full-library official factor rating rerun.
- No RD-Agent asset, QE artifact, model weight, or StrategyPackage asset changes.

## Commands

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
conda run -n AIstock python -m py_compile backend/services/quantevolver/qe_eval_v2_metric_engine.py backend/services/quantevolver/qe_eval_v2_qlib_reader.py backend/services/quantevolver/factor_official_evaluation_service.py backend/services/quantevolver/factor_analyst.py scripts/compute_factor_metrics_unified.py backend/tests/test_factor_metrics_authority_static.py
conda run -n AIstock pytest backend/tests/test_manual_factor_service_wsl_output.py backend/tests/test_factor_cache_wsl_env.py backend/tests/test_factor_metrics_authority_static.py -q -p no:cacheprovider
npm exec tsc -- --noEmit  # from frontend/
conda run -n AIstock python -m nox -s l0
```

## Results

- Py compile: PASS.
- Targeted pytest: `22 passed in 19.07s` under conda `AIstock`.
- Frontend TypeScript: PASS.
- L0 guardrail: PASS. Existing MEDIUM findings remained in Paper v2 validation tests and were not introduced by this change.
- Synthetic in-memory metric engine smoke: PASS; all five eval windows returned ok with coverage_full=1.0. Expected NumPy warnings were emitted for warm-up/terminal all-NaN slices.

## Static Evidence

- Official Quantevolver metric paths and `scripts/compute_factor_metrics_unified.py` no longer import `rdagent.app.factor_metrics`.
- `scripts/quick_ic_screen.py` still imports the RD-Agent qlib reader as a non-official diagnostic script.
- Tests cover PIT coverage denominator behavior, full-series warm-up slicing, suspend exclusion, authority metadata, and duplicate classification write guard presence.
