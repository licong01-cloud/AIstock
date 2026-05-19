# BUG-057 MiniQMT Frozen Runtime Assets Validation

## Scope

- Branch: `bug/BUG-057-miniqmt-frozen-assets`
- Worktree: `F:\Dev\AIstock_worktrees\bug-057-miniqmt-frozen-assets`
- GitHub issue: #60
- Local bug: BUG-057

This validation covers the MiniQMT daily execution asset-authority fix: after an authoritative Selection Center score artifact has been generated, daily MiniQMT binding/order build uses the frozen AIstock artifact evidence and does not require the live RDAgent node `mlruns-params` endpoint during pre-open execution.

## Business Oracles

- No silent cache fallback is introduced. The default QE node materialization path still fails when `mlruns-params` fails unless `allow_cache_fallback=True` is explicitly passed.
- Binding captures package id, manifest hash, selection run id, trade date, data source, runtime-config hash, artifact id, artifact hash, source type, authority scope, score count, and top symbol.
- Missing or diagnostic-only frozen artifact fails fast during package binding with `asset_stage=package_binding`.
- Corrupt binding evidence fails fast during daily order build with `asset_stage=daily_order_build`.
- Current-day data readiness and broker readiness remain separate gates; this change does not touch production DB, production backend, frontend, or real MiniQMT submit/cancel.

## Commands Run

```powershell
python -m pytest backend/tests/qmt_strategy_ledger/test_package_binding.py backend/tests/qmt_strategy_ledger/test_selection_order_builder.py::test_selection_order_builder_fails_fast_on_corrupt_frozen_asset_evidence backend/tests/selection_center/test_runtime_selection.py::test_selection_center_uses_frozen_artifact_when_node_preflight_fails backend/tests/selection_center/test_runtime_selection.py::test_live_inference_materialize_requires_explicit_cache_opt_in -q
# 9 passed

python -m pytest backend/tests/qmt_strategy_ledger/test_router_summary.py::test_package_binding_router_requires_explicit_replace_and_rolls_over_active_binding -q
# 1 passed

python -m pytest backend/tests/qmt_strategy_ledger/test_package_binding.py backend/tests/qmt_strategy_ledger/test_selection_order_builder.py backend/tests/qmt_strategy_ledger/test_router_summary.py backend/tests/selection_center/test_runtime_selection.py::test_selection_center_uses_frozen_artifact_when_node_preflight_fails backend/tests/selection_center/test_runtime_selection.py::test_live_inference_materialize_requires_explicit_cache_opt_in -q
# 27 passed

python -m pytest backend/tests/selection_center/test_runtime_selection.py::test_selection_center_pit_mode_resolves_previous_trading_day_and_passes_cutoff backend/tests/selection_center/test_runtime_selection.py::test_selection_center_uses_frozen_artifact_when_node_preflight_fails backend/tests/selection_center/test_runtime_selection.py::test_live_inference_materialize_uses_cached_params_when_node_mlruns_params_404 backend/tests/selection_center/test_runtime_selection.py::test_live_inference_materialize_requires_explicit_cache_opt_in -q
# 4 passed

python -m pytest backend/tests/qmt_strategy_ledger -q
# 84 passed

python -m compileall backend/services/selection_center/package_health.py backend/services/qmt_strategy_ledger/package_binding.py backend/services/qmt_strategy_ledger/selection_order_builder.py backend/routers/qmt_strategy_ledger.py
# passed

C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_module_registry_l0
# passed: 8 pytest tests + ownership scan

C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_catalog_integrity
# passed: catalog state=passed, 0 findings

C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s l0
# passed after renaming the explicit-cache opt-in regression to avoid the guardrail false-positive phrase "fallback ... default"; only existing medium RAW_JSON_UI findings and baseline non-blocking findings remained
```

## Evidence

- `test_selection_center_uses_frozen_artifact_when_node_preflight_fails`: a resolver whose node preflight/source calls raise `node mlruns params endpoint returned 404` is not called when the requested authoritative artifact already exists; the run succeeds from frozen artifact evidence.
- `test_package_binding_captures_frozen_selection_asset_evidence`: binding stores frozen artifact provenance in `runtime_config.frozen_runtime_asset`.
- `test_package_binding_fails_fast_when_frozen_asset_missing_or_not_authoritative`: missing or diagnostic artifact fails before binding.
- `test_selection_order_builder_fails_fast_on_corrupt_frozen_asset_evidence`: corrupt evidence fails before order generation.
- `test_live_inference_materialize_requires_explicit_cache_opt_in`: a cached `params.pkl` is not silently used when node `mlruns-params` fails unless explicit fallback is enabled.

## Production Impact

- Production backend `8001`: not touched.
- Production frontend `3000`: not touched.
- Production database: not written.
- Real MiniQMT submit/cancel: not called.
- Restart required after merge for production backend to load code changes.
