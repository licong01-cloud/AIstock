# StrategyPackage Freeze Completeness Read-only Tools (2026-07-01)

This directory preserves the reproducible read-only tools used to verify StrategyPackage freeze completeness on 2026-07-01.

## Boundaries

- Debug-only evidence tools for humans/agents; production services, schedulers, formal APIs, and release gates must not import them.
- The tools set `PGOPTIONS=-c default_transaction_read_only=on` before opening PostgreSQL connections.
- They do not call `artifact_repository.save()`, do not execute DML/DDL, and do not start or restart services.
- `frozen_runtime_oracle_readonly.py` invokes real WSL+qlib inference and may take minutes; run it only when the local WSL/Qlib environment is ready.

## Tools

- `frozen_feature_count_runtime_audit.py`: authoritative feature-count audit using runtime frozen resolve + prepare path, then `load_model_from_pkl()` to inspect expected model features.
- `frozen_runtime_oracle_readonly.py`: single-package runtime oracle. It passes a bogus QE source id to prove resolution comes from `package_asset`, then runs real WSL inference.
- `frozen_feature_count_blob_audit.py`: fast auxiliary pre-check that reads frozen `MODEL_WEIGHT` blob feature counts. Custom-model unpickle errors are reported explicitly.

## Examples

```powershell
rtk python debug_tools/strategy_package/freeze_completeness_20260701/frozen_feature_count_runtime_audit.py --limit 20
rtk python debug_tools/strategy_package/freeze_completeness_20260701/frozen_runtime_oracle_readonly.py pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27 2026-06-30 --json-output tmp/frozen_oracle_pkg_5a5ccb56_2026-06-30.json
```

Authoritative findings are recorded in `docs/analysis/strategy_package_asset_freeze_runtime_oracle_findings_20260701.md`.
