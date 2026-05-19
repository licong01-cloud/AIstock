# Local Data Management Validation Matrix

This matrix covers the local ingestion scheduler, dataset/date refresh audit,
and audit-first health checks used by Paper v2 and Selection Center readiness.

## Business Oracles

- `market.dataset_date_refresh_audit.status = 'success'` must mean the dataset/date is ready for consumers.
- Every field in `market.dataset_date_refresh_audit`, `market.data_sync_targets`, and `market.data_sync_attempts` must have a PostgreSQL comment.
- Routine health checks must prefer the compact audit ledger and avoid full-table scans of `kline_minute_raw`.
- Failed, empty-invalid, or low-coverage audit rows must trigger retry/fail-fast behavior instead of fake success.
- Missing audit rows must never trigger blind full sync; audit-backed datasets must first reconcile from physical rows, then fetch the first remaining gap.
- `market.data_stats` is dashboard/cache only and cannot decide business readiness.
- `cyq_perf` must use the unified `TushareSyncEngine` route and write audit success/failure per date.
- `cyq_chips` must remain on the legacy `ingest_tushare_cyq.py` stock-loop route until a separate BY_CODE/per-date audit policy is implemented; do not pretend it is an engine-managed BY_DATE dataset.
- Automation must not restart production backend port `8001`.

## Nox Entry Points

```powershell
python -m nox -s local_data_management_audit
python -m nox -s data_sync_autonomy_backend
python -m nox -s paper_v2_data_quality
```

## Evidence

Each implementation run should save a run record under
`tests/aistock_validation/history/data_ingestion/` or
`tests/aistock_validation/history/local_data_management/` with:

- Exact commands and outputs.
- Schema/comment check result.
- Pytest and nox result.
- DB migration application status.
- Bugs found, fixes, reruns, and residual risks.

## Data Sync Autonomy Regression Scope

`data_sync_autonomy_backend` must cover these merge-blocking cases before Main approval:

- `TushareSyncEngine.sync()` uses audit/physical reconciliation metadata and does not reference an undefined cursor summary.
- `cyq_perf` spec is registered as an engine-managed Tushare dataset and routes through the engine registry.
- `cyq_chips` is explicitly excluded from the engine registry and covered by legacy-route tests to avoid unsafe BY_DATE assumptions.
- Physical rows can seed missing audit rows, but empty dates never fabricate success.
- Target status tables are idempotent, commented, and available to future retry/watchdog workers.
- Validation Center allowlist can run the backend plan without production ports or business-state writes.


## Merge Acceptance Standard

A data-sync autonomy branch can be proposed for Main only when all of these are true:

- Backend unit tests pass for TushareSyncEngine audit reconciliation, missing-table fail-fast, data_sync_targets lifecycle, scheduler routing/retry target persistence, final-deadline alert deferral, data-stats readiness overlay, preset-stats/auto-range audit semantics, dataset audit schema, and Validation Center catalog allowlist.
- `python -m nox -s data_sync_autonomy_backend` passes and is listed in `tests/aistock_validation/catalog/test_plans.yaml` with no production ports and no business-state writes.
- `python -m nox -s local_data_management_audit` passes or records an `offline_schema_review` warning only when this worktree cannot connect to the local DB; the static DDL/comment review must still pass for `dataset_date_refresh_audit`, `data_sync_targets`, and `data_sync_attempts`.
- `git diff --check` is clean, `.coverage` and other temporary artifacts are not staged, and a validation run record exists under `tests/aistock_validation/history/local_data_management/`.
- Production backend `8001`, frontend `3000`, and production DB runtime are not restarted or mutated during validation unless the user explicitly approves it.
