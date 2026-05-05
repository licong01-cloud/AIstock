# Local Data Management Validation Matrix

This matrix covers the local ingestion scheduler, dataset/date refresh audit,
and audit-first health checks used by Paper v2 and Selection Center readiness.

## Business Oracles

- `market.dataset_date_refresh_audit.status = 'success'` must mean the dataset/date is ready for consumers.
- Every field in `market.dataset_date_refresh_audit` must have a PostgreSQL comment.
- Routine health checks must prefer the compact audit ledger and avoid full-table scans of `kline_minute_raw`.
- Failed, empty-invalid, or low-coverage audit rows must trigger retry/fail-fast behavior instead of fake success.
- Automation must not restart production backend port `8001`.

## Nox Entry Points

```powershell
python -m nox -s local_data_management_audit
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
