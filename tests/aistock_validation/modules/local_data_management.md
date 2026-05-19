# Local Data Management Validation Matrix

This matrix covers the local ingestion scheduler, dataset/date refresh audit,
autonomous data sync control plane, and audit-first health checks used by
local-data dashboards, Paper v2, and Selection Center readiness.

## Business Oracles

- `market.dataset_date_refresh_audit.status = 'success'` is the only business readiness authority.
- `market.ingestion_jobs.status = 'success'` is attempt evidence only; it must not mean business ready by itself.
- `market.data_stats` is a dashboard/cache table only. Stale cache may be shown and rebuilt, but it must not block audit-ready consumers.
- `cyq_perf` must use the unified Tushare engine path and write per-date audit success/failure rows; `scripts/ingest_tushare_cyq.py` is not the long-term readiness path for `cyq_perf`.
- `cyq_chips` remains legacy until a separate BY_CODE/per-date audit policy is defined; it must not receive fake per-date success rows.
- Routine freshness checks must create summaries/targets only. They must not write `market.data_alerts` before retry/reconciliation finishes.
- Only Alert Gate may write final operator alerts, and only for `final_blocked`, database unavailable, provider contract errors, or retry exhaustion after final deadline.
- Failed, empty-invalid, or low-coverage audit rows must trigger retry/fail-fast behavior instead of fake success.
- New `market.data_sync_targets` and `market.data_sync_attempts` fields must all have PostgreSQL comments.
- `market.data_sync_targets` must be unique by dataset/date; source and reason are mutable evidence so freshness, retry, and weekend compensation do not create duplicate alert targets.
- Delayed retry timers are only a fast path. Every delayed retry must persist `next_retry_at` in `market.data_sync_targets`, so scheduler restart does not lose recoverable gaps.
- Missing `dataset_date_refresh_audit` rows mean "audit unknown", not "physical table empty". Audit-backed incremental datasets must seed audit from the physical table before any bootstrap/cold-start range is allowed.
- First incremental sync for audit-backed Tushare datasets may use the declared bootstrap start date only when both audit cursor and physical table cursor are empty.
- If physical audit seeding finds dense-date gaps, the safe cursor must stop before the first gap so the next automated sync fills the gap instead of jumping to the physical max date.
- If audit contains a dense-date failed/missing gap before later success, the safe cursor must still stop before that gap; `MAX(success_date)` alone is not a valid cursor for dense audit-backed datasets.
- Provider contract drift such as missing required fields or returned wrong trade date must fail fast as `provider_contract_error` and require operator action.
- Automation must not restart or depend on production backend port `8001` or frontend port `3000`.

## Required Test Coverage

| Level | Scope | Required assertions |
|---|---|---|
| L0 | Static guardrails | Changed backend/data files compile or scan cleanly; no protected-asset or silent-fallback regressions. |
| L1 | Policy/unit | Release-window wait, zero-row retry before final, final-blocked alert only after deadline, target fingerprint de-duplication. |
| L1 | `cyq_perf` engine path | `DATASET_REGISTRY["cyq_perf"]` exists, is `BY_DATE`, uses pagination, reads incremental cursor from audit, seeds missing audit from `market.cyq_perf` before bootstrap, validates returned trade_date, and writes `dataset_date_refresh_audit`. |
| L1 | Audit-cursor Tushare datasets | `stock_st_events`, `cyq_perf`, `tushare_forecast_raw`, `tushare_express_raw`, and `tushare_fina_indicator_raw` must not full-bootstrap when audit is empty but physical rows exist. |
| L1 | Scheduler | `cyq_perf` routes to `_run_tushare_engine_sync`; audit-backed auto-range seeds audit from the physical table before bootstrap; `cyq_chips` remains on legacy script; `_data_freshness_check` does not call alerter flush; delayed retries persist due targets. |
| L2 | API contract | `/api/data-stats` overlays `ready_date`, `audit_ready_date`, `cache_state`, retry metadata, and `operator_action_required` without treating cache stale as not ready; provider contract errors are operator-action required. |
| L2 | Schema | `data_sync_targets` and `data_sync_attempts` DDL includes table/column comments and idempotent indexes. |
| L3 | Dashboard | `/local-data` shows audit readiness, cache state, retry state, and final operator action in Chinese business terms. |
| L4/L5 | Live/dev DB smoke | Use dev/test ports or read-only DB smoke; never validate by restarting production `8001`/`3000`. |

## Nox Entry Points

```powershell
python -m nox -s data_sync_autonomy_backend
python -m nox -s local_data_management_audit
python -m nox -s paper_v2_data_quality
```

## Evidence

Each implementation run should save a run record under
`tests/aistock_validation/history/data_ingestion/` or
`tests/aistock_validation/history/local_data_management/` with:

- Exact commands and outputs.
- Schema/comment check result.
- Pytest, nox, compile, guardrail, and UI/typecheck result.
- DB migration application status or explicit note that migration was not applied.
- Data-result validation SQL for audit, physical table, stats cache, and alert count.
- Bugs found, fixes, reruns, and residual risks.
