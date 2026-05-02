# QE archive realtime warehouse validation

- Module: qe_archive
- Level: L3
- Date: 2026-05-02T16:26:57
- Git commit: 3aa03e9
- Operator: lc999

## Scope

- Changed files: `backend/services/qe_archive/models.py`, `backend/services/qe_archive/repository.py`, `backend/services/qe_archive/worker.py`, `backend/tests/test_qe_archive_repository_static.py`, `scripts/qe_archive_data_quality_smoke.py`, `tests/aistock_validation/modules/qe_archive.md`, `noxfile.py`
- Impacted flows: disabled-by-default QE archive outbox worker state machine, repository outbox/archive-job status transitions, read-only data-quality smoke.
- Business goal: prove the warehouse ingestion worker foundation can claim, complete, fail, and retry archive events without wiring into current QE production runtime.
- Out of scope: QE webhook integration, automatic scheduler startup, artifact parsing, historical backfill, backend API, frontend UI.
- Protected assets reviewed: no StrategyPackage manifests, model weights, HMM snapshots, QE/RD-Agent worker workspaces, or production service processes were modified.

## Environment

- Backend port: not started; production 8001 was not restarted.
- Frontend port: not started; UI is skipped until QE archive UI exists.
- TDX port: not used.
- Conda/env: base Python for targeted pytest; `C:/Users/lc999/miniconda3/envs/AIstock/python.exe` for nox sessions.
- Database: local PostgreSQL `aistock` via `.env`; `qe_archive_v1_20260502`.
- Browser/headless: not used.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `scan_quality_guardrails.py ... --fail-on HIGH` returned 0 findings | PASS |
| Backend tests | QE archive schema/repository/worker unit tests pass | `20 passed` | PASS |
| DB smoke | Managed schema and comments are complete, no pending archive backlog remains | 27/27 tables, 458/458 commented columns, pending outbox 0 | PASS |
| Outbox DB state machine | Real DB insert/claim/job-complete/job-fail/retry works and synthetic rows are cleaned up | synthetic success job completed; synthetic failure job failed; outbox retry_count 1, then cleanup | PASS |
| UI E2E | UI is not required before archive UI exists | `QE_ARCHIVE_L3_SKIP_UI=1` documented skip | SKIP |
| Asset safety | No protected asset modified silently | no runtime hook, no worker workspace path access, no production restart | PASS |

## Commands

```bash
python -m compileall noxfile.py scripts/qe_archive_data_quality_smoke.py backend/db/init_qe_archive_schema.py backend/services/qe_archive backend/tests/test_qe_archive_repository_static.py backend/tests/test_qe_archive_schema.py

python -m pytest backend/tests/test_qe_archive_schema.py backend/tests/test_qe_archive_repository_static.py -q

python scripts/qe_archive_data_quality_smoke.py --output tmp/qe_archive_data_quality_smoke.json

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_backend

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_data_quality

$env:QE_ARCHIVE_L3_SKIP_UI='1'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_l3

# Additional DB integration smoke for repository outbox/archive_job transitions.
# The script inserted two codex_validation outbox rows, claimed them, completed one job,
# failed one job, verified retry state, and deleted both synthetic rows.
@'...python repository smoke...'@ | python -
```

## Evidence

- API calls: none; runtime integration is intentionally not wired.
- DB checks: `tmp/qe_archive_data_quality_smoke.json` reports `schema_version_present=true`, `expected_table_count=27`, `existing_table_count=27`, `expected_column_count=458`, `commented_table_count=27`, `commented_column_count=458`, `pending_outbox_count=0`, `archive_job_status_counts={}`.
- Log files: command output in Codex session; no backend process log because no service was started.
- Playwright report/trace: none; UI skipped by `QE_ARCHIVE_L3_SKIP_UI=1`.
- Screenshots: none.
- Business output summary: disabled worker does not run unless enabled; worker has no QE router/FastAPI/scheduler wiring; repository outbox/job transitions work against PostgreSQL and leave no synthetic backlog.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Initial ad-hoc DB smoke failed with `fe_sendauth: no password supplied` | one-off script did not load `.env`; product code and nox smoke already load `.env` correctly | reran the one-off smoke with `load_dotenv(Path('.env'), override=True)` | synthetic DB state-machine smoke passed, then read-only smoke confirmed pending outbox 0 |

## Result

- Final status: PASS.
- Remaining risks: no real QE completion payload has been parsed yet; no QE webhook/scheduler integration exists by design; UI tests remain skipped until QE archive pages are implemented.
- Need production backend restart: no
- Need dev service restart: no
