# QE archive realtime warehouse validation

- Module: qe_archive
- Level: L3
- Date: 2026-05-20T00:16:06
- Git commit: e941ce6
- Operator: lc999

## Scope

- Changed files: QE Archive backfill API/service/MCP, QE/RP frontend archive status surfaces, backend/UI regression tests, and one guardrail cleanup in `backend/services/qe_archive/handlers/_synthesize.py`.
- Impacted flows: QE Archive manual experiment/task/loop selection, source archive status lookup, QE experiment/evolution UI manual warehouse preview/execute, RP read-only archive status display.
- Business goal: keep Research Pipeline automatic research recording separate from manually confirmed QE Archive warehouse ingestion, with experiment-level and loop-level selection.
- Out of scope: production backend restart, production write validation, automatic full QE warehouse ingestion.
- Protected assets reviewed: no production `8001` restart; QE Archive data-quality smoke used local DB credentials for read-only schema/count checks only.

## Environment

- Backend port: `8012` validation port check only.
- Frontend port: `3012` Playwright web server.
- TDX port: not used.
- Conda/env: current Codex PowerShell Python / Node toolchain.
- Database: local PostgreSQL `127.0.0.1:5432/aistock`, read-only smoke checks.
- Browser/headless: Playwright Chromium headless.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `scan_quality_guardrails.py --fail-on HIGH`: 4 medium RAW_JSON_UI findings, 0 high | PASS |
| Backend tests | QE Archive backend and MCP contracts pass | `qe_archive_backend`: 104 passed | PASS |
| API flow | Archive schema/count smoke can read warehouse state | `qe_archive_data_quality`: 32/32 tables, 546/546 columns, run_count=70, failures=[] | PASS |
| Data quality deep | Existing DB quality checks do not regress | `data_quality_deep`: 10 passed, 21 skipped | PASS |
| UI E2E | User-visible QE Archive flow works with mocked API | `qe_archive_ui`: 6 passed | PASS |
| Asset safety | No protected asset modified silently | no production backend restart; no production write action | PASS |

## Commands

```bash
$env:QE_ARCHIVE_UI_MOCK_API='1'
python -m nox -s qe_archive_l3

# Rerun with local DB env loaded for the read-only qe_archive_data_quality step:
$env:QE_ARCHIVE_UI_MOCK_API='1'
python -m nox -s qe_archive_l3
```

## Evidence

- API calls: mocked QE Archive UI API in Playwright; backend unit tests cover router/MCP payloads.
- DB checks: `qe_archive_data_quality_smoke.py` reported schema version `qe_archive_v2_20260516`, 32 tables, 546 columns, 70 archived runs, 1842 pending outbox events as informational warning.
- Log files: terminal output from `python -m nox -s qe_archive_l3`.
- Playwright report/trace: no retained failure artifacts on final pass; `6 passed`.
- Screenshots: none retained on final pass.
- Business output summary: manual ingestion remains preview -> confirmed execute; RP records stay automatic/read-only; QE Archive writes are user-confirmed only.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Guardrail HIGH `SILENT_EMPTY_SUCCESS` in `_synthesize.py` | `_to_decimal` caught broad `Exception` and returned `None`, matching the silent-success guardrail | narrowed to `(InvalidOperation, TypeError, ValueError)` | guardrail rerun completed with 0 high findings |
| First `qe_archive_data_quality` attempt lacked DB password | isolated worktree has no `.env`; smoke script defaults to local DB without password | reran L3 with local `TDX_DB_*` env loaded for read-only DB checks | `qe_archive_data_quality` success, failures=[] |

## Result

- Final status: PASS.
- Remaining risks: 4 existing medium RAW_JSON_UI guardrail findings are review-only and do not fail L3; pending outbox count is informational.
- Need production backend restart: no.
- Need dev service restart: no persistent service left running by validation.
