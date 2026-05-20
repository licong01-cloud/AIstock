# QE Archive L3 Validation - BUG-079

- Module: qe_archive
- Level: L3
- Date: 2026-05-20T13:03:02
- Branch: bug/BUG-079-qe-archive-design-compliance
- Base commit before fix: 7affcae
- Operator: codex-app

## Scope

- Changed files: QE Archive source assembler, manual ingestion UI/API SDK, QE evolution/experiment pages, Playwright and backend regression tests.
- Impacted flows: source-status, explicit experiment/task/loop preview, confirmed write, QE Archive candidate loop selection.
- Business goal: ensure QE Archive manual ingestion design is fully implemented, with no simplified subset delivery.
- Out of scope: production service restart, production write operations, QE source deletion.
- Protected assets reviewed: production frontend 3000 and backend 8001 were not restarted; DB smoke was read-only.

## Environment

- Backend port: no production backend restart; mock UI used 8012 via Playwright webServer.
- Frontend port: Playwright dev port 3012.
- Database: read-only QE Archive data-quality smoke using `F:/Dev/AIstock/.env` DB credentials.
- Browser/headless: Playwright Chromium headless.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No P1 blocking guardrail findings | `python -m nox -s guardrail_changed_files -- --changed-only`; `python -m nox -s l0 -- <changed files>` | Pass; P2 review-only findings remain non-blocking |
| Backend tests | QE Archive backend and MCP tests pass | `python -m nox -s qe_archive_backend`; direct `backend/tests/qe_archive` | Pass |
| Source status | recommended/manual/skipped/not_recommended states are deterministic | `backend/tests/qe_archive/test_manual_ingestion_selection.py` | Pass |
| API flow | explicit task/loop selection uses `/qe-archive/backfill` and `QE_ARCHIVE_WRITE` for writes | backend tests + Playwright payload assertions | Pass |
| UI E2E | `/qe-archive` can expand task loops and preview selected loop ids | `cd frontend && npm run test:e2e -- tests/qe-archive` | Pass, 7 tests |
| Data quality | QE Archive schema exists and is readable | `python -m nox -s qe_archive_data_quality` with DB env loaded | Pass; warning only for existing pending outbox events |

## Commands

```bash
python -m compileall backend/services/qe_archive/source_assembler.py backend/services/qe_archive/backfill_service.py backend/routers/qe_archive.py scripts/aistock_qe_archive_mcp_server.py scripts/aistock_qe_experiment_mcp_server.py
python -m pytest backend/tests/qe_archive/test_manual_ingestion_selection.py backend/tests/test_aistock_qe_mcp_servers.py -q -p no:cacheprovider
python -m pytest backend/tests/qe_archive -q -p no:cacheprovider
python -m nox -s qe_archive_backend
# with TDX_DB_* loaded from F:/Dev/AIstock/.env for read-only DB smoke:
python -m nox -s qe_archive_l3
cd frontend && npm exec tsc -- --noEmit --incremental false
cd frontend && npm run test:e2e -- tests/qe-archive/qe-archive-flows.spec.ts tests/qe/qe-candidate-strategy-actions.spec.ts
```

## Evidence

- Backend: `110 passed, 1 skipped` for `backend/tests/qe_archive`.
- Backend nox: `qe_archive_backend` passed with `106 passed`.
- QE Archive L3: final rerun passed all notified sessions, including data quality and UI.
- UI: `tests/qe-archive` passed `7 passed`; targeted QE+QE Archive Playwright passed `10 passed`.
- DB smoke: schema table count 32/32, expected column count 546, failures empty; pending outbox warning is informational.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| First `qe_archive_l3` data-quality sub-session failed | Worktree has no `.env`, script loads only repo-root `.env`; DB password was empty | Reran with `TDX_DB_*` loaded from `F:/Dev/AIstock/.env` without echoing secrets | Final `qe_archive_l3` passed |

## Result

- Final status: Pass
- Remaining risks: P2 guardrail findings are existing review-only raw JSON/complexity signals and non-blocking for this bug.
- Need production backend restart: no
- Need production frontend restart: no
