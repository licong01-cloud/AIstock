# QE archive realtime warehouse validation

- Module: qe_archive
- Level: L3
- Date: 2026-05-03T00:02:18
- Git commit: 6d06aa5
- Operator: lc999

## Scope

- Changed files: `backend/services/qe_archive/source_assembler.py`, `backend/services/qe_archive/backfill_service.py`, `backend/routers/qe_archive.py`, `backend/tests/test_qe_archive_repository_static.py`, `frontend/src/lib/qe-archive/api.ts`, `frontend/src/app/qe-archive/page.tsx`, `frontend/tests/qe-archive/qe-archive-dashboard.spec.ts`, `tests/aistock_validation/modules/qe_archive.md`, `docs/architecture/qe_realtime_experiment_warehouse_detailed_design_20260502.md`, `docs/codex_project_memory.md`.
- Impacted flows: QE archive historical backfill API, task-level loop expansion, selectable backfill candidate UI, run quality panel, mocked QE archive UI E2E.
- Business goal: operators can list QE experiments/tasks not fully in the archive, select one or more candidates, dry-run preview, and confirmed-write all runs for each selected experiment/task without shell scripts.
- Out of scope: production FastAPI `8001` restart, enabling realtime ingestion flags, artifact deep parsing, direct worker workspace reads, new DB schema changes.
- Protected assets reviewed: no QE/RD-Agent worker workspaces, model weights, StrategyPackage manifests, HMM snapshots, or Qlib bin files were modified.

## Environment

- Backend port: production `8001` not restarted; UI validation used mocked API through Playwright dev server.
- Frontend port: `3011` via nox/Playwright web server; preflight reported `3011 free`.
- TDX port: not used by this validation.
- Conda/env: `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`.
- Database: local PostgreSQL / TimescaleDB through `scripts/qe_archive_data_quality_smoke.py`.
- Browser/headless: Playwright Chromium headless.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `qe_archive_l3` guardrail scan found 0 HIGH; 3 MEDIUM RAW_JSON_UI findings are API/test-client JSON handling and were reviewed as non-operator raw JSON exposure. | Passed |
| Backend tests | QE archive backend unit/API contracts pass | `qe_archive_backend`: 37 tests passed. | Passed |
| API flow | Candidate API returns selectable task; backfill task_ids expand all loops | `test_backfill_service_task_ids_expand_to_all_completed_loops`, `test_qe_archive_backfill_candidates_api_returns_selectable_sources`. | Passed |
| UI E2E | User-visible candidate selection, dry-run, write, worker, quality lookup work with no console/page/request errors | `qe_archive_ui`: 1 Playwright test passed. | Passed |
| Asset safety | No protected asset modified silently | Git diff limited to QE archive code/UI/docs/tests; no artifact/model/Qlib paths touched. | Passed |

## Commands

```bash
npm exec tsc -- --noEmit --incremental false
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_data_quality
$env:QE_ARCHIVE_UI_MOCK_API='1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_ui
$env:QE_ARCHIVE_UI_MOCK_API='1'; Remove-Item Env:QE_ARCHIVE_L3_SKIP_UI -ErrorAction SilentlyContinue; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_l3
```

## Evidence

- API calls: mocked `/api/v1/qe-archive/backfill-candidates` returned one selectable evolution task; mocked `/backfill` asserted `task_ids=["qe_task_demo"]` and quality thresholds `60/3000/1`.
- DB checks: `schema_version=qe_archive_v1_20260502`, 27/27 managed tables present, 458/458 columns commented, `run_count=11`, `pending_outbox_count=0`, no data-quality failures.
- Log files: no production log mutation required; nox output captured in terminal session.
- Playwright report/trace: `frontend/playwright-report` / standard nox Playwright output; test did not require retained trace on success.
- Screenshots: not retained because mocked UI E2E passed without failure.
- Business output summary: UI selects a QE evolution task, previews two loop runs, confirms write with `QE_ARCHIVE_WRITE`, displays passed quality, runs one-shot worker with `QE_ARCHIVE_WORKER_RUN`, and verifies run quality.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| None | N/A | N/A | Full `qe_archive_l3` passed after implementation. |

## Result

- Final status: Passed.
- Remaining risks: artifact deep parsing and remote artifact collection remain future phases; the current candidate-list backfill writes all currently parseable DB payload data and raw payload snapshots, not uncollected worker-side files.
- Need production backend restart: no
- Need dev service restart: no persistent dev service left running by this task.
