# QE Archive 8011/3011 Live Backfill Validation

- Module: qe_archive
- Level: L3 live-dev
- Date: 2026-05-03
- Backend port: 8011
- Frontend port: 3011
- Production impact: production backend 8001 was not restarted or changed.

## Scope

- Validate the candidate-list historical backfill workflow against the real local PostgreSQL data warehouse through dev backend `8011` and dev frontend `3011`.
- Confirm selectable QE evolution tasks can be dry-run previewed and confirmed-written into `qe_archive` without direct WSL/remote file access.
- Confirm the live UI can load candidates from `8011`, select one task, dry-run, confirmed-write, and display successful quality results.

## Commands

```powershell
# Start fresh dev backend on 8011 after replacing stale 8011 test process.
powershell -NoProfile -ExecutionPolicy Bypass -Command "... python -m uvicorn backend.main:app --host 127.0.0.1 --port 8011"

# Backend health and candidates.
Invoke-RestMethod http://127.0.0.1:8011/api/v1/qe-archive/health
GET http://127.0.0.1:8011/api/v1/qe-archive/backfill-candidates?status=completed&limit=50&include_archived=false

# API dry-run/write for task qe_20260501_154127_b0be.
POST http://127.0.0.1:8011/api/v1/qe-archive/backfill

# Start dev frontend on 3011 with NEXT_PUBLIC_API_BASE=http://127.0.0.1:8011/api/v1.
npm run dev -- --hostname 127.0.0.1 --port 3011

# Live UI Playwright smoke against 3011/8011.
node frontend/tmp/qe_archive_live_ui_test.js

# DB smoke after writes.
python scripts/qe_archive_data_quality_smoke.py --output tmp/qe_archive_after_live_ui_write_smoke.json
```

## Evidence

- `8011` health returned success: initial `run_count=11`, `pending_outbox_count=0`.
- Candidate list returned real candidates with task type, description, loop counts, archived/pending counts, model/label metadata, and execution timestamps.
- API dry-run for `task_id=qe_20260501_154127_b0be` expanded to 2 loops and wrote nothing:
  - Loop1: 85 metrics, 3,489 curve rows, 57 factor rows, 1 account summary, 3 raw payloads.
  - Loop2: 67 metrics, 3,489 curve rows, 57 factor rows, 1 account summary, 3 raw payloads.
- API confirmed write for `task_id=qe_20260501_154127_b0be` wrote both loops:
  - `qear_run_ad52b8071a234fad59badc7d`
  - `qear_run_d2716069c4de54ba3a7f8a60`
- Live UI on `3011` used `8011` API, loaded real candidates, selected `task_id=qe_20260426_234914_9c7b`, dry-run previewed it, then confirmed-wrote all 3 completed loops through the UI:
  - `qear_run_0b209d4f04b3bfda246576a7`
  - `qear_run_7e968132520eb691e6a031ad`
  - `qear_run_24c216d33d90ce21daddd21f`
- Live UI Playwright result: `ok=true`, `consoleErrors=[]`, `pageErrors=[]`, `requestFailures=[]`, `httpErrors=[]`.
- Run quality checks for all 5 newly written runs met the current completeness gate:
  - metrics: 62-85 rows
  - curves: 3,201-3,489 rows
  - factor rows: 50-57
  - account summary: 1
  - raw payloads: 3
  - config_capture_complete: true
  - reproducibility_level: full
  - research_valid: true
- Final DB smoke: `run_count=16`, 27/27 managed tables present, 458/458 columns commented, `pending_outbox_count=0`, no failures or warnings.

## Red-Line Review

- No WSL path, remote machine file, worker workspace, model weight, Qlib bin, or artifact file was read for this validation.
- All backfill data came from the backend API and local PostgreSQL source tables already exposed to AIstock.
- Artifact deep parsing remains out of scope and must later use controlled API/artifact collector flow.

## Result

- Final status: passed.
- Confirmed business result: historical backfill candidate selection, task-level all-loop expansion, dry-run preview, confirmed write, and UI workflow all work in the 8011/3011 test environment.
- Safe to proceed to the next development phase after user confirmation.
- Production backend restart needed: no.
