# Backend Async UI UAT Plan - 2026-05-12

Date: 2026-05-12
Owner: Task 23 worker D
Scope: UAT plan only; no service start, DB write, production port, commit, or push

## 1. Verdict

Use a dev-port-only UAT to prove that the UI receives quick acknowledgements
from backend submit paths, then follows status endpoints instead of keeping
long HTTP requests open. Do not use production backend `8001`, production
frontend `3000`, production DB, Paper daemon, live broker, or Claude
worktrees for this UAT.

The UAT must cover four visible flows:

- QE submit responsiveness on `/quantevolver/evolution`.
- Selection Center health gating on `/paper-v2/selection`.
- QE evidence/status UX on `/qe-archive`.
- Rollback/no-go detection when a request blocks, targets production, or hides
  backend failure state.

This document is a plan. It is not authorization to run services or touch any
database.

## 2. Static Evidence Used

| Area | Evidence | UAT implication |
|---|---|---|
| QE API route | `backend/routers/quantevolver_evolution.py:167` defines prefix `/quantevolver/evolution`; `backend/main.py:489` mounts it under `/api/v1`. | QE submit endpoint is `/api/v1/quantevolver/evolution/tasks`. |
| QE submit offload | `backend/routers/quantevolver_evolution.py:212` creates a task; `backend/routers/quantevolver_evolution.py:467` calls `background_tasks.add_task(scheduler.submit_next_loop, task_id)` before returning `task_id`. | UAT should measure fast response and verify later status polling/logs carry execution state. |
| QE retry offload | `backend/routers/quantevolver_evolution.py:700` preflights failed/cancelled loop state and `backend/routers/quantevolver_evolution.py:735` enqueues `scheduler.retry_loop`. | Retry UAT should also return quickly and show follow-up status. |
| QE UI status model | `frontend/src/app/quantevolver/evolution/page.tsx:756` fetches tasks; `frontend/src/app/quantevolver/evolution/page.tsx:777` polls every 10 seconds only when active statuses exist. | UAT should verify the task list refreshes after acknowledgement without permanent polling when idle. |
| QE UI log model | `frontend/src/app/quantevolver/evolution/page.tsx:982` opens SSE logs only for active tasks and falls back to tail logs for terminal tasks. | UAT should verify logs/status are separate from submit acknowledgement. |
| Selection health UI | `frontend/src/app/paper-v2/selection/page.tsx:42` requires `selection_health.runnable === true`; `frontend/src/app/paper-v2/selection/page.tsx:246` blocks selected non-runnable packages; `frontend/src/app/paper-v2/selection/page.tsx:386` disables blocked rows. | UAT should verify a blocked package cannot be selected and backend bypass returns structured 400. |
| Selection ST PIT request | `frontend/src/app/paper-v2/selection/page.tsx:191` builds runtime config and `frontend/src/app/paper-v2/selection/page.tsx:212` sends `st_pit_authoritative: true`. | UAT must use health results that are current for the dev backend. |
| Selection root-cause doc | `docs/operations/selection_center_health_preflight_analysis_20260512.md` records the expected preflight failure for legacy non-ST-PIT packages. | UAT should treat this as an intentional no-go for legacy packages, not as an async regression. |
| QE Archive API | `backend/routers/qe_archive.py:65` health, `backend/routers/qe_archive.py:73` outbox, `backend/routers/qe_archive.py:104` jobs, `backend/routers/qe_archive.py:133` backfill, and `backend/routers/qe_archive.py:170` worker run-once. | Evidence/status UX should show health/outbox/jobs and make write/worker actions explicit. |
| QE Archive UI API | `frontend/src/lib/qe-archive/api.ts:244` through `frontend/src/lib/qe-archive/api.ts:286` wrap health, outbox, jobs, runs, backfill candidates, backfill, worker run-once, and quality. | UAT can use the page network log to confirm which endpoints are hit. |
| Worker boundary | `backend/services/qe_archive/worker.py:93` states the worker is not registered with FastAPI startup or a scheduler; `backend/routers/qe_archive.py:170` exposes a confirmed run-once endpoint. | UAT should not expect hidden background archive work unless a confirmed one-shot worker is run in dev. |

## 3. UAT Safety Envelope

### 3.1 Allowed targets

Use only temporary non-production ports selected by the operator, for example:

- Backend: `127.0.0.1:8013` or another explicitly assigned dev port.
- Frontend: `127.0.0.1:3013` or another explicitly assigned dev port.
- API base: `NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:<DEV_BACKEND_PORT>/api/v1`.
- Database: only an approved non-production database fixture or dev database
  explicitly authorized for this UAT.

### 3.2 Forbidden targets

- No `http://127.0.0.1:8001` or production backend alias.
- No `http://127.0.0.1:3000` or production frontend alias.
- No production DB port `5432` unless a separate production read/write window
  is explicitly approved outside this UAT.
- No Paper daemon, live broker, miniQMT, QMT connect, broker adapter, or live
  account path.
- No Claude Code worktrees.

### 3.3 Preflight checks before any live UAT run

Template only; do not run unless the operator has authorized a dev UAT:

```powershell
git status --short --branch
git rev-parse --short HEAD
git rev-parse --short origin/main

# Confirm chosen ports are not production ports and are free.
netstat -ano | findstr ":8013"
netstat -ano | findstr ":3013"

# Confirm the UI API base points to the dev backend, not production.
$env:NEXT_PUBLIC_API_BASE_URL
```

No-go if any target resolves to `8001`, `3000`, prod DB, or an unknown host.

## 4. Dev-Port-Only Setup Plan

The exact commands depend on the current repo launch scripts and are intentionally
templates. Run only in the assigned worktree after the user authorizes UAT.

```powershell
# Backend template. Use an assigned dev port only.
$env:AISTOCK_ENV='dev'
$env:PYTHONDONTWRITEBYTECODE='1'
python -m uvicorn backend.main:app --host 127.0.0.1 --port <DEV_BACKEND_PORT>

# Frontend template. Use an assigned dev port only and point to the dev backend.
$env:NEXT_PUBLIC_API_BASE_URL='http://127.0.0.1:<DEV_BACKEND_PORT>/api/v1'
npm run dev -- --hostname 127.0.0.1 --port <DEV_FRONTEND_PORT>
```

Required operator notes:

- Record backend port, frontend port, commit, branch, DB target, and start time.
- Open only `http://127.0.0.1:<DEV_FRONTEND_PORT>`.
- Keep browser devtools Network tab open.
- Capture request duration, status code, response body summary, and UI state.
- Stop dev processes after UAT and confirm both ports are free.

## 5. UAT Matrix

### 5.1 QE submit responsiveness

Purpose: prove submit returns an acknowledgement quickly and does not run a full
loop in the request path.

Preconditions:

- Use a small approved dev payload or a known fixture that does not trigger
  production assets.
- If the test would create DB rows, the operator must confirm the target is the
  approved non-production UAT DB.
- Network throttling should be disabled unless a separate latency scenario is
  being tested.

Steps:

1. Open `/quantevolver/evolution` on the dev frontend.
2. Fill the create task form with the approved dev payload.
3. Start a stopwatch immediately before clicking create.
4. In Network, observe `POST /api/v1/quantevolver/evolution/tasks`.
5. Confirm the response body contains `status="success"` and a `task_id`.
6. Confirm the create modal closes or shows a clear acknowledgement.
7. Confirm the task list refreshes through `GET /api/v1/quantevolver/evolution/tasks`.
8. Select the created task and verify status/logs come from polling or SSE, not
   from the original POST staying open.

Pass criteria:

- The POST returns in <= 3 seconds for ordinary dev validation paths.
- The UI remains interactive while the backend task proceeds.
- The response includes `task_id`.
- `GET /tasks` shows the task with a non-terminal or terminal status.
- Logs/status panels update through SSE, polling, or tail-log endpoints.
- The UI does not imply the full QE loop completed just because submit returned.

No-go signals:

- POST remains pending for > 10 seconds without a clear validation error.
- Browser main thread freezes.
- The response lacks `task_id`.
- UI status remains stale for two polling intervals after acknowledgement.
- Network shows the request hitting `8001`, `3000`, prod DB, or an external prod
  host.

### 5.2 QE retry responsiveness

Purpose: prove retry validates quickly and queues background retry work.

Steps:

1. Select an approved dev task with a failed or cancelled loop.
2. Trigger retry from the UI.
3. Observe `POST /api/v1/quantevolver/evolution/tasks/{task_id}/loops/{loop_index}/retry`.
4. Confirm response includes `status="success"` and `loop_id`.
5. Confirm the task list refreshes and the log/status path carries the retry.

Pass criteria:

- Retry POST returns in <= 3 seconds when loop status is eligible.
- Ineligible loop status fails fast with a clear error.
- The UI does not keep a long blocking retry request open.

No-go signals:

- Retry starts heavy work before returning acknowledgement.
- Retry accepts a loop that is not failed or cancelled.
- UI presents retry as complete before the background status confirms it.

### 5.3 Selection Center health

Purpose: prove current UI honors `selection_health` and backend preflight blocks
legacy/non-runnable packages if the UI is bypassed.

Steps:

1. Open `/paper-v2/selection` on the dev frontend.
2. Observe `GET /api/v1/selection-center/selectable-packages`.
3. Confirm every package row includes `selection_health.status`,
   `selection_health.runnable`, and checks or hint text.
4. Confirm rows with `runnable !== true` have disabled checkboxes.
5. Attempt to run selection using only UI controls. The UI must block any
   selected non-runnable package before POST.
6. Optional API-bypass test on dev only: submit a known blocked package to
   `POST /api/v1/selection-center/runs` with `st_pit_authoritative=true`.
7. Confirm backend returns HTTP 400 with health detail instead of starting a
   selection run.

Pass criteria:

- Health status is visible in the table.
- Blocked rows cannot be selected in the current UI.
- Backend bypass returns structured 400 for the same blocked package.
- Runnable packages, if present, can proceed to the next validation stage
  without weakening the health gate.

No-go signals:

- Package rows omit `selection_health`.
- Blocked package checkbox is enabled.
- UI sends selection for a blocked package without warning.
- Backend accepts a legacy/non-ST-PIT package in ST PIT authoritative mode.
- Error message hides the failing package or health check.

### 5.4 QE evidence/status UX

Purpose: prove evidence pages show archive health, outbox, job state, preview,
and confirmed one-shot worker boundaries clearly.

Steps:

1. Open `/qe-archive` on the dev frontend.
2. Observe the initial API set:
   - `GET /api/v1/qe-archive/health`
   - `GET /api/v1/qe-archive/outbox`
   - `GET /api/v1/qe-archive/jobs`
   - `GET /api/v1/qe-archive/backfill-candidates`
   - `GET /api/v1/qe-archive/runs`
3. Run a dry-run backfill preview if the approved dev target allows it:
   `POST /api/v1/qe-archive/backfill` with `write=false`.
4. Confirm the preview shows `dry_run=true`, processed count, and candidate
   details without implying writes.
5. Verify a write action is disabled or rejected unless the exact write
   confirmation is supplied on an approved non-production target.
6. Verify worker run-once is disabled or rejected unless
   `confirm_run=QE_ARCHIVE_WORKER_RUN` is supplied.
7. If a confirmed dev worker run is authorized, run one batch and observe
   `GET /outbox` and `GET /jobs` status changes.

Pass criteria:

- Health, outbox, jobs, runs, and candidates load independently.
- Dry-run preview is visually distinct from write/apply.
- Worker run-once requires exact confirmation text.
- Job rows show status, retry count, error, and updated time.
- UI does not imply a hidden always-on archive worker is active.

No-go signals:

- The page can trigger writes without exact confirmation.
- Worker action runs without `QE_ARCHIVE_WORKER_RUN`.
- Outbox/job failures are hidden from the operator.
- Backfill preview blocks the UI for > 10 seconds without progress.
- Any request targets production.

## 6. Rollback And No-Go Signals

Stop the UAT immediately and preserve evidence if any of these occur:

- A request hits `8001`, `3000`, prod DB `5432`, live broker, or Paper daemon.
- A supposedly async submit keeps the HTTP request open past the no-go budget.
- UI freezes or cannot navigate while a backend job is running.
- Backend returns success without `task_id`, job id, run id, or other durable
  status handle.
- Selection Center rows omit `selection_health`.
- Backend accepts a package that health preflight marks non-runnable.
- QE Archive write or worker run executes without exact confirmation.
- Evidence status remains stale after two expected polling/refresh intervals.
- Job error, retry count, or dead-letter status is not visible.
- Dev process cannot be stopped cleanly or assigned ports remain bound.

Rollback steps for a dev-only UAT:

1. Stop the dev frontend and backend processes.
2. Confirm assigned ports are free.
3. Save browser HAR, screenshots, and terminal logs under an approved artifact
   directory if the operator requested evidence capture.
4. If a dev DB write was authorized, run only the approved read-only verification
   queries or rollback script named in that UAT authorization.
5. Do not clean, reset, or delete unrelated files from other workers.

## 7. Evidence Capture Template

For each scenario, record:

```text
scenario:
worktree:
branch:
commit:
frontend_url:
backend_api_base:
db_target_label:
start_time:
end_time:
request:
response_status:
response_time_ms:
response_handle:
followup_status_endpoint:
ui_observation:
pass_fail:
no_go_signal:
artifact_paths:
```

## 8. Commands Run By This Worker

Static inspection only:

```powershell
git status --short --branch
git rev-parse --abbrev-ref HEAD
git rev-parse --short HEAD
git rev-parse --short origin/main
rg -n "APIRouter\(|@router\.post\(|@router\.get\(" backend/routers/quantevolver_evolution.py
rg -n "quantevolver/evolution|createEvolution|/tasks|submit|custom-tasks|retry" frontend/src -S --glob '!node_modules/**'
rg -n "def create_evolution_task|background_tasks\.add_task|def retry_evolution_loop|def create_custom_evolution_task" backend/routers/quantevolver_evolution.py
rg -n "/qe-archive|WORKER_CONFIRM_TEXT|class QEArchiveWorker|process_payload|selection_health|st_pit_authoritative" backend frontend docs -S
```

Not run:

- No backend or frontend server start.
- No API smoke against any port.
- No production or dev DB query/write.
- No Paper daemon, broker, miniQMT, QMT, or live trading path.
- No commit or push.
