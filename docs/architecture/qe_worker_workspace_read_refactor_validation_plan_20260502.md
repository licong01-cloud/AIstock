# QE Worker Workspace Read Refactor And Validation Plan

Created: 2026-05-02
Scope: QuantEvolver / QE read-only experiment data access, UI observability, and automated validation.

## 1. Background

WSL QE/RD-Agent runtime must now be treated as an independent Linux compute node, equivalent to a remote machine. Windows-side AIstock services must not directly read or mutate the QE worker workspace through `F:\...`, `/mnt/f/...` conversion, `\\wsl$`, `QE_WORKSPACE_WIN`, `RDAGENT_WORKSPACE_WIN`, or DB `workspace_path`.

The immediate production symptom is that `GET /api/v1/quantevolver/evolution/tasks/qe_20260414_173338_d1c5` returns HTTP 500 because Windows FastAPI tries to inspect a WSL-created `mlruns` symlink/reparse point:

```text
F:\Dev\RD-Agent-main\qe_workspace\qe_20260414_173338_d1c5\Loop1\mlruns
[WinError 1920] The system cannot access this file.
```

This is a read-path failure, not evidence that the QE task is still running. The affected task is terminal (`completed`, `current_loop=2`, `max_loops=2`).

## 2. Non-Negotiable Red Lines

- Windows FastAPI request paths must not directly read, scan, copy, mutate, or delete QE/RD-Agent worker workspace files.
- `QE_WORKSPACE_WIN`, `RDAGENT_WORKSPACE_WIN`, and DB `workspace_path` are legacy/local metadata only and must not be used as authoritative artifact access paths.
- QE artifact access must go through node APIs (`QEWorkspaceClient`, Results API), explicit SSH/node cleanup commands, DB-cached summaries, or an AIstock-owned local artifact store created by explicit sync/download.
- User-facing APIs and UI pages must not return 500 because optional QE artifacts are unavailable from Windows.
- Optional artifact enrichment must be truthful and observable: no fake data, no empty-success masking, no silent default values.

## 3. Explicitly Out Of Scope Until User Confirms

These flows are intentionally excluded from the first implementation wave because QE experiments may be running:

- Experiment creation / config generation behavior.
- Experiment dispatch / scheduler / scanner behavior.
- Retry loop, rerun loop, resume task, fork task, or append custom loop.
- Deleting tasks or cleaning worker workspaces.
- Modifying RD-Agent/QE worker assets, model weights, `mlruns`, StrategyPackage frozen manifests, or HMM snapshots.

Existing code may still contain issues in those areas, but they must not be changed in the read-path rollout.

## 4. Phased Refactor Strategy

### Phase 0 - QE Validation Harness

Goal: make QE read-path validation first-class, following the Paper v2 validation pattern.

Deliverables:

- `tests/aistock_validation/modules/qe.md` with the QE validation matrix.
- `noxfile.py` sessions for read-only QE validation:
  - `qe_read_backend`
  - `qe_read_ui`
  - `qe_read_l3`
- Read-only Playwright coverage under `frontend/tests/qe/`.
- Validation evidence under `tests/aistock_validation/history/qe/`.

Required environment:

```powershell
$env:DISABLE_INGESTION_SCHEDULER='1'
$env:DISABLE_STRATEGY_SCHEDULER='1'
$env:DISABLE_PAPER_TRADING_SCHEDULER='1'
$env:ENABLE_PAPER_TRADING_V2_SCHEDULER='0'
$env:DISABLE_NODE_HEALTH_SCHEDULER='1'
$env:DISABLE_HMM_SCHEDULER='1'
$env:DISABLE_EVOLUTION_SCANNER='1'
$env:DISABLE_QE_EXPERIMENT_SCANNER='1'
python -m uvicorn backend.main:app --host 127.0.0.1 --port 8011
```

The validation backend/frontend must use only ports `8011`/`8012` and `3011`/`3012`. Production port `8001` must not be restarted or managed by Codex.

### Phase 1 - Evolution Task Detail Read Path

Target APIs:

- `GET /api/v1/quantevolver/evolution/tasks`
- `GET /api/v1/quantevolver/evolution/tasks/{task_id}`

Target UI:

- `/quantevolver/evolution`
- Click terminal QE task and inspect loop topology / detail panel.
- Dashboard task-list refresh and selected-task detail refresh behavior.

Current issue:

- `backend/routers/quantevolver_evolution.py` directly scans local worker workspace for `positions_normal_1day.pkl`.
- Optional position enrichment can fail the whole detail response.
- GET detail can attempt to update DB after local artifact enrichment.
- The dashboard can keep polling task lists/details even when no task is actually `running` or `processing`, which turns completed experiment pages into a continuous database/API load source.

Refactor rules:

- Remove Windows-side worker workspace scanning from task detail.
- Return DB-cached loop metrics as authoritative display data.
- If position summary is absent, leave it absent; do not fabricate values.
- Do not write to DB from the task detail read path for optional enrichment.
- API must return HTTP 200 for completed tasks even if optional worker artifacts are unavailable.
- Frontend automatic polling is allowed only while the task list contains a `running`/`processing` task.
- Clicking a terminal/non-active task must perform exactly one detail load and, for terminal tasks, at most one log-tail load; it must not open SSE or detail intervals.
- The refresh control must truthfully show `手动` when no automatic polling is active.

Business validation oracle:

- The task status, current/max loops, loop statuses, and numeric metrics displayed in UI must match the API response.
- Missing optional position summary must not be shown as a fake zero/default.
- A no-active-task UI test must prove task-list/detail request counts do not increase after the old 60s polling window.
- Browser must report no page errors, console errors, failed API requests, or unexpected HTTP 5xx.

### Phase 2 - Single Experiment Logs And Enhanced Metrics

Target APIs:

- `GET /api/v1/quantevolver/experiments/{experiment_id}/logs/tail`
- `GET /api/v1/quantevolver/experiments/{experiment_id}/logs`
- `GET /api/v1/quantevolver/experiments/{experiment_id}/enhanced-metrics`

Rules:

- Use node API / DB cache / AIstock-owned log cache only.
- Do not read `run.log` or `qlib_results_enhanced.json` from worker workspace via Windows paths.
- Surface artifact unavailability clearly in API/UI.

### Phase 3 - Experiment Analysis And Evolution Context

Target APIs:

- Experiment analysis / feedback report endpoints.
- Evolution context endpoints.

Rules:

- Prefer DB-persisted metrics and loop records.
- Use node API only when artifact content is required.
- Do not read `workspace_path/qlib_results.json` from Windows request paths.

### Phase 4 - StrategyPackage / Selection / Paper QE Artifact Consumption

Target modules:

- `backend/services/strategy_package/selection_artifact.py`
- `backend/services/strategy_package/live_inference.py`
- `backend/services/strategy_package/qe_source_resolver.py`
- legacy Paper/Selection QE workspace readers.

Rules:

- Consume explicit AIstock-owned artifact stores, not raw QE worker workspace.
- Artifact store records must include source task/loop/node and content hash.
- Paper v2 L3 must remain green after these changes.

### Phase 5 - Create / Retry / Dispatch / Cleanup (Deferred)

This phase is intentionally blocked until user confirmation. It will later address direct worker workspace writes, retry workspace reuse, node cleanup, and `QE_WORKSPACE_WSL` path derivation.

## 5. Per-Feature Verification Contract

Every completed feature modification must run a full validation cycle before commit:

1. L0 guardrails: path red lines, silent fallback, secret scan, protected asset diff review.
2. Backend unit tests: no local worker filesystem access, correct DB/API behavior, actionable artifact unavailable states.
3. API flow on dev backend: 8011/8012 only, with production 8001 untouched.
4. UI E2E on dev frontend: 3011/3012 only, fail on pageerror, console error, requestfailed, or unexpected HTTP 4xx/5xx.
5. Business data oracle: visible UI status/loop/metrics must match API data, not merely avoid exceptions.
6. Evidence record: save commands, ports, API samples, UI observations, failures/fixes, and residual risks under `tests/aistock_validation/history/qe/`.
7. Scoped Git commit and push: stage only files changed for the completed feature.

## 6. Initial Implementation Slice

This first slice implements only:

- QE validation harness for read-only flows.
- Evolution task detail read-path fix for `qe_20260414_173338_d1c5` and equivalent terminal tasks.
- QE evolution dashboard read-only polling throttle: no automatic list/detail polling when the task list has no active task, and no selected terminal-task detail interval.

It does not modify creation, dispatch, retry, rerun, resume, fork, append, delete, or cleanup logic.
