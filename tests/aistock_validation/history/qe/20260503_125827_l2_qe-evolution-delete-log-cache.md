# QE Evolution Delete Log Cache Validation - 2026-05-03 12:58

## Scope

- Fix the auto-evolution page so selecting a task does not read `evolution.log` or open `/logs` SSE unless the log panel is expanded.
- Ensure deleting an evolution task closes the active log stream in the UI before calling the DELETE API.
- Ensure the backend deletes the remote QE worker workspace through the node API only, then deletes AIstock-owned local caches including `rdagent_assets/qe_sota_assets/{task_id}` and Optuna files before DB deletion.
- Ensure experiment deletion does not hide local-cache cleanup failures behind `ignore_errors=True`.

## Business Goal

Deleting a QE evolution task must not fail because the page passively selected the task and opened its log file. A successful delete must remove all task-related local SOTA log cache and AIstock-side local artifacts, while worker workspaces are cleaned only through API calls.

## Evidence

- Residual task cache checked: `F:\Dev\AIstock\rdagent_assets\qe_sota_assets\qe_20260410_002138_78be` -> `Exists=False`.
- Changed cleanup paths remove only AIstock-owned roots:
  - `QE_EXPERIMENTS_ROOT / task_id`
  - `QE_SOTA_ASSETS_DIR / task_id`
  - `QE_SOTA_ASSETS_DIR / optuna_studies / {task_id}_*.db`
- Worker workspace cleanup remains node-API-only via `QEWorkspaceClient.cleanup_task_workspace`.

## Commands

```powershell
python -m pytest backend/tests/unified_engine/test_qe_log_stream_lifecycle.py backend/tests/unified_engine/test_qe_cleanup_path_policy.py -q
npx tsc --noEmit --pretty false
git diff --check -- backend/services/quantevolver/qe_evolution_service.py backend/routers/quantevolver.py frontend/src/app/quantevolver/evolution/page.tsx backend/tests/unified_engine/test_qe_log_stream_lifecycle.py
rg -n "QE_WORKSPACE_WIN|RDAGENT_WORKSPACE_WIN|/mnt/|wsl|WSL|subprocess|shutil\.rmtree" backend/services/quantevolver/qe_evolution_service.py backend/routers/quantevolver.py frontend/src/app/quantevolver/evolution/page.tsx backend/tests/unified_engine/test_qe_log_stream_lifecycle.py backend/tests/unified_engine/test_qe_cleanup_path_policy.py
rg -n "password|secret|api[_-]?key|token" backend/services/quantevolver/qe_evolution_service.py backend/routers/quantevolver.py frontend/src/app/quantevolver/evolution/page.tsx backend/tests/unified_engine/test_qe_log_stream_lifecycle.py tests/aistock_validation/history/qe/20260503_125827_l2_qe-evolution-delete-log-cache.md
```

## Results

- `pytest`: 19 passed in 7.73s.
- `tsc`: passed with no output.
- `git diff --check`: passed; only Git line-ending warnings.
- Guardrail scan: no new cleanup-path direct worker filesystem access. Existing matches are legacy/non-cleanup WSL or test-policy assertions in `backend/routers/quantevolver.py`, `frontend/src/app/quantevolver/evolution/page.tsx`, and `backend/tests/unified_engine/test_qe_cleanup_path_policy.py`.
- Secret keyword scan: only existing variable/header names such as `QE_WEBHOOK_SECRET`, no literal secret values added.

## Residual Risks

- I did not restart production backend `8001` or frontend `3000`.
- I did not run browser Playwright against dev ports because this fix is covered by a static frontend regression plus TypeScript. Production services need a restart/redeploy to load the code change.
