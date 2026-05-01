# L2 QE Custom Evolution Mutation Lock Fix - 2026-05-01

## Scope

- Fix custom_evo loop rerun / append submission failure: `'_GeneratorContextManager' object has no attribute 'close'`.
- Business goal: rerun loop and continue/append loop flows must acquire and release the DB advisory mutation lock without crashing before QE execution is queued.
- No real QE training or backtest was executed.

## Root Cause

- `get_conn()` is a `contextlib.contextmanager` and returns a `_GeneratorContextManager` until entered.
- `rerun_custom_evo_loop` and `append_custom_evo_loops` assigned `lock_conn = get_conn()` directly, then attempted to use/release/close it as if it were a psycopg2 connection.
- The `finally` block called `lock_conn.close()`, which masked the earlier invalid connection use and surfaced as HTTP 500.

## Fix

- Enter the context manager explicitly for the long-lived advisory-lock connection:
  - `lock_cm = get_conn()`
  - `lock_conn = lock_cm.__enter__()`
  - release advisory lock in `finally`
  - close/return the connection with `lock_cm.__exit__(None, None, None)`
- Applied the same fix to both rerun and append custom_evo mutation paths.

## Commands Run

```powershell
python -m py_compile backend/services/quantevolver/qe_evolution_service.py
python -m pytest backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py -q
python -m py_compile backend/services/quantevolver/qe_evolution_service.py backend/routers/quantevolver_evolution.py backend/services/quantevolver/qe_workspace_client.py
python -m pytest backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py backend/tests/unified_engine/test_qe_log_stream_lifecycle.py backend/tests/unified_engine/test_qe_node_execution.py -q
git diff --check -- backend/services/quantevolver/qe_evolution_service.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py
rg -n "lock_conn\s*=\s*get_conn\(|lock_conn\.close\(|_GeneratorContextManager|\.close\(\)" backend/services/quantevolver/qe_evolution_service.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py
```

## Results

- Targeted mutation tests: 8 passed.
- Wider QE custom-evo/log/node targeted suite: 18 passed.
- `py_compile`: passed for changed service and adjacent router/client files.
- `git diff --check`: passed; only existing line-ending normalization warning was reported.
- Static scan found no remaining `lock_conn = get_conn()` or `lock_conn.close()` pattern in `qe_evolution_service.py`.

## Regression Coverage Added

- Service-level rerun test uses a fake `get_conn()` context manager without `.close()` to reproduce the original failure mode.
- Service-level append test covers the same advisory-lock lifecycle.
- Both tests assert advisory unlock is executed and all entered DB contexts are exited.

## Restart / Port Impact

- Backend restart is required for production port 8001 to load the fix.
- No frontend files changed.
