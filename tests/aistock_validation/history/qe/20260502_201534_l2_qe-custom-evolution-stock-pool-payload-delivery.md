# QE custom evolution stock-pool payload delivery

- Module: qe
- Level: L2
- Date: 2026-05-02T20:15:34+08:00
- Git commit: pending at record creation; final commit is reported after push
- Operator: lc999

## Scope

- Changed files:
- `backend/services/quantevolver/stock_pool_sync.py`
- `backend/services/quantevolver/executors/backtest.py`
- `backend/services/quantevolver/config_composer.py`
- `backend/services/strategy_package/workspace_policy.py`
- `backend/tests/unified_engine/test_qe_config_truth.py`
- `backend/tests/unified_engine/test_backtest_executor.py`
- `tests/aistock_validation/modules/qe.md`
- Impacted flows:
- QE create/custom_evo/strategy-fork stock_pool preflight and loop submission.
- Cross-node custom_evo loop packaging through `QEWorkspaceClient.create_and_run_loop`.
- HMM coefficient lookup during QE config composition when a local precomputed artifact exists.
- Business goal:
- Creating a new QE custom evolution from a historical loop must not fail because Windows FastAPI shells out to the execution node to create `/instruments` directories.
- Filtered stock pools must be read from AIstock-owned local `stock_pools` cache and delivered through the existing QE workspace loop payload API.
- The node installs the packaged stock-pool file into its own Qlib instruments directory immediately before `qrun`, with checksum verification.
- Out of scope:
- Restarting production FastAPI `8001` or RD-Agent/QE node service `9000`.
- Creating, retrying, restoring, or deleting real QE/RD-Agent tasks during validation.
- Mutating real Qlib instruments, HMM snapshots, model weights, `mlruns`, or worker workspaces.
- Protected assets reviewed:
- No QE/RD-Agent worker workspace, real Qlib instruments file, model artifact, HMM snapshot, StrategyPackage manifest, or production task artifact was intentionally modified.
- Tests use temp `stock_pools` files and mocked loop payloads.

## Root Cause Analysis

- The submitted task `qe_20260502_193154_17a2` failed at Loop 1 submission before the loop became visible on RD-Agent.
- The direct exception was `stock_pool_sync._run_checked()` timing out while running a Windows-side command equivalent to remote `mkdir -p /home/lc999/data/qlib_bin/instruments` over a shell transport.
- The same filtered pool had already been synced once during custom_evo preflight, then `BacktestExecutor.submit()` synced again per loop. This duplicate per-loop remote directory operation exposed a transient node/connectivity delay and turned Loop 1 into failed.
- The later `QE log stream failed ... httpx.ConnectTimeout` is a node API connectivity/log-stream symptom, not the primary task-creation failure.
- The HMM log `model_path converted to WSL path` came from config composition reading precomputed coefficients through a local fallback path. For this scenario, the coefficient file can be read from the AIstock-owned local artifact directly and shipped as `hmm_sector_coefficients.json`; no node path probing is needed.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `scan_quality_guardrails.py --fail-on HIGH ...` returned 0 findings | PASS |
| Static policy | `stock_pool_sync.py` has no shell/direct remote directory commands | `Select-String` for `subprocess`, `wsl`, `ssh`, `scp`, `_run_checked`, `rsync` returned no matches | PASS |
| Backend stock_pool tests | Local cache resolution, payload packaging, checksum install command, missing-cache fail-fast all pass | `test_qe_config_truth.py -k stock_pool`: 6 passed | PASS |
| BacktestExecutor tests | Loop payload includes filtered pool file and install command is injected after `cd` | `test_backtest_executor.py -k stock_pool`: 1 passed | PASS |
| HMM local coefficient test | Local precomputed HMM coefficients are read before legacy fallback | Included in `test_qe_config_truth.py`; local temp artifact returned expected coefficient | PASS |
| Surrounding QE regression | QE read/cleanup/path-policy/backtest executor/model resolver suite remains green | Expanded matrix: 100 passed | PASS |
| API smoke | Existing dev QE API still returns observable task/detail data | 8011 read-only smoke returned 50 tasks and task detail with 2 loops | PASS |
| UI E2E | No UI code changed | Not run; backend-only submission packaging path | NOT RUN |

## Commands

```powershell
python -m py_compile backend/services/quantevolver/stock_pool_sync.py backend/services/quantevolver/executors/backtest.py backend/services/quantevolver/config_composer.py backend/services/strategy_package/workspace_policy.py backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_backtest_executor.py

python -m pytest backend/tests/unified_engine/test_qe_config_truth.py -k "stock_pool" -q
python -m pytest backend/tests/unified_engine/test_backtest_executor.py -k "stock_pool" -q
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_backtest_executor.py -q

python -m pytest backend/tests/unified_engine/test_qe_evolution_read_paths.py backend/tests/unified_engine/test_qe_experiment_read_paths.py backend/tests/unified_engine/test_qe_log_stream_lifecycle.py backend/tests/unified_engine/test_qe_stop_task.py backend/tests/unified_engine/test_qe_cleanup_path_policy.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py backend/tests/unified_engine/test_factor_cache_remote_sync_policy.py backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_backtest_executor.py backend/tests/strategy_package/test_model_asset_resolver.py -q

Select-String -Path backend/services/quantevolver/stock_pool_sync.py -Pattern 'subprocess','wsl','ssh','scp','_run_checked','rsync' -SimpleMatch

python .codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py --fail-on HIGH backend/services/quantevolver/stock_pool_sync.py backend/services/quantevolver/executors/backtest.py backend/services/quantevolver/config_composer.py backend/services/strategy_package/workspace_policy.py backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_backtest_executor.py tests/aistock_validation/modules/qe.md

$base='http://127.0.0.1:8011/api/v1'
Invoke-RestMethod -Uri "$base/quantevolver/evolution/tasks" -Method Get -TimeoutSec 20
Invoke-RestMethod -Uri "$base/quantevolver/evolution/tasks/qe_20260502_193154_17a2" -Method Get -TimeoutSec 20
```

## Evidence

- Test results:
- `python -m py_compile ...` -> passed.
- `python -m pytest backend/tests/unified_engine/test_qe_config_truth.py -k "stock_pool" -q` -> `6 passed, 18 deselected`.
- `python -m pytest backend/tests/unified_engine/test_backtest_executor.py -k "stock_pool" -q` -> `1 passed, 18 deselected`.
- `python -m pytest backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_backtest_executor.py -q` -> `43 passed`.
- Expanded QE matrix -> `100 passed`.
- Static direct-command scan -> no matches in `stock_pool_sync.py`.
- Guardrail scan -> `0 finding(s)`.
- API smoke:
- `GET http://127.0.0.1:8011/api/v1/quantevolver/evolution/tasks` -> `status=success`, `task_count=50`, `running_or_processing=1`, target task `qe_20260502_193154_17a2`, target status `failed`.
- `GET http://127.0.0.1:8011/api/v1/quantevolver/evolution/tasks/qe_20260502_193154_17a2` -> `status=success`, `detail_loop_count=2`, task status `failed`.
- Business output summary:
- Windows-side stock_pool delivery no longer runs remote shell commands or probes local node filesystems.
- BacktestExecutor now packages `filtered_pool_*.txt` into loop `experiment_files` and injects a node-side checksum install step after the loop `cd`.
- Missing local `stock_pools/filtered_pool_*.txt` fails before submission with an actionable error instead of hanging during remote directory creation.
- HMM precomputed coefficients are read from AIstock-local artifacts first, avoiding the observed local path conversion in the current successful-cache case.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Full `test_backtest_executor.py` initially failed on HMM imports after switching to the full touched-file suite | The test imported `services.*` as a top-level package, causing HMM relative imports to resolve outside `backend` | Updated this touched test to import `backend.services.*` and mock HMM DB config lookup for fixture snapshots | `test_qe_config_truth.py test_backtest_executor.py`: 43 passed |

## Result

- Final status: PASS
- Remaining risks:
- Running backend `8011` was not restarted, so its live process still uses the previously loaded implementation until a dev/prod reload is chosen by the user.
- The old task `qe_20260502_193154_17a2` remains failed; this change prevents the same stock_pool submission failure for future tasks but does not mutate historical task state.
- Legacy non-hit HMM coefficient generation still has older fallback behavior outside this successful-cache scenario; the current fix ensures already-precomputed local coefficients are used without local path conversion or node path probing.
- Need production backend restart: no
- Need RD-Agent/QE node restart: no for this AIstock-side payload packaging change
- Asset-safety status: PASS; no protected runtime asset was modified
