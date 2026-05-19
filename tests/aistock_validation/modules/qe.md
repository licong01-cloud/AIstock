# QE Read And Cleanup Validation Matrix

This matrix covers QuantEvolver / QE read-only experiment data access, cleanup flows, and the approved creation/execution mutation paths that must not directly access WSL/RD-Agent worker filesystems.

## Business Goal

QE experiment pages must display accurate task, loop, and metric data obtained through supported backend APIs, and destructive cleanup flows must remove worker artifacts through node APIs only. Windows-side FastAPI code must not directly read, scan, copy, mutate, or delete WSL/RD-Agent worker workspaces.

## Red Lines

- No Windows FastAPI request path may directly read, scan, copy, mutate, or delete QE worker workspace files.
- Do not use `QE_WORKSPACE_WIN`, `RDAGENT_WORKSPACE_WIN`, normalized `/mnt/... -> drive:` paths, or DB `workspace_path` as authoritative artifact paths.
- Missing optional artifacts must be visible as unavailable/missing, not fake zero/default data.
- When no QE task is `running` or `processing`, the evolution dashboard must not automatically poll task lists or selected task details; manual refresh or a user task click is the trigger.
- UI success requires observable, accurate task/loop/metric display, not just absence of exceptions.
- Dev validation must use backend `8011`/`8012` and frontend `3011`/`3012`; production `8001` must not be restarted.

## L0 Guardrails

- Scan QE router/service/frontend test paths for hardcoded WSL/Windows workspace access, secrets, silent fallback, and protected asset changes.
- Confirm changed files do not modify QE/RD-Agent `mlruns`, model weights, HMM snapshots, StrategyPackage frozen manifests, or worker workspace assets.
- Confirm read-path changes do not change create/dispatch/retry/resume code.
- Confirm cleanup-path changes use node APIs for worker workspaces and guarded AIstock-owned roots for local artifacts.

## Backend L1/L2

- Evolution task detail returns HTTP 200 for terminal tasks whose worker artifacts are inaccessible from Windows.
- Task detail does not scan `mlruns` or optional position pickle files from local paths.
- Task detail does not write DB updates merely to enrich optional position statistics.
- Loop metrics remain exactly the DB/API values; missing optional position summary is not fabricated.
- Single experiment enhanced-metrics reads DB-cached details first, then QE node API, and never falls back to Windows/WSL workspace paths.
- Single experiment terminal log tail reads `run.log` through QE node API only; unavailable logs are explicit and never read from `workspace_path`.
- Experiment analysis/evolution-context uses DB-cached metadata/results and must not dereference `workspace_path`.
- Artifact-unavailable states are explicit and actionable when added by later phases.

## Read/Catalog Integration L1/L2

These checks cover non-create, non-retry, non-resume paths remediated after the initial QE read and cleanup phases.

- `InferenceEngine._load_experiment_manifest` accepts only AIstock-owned runtime caches and refuses worker paths even if an old caller passes `workspace_path`.
- RD-Agent task manifest text, task selection asset status, and local asset audit read only guarded AIstock-owned `rdagent_assets/rdagent_tasks` cache files; worker manifest paths return explicit policy errors.
- RD-Agent production bundle cache must stay under AIstock-owned artifact roots and must refuse worker base directories before extracting or reading bundle files.
- RD-Agent production bundle cache must reject path traversal in bundle ids, downloaded zip members, workspace ids, and manifest relative paths before any local read/write/extract operation.
- RD-Agent factor/model catalog sync writes or reads source files only below the guarded task cache and refuses worker `task_dir` inputs.
- RD-Agent catalog ETL treats `workspace_path` as remote metadata only; it must not convert `/mnt/...` to Windows paths or read `model_meta.json` from a worker workspace.
- RD-Agent sync admin `/tasks/{task_id}/complete_assets` proxies the RD-Agent node API; it must not run `wsl`, `subprocess`, or WSL-only helper scripts.
- QE factor-cache remote sync must communicate through execution-node factor-cache APIs only; it must not shell out to WSL, SSH, rsync, or directly create/read remote cache directories.
- QE filtered stock-pool delivery for create/custom_evo/fork flows must use AIstock-owned local `stock_pools` cache plus QE workspace loop payloads; it must not shell out to WSL, SSH, scp, or directly create/read remote instruments directories from Windows.
- QE experiment generation/regeneration, retry, fork/clone, custom_evo rerun/append, and cross-node backtest-only submission must package files through API/payload flows; they must not copy to `QE_WORKSPACE_WIN`, read `RDAGENT_FACTOR_TEMPLATE_WIN` from a Linux/WSL path, or dynamically run WSL HMM precompute from Windows.
- QE backtest-only retry/cross-node submission must obtain reusable `mlruns` model parameters through the QE node API and attach them as loop payload files; Windows must not rely on local symlinks or worker path probes.
- QE custom-evo builders must accept both UI-time `factor_keys` and persisted `factor_list`/`factor_names`; historical-loop retry or clone must not start with an empty factor set.
- QE generation local roots (`QE_EXPERIMENTS_ROOT`, `QE_PROGRAMS_WIN`, `FACTOR_CACHE_ROOT_WIN`) must resolve to guarded AIstock-owned caches or bundled templates; legacy worker paths in env are ignored or rejected before any local read/write.
- QE custom strategy dependency packaging must include catalog/file dependencies such as `score_weighted_strategy.py` for `score_weighted_topk_v2`; missing imports in the loop workspace are a business failure, not a successful submission.
- HMM-enabled QE generation/retry/clone must use a precomputed local artifact or node-sourced loop artifact; missing coefficients fail fast instead of invoking WSL or converting `/mnt/...` to Windows paths.
- Selection Center HMM runtime must not convert `/mnt/...` model/coefficient artifact paths into Windows paths; remote worker paths fail fast instead of being read locally.
- StrategyPackage execution model resolver must reject `/mnt/...` and WSL/worker model paths; it must not translate worker paths into Windows drive paths or probe them locally.

## Cleanup L1/L2

Cleanup validation must use mocked DB/API clients and test-owned temporary directories unless the user explicitly authorizes a real destructive test.

- QE experiment delete calls `QEWorkspaceClient.cleanup_task_workspace` for the default node and assigned multi-alpha nodes.
- QE experiment delete resolves the real worker workspace id from `qe_experiments.qe_task_id` / `qe_evolution_tasks.task_id`; `experiment_id` is only a legacy fallback and must not be the only cleanup key.
- Deleting a child evolution Loop calls `QEWorkspaceClient.cleanup_loop_workspace(qe_task_id, qe_loop_id)` and must not delete the parent task workspace or sibling Loop records.
- QE experiment delete must not import or dereference `QE_WORKSPACE_WIN`, `RDAGENT_WORKSPACE_WIN`, DB `workspace_path`, `/mnt/...` conversions, or WSL UNC paths.
- QE experiment delete may remove only AIstock-owned local artifacts under `QE_EXPERIMENTS_ROOT` and `QE_SOTA_ASSETS_DIR`, and may remove Optuna study files only under the SOTA root.
- QE evolution task delete captures `node_id` before DB deletion, calls node API cleanup for worker workspace, and removes only AIstock-owned local artifact dirs.
- QE custom_evo loop delete/rerun cleanup calls loop-level node API; if the API is unavailable, it must fail fast before local cleanup or DB deletion.
- RD-Agent task delete calls `delete_task_on_node` for remote worker cleanup, then removes only guarded local `dispatch_logs/{task_id}` and mocked/authorized DB rows.
- Local cleanup helpers must refuse worker roots, root-directory deletion, and path traversal; they must leave simulated worker dirs intact in tests.
- Benign parse failures in touched cleanup/read helpers must be logged before returning unavailable/None states.

## API L2

Read-only probes against the dev backend must validate:

- `/api/v1/quantevolver/evolution/tasks` returns task rows.
- `/api/v1/quantevolver/evolution/tasks/{task_id}` returns the selected task detail.
- The selected task detail contains the expected task id, status, current/max loop counts, loop count, loop indexes, loop statuses, and numeric metrics when present.
- `/api/v1/quantevolver/experiments/{experiment_id}/enhanced-metrics` returns the expected DB/node enhanced fields (`summary`, IC series, return curves, all-stocks/diagnostics when present).
- `/api/v1/quantevolver/experiments/{experiment_id}/logs/tail` returns node-sourced terminal tail metadata (`log_source=qe_workspace_api`, `node_id`, logs or explicit unavailable reason).
- No response returns unexpected HTTP 5xx.

## Cleanup API L2

Destructive API probes must not run against production or real active tasks. Use one of:

- Unit/router tests with mocked DB and node API clients.
- A dedicated disposable task/experiment created for validation only.
- A dry-run endpoint when available.

Required assertions:

- Worker cleanup mode is observable as node-API-only or explicit fail-fast.
- Local cleanup result lists only AIstock-owned artifact paths.
- DB delete statements are executed only after required remote cleanup preconditions pass.
- If node workspace cleanup fails, the experiment delete endpoint returns an actionable failure before deleting DB rows or AIstock-owned local caches.
- Missing node cleanup capability returns an actionable failure and does not silently delete local/DB state.

## UI L3

Playwright must:

- Open `/quantevolver/evolution` on the dev frontend.
- Wait for the target task id to appear in the table.
- Click the target task without triggering create/retry/resume/delete controls.
- Verify current/max loop count and loop cards/details are visible.
- Compare visible loop labels and numeric metric chips with API data where metrics exist.
- Verify a controlled no-active-task page state shows manual refresh mode and does not issue extra task-list/detail requests after the old 60s polling window.
- Open `/quantevolver/experiments/{experiment_id}` and compare visible metric cards, stock rows, and chart presence against the enhanced-metrics API payload.
- Verify the experiment detail page no longer issues guessed `/evolution/tasks/{experiment_id}/loops/{experiment_id}_Loop1/enhanced-metrics` fallback requests.
- Verify terminal log UI wording says QE/node log tail and does not expose "local run.log" as the data source.
- Fail on `pageerror`, console error, request failure, or unexpected API 4xx/5xx.

## First Read-Only Command Targets

```powershell
# Start dev backend separately with all schedulers/scanners disabled.
$env:DISABLE_INGESTION_SCHEDULER='1'
$env:DISABLE_STRATEGY_SCHEDULER='1'
$env:DISABLE_PAPER_TRADING_SCHEDULER='1'
$env:ENABLE_PAPER_TRADING_V2_SCHEDULER='0'
$env:DISABLE_NODE_HEALTH_SCHEDULER='1'
$env:DISABLE_HMM_SCHEDULER='1'
$env:DISABLE_EVOLUTION_SCANNER='1'
$env:DISABLE_QE_EXPERIMENT_SCANNER='1'
python -m uvicorn backend.main:app --host 127.0.0.1 --port 8011

# Read-only validation.
$env:BACKEND_PORT='8011'
$env:FRONTEND_PORT='3011'
$env:QE_API_BASE='http://127.0.0.1:8011/api/v1'
$env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8011/api/v1'
$env:QE_READ_TASK_ID='qe_20260414_173338_d1c5'
python -m nox -s qe_read_l3

# Cleanup validation uses mocked destructive dependencies.
python -m pytest backend/tests/unified_engine/test_qe_cleanup_path_policy.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py -q
python -m pytest backend/tests/unified_engine/test_qe_evolution_read_paths.py backend/tests/unified_engine/test_qe_experiment_read_paths.py backend/tests/unified_engine/test_qe_log_stream_lifecycle.py backend/tests/unified_engine/test_qe_stop_task.py backend/tests/unified_engine/test_qe_cleanup_path_policy.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py -q

# Remaining non-create/retry/resume worker workspace boundary checks.
python -m pytest backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py -q
python -m pytest backend/tests/unified_engine/test_factor_cache_remote_sync_policy.py -q
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py -k "stock_pool" -q
python -m pytest backend/tests/unified_engine/test_backtest_executor.py -k "stock_pool" -q
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py -k "workspace_direct_access or hmm" -q
python -m pytest backend/tests/unified_engine/test_custom_evo_mutation_routes.py backend/tests/unified_engine/test_backtest_executor.py -q
```

## Evidence

Every run must create or update a Markdown record under `tests/aistock_validation/history/qe/` with exact commands, ports, API samples, UI observations, Playwright trace/report paths, failures/fixes, residual risks, and protected asset review.
