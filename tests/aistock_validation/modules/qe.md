# QE Read-Path Validation Matrix

This matrix covers QuantEvolver / QE read-only experiment data access. It is the first step toward full QE automation and intentionally excludes experiment creation, dispatch, retry, rerun, resume, fork, append, delete, and worker workspace cleanup until the user explicitly approves those phases.

## Business Goal

QE experiment pages must display accurate task, loop, and metric data obtained through supported backend APIs, without Windows-side direct access to WSL/RD-Agent worker workspaces.

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
- Confirm this read-path rollout does not change create/dispatch/retry/delete code.

## Backend L1/L2

- Evolution task detail returns HTTP 200 for terminal tasks whose worker artifacts are inaccessible from Windows.
- Task detail does not scan `mlruns` or optional position pickle files from local paths.
- Task detail does not write DB updates merely to enrich optional position statistics.
- Loop metrics remain exactly the DB/API values; missing optional position summary is not fabricated.
- Single experiment enhanced-metrics reads DB-cached details first, then QE node API, and never falls back to Windows/WSL workspace paths.
- Single experiment terminal log tail reads `run.log` through QE node API only; unavailable logs are explicit and never read from `workspace_path`.
- Experiment analysis/evolution-context uses DB-cached metadata/results and must not dereference `workspace_path`.
- Artifact-unavailable states are explicit and actionable when added by later phases.

## API L2

Read-only probes against the dev backend must validate:

- `/api/v1/quantevolver/evolution/tasks` returns task rows.
- `/api/v1/quantevolver/evolution/tasks/{task_id}` returns the selected task detail.
- The selected task detail contains the expected task id, status, current/max loop counts, loop count, loop indexes, loop statuses, and numeric metrics when present.
- `/api/v1/quantevolver/experiments/{experiment_id}/enhanced-metrics` returns the expected DB/node enhanced fields (`summary`, IC series, return curves, all-stocks/diagnostics when present).
- `/api/v1/quantevolver/experiments/{experiment_id}/logs/tail` returns node-sourced terminal tail metadata (`log_source=qe_workspace_api`, `node_id`, logs or explicit unavailable reason).
- No response returns unexpected HTTP 5xx.

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
```

## Evidence

Every run must create or update a Markdown record under `tests/aistock_validation/history/qe/` with exact commands, ports, API samples, UI observations, Playwright trace/report paths, failures/fixes, residual risks, and protected asset review.
