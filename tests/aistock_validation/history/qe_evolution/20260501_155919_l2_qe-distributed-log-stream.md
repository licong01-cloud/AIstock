# L2 QE Distributed Log Stream Validation - 2026-05-01

## Scope

- Fix QE evolution realtime log streaming for distributed custom_evo tasks.
- Business goal: when loops run on remote execution nodes, `/api/v1/quantevolver/evolution/tasks/{task_id}/logs` must stream logs from every resolved loop node, not only the task-level WSL/default node.
- No real QE training or backtest was executed.

## Risk Review

- False-success risk: UI shows only Loop1/WSL logs while remote node loops are running silently.
- Data/asset risk: change only reads task/loop node metadata and forwards logs; no QE artifacts, model files, stock pools, or experiment results are modified.
- Silent-fallback risk: custom_evo node-resolution failures are emitted as visible `log_node_resolution_warning` SSE messages and persisted to `evolution.log`; node stream failures are emitted as visible `node_log_stream_error` messages.

## Implementation Evidence

- `AutoEvolutionScheduler._get_log_stream_node_plan_for_task` now resolves log nodes from task-level `node_id`, custom_evo `strategy_evo_config.loops`, and persisted `qe_evolution_loops.node_id` rows.
- `AutoEvolutionScheduler.stream_task_logs` now fans out to all resolved nodes concurrently for distributed tasks.
- Multi-node log lines are decorated with `[node_id]` before the existing `[LoopN]` prefix so the frontend can show the remote source without UI changes.
- The local cached `rdagent_assets/qe_sota_assets/{task_id}/logs/evolution.log` receives the same node-decorated SSE payloads for later tail review.

## Commands Run

```powershell
python -m py_compile backend/services/quantevolver/qe_evolution_service.py
python -m pytest backend/tests/unified_engine/test_qe_log_stream_lifecycle.py -q
python -m py_compile backend/services/quantevolver/qe_evolution_service.py backend/routers/quantevolver_evolution.py backend/services/quantevolver/qe_workspace_client.py
python -m pytest backend/tests/unified_engine/test_qe_log_stream_lifecycle.py backend/tests/unified_engine/test_qe_node_execution.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py -q
git diff --check -- backend/services/quantevolver/qe_evolution_service.py backend/tests/unified_engine/test_qe_log_stream_lifecycle.py
```

## Results

- `test_qe_log_stream_lifecycle.py`: 6 passed.
- Combined targeted suite: 16 passed.
- `py_compile`: passed for the changed service and adjacent router/client files.
- `git diff --check`: passed; only existing line-ending normalization warnings were reported.

## Regression Coverage Added

- Custom evolution log-node plan includes nodes from both strategy config and persisted loop rows.
- Distributed realtime log stream fans in payloads from multiple node clients.
- Node source prefixes are persisted to local `evolution.log`.

## Residual Risks

- Terminal tasks opened only through the frontend tail endpoint still read the local cached `evolution.log`; remote logs are complete there only if a realtime stream connected during or after execution and cached them.
- No live remote RD-Agent API was contacted in validation; tests used fake clients to avoid starting real QE jobs.

## Restart / Port Impact

- Backend restart is required for production port 8001 to load the new log-stream implementation.
- No frontend restart is required because no frontend files changed.
