# HMM backtest-only remote QE task - 2026-05-05 17:15

## Scope

- Validate the fixed custom_evo `backtest_only` path with real QE execution.
- Reuse already trained model artifacts from `qe_20260505_123035_bf80` Loop1-Loop7.
- Avoid duplicate model training; only rerun backtest with the preserved loop configs.
- Execute on remote node `rdagent-node1` with `parallel_4` / node parallelism 4.

## Source And Candidate Set

Source task: `qe_20260505_123035_bf80`.

The submitted task cloned all 7 Stage3 validation loops and set each new loop to:

- `backtest_only=true`
- `model_source_task_id=qe_20260505_123035_bf80`
- `model_source_loop_index=<same loop index>`
- `node_id=rdagent-node1`

Candidate loops:

| Loop | Source loop | HMM snapshot | Purpose |
| --- | --- | --- | --- |
| 1 | `qe_20260505_123035_bf80` Loop1 | none | no-HMM control |
| 2 | `qe_20260505_123035_bf80` Loop2 | `bbec3863-fb67-445f-938e-66f092d18696` | Loop2 old-covfix retained control |
| 3 | `qe_20260505_123035_bf80` Loop3 | `6ea64754-003d-48d8-ad9e-d0e7857716c8` | Loop10 penalty-only retained best |
| 4 | `qe_20260505_123035_bf80` Loop4 | `1d7ca1b9-fa65-4a97-9726-a6896f168121` | Stage3 FBT robust penalty |
| 5 | `qe_20260505_123035_bf80` Loop5 | `858b2ed9-4089-467f-98f0-54e6ce6b06b6` | Stage3 FBT symmetric |
| 6 | `qe_20260505_123035_bf80` Loop6 | `76267343-d182-4f12-974e-b8bfacfa56ee` | Stage3 FBT aggressive |
| 7 | `qe_20260505_123035_bf80` Loop7 | `ef6e044a-095c-46a4-8c91-fcecf107764a` | Stage3 turnover-light penalty |

All source loops had the required reusable model params available from the node API:

- `GET /api/v1/qe_workspace/tasks/qe_20260505_123035_bf80/loops/LoopN/status`: HTTP 200, `completed` for Loop1-Loop7.
- `GET /api/v1/qe_workspace/tasks/qe_20260505_123035_bf80/loops/LoopN/mlruns-params`: HTTP 200, gzip payloads about 47 MB for Loop1-Loop7.

## Remote API Restart

Remote `rdagent-node1` API was restarted before submission.

- Old API parent pid: `2793`.
- New API processes after restart:
  - `254095`: `python -m rdagent.app.cli results_api --host 0.0.0.0 --port 9000`
  - `254182`: `uvicorn rdagent.app.results_api_server:create_app --host=0.0.0.0 --port=9000`
- Health check: `GET http://192.168.50.215:9000/health` returned `{"status":"ok"}`.
- Restart log: `/home/lc999/projects/RD-Agent-main/aistock_results_api_restart_20260505_170434.log`.

Evidence: `.codex_tmp/hmm_backtest_only_qe_20260505/remote_restart_postcheck.json`.

## Submitted QE Task

Created through production backend API `http://127.0.0.1:8001/api/v1/quantevolver/evolution/custom-tasks`.

- Task ID: `qe_20260505_170914_a010`
- Task name: `HMM_stage3_backtest_only_reuse_qe_20260505_123035_bf80_remote_p4_20260505_170913`
- Status at initial validation: `running`
- Total loops: 7
- Submitted first batch: 4 running loops
- Execution mode: `parallel_4`
- Node assignment: all loops `rdagent-node1`
- Node parallelism: `{"rdagent-node1": 4}`
- UI/API visibility: task appears as the first row in `/quantevolver/evolution/tasks?limit=5` and detail endpoint returns HTTP 200.

Evidence:

- Payload: `.codex_tmp/hmm_backtest_only_qe_20260505/payload.json`
- Create response: `.codex_tmp/hmm_backtest_only_qe_20260505/create_response.json`
- DB/API initial status: `.codex_tmp/hmm_backtest_only_qe_20260505/initial_status_db_api.json`

## No-Training Evidence

Initial running loops Loop1-Loop4 have node run logs showing both model reuse and the explicit backtest-only command:

- Loop1: `Symlink mlruns` from source Loop1 and `qrun_limit_minute.py conf.yaml --backtest-only && python read_exp_res.py`
- Loop2: `Symlink mlruns` from source Loop2 and `qrun_limit_minute.py conf.yaml --backtest-only && python read_exp_res.py`
- Loop3: `Symlink mlruns` from source Loop3 and `qrun_limit_minute.py conf.yaml --backtest-only && python read_exp_res.py`
- Loop4: `Symlink mlruns` from source Loop4 and `qrun_limit_minute.py conf.yaml --backtest-only && python read_exp_res.py`

The initial log scan did not find training tokens such as `start training`, `model training`, `fit model`, or `training model` in Loop1-Loop4.

Evidence: `.codex_tmp/hmm_backtest_only_qe_20260505/remote_backtest_only_proof.json`.

## Current State And Residual Risk

- The task is created successfully, visible via API/UI, and running on the remote node with 4-way parallelism.
- The first 4 loops are proven to use `--backtest-only` and symlink source `mlruns` instead of training new models.
- Loops 5-7 have not started yet at this evidence snapshot because `parallel_4` starts the first four loops first; they should be checked after one first-batch loop completes.
- Final backtest metrics are not available yet; completion/metric comparison should be run after the task reaches `completed`.
